package org.apache.spark.sql.julia

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowFileWriter, ArrowStreamReader, ArrowStreamWriter, SeekableReadChannel}
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.{ExplainMode, SQLExecution}
import org.apache.spark.sql.execution.arrow.{ArrowConverters, ArrowWriter}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

import java.io.{File, FileInputStream}
import java.nio.channels.FileChannel
import java.nio.file.{OpenOption, Path, Paths, StandardOpenOption}

object DatasetUtils {
  // df.explain prints the plan to standard output
  def explain[R](df: Dataset[R], mode: String): String =
    df.queryExecution.explainString(ExplainMode.fromString(mode))
  def collectToArrow[R](df: Dataset[R], tempFilePath: String): Unit = {
    // TODO: see df.collectAsArrowToR() and collectAsArrowToPython, we should probably do something similar
    val rows = SQLExecution.withNewExecutionId(df.queryExecution, Some("collectToArrow")) {
      df.queryExecution.executedPlan.resetMetrics()
      df.queryExecution.executedPlan.executeCollect()
    }

    val timeZone = df.sqlContext.conf.sessionLocalTimeZone
    val arrowSchema = ArrowUtils.toArrowSchema(df.schema, timeZone)

    val allocator = ArrowUtils.rootAllocator.newChildAllocator(s"Julia collectToArrow", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)

    try {
      Utils.tryWithResource(FileChannel.open(Paths.get(tempFilePath), StandardOpenOption.WRITE)) { tempFile =>
        val arrowWriter = ArrowWriter.create(root)
        val writer = new ArrowFileWriter(root, null, tempFile)
        writer.start()

        for (row <- rows) {
          arrowWriter.write(row)
        }
        arrowWriter.finish()
        writer.writeBatch()
        arrowWriter.reset()
        writer.end()
      }
    } catch {
      case x: Throwable =>
        x.printStackTrace()
        throw x
    } finally {
      root.close()
      allocator.close()
    }
  }

  def fromArrow(sess: SparkSession, tempFilePath: String): DataFrame =
    Utils.tryWithResource(FileChannel.open(Paths.get(tempFilePath))) { tempFile =>
      val allocator = ArrowUtils.rootAllocator.newChildAllocator(s"Julia fromArrow", 0, Long.MaxValue)
      val reader = new ArrowStreamReader(tempFile, allocator)
      //val reader = new ArrowStreamReader(new ByteArrayReadableSeekableByteChannel(bytes), allocator)
      val root = reader.getVectorSchemaRoot
      try {
        val schema = ArrowUtils.fromArrowSchema(root.getSchema)
        val batches = ArrowConverters.getBatchesFromStream(tempFile).toArray
        ArrowConverters.toDataFrame(
          sess.sparkContext.parallelize(batches.toSeq, batches.length).toJavaRDD(),
          schema.json,
          sess.sqlContext
        )
      } finally {
        reader.close()
        root.close()
        allocator.close()
      }
    }
}
