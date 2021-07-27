package org.apache.spark.sql.julia

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.{ExplainMode, SQLExecution}
import org.apache.spark.sql.execution.arrow.{ArrowConverters, ArrowWriter}
import org.apache.spark.sql.util.ArrowUtils

object DatasetUtils {
  // df.explain prints the plan to standard output
  def explain[R](df: Dataset[R], mode: String): String =
    df.queryExecution.explainString(ExplainMode.fromString(mode))
  def collectToArrow[R](df: Dataset[R]): Array[Byte] = {
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
      val outStream = new ByteArrayOutputStream()
      val arrowWriter = ArrowWriter.create(root)
      val writer = new ArrowStreamWriter(root, null, outStream)
      writer.start()

      for (row <- rows) {
        arrowWriter.write(row)
      }
      arrowWriter.finish()
      writer.writeBatch()
      arrowWriter.reset()
      writer.end()

      outStream.toByteArray
    } finally {
      root.close()
      allocator.close()
    }
  }

  def fromArrow(sess: SparkSession, bytes: Array[Byte]): DataFrame = {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator(s"Julia fromArrow", 0, Long.MaxValue)
    //val reader = new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(bytes), allocator)
    val reader = new ArrowStreamReader(new ByteArrayReadableSeekableByteChannel(bytes), allocator)
    val root = reader.getVectorSchemaRoot
    val schema = try {
      ArrowUtils.fromArrowSchema(root.getSchema)
    } finally {
      reader.close()
      root.close()
      allocator.close()
    }
    val batches = ArrowConverters.getBatchesFromStream(new ByteArrayReadableSeekableByteChannel(bytes)).toArray
    ArrowConverters.toDataFrame(
      sess.sparkContext.parallelize(batches.toSeq, batches.length).toJavaRDD(),
      schema.json,
      sess.sqlContext
    )
  }
}
