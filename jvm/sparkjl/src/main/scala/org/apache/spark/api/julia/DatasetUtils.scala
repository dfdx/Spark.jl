package org.apache.spark.sql.julia

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.message.{IpcOption, MessageSerializer}
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowFileWriter, ArrowStreamReader, ArrowStreamWriter, SeekableReadChannel, WriteChannel}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.execution.{ExplainMode, SQLExecution}
import org.apache.spark.sql.execution.arrow.{ArrowConverters, ArrowWriter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

import java.io.{File, FileInputStream}
import java.nio.channels.FileChannel
import java.nio.file.{OpenOption, Path, Paths, StandardOpenOption}

object DatasetUtils {
  /** Based on a row iterator and Spark's ArrowWriter */
  def collectToArrow1[R](df: Dataset[R], tempFilePath: String): Unit = {
    // Get rows Iterator
    // Can't use df.collectToIterator() because we need InternalRow to be able to use Spark's ArrowWriter
    val rows = SQLExecution.withNewExecutionId(df.queryExecution, Some("collectToArrow")) {
      df.queryExecution.executedPlan.resetMetrics()
      df.queryExecution.executedPlan.executeToIterator()
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
    } finally {
      root.close()
      allocator.close()
    }
  }

  private def writeArrowSchema(schema: StructType, timeZone: String, out: WriteChannel): Unit = {
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZone)
    MessageSerializer.serialize(out, arrowSchema)
  }
  
  private def iterateRdd[T: scala.reflect.ClassTag](rdd: RDD[T], preserveOrder: Boolean, f: T => Unit): Unit = {
    if (preserveOrder) {
      // toLocalIterator has the disadvantage of running a job for each partition, one after the other
      // so it might be much slower for small datasets with many partitions
      // easy fix for the user is to use coalesce(1) before calling collectToArrow
      for (x <- rdd.toLocalIterator) {
        f(x)
      }
    } else {
      // this is a nice way to process partitions as they are arriving to the driver, stolen from how PySpark makes Arrow batches
      rdd.sparkContext.runJob(
        rdd,
        (it: Iterator[T]) => it.toArray,
        (_, xs: Array[T]) => {
          for (x <- xs) {
            f(x)
          }
        })
    }
  }
  
  /** Based on Spark's toArrowBatchRdd */
  def collectToArrow2[R](df: Dataset[R], outputFilePath: String, preserverOrder: Boolean): Unit = {
    val batchRdd = df.toArrowBatchRdd
    
    try {
      Utils.tryWithResource(FileChannel.open(Paths.get(outputFilePath), StandardOpenOption.WRITE)) { outputFilePath =>
        val writeChannel = new WriteChannel(outputFilePath)
        writeArrowSchema(df.schema, df.sqlContext.conf.sessionLocalTimeZone, writeChannel)

        iterateRdd(batchRdd, preserverOrder, (batch: Array[Byte]) => writeChannel.write(batch))
      }
    } catch {
      case x: Throwable =>
        x.printStackTrace()
        throw x
    }
  }
  
  private def readArrowSchema(inputFilePath: String): Schema = {
    Utils.tryWithResource(FileChannel.open(Paths.get(inputFilePath))) { file =>
      readArrowSchema(file)
    }
  }
  private def readArrowSchema(input: java.nio.channels.ReadableByteChannel): Schema = {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("Julia readArrowSchema", 0, Long.MaxValue)
    val reader = new ArrowStreamReader(input, allocator)
    try {
        reader.getVectorSchemaRoot.getSchema
    } finally {
      reader.close()
      allocator.close()
    }
  }

  /** Based on ArrowConverters.toDataFrame */
  def fromArrow1(sess: SparkSession, inputFilePath: String): DataFrame = {
    val schema = ArrowUtils.fromArrowSchema(readArrowSchema(inputFilePath))
    val batches = ArrowConverters.readArrowStreamFromFile(sess.sqlContext, inputFilePath)
    ArrowConverters.toDataFrame(batches, schema.json, sess.sqlContext)
  }
  
  /** Based on ArrowConverters.fromBatchIterator, creates a LocalRelation which is nice for smaller tables - it should enable filter pushdown on JOINs with this relation. */
  def fromArrow2(sess: SparkSession, inputFilePath: String): DataFrame = {
    val timeZone = sess.sessionState.conf.sessionLocalTimeZone
    
    val taskContext = TaskContext.empty()
    try {
      val arrowSchema = readArrowSchema(inputFilePath)
      val schema = ArrowUtils.fromArrowSchema(arrowSchema)
      val rows = Utils.tryWithResource(FileChannel.open(Paths.get(inputFilePath))) { tempFile =>
        ArrowConverters.fromBatchIterator(ArrowConverters.getBatchesFromStream(tempFile), schema, timeZone, taskContext).map(cloneRow(schema)).toArray
      }
      val relation = LocalRelation(schema.toAttributes, rows, isStreaming = false)
      Dataset.ofRows(sess, relation)
    } finally {
      taskContext.markTaskCompleted(None)
    }
  }
  private def cloneRow(
    schema: StructType
  ): InternalRow => InternalRow = {
    // ColumnarBatchRow.copy is buggy - it cannot handle nested objects, arrays and so on
    val projection = UnsafeProjection.create(schema)
    (r: InternalRow) => projection.apply(r).copy()
  }
}
