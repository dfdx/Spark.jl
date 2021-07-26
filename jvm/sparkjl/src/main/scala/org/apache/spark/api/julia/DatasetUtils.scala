package org.apache.spark.sql.julia

import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.{FieldVector, TimeStampMicroTZVector, TimeStampMilliTZVector, TimeStampVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.message.MessageChannelReader
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.network.util.ByteArrayReadableChannel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{ExplainMode, SQLExecution}
import org.apache.spark.sql.execution.arrow.{ArrowConverters, ArrowWriter}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.apache.spark.util.Utils

import java.sql.Timestamp
import java.time.Instant
import scala.collection.JavaConverters._

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

    Utils.tryWithSafeFinally {
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
    } {
      root.close()
      allocator.close()
    }
  }

  def fromArrow(sess: SparkSession, bytes: Array[Byte]): DataFrame = {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator(s"Julia fromArrow", 0, Long.MaxValue)
    //val reader = new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(bytes), allocator)
    val reader = new ArrowStreamReader(new ByteArrayReadableSeekableByteChannel(bytes), allocator)
    val root = reader.getVectorSchemaRoot
    try {
      val schema = ArrowUtils.fromArrowSchema(root.getSchema)
//      val vectors = root.getFieldVectors.asScala.toArray

      // probably not the most efficient way to copy Arrow data into dataframe, but should work
      // it would be nicer to use spark utils that convert Arrow directly to DataFrame
      // see ArrowConverters.fromBatchIterator, but that returns InternalRow :/
//      val readRows = new java.util.ArrayList[Row]()
//      while (reader.loadNextBatch()) {
//        val numRows = root.getRowCount
//        val loadedVectors = vectors.zip(schema.fields).map {
//          case (v, f) => readVector(v, f.dataType, 0, numRows)
//        }
//        for (i <- 0 until numRows)
//          readRows.add(Row(loadedVectors.map(_(i)): _*))
//      }
//      val df = sess.createDataFrame(readRows, schema)
//      val rowsCopy = df.collect()
//      assert(rowsCopy.length == readRows.size)
//      df

      val batches = ArrowConverters.getBatchesFromStream(new ByteArrayReadableSeekableByteChannel(bytes)).toArray
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

  private def readVector(vector: FieldVector, dataType: DataType, start: Int, end: Int): Array[Any] = {
    if (start == end)
      return Array.empty
    val result = new Array[Any](end - start)
    vector match {
      case mapVector: MapVector =>
        val mapType = dataType.asInstanceOf[MapType]
        val entries = mapVector.getDataVector.asInstanceOf[StructVector]
        val keyVector = entries.getChild(MapVector.KEY_NAME)
        val valueVector = entries.getChild(MapVector.VALUE_NAME)
        for (i <- start until end) {
          val elStart = mapVector.getElementStartIndex(i)
          val elEnd = mapVector.getElementEndIndex(i)
          val keys = readVector(keyVector, mapType.keyType, elStart, elEnd)
          val values = readVector(valueVector, mapType.valueType, elStart, elEnd)
          result(i - start) = keys.zip(values).toMap
        }

      case listVector: ListVector =>
        val arrayType = dataType.asInstanceOf[ArrayType]
        for (i <- start until end)
          if (!listVector.isNull(i))
            result(i - start) = readVector(
              listVector.getDataVector,
              arrayType.elementType,
              listVector.getElementStartIndex(i),
              listVector.getElementEndIndex(i)
            )
      case timeVector: TimeStampMilliTZVector =>
        for (i <- start until end)
          if (!timeVector.isNull(i))
            result(i - start) = Instant.ofEpochMilli(i)
      case timeVector: TimeStampMicroTZVector =>
        for (i <- start until end)
          if (!timeVector.isNull(i)) {
            result(i - start) = Instant.ofEpochSecond(i)
          }
      case structVector: StructVector =>
        val structType = dataType.asInstanceOf[StructType]
        val fieldVectors = structType.fields.map(f =>
          readVector(
            structVector.getChild(f.name),
            f.dataType,
            start,
            end
          ))
        fieldVectors.transpose.map(Row(_))
      case stringVector: VarCharVector =>
        for (i <- start until end)
          result(i - start) = stringVector.getObject(i).toString
      case _ =>
        for (i <- start until end)
          result(i - start) = vector.getObject(i)
    }
    result
  }
}
