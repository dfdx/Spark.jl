package org.apache.spark.sql.julia

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowFileWriter, ArrowStreamReader, ArrowStreamWriter, SeekableReadChannel}
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, ExprId, Expression, GenericInternalRow, JoinedRow, MutableProjection, NamedExpression, NonSQLExpression, PredicateHelper, PythonUDF, Unevaluable, UnsafeProjection, UnsafeRow, UserDefinedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{ArrowEvalPython, BatchEvalPython, LogicalPlan, Project, Subquery, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Encoder, Encoders, SparkSession, Strategy}
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.execution.{ExplainMode, SQLExecution, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.arrow.{ArrowConverters, ArrowWriter}
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, ArrowPythonRunner, BatchEvalPythonExec, BatchIterator, EvalPythonExec, HybridRowQueue}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

import java.io.{File, FileInputStream}
import java.nio.channels.FileChannel
import java.nio.file.{OpenOption, Path, Paths, StandardOpenOption}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DatasetUtils {
  // df.explain prints the plan to standard output
  def explain[R](df: Dataset[R], mode: String): String =
    df.queryExecution.explainString(ExplainMode.fromString(mode))
  def collectToArrow[R](df: Dataset[R], tempFilePath: String): Unit = {
    // TODO: see df.collectAsArrowToR() and collectAsArrowToPython, we should probably do something similar

    val timeZone = df.sqlContext.conf.sessionLocalTimeZone
    val arrowSchema = ArrowUtils.toArrowSchema(df.schema, timeZone)

    val allocator = ArrowUtils.rootAllocator.newChildAllocator(s"Julia collectToArrow", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)

    try {
      val rows = SQLExecution.withNewExecutionId(df.queryExecution, Some("collectToArrow")) {
        df.queryExecution.executedPlan.resetMetrics()
        df.queryExecution.executedPlan.executeCollect()
      }
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

  def addExperimentalUdfs(s: SparkSession): Unit = {
    s.experimental.extraOptimizations ++= Seq(ExtractVectorUDFs)
    s.experimental.extraStrategies ++= Seq(ScalaVectorStrategy)

    def regFunction[T](name: String, f: Array[InternalRow] => Array[T])(implicit encoder: Encoder[T]): Unit = {
      val ser = encoder.asInstanceOf[ExpressionEncoder[T]].createSerializer()
      s.sessionState.functionRegistry.registerFunction(FunctionIdentifier(name), input =>
        ScalaVectorUDF(name, encoder.schema, input, a => f(a).map(ser.apply(_).copy()), true, false): Expression)
    }

    s.udf.register("test0", (i: Int) => i + 10)

    regFunction("test1", a => a.map(_ => "ahoj"))(Encoders.STRING)
    regFunction("test2", a => a.map(r => {
      println(s"Running on ${r} ${r.getString(0)}")
      "ahoj " + r.getString(0)
    }))(Encoders.STRING)
    regFunction("test3", a => a.zipWithIndex.map { case (r, i) =>
      s"$i: ${r.getString(0)}"
    })(Encoders.STRING)
  }
}

case class ScalaVectorUDF(
  name: String,
  dataType: StructType,
  children: Seq[Expression],
  function: Array[InternalRow] => Array[InternalRow],
  udfDeterministic: Boolean,
  nullable: Boolean,
  resultId: ExprId = NamedExpression.newExprId,
)   extends Expression with Unevaluable with NonSQLExpression with UserDefinedExpression {

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  override def toString: String = s"$name(${children.mkString(", ")})"

  lazy val resultAttribute: Attribute = AttributeReference(toPrettySQL(this), dataType, nullable)(
    exprId = resultId)

  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    // `resultId` can be seen as cosmetic variation in PythonUDF, as it doesn't affect the result.
    this.copy(resultId = ExprId(-1)).withNewChildren(canonicalizedChildren)
  }
}


/**
 * Adaptation of ExtractPythonUDFs
 *
 * Extracts ScalaVectorUDF from operators, rewriting the query plan so that the UDF can be evaluated
 * alone in a batch.
 *
 * Only extracts the PythonUDFs that could be evaluated in Python (the single child is PythonUDFs
 * or all the children could be evaluated in JVM).
 *
 * This has the limitation that the input to the Python UDF is not allowed include attributes from
 * multiple child operators.
 */
object ExtractVectorUDFs extends Rule[LogicalPlan] with PredicateHelper {
  private type EvalType = Int
  private type EvalTypeChecker = EvalType => Boolean

  private def collectUDFsFromExpressions(expressions: Seq[Expression]): Seq[ScalaVectorUDF] = {
    def collectEvaluableUDFs(expr: Expression): Seq[ScalaVectorUDF] = expr match {
      case udf: ScalaVectorUDF =>
        Seq(udf)
      case e => e.children.flatMap(collectEvaluableUDFs)
    }

    expressions.flatMap(collectEvaluableUDFs)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // SPARK-26293: A subquery will be rewritten into join later, and will go through this rule
    // eventually. Here we skip subquery, as Python UDF only needs to be extracted once.
    case s: Subquery if s.correlated => plan

    case _ => plan transformUp {
      // A safe guard. This only runs once, so we will not hit and
      // `EvalScalaVectorUDF` in the input plan. However if we hit them, we must skip them, as we can't
      // extract UDFs from them again.
      case p: EvalScalaVectorUDF => p

      case plan: LogicalPlan => extract(plan)
    }
  }

  /**
   * Extract all the PythonUDFs from the current operator and evaluate them before the operator.
   */
  private def extract(plan: LogicalPlan): LogicalPlan = {
    val udfs = collectUDFsFromExpressions(plan.expressions)
      // ignore the PythonUDF that come from second/third aggregate, which is not used
      .filter(udf => udf.references.subsetOf(plan.inputSet))
    if (udfs.isEmpty) {
      // If there aren't any, we are done.
      plan
    } else {
      println(s"Rewriting $plan")
      val attributeMap = mutable.HashMap[ScalaVectorUDF, Expression]()
      // Rewrite the child that has the input required for the UDF
      val newChildren = plan.children.map { child =>
        // Pick the UDF we are going to evaluate
        val validUdfs = udfs.filter { udf =>
          // Check to make sure that the UDF can be evaluated with only the input of this child.
          udf.references.subsetOf(child.outputSet)
        }
        if (validUdfs.nonEmpty) {
          val resultAttrs = validUdfs.zipWithIndex.map { case (u, i) =>
            AttributeReference(s"scalaVectorUDF$i", u.dataType)()
          }

          val evaluation = EvalScalaVectorUDF(validUdfs.toArray, resultAttrs, child)

          attributeMap ++= validUdfs.zip(resultAttrs)
          evaluation
        } else {
          child
        }
      }
      // Other cases are disallowed as they are ambiguous or would require a cartesian
      // product.
      udfs.filterNot(attributeMap.contains).foreach { udf =>
        sys.error(s"Invalid ScalaVectorUDF $udf, requires attributes from more than one child.")
      }

      val rewritten = plan.withNewChildren(newChildren).transformExpressions {
        case p: ScalaVectorUDF if attributeMap.contains(p) =>
          attributeMap(p)
      }

      // extract remaining UDFs recursively
      val newPlan = extract(rewritten)

      println(s"Rewritten $plan => $newPlan")
      if (newPlan.output != plan.output) {
        // Trim away the new UDF value if it was only used for filtering or something.
        Project(plan.output, newPlan)
      } else {
        newPlan
      }
    }
  }
}

case class EvalScalaVectorUDF(
  udfs: Array[ScalaVectorUDF],
  resultAttrs: Seq[Attribute],
  child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)
}

object ScalaVectorStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case EvalScalaVectorUDF(udfs, output, child) =>
      ScalaVectorUdfExec(udfs, output, planLater(child)) :: Nil
    case _ =>
      Nil
  }
}

case class ScalaVectorUdfExec(udfs: Array[ScalaVectorUDF], resultAttrs: Seq[Attribute], child: SparkPlan)
  extends UnaryExecNode {

  protected def evaluate(
    funcs: Array[ScalaVectorUDF],
    argOffsets: Array[Array[Int]],
    rowArray: Array[InternalRow],
    schema: StructType): Array[InternalRow] = {

    val outputTypes = resultAttrs.map(_.dataType)

    val outputs = funcs.zipWithIndex.map { case (fn, i) =>

      val input = rowArray.map(row =>
        new GenericInternalRow(
          argOffsets(i).map(offset =>
            row.get(offset, schema.fields(offset).dataType): Any
          )
        ): InternalRow
      )

      fn.function(input)
      // fn.function(input).map(_.toSeq(fn.dataType).toArray)
    }
    outputs.transpose.map {
      x => InternalRow.fromSeq(x)
    }
  }

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())

    println(s"Executing functions $udfs: ${inputRDD.count()}")
    println(s"Executing functions $udfs: ${inputRDD.collect().mkString(";")}")

    inputRDD.mapPartitions { iter =>
      val inputRows = iter.map(_.copy()).toArray
      //val context = TaskContext.get()

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      // val queue = HybridRowQueue(context.taskMemoryManager(),
      //  new File(Utils.getLocalDir(SparkEnv.get.conf)), child.output.length)
      //context.addTaskCompletionListener[Unit] { ctx =>
      //  queue.close()
      //}

      val inputs = udfs.map(_.children)

      // flatten all the arguments
      val allInputs = new ArrayBuffer[Expression]
      val dataTypes = new ArrayBuffer[DataType]
      val argOffsets = inputs.map { input =>
        input.map { e =>
          if (allInputs.exists(_.semanticEquals(e))) {
            allInputs.indexWhere(_.semanticEquals(e))
          } else {
            allInputs += e
            dataTypes += e.dataType
            allInputs.length - 1
          }
        }.toArray
      }
      val projection = MutableProjection.create(allInputs, child.output)
      val schema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      })

      println(s"Executing functions $udfs on <${inputRows.mkString(";")}>")

      // Add rows to queue to join later with the result.
      val projectedRowIter = inputRows.map { inputRow =>
        //queue.add(inputRow.asInstanceOf[UnsafeRow])
        projection(inputRow).copy()
      }.toArray

      val outputRows = evaluate(
        udfs, argOffsets, projectedRowIter, schema)

      val resultProj = UnsafeProjection.create(output, output)

      println(s"Done: $inputRows => ${outputRows.mkString("; ")}")

      val join = new JoinedRow

      outputRows.iterator.zip(inputRows.iterator).map { case(outputRow, inputRow) =>
        resultProj(join(inputRow, outputRow))
      }
    }
  }

}
