package tech.ytsaurus.spyt.format.columnar

import org.apache.arrow.vector.{FieldVector, UInt8Vector}
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.YtColumnarFunctionRegistration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BindReferences, BoundReference,
  Expression, NamedExpression}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.spyt.types.UInt64Type
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}

import tech.ytsaurus.core.tables.ColumnValueType
import tech.ytsaurus.spyt.format.batch.{ArrowColumnVector => YtArrowColumnVector}
import tech.ytsaurus.spyt.serialization.IndexedDataType

import java.nio.file.Paths

import scala.annotation.tailrec

/** Executes row-preserving plugin projections on Spark columnar batches.
 *
 * Created by [[YtColumnarUdfRule]], this operator binds arguments to child columns and creates one evaluator
 * per function occurrence per partition. Returned batches own pass-through vector views and plugin output roots.
 * Each batch remains valid until it is closed, the next batch is requested, or the task completes.
 * Task completion also closes outstanding outputs, evaluators, and the partition allocator.
 *
 * @param projectList supported named expressions preserving their Catalyst output attributes
 * @param child input plan, made columnar by Spark's transition rules when necessary
 */
case class YtColumnarUdfExec(projectList: Seq[NamedExpression], child: SparkPlan) extends UnaryExecNode {

  /** Advertises batch execution so Spark inserts the required transitions around this operator. */
  override def supportsColumnar: Boolean = true

  /** Preserves names, expression identifiers, types, and nullability from the original projection. */
  override lazy val output: Seq[Attribute] = projectList.map(_.toAttribute)

  /** Creates partition iterators with task-scoped evaluators and deterministic Arrow resource cleanup.
   *
   * The driver's session-relative artifact directory is captured before distribution so executor artifact
   * lookup supports both Connect uploads and shared Spark files. Native/provider objects stay executor-local.
   */
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val expressions = projectList.map(expression => BindReferences.bindReference[Expression](expression, child.output))
    val passThroughOrdinals = expressions.iterator.map(YtColumnarUdfExec.unalias).collect {
      case reference: BoundReference => reference.ordinal
    }.toVector.distinct
    val passThroughSchemaJson = Option(passThroughOrdinals).filter(_.nonEmpty).map { ordinals =>
      val fields = ordinals.map { ordinal =>
        val attribute = child.output(ordinal)
        StructField(s"column_$ordinal", attribute.dataType, attribute.nullable)
      }
      YtColumnarFunctionRegistration.toArrowSchema(StructType(fields), conf.sessionLocalTimeZone).toJson
    }
    val artifactDirectory = Paths.get(SparkFiles.getRootDirectory()).relativize(Paths.get(SparkFiles.get(""))).toString
    child.executeColumnar().mapPartitions { input =>
      new ColumnarFunctionIterator(input, expressions, passThroughOrdinals, passThroughSchemaJson, artifactDirectory)
    }
  }

  /** Rejects direct row execution; Spark must wrap the columnar path in a transition when rows are needed. */
  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("YtColumnarUdfExec requires columnar execution")
  }

  /** Supports Spark plan rewrites while retaining the original projection. */
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)
}

/** Expression utilities shared by the columnar projection executor. */
object YtColumnarUdfExec {

  /** Wraps a root-owned vector, using YT's accessor for unsigned longs unsupported by Spark's wrapper. */
  private[columnar] def borrowedColumn(vector: FieldVector): ColumnVector = vector match {
    case unsigned: UInt8Vector =>
      val dataType = IndexedDataType.AtomicType(UInt64Type)
      new YtArrowColumnVector(dataType, unsigned, None, ColumnValueType.UINT64) {

        /** Leaves vector ownership with the root retained by the output batch. */
        override def close(): Unit = ()
      }
    case nested if NestedArrowColumnVector.required(nested) => new NestedArrowColumnVector(nested)
    case _ => new ArrowColumnVector(vector) {

      /** Leaves vector ownership with the root retained by the output batch. */
      override def close(): Unit = ()
    }
  }

  /** Removes naming aliases before matching a bound column or plugin call during batch evaluation. */
  @tailrec
  private[columnar] def unalias(expression: Expression): Expression = expression match {
    case Alias(child, _) => unalias(child)
    case other => other
  }
}
