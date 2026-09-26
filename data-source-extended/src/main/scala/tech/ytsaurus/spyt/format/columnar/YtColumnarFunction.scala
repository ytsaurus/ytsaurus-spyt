package tech.ytsaurus.spyt.format.columnar

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.YtColumnarFunctionRegistration
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpectsInputTypes, Unevaluable}
import org.apache.spark.sql.types.DataType

/** Serializable Catalyst expression for a registered, row-preserving Arrow plugin function.
 *
 * Registration constructs this expression from driver-side provider metadata. It has no scalar evaluation path:
 * [[YtColumnarUdfRule]] extracts it into a columnar stage, materializing computed arguments first.
 * Evaluator instances and native resources are created only by the executor, never stored in the expression.
 *
 * @param children argument expressions in the provider's declared input order
 * @param providerClass public provider class with a public no-argument constructor
 * @param options immutable configuration passed to provider description and evaluator creation
 * @param inputSchemaJson Arrow input schema captured during registration
 * @param outputSchemaJson Arrow schema containing exactly one output field, which may be a struct
 * @param dataType Spark type corresponding to the output field
 * @param prettyName registered session-local SQL name
 * @param isDeterministic provider's determinism declaration, combined with that of the arguments
 */
case class YtColumnarFunction(
  children: Seq[Expression],
  providerClass: String,
  options: Map[String, String],
  inputSchemaJson: String,
  outputSchemaJson: String,
  override val dataType: DataType,
  override val prettyName: String,
  isDeterministic: Boolean)
  extends Expression with ExpectsInputTypes with Unevaluable {

  @transient private[columnar] lazy val inputSchema: Schema = Schema.fromJSON(inputSchemaJson)
  @transient private[columnar] lazy val outputSchema: Schema = Schema.fromJSON(outputSchemaJson)

  /** Supplies declared argument types to Catalyst's input type checking. */
  @transient override lazy val inputTypes: Seq[DataType] = {
    YtColumnarFunctionRegistration.fromArrowSchema(inputSchema).fields.iterator.map(_.dataType).toVector
  }

  /** Reports the output field's declared nullability for Spark schema propagation. */
  override def nullable: Boolean = outputSchema.getFields.get(0).isNullable

  /** Allows deterministic-expression optimizations only when both provider and arguments permit them. */
  override lazy val deterministic: Boolean = isDeterministic && children.forall(_.deterministic)

  /** Lets Catalyst replace arguments while preserving the provider metadata captured at registration. */
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): YtColumnarFunction = {
    copy(children = newChildren)
  }
}
