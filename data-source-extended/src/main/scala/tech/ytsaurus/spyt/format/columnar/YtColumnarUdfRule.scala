package tech.ytsaurus.spyt.format.columnar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, ProjectExec, SparkPlan}

import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.ColumnarUdfEnabled

import scala.annotation.tailrec
import scala.collection.mutable

/** Replaces supported plugin projections with batch execution before Spark inserts row/column transitions.
 *
 * Install through SPYT's columnar session extension. Plugin calls in projections are extracted into
 * batch stages; surrounding expressions and computed arguments use ordinary Spark projections.
 */
class YtColumnarUdfRule(session: SparkSession) extends ColumnarRule {

  /** Supplies the physical-plan rewrite that Spark runs before choosing columnar transition operators. */
  override def preColumnarTransitions: Rule[SparkPlan] = columnarTransitions

  private val columnarTransitions: Rule[SparkPlan] = new Rule[SparkPlan] {

    /** Rewrites plugin projections and rejects plugin expressions left in unsupported plan positions. */
    override def apply(plan: SparkPlan): SparkPlan = {
      if (!ColumnarUdfEnabled.get(session.conf.getOption(ColumnarUdfEnabled.name)).get) return plan
      val transformed = plan.transformUp {
        case project: ProjectExec if project.projectList.exists(containsFunction) =>
          if (project.projectList.forall(supported)) YtColumnarUdfExec(project.projectList, project.child)
          else extract(project)
      }
      transformed.foreach {
        case _: YtColumnarUdfExec =>
        case node =>
          val unsupported = node.expressions.filter(containsFunction)
          if (unsupported.nonEmpty) {
            throw new UnsupportedColumnarFunctionException(node.nodeName, unsupported.map(_.sql))
          }
      }
      transformed
    }
  }

  /** Materializes innermost calls and their computed arguments before rebuilding the original projection.
   *
   * Every occurrence receives its own attribute, preserving independent nondeterministic calls. A new
   * stage is needed for calls depending on other plugin results; sibling calls share a batch operator.
   */
  @tailrec
  private def extract(project: ProjectExec): SparkPlan = {
    val arguments = mutable.ArrayBuffer.empty[NamedExpression]
    val functions = mutable.ArrayBuffer.empty[NamedExpression]

    def materializeArgument(argument: Expression): Attribute = argument match {
      case attribute: AttributeReference => attribute
      case other =>
        val alias = Alias(other, "columnar_argument")()
        arguments += alias
        alias.toAttribute
    }

    def materializeFunction(function: YtColumnarFunction): Attribute = {
      val inputs = function.children.map(materializeArgument)
      val alias = Alias(function.copy(children = inputs), function.prettyName)()
      functions += alias
      alias.toAttribute
    }

    val remaining = project.projectList.map { expression =>
      expression.transformDown {
        case function: YtColumnarFunction if !function.children.exists(containsFunction) =>
          materializeFunction(function)
      }.asInstanceOf[NamedExpression]
    }
    val child = project.child
    val input = if (arguments.isEmpty) child else ProjectExec(child.output ++ arguments, child)
    val result = ProjectExec(remaining, YtColumnarUdfExec(child.output ++ functions, input))
    if (remaining.exists(containsFunction)) extract(result) else result
  }

  /** Detects plugin calls, including those nested inside ordinary Spark expressions. */
  private def containsFunction(expression: Expression): Boolean = expression.exists(_.isInstanceOf[YtColumnarFunction])

  /** Recognizes aliases, pass-through columns, and plugin calls whose arguments are direct columns. */
  @tailrec
  private def supported(expression: Expression): Boolean = expression match {
    case Alias(child, _) => supported(child)
    case _: AttributeReference => true
    case function: YtColumnarFunction => function.children.forall(_.isInstanceOf[AttributeReference])
    case _ => false
  }
}
