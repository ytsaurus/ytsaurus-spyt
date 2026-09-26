package tech.ytsaurus.spyt.format.columnar

import org.apache.spark.sql.catalyst.expressions.BoundReference

import scala.jdk.CollectionConverters._

/** Caches bound argument positions and an evaluator for one function occurrence within a partition.
 *
 * Creates and validates an evaluator on first use, then registers it with the iterator for cleanup.
 * Empty partitions create no evaluators; each function occurrence keeps independent evaluator state.
 */
private[columnar] class PreparedColumnarFunction(
  val function: YtColumnarFunction,
  context: ColumnarFunctionContext,
  registerEvaluator: ColumnarEvaluator => Unit) {

  val ordinals: Seq[Int] = function.children.map(_.asInstanceOf[BoundReference].ordinal)

  /** Creates a call-local evaluator after checking cached metadata against driver registration.
   *
   * Provider loading uses the executor thread's artifact classloader. Successfully created instances
   * are registered immediately so a later provider failure still closes earlier instances.
   */
  lazy val evaluator: ColumnarEvaluator = {
    val provider = Class.forName(function.providerClass, true, Thread.currentThread().getContextClassLoader)
      .asSubclass(classOf[ColumnarFunctionProvider]).getConstructor().newInstance()
    require(provider.apiVersion() == ColumnarFunctionProvider.API_VERSION,
      "Unsupported columnar function API version")
    val options = java.util.Collections.unmodifiableMap[String, String](function.options.asJava)
    val descriptor = provider.describe(options)
    require(descriptor.inputSchema() == function.inputSchema && descriptor.outputSchema() == function.outputSchema &&
      descriptor.deterministic() == function.isDeterministic,
      s"Columnar function descriptor differs between driver and executor: ${function.providerClass}")
    val evaluator = Option(provider.create(context, options)).getOrElse(
      throw new IllegalStateException(s"${function.providerClass} returned no evaluator"))
    registerEvaluator(evaluator)
    evaluator
  }
}
