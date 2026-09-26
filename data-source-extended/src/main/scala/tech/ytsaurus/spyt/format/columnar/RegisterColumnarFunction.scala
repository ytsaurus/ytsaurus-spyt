package tech.ytsaurus.spyt.format.columnar

import org.apache.spark.sql.{Row, SparkSession, YtColumnarFunctionRegistration}
import org.apache.spark.sql.execution.command.LeafRunnableCommand

/** Driver-side SQL command produced by the columnar function parser for plugin registration.
 *
 * Executing it validates provider metadata and creates or replaces a temporary function in the executing session.
 *
 * @param name temporary SQL function name
 * @param provider provider class available through the session artifact classloader
 * @param options string configuration passed to the provider
 */
case class RegisterColumnarFunction(name: String, provider: String, options: Map[String, String])
  extends LeafRunnableCommand {

  /** Registers the function in the supplied session and returns no result rows. */
  override def run(sparkSession: SparkSession): Seq[Row] = {
    YtColumnarFunctionRegistration.register(sparkSession, name, provider, options)
    Seq.empty
  }
}
