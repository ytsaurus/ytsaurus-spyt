package org.apache.spark.sql

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.format.columnar.{ColumnarFunctionProvider, YtColumnarFunction}

import scala.jdk.CollectionConverters._

/** Driver-side bridge between plugin metadata and Spark's session-local function registry.
 *
 * Use [[register]] directly from JVM clients or through SPYT's REGISTER COLUMNAR FUNCTION command after adding
 * the companion JAR to the session. This object resides in Spark's SQL package to access its internal APIs;
 * plugin implementations should depend on the columnar API instead.
 */
object YtColumnarFunctionRegistration {

  /** Converts a provider's Arrow schema to Spark types for registration and Catalyst argument checking. */
  def fromArrowSchema(schema: Schema): StructType = ArrowUtils.fromArrowSchema(schema)

  /** Converts pass-through column types to an Arrow schema for independently owned batch views. */
  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    ArrowUtils.toArrowSchema(schema, timeZoneId, false, false)
  }

  /** Retrieves the provider JAR loader, including session artifacts uploaded by Spark 4 Connect clients.
   *
   * Callers must use the requesting session rather than cache a loader globally across sessions.
   */
  def providerClassLoader(session: SparkSession): ClassLoader = {
    SparkAdapter.instance.sparkClassLoader(session)
  }

  /** Creates or replaces a temporary columnar SQL function using metadata from a session-loaded provider.
   *
   * Requires a compatible provider API version and exactly one output field. Description happens on the
   * driver without creating an evaluator; serialized schemas and options are later validated on each executor.
   * The provider class must implement ColumnarFunctionProvider and expose a public no-argument constructor.
   *
   * @param session session owning both provider artifacts and the temporary function registration
   * @param name SQL name used by subsequent queries in this session
   * @param providerClass fully qualified class name from the companion JAR
   * @param options provider configuration; providers receive an unmodifiable Java view
   */
  def register(session: SparkSession, name: String, providerClass: String, options: Map[String, String]): Unit = {
    val loader = providerClassLoader(session)
    Utils.withContextClassLoader(loader) {
      val provider = Class.forName(providerClass, true, loader)
        .asSubclass(classOf[ColumnarFunctionProvider]).getConstructor().newInstance()
      require(provider.apiVersion() == ColumnarFunctionProvider.API_VERSION,
        s"Unsupported columnar function API version: ${provider.apiVersion()}")
      val descriptor = provider.describe(java.util.Collections.unmodifiableMap(options.asJava))
      require(descriptor != null, s"Columnar function provider $providerClass returned no descriptor")
      val input = descriptor.inputSchema()
      val output = descriptor.outputSchema()
      require(input != null && output != null, "Columnar function schemas must be specified")
      require(output.getFields.size() == 1, "A columnar SQL function must return exactly one field")
      val inputSchemaJson = input.toJson
      val outputSchemaJson = output.toJson
      val inputCount = input.getFields.size()
      val resultType = fromArrowSchema(output).fields.head.dataType
      val deterministic = descriptor.deterministic()
      session.sessionState.functionRegistry.createOrReplaceTempFunction(
        name,
        arguments => {
          require(arguments.size == inputCount, s"$name expects $inputCount arguments, got ${arguments.size}")
          YtColumnarFunction(
            arguments,
            providerClass,
            options,
            inputSchemaJson,
            outputSchemaJson,
            resultType,
            name,
            deterministic)
        },
        "internal")
    }
  }
}
