package tech.ytsaurus.spyt.format.columnar

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParameterContext, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

import scala.jdk.CollectionConverters._

/** Adds columnar registration syntax and delegates all other parsing to the supplied Spark 4.1+ parser.
 *
 * Create through SparkAdapter with a callback that builds the registration command. OPTIONS must be a JSON object
 * containing string values. Ordinary SQL retains the delegate's parameter handling and parsing errors.
 *
 * @param delegate parser providing ordinary Spark SQL syntax
 * @param register creates a command from the function name, provider class and options
 */
class YtColumnarFunctionParser(
  delegate: ParserInterface,
  register: (String, String, Map[String, String]) => LogicalPlan) extends ParserInterface {

  private val registration = ("(?is)^\\s*REGISTER\\s+COLUMNAR\\s+FUNCTION\\s+" +
    "(`(?:``|[^`])+`|[a-zA-Z_][a-zA-Z_0-9]*)\\s+AS\\s+'((?:''|[^'])*)'" +
    "(?:\\s+OPTIONS\\s+'((?:''|[^'])*)')?\\s*;?\\s*$").r

  /** Recognizes registration without evaluating the delegate unless the SQL is an ordinary statement. */
  private def parseColumnar(sqlText: String)(fallback: => LogicalPlan): LogicalPlan = {
    sqlText match {
      case registration(identifier, provider, optionsJson) =>
        val name = if (identifier.startsWith("`")) {
          identifier.substring(1, identifier.length - 1).replace("``", "`")
        } else {
          identifier
        }
        val options = Option(optionsJson).map { json =>
          val node = new ObjectMapper().readTree(json.replace("''", "'"))
          require(node != null && node.isObject, "Columnar function OPTIONS must be a JSON object")
          node.properties().asScala.map { entry =>
            require(entry.getValue.isTextual, "Columnar function option values must be strings")
            entry.getKey -> entry.getValue.asText()
          }.toMap
        }.getOrElse(Map.empty[String, String])
        register(name, provider.replace("''", "'"), options)
      case _ => fallback
    }
  }

  /** Intercepts registration in the statement entry point. */
  override def parsePlan(sqlText: String): LogicalPlan = parseColumnar(sqlText)(delegate.parsePlan(sqlText))

  /** Intercepts registration in the query entry point. */
  override def parseQuery(sqlText: String): LogicalPlan = parseColumnar(sqlText)(delegate.parseQuery(sqlText))

  /** Preserves explicit parameter context when delegating ordinary SQL. */
  override def parsePlanWithParameters(sqlText: String, parameterContext: ParameterContext): LogicalPlan = {
    parseColumnar(sqlText)(delegate.parsePlanWithParameters(sqlText, parameterContext))
  }

  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier = delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)

  override def parseRoutineParam(sqlText: String): StructType = delegate.parseRoutineParam(sqlText)
}
