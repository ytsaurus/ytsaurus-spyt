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

  private class RegistrationScanner(sqlText: String) {

    private var position = 0

    private def skipWhitespace(): Unit = {
      while (position < sqlText.length && " \t\n\r\f\u000b".contains(sqlText.charAt(position))) {
        position += 1
      }
    }

    private def keyword(value: String, leadingWhitespace: Boolean): Boolean = {
      val start = position
      skipWhitespace()
      if ((!leadingWhitespace || position > start) &&
          sqlText.regionMatches(true, position, value, 0, value.length)) {
        position += value.length
        true
      } else {
        false
      }
    }

    private def quoted(delimiter: Char): Option[String] = {
      if (position >= sqlText.length || sqlText.charAt(position) != delimiter) return None
      position += 1
      val value = new StringBuilder
      while (position < sqlText.length) {
        val character = sqlText.charAt(position)
        position += 1
        if (character != delimiter) {
          value.append(character)
        } else if (position < sqlText.length && sqlText.charAt(position) == delimiter) {
          value.append(delimiter)
          position += 1
        } else {
          return Some(value.toString())
        }
      }
      None
    }

    private def identifier(): Option[String] = {
      if (position < sqlText.length && sqlText.charAt(position) == '`') {
        quoted('`').filter(_.nonEmpty)
      } else {
        val start = position
        def isLetter(character: Char): Boolean = {
          character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character == '_'
        }
        if (position < sqlText.length && isLetter(sqlText.charAt(position))) {
          position += 1
          while (position < sqlText.length &&
              (isLetter(sqlText.charAt(position)) ||
                sqlText.charAt(position) >= '0' && sqlText.charAt(position) <= '9')) {
            position += 1
          }
        }
        Option.when(position > start)(sqlText.substring(start, position))
      }
    }

    def parse(): Option[(String, String, Option[String])] = {
      if (!keyword("REGISTER", false) || !keyword("COLUMNAR", true) || !keyword("FUNCTION", true)) return None
      val beforeName = position
      skipWhitespace()
      if (position == beforeName) return None
      val name = identifier()
      if (name.isEmpty || !keyword("AS", true)) return None
      val beforeProvider = position
      skipWhitespace()
      if (position == beforeProvider) return None
      val provider = quoted('\'')
      if (provider.isEmpty) return None
      val beforeOptions = position
      skipWhitespace()
      val options = if (position < sqlText.length && sqlText.charAt(position) != ';') {
        position = beforeOptions
        if (!keyword("OPTIONS", true)) return None
        val beforeValue = position
        skipWhitespace()
        if (position == beforeValue) return None
        val value = quoted('\'')
        if (value.isEmpty) return None
        value
      } else {
        None
      }
      skipWhitespace()
      if (position < sqlText.length && sqlText.charAt(position) == ';') position += 1
      skipWhitespace()
      if (position == sqlText.length) Some((name.get, provider.get, options)) else None
    }
  }

  /** Recognizes registration without evaluating the delegate unless the SQL is an ordinary statement. */
  private def parseColumnar(sqlText: String)(fallback: => LogicalPlan): LogicalPlan = {
    new RegistrationScanner(sqlText).parse() match {
      case Some((name, provider, optionsJson)) =>
        val options = optionsJson.map { json =>
          val node = new ObjectMapper().readTree(json)
          require(node != null && node.isObject, "Columnar function OPTIONS must be a JSON object")
          node.properties().asScala.map { entry =>
            require(entry.getValue.isTextual, "Columnar function option values must be strings")
            entry.getKey -> entry.getValue.asText()
          }.toMap
        }.getOrElse(Map.empty[String, String])
        register(name, provider, options)
      case None => fallback
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
