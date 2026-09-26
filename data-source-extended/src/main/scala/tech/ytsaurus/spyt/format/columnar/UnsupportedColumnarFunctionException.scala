package tech.ytsaurus.spyt.format.columnar

import org.apache.spark.SparkException

/** Reports columnar calls remaining outside supported projections after the physical-plan rewrite.
 *
 * The planning rule supplies the rejected operator name and the SQL expressions containing columnar calls.
 */
class UnsupportedColumnarFunctionException(nodeName: String, expressions: Seq[String])
  extends SparkException(
    s"Columnar functions cannot be evaluated in Spark plan node '$nodeName' " +
      s"(expressions: ${expressions.mkString(", ")}). " +
      "Columnar functions are supported only in projections. " +
      "Move these calls into a projection before using their results in filters, joins, or aggregates.")
