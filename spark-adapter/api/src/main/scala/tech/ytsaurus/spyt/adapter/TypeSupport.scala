package tech.ytsaurus.spyt.adapter

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, ExprValue}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser
import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport.CastFunction

import java.util.ServiceLoader

trait TypeSupport {
  val uInt64DataType: DataType
  def uInt64Serializer(inputObject: Expression): Expression
  def uInt64Deserializer(path: Expression): Expression
  def uInt64Cast(from: DataType): Any => Any
  val uInt64CastToString: Any => Any
  val uInt64CastToStringCode: CastFunction
  def extractUint64Opt(ctx: SqlBaseParser.PrimitiveDataTypeContext): Option[DataType]

  val ysonDataType: DataType
  def ysonCast(from: DataType): Any => Any
  val ysonCastToBinary: Any => Any
  val ysonCastToBinaryCode: CastFunction
  val binaryCastToYsonCode: CastFunction
}

object TypeSupport {
  lazy val instance: TypeSupport = ServiceLoader.load(classOf[TypeSupport]).findFirst().get()

  // A copy of org.apache.spark.sql.catalyst.expressions.CastBase#CastFunction which was moved
  // to org.apache.spark.sql.catalyst.expressions.Cast#CastFunction in Spark 3.4.0, but wasn't changed
  type CastFunction = (ExprValue, ExprValue, ExprValue) => Block
}
