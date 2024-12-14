package org.apache.spark.sql.spyt.types

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, ExprValue}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.types.UInt64Long

import scala.math.Numeric.LongIsIntegral
import scala.reflect.runtime.universe.typeTag

class UInt64Type private() extends IntegralType with NumericCompat {
  private[sql] type InternalType = Long

  @transient private[sql] lazy val tag = typeTag[InternalType]

  private[sql] val ordering: Ordering[InternalType] = UInt64IsIntegral

  override def defaultSize: Int = 8

  private[spark] override def asNullable: UInt64Type = this

  override def catalogString: String = "uint64"

  override def simpleString: String = "uint64"

  private[sql] val numeric = UInt64IsIntegral

  private[sql] val integral = UInt64IsIntegral
}

/**
 * The only purpose of this trait is to make UInt64Type binary compatible with older Spark versions less than 3.5.0
 */
trait NumericCompat {
  private[sql] val numeric: Numeric[_]
}

object UInt64IsIntegral extends LongIsIntegral with Ordering[Long] {
  override def compare(x: Long, y: Long): Int = java.lang.Long.compareUnsigned(x, y)
}

case object UInt64Type extends UInt64Type

object UInt64Support {

  val toStringUdf: UserDefinedFunction = udf((number: UInt64Long) => number match {
    case null => null
    case _ => number.toString
  })

  val fromStringUdf: UserDefinedFunction = udf((number: String) => number match {
    case null => null
    case _ => UInt64Long(number)
  })

  def createSerializer(inputObject: Expression): Expression = {
    Invoke(inputObject, "toLong", UInt64Type)
  }

  def createDeserializer(path: Expression): Expression = {
    StaticInvoke(
      UInt64Long.getClass,
      ObjectType(classOf[UInt64Long]),
      "apply",
      path :: Nil,
      returnNullable = false)
  }

  def cast(from: DataType): Any => Any = from match {
    case StringType => s => UInt64Long.fromString(s.asInstanceOf[UTF8String].toString)
    case BooleanType => b => if (b.asInstanceOf[Boolean]) 1L else 0L
    case x: NumericType => SparkAdapter.instance.castToLong(x)
  }
}

object UInt64CastToString extends (Any => Any) {
  def apply(t: Any): Any = UTF8String.fromString(UInt64Long.toString(t.asInstanceOf[Long]))
}

object UInt64CastToStringCode extends ((ExprValue, ExprValue, ExprValue) => Block) {
  def apply(c: ExprValue, evPrim: ExprValue, evNull: ExprValue): Block =
    code"$evPrim = UTF8String.fromString(tech.ytsaurus.spyt.types.UInt64Long$$.MODULE$$.toString($c));"
}
