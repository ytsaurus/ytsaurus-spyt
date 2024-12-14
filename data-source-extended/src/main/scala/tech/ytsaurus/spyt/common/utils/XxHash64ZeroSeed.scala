package tech.ytsaurus.spyt.common.utils

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExpressionInfo, HashExpression, XXH64, XxHash64Function}
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.spyt.types.UInt64Type
import org.apache.spark.sql.{Column, SparkSession}
import tech.ytsaurus.spyt.SparkAdapter

case class XxHash64ZeroSeed(children: Seq[Expression], seed: Long) extends HashExpression[Long] {
  def this(arguments: Seq[Expression]) = this(arguments, 0L)

  override def dataType: DataType = LongType

  override def prettyName: String = "xxhash64zeroseed"

  override protected def hasherClassName: String = classOf[XXH64].getName

  override protected def computeHash(value: Any, dataType: DataType, seed: Long): Long = {
    XxHash64Function.hash(value, dataType, seed)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): XxHash64ZeroSeed =
    copy(children = newChildren)
}

object XxHash64ZeroSeed {
  @scala.annotation.varargs
  def xxHash64ZeroSeedUdf(source: Column*): Column = {
    new Column(new XxHash64ZeroSeed(source.map(_.expr))).cast(UInt64Type)
  }

  def registerFunction(spark: SparkSession): Unit = {
    spark.sessionState.functionRegistry.registerFunction(
      new FunctionIdentifier("xxhash64zeroseed"),
      new ExpressionInfo("tech.ytsaurus.spyt.common.utils.XxHash64ZeroSeed", "xxhash64zeroseed"),
      (children: Seq[Expression]) =>
        SparkAdapter.instance.createCast(new XxHash64ZeroSeed(children), UInt64Type)
    )
  }
}
