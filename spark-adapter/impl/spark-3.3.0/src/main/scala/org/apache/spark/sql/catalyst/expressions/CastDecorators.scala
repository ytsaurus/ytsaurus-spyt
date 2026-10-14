package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.{BinaryType, DataType, DecimalType, TimestampType}
import tech.ytsaurus.spyt.adapter.TypeSupport
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.Cast$")
object CastDecorators {

  @DecoratedMethod
  def canCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (TypeSupport.instance.ysonDataType, BinaryType) => true
    case (BinaryType, TypeSupport.instance.ysonDataType) => true
    case (dt, TimestampType) if TypeSupport.instance.isDateTimeDataType(dt) => true
    case _ => __canCast(from, to)
  }

  def __canCast(from: DataType, to: DataType): Boolean = ???

  @DecoratedMethod
  def canUpCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (dt, TimestampType) if TypeSupport.instance.isDateTimeDataType(dt) => true
    case (TypeSupport.instance.uInt64DataType, _: DecimalType) => false
    case _ => __canUpCast(from, to)
  }

  def __canUpCast(from: DataType, to: DataType): Boolean = ???

  @DecoratedMethod
  def canNullSafeCastToDecimal(from: DataType, to: DecimalType): Boolean = from match {
    case TypeSupport.instance.uInt64DataType => to.isWiderThan(DecimalType.LongDecimal)
    case _ => __canNullSafeCastToDecimal(from, to)
  }

  def __canNullSafeCastToDecimal(from: DataType, to: DecimalType): Boolean = ???
}
