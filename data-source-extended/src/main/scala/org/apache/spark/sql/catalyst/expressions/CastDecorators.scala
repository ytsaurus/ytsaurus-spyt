package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.spyt.types.{DatetimeType, YsonType}
import org.apache.spark.sql.types.{BinaryType, DataType, LongType, TimestampType}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.Cast")
object CastDecorators {

  @DecoratedMethod
  def canCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (YsonType, BinaryType) => true
    case (BinaryType, YsonType) => true
    case (_: DatetimeType, TimestampType) => true
    case _ => __canCast(from, to)
  }

  def __canCast(from: DataType, to: DataType): Boolean = ???

  @DecoratedMethod
  def canUpCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (_: DatetimeType, TimestampType) => true
    case _ => __canUpCast(from, to)
  }

  def __canUpCast(from: DataType, to: DataType): Boolean = ???
}
