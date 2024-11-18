package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.{BinaryType, DataType}
import tech.ytsaurus.spyt.adapter.TypeSupport
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.Cast$")
object CastDecorators {

  @DecoratedMethod
  def canCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (TypeSupport.instance.ysonDataType, BinaryType) => true
    case (BinaryType, TypeSupport.instance.ysonDataType) => true
    case _ => __canCast(from, to)
  }

  def __canCast(from: DataType, to: DataType): Boolean = ???
}
