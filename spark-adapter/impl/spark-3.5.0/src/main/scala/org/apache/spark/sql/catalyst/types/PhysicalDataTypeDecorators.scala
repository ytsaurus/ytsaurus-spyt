package org.apache.spark.sql.catalyst.types

import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.types.PhysicalDataType$")
@Applicability(from = "3.5.0")
object PhysicalDataTypeDecorators {

  @DecoratedMethod
  def apply(dt: DataType): PhysicalDataType = dt match {
    case ts.uInt64DataType => PhysicalLongType
    case _ => __apply(dt)
  }

  def __apply(dt: DataType): PhysicalDataType = ???

}
