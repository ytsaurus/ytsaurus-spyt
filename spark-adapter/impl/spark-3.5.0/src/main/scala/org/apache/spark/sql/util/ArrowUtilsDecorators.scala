package org.apache.spark.sql.util

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.util.ArrowUtils$")
@Applicability(from = "3.5.0")
private[sql] object ArrowUtilsDecorators {

  @DecoratedMethod
  def toArrowType(dt: DataType, timeZoneId: String, largeVarTypes: Boolean): ArrowType = dt match {
    case ts.uInt64DataType => new ArrowType.Int(8 * 8, false)
    case ts.ysonDataType if !largeVarTypes => ArrowType.Binary.INSTANCE
    case ts.ysonDataType if largeVarTypes => ArrowType.LargeBinary.INSTANCE
    case _ => __toArrowType(dt, timeZoneId, largeVarTypes)
  }

  def __toArrowType(dt: DataType, timeZoneId: String, largeVarTypes: Boolean): ArrowType = ???

  @DecoratedMethod
  def fromArrowType(dt: ArrowType): DataType = dt match {
    case int: ArrowType.Int if !int.getIsSigned && int.getBitWidth == 8 * 8 => ts.uInt64DataType
    case _ => __fromArrowType(dt)
  }

  def __fromArrowType(dt: ArrowType): DataType = ???
}
