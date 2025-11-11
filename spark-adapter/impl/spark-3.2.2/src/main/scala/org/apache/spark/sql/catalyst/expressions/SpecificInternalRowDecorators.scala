package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.SpecificInternalRow")
class SpecificInternalRowDecorators {

  @DecoratedMethod
  private[this] def dataTypeToMutableValue(dataType: DataType): MutableValue = dataType match {
    case ts.uInt64DataType => new MutableLong
    case dt if ts.isDateTimeDataType(dt) => new MutableLong
    case _ => __dataTypeToMutableValue(dataType)
  }

  private[this] def __dataTypeToMutableValue(dataType: DataType): MutableValue = ???
}
