package org.apache.spark.sql.catalyst.encoders

import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.encoders.RowEncoder$")
@Applicability(from = "3.4.0")
object RowEncoderDecorators340 {

  @DecoratedMethod
  private[catalyst] def encoderForDataType(dataType: DataType, lenient: Boolean): AgnosticEncoder[_] = dataType match {
    case ts.uInt64DataType => UInt64Encoder
    case _ => __encoderForDataType(dataType, lenient)
  }

  private[catalyst] def __encoderForDataType(dataType: DataType, lenient: Boolean): AgnosticEncoder[_] = ???
}
