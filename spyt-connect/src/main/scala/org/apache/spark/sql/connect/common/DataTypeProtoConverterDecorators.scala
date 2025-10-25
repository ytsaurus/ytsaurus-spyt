package org.apache.spark.sql.connect.common

import org.apache.spark.connect.proto
import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.connect.common.DataTypeProtoConverter$")
@Applicability(from = "3.5.0")
object DataTypeProtoConverterDecorators {

  @DecoratedMethod
  def toConnectProtoType(t: DataType): proto.DataType = t match {
    case ts.uInt64DataType => ProtoDataTypes.LongType
    case ts.ysonDataType => ProtoDataTypes.BinaryType
    case _ => __toConnectProtoType(t)
  }

  def __toConnectProtoType(t: DataType): proto.DataType = ???

}
