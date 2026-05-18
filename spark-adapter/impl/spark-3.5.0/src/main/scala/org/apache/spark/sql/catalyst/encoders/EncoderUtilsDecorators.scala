package org.apache.spark.sql.catalyst.encoders

import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}
import tech.ytsaurus.spyt.types.UInt64Long

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.encoders.EncoderUtils$")
@Applicability(from = "3.5.0")
object EncoderUtilsDecorators {

  @DecoratedMethod
  def dataTypeJavaClass(dt: DataType): Class[_] = dt match {
    case ts.uInt64DataType => classOf[UInt64Long]
    case _ => __dataTypeJavaClass(dt)
  }

  def __dataTypeJavaClass(dt: DataType): Class[_] = ???
}
