package org.apache.spark.sql.catalyst.encoders

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, ObjectType}
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}
import tech.ytsaurus.spyt.types.UInt64Long

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.encoders.RowEncoder$")
@Applicability(to = "3.3.4")
object RowEncoderDecorators322 {

  @DecoratedMethod
  def externalDataTypeFor(dt: DataType): DataType = dt match {
    case ts.uInt64DataType => ObjectType(classOf[UInt64Long])
    case _ => __externalDataTypeFor(dt)
  }

  def __externalDataTypeFor(dt: DataType): DataType = ???

  @DecoratedMethod
  @Applicability(to = "3.2.4")
  private def serializerFor(inputObject: Expression, inputType: DataType): Expression = inputType match {
    case ts.uInt64DataType => ts.uInt64Serializer(inputObject)
    case _ => __serializerFor(inputObject, inputType)
  }
  private def __serializerFor(inputObject: Expression, inputType: DataType): Expression = ???

  @DecoratedMethod
  @Applicability(from = "3.3.0")
  private def serializerFor(inputObject: Expression, inputType: DataType, lenient: Boolean): Expression =
    inputType match {
      case ts.uInt64DataType => ts.uInt64Serializer(inputObject)
      case _ => __serializerFor(inputObject, inputType, lenient)
    }

  private def __serializerFor(inputObject: Expression, inputType: DataType, lenient: Boolean): Expression = ???

  @DecoratedMethod
  private def deserializerFor(input: Expression, dataType: DataType): Expression = dataType match {
    case ts.uInt64DataType => ts.uInt64Deserializer(input)
    case _ => __deserializerFor(input, dataType)
  }
  private def __deserializerFor(input: Expression, dataType: DataType): Expression = ???
}
