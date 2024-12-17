package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.ScalaReflection.universe.Type
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, UInt64Encoder}
import org.apache.spark.sql.catalyst.expressions.Expression
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}
import tech.ytsaurus.spyt.types.UInt64Long

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.ScalaReflection$")
@Applicability(from = "3.4.0")
object ScalaReflectionDecorators340 {

  @DecoratedMethod
  @Applicability(to = "3.4.4")
  private def serializerFor(enc: AgnosticEncoder[_], input: Expression): Expression = enc match {
    case UInt64Encoder => ts.uInt64Serializer(input)
    case _ => __serializerFor(enc, input)
  }

  private def __serializerFor(enc: AgnosticEncoder[_], input: Expression): Expression = ???

  @DecoratedMethod
  @Applicability(to = "3.4.4")
  private def deserializerFor(enc: AgnosticEncoder[_],
                              path: Expression,
                              walkedTypePath: WalkedTypePath): Expression = enc match {
    case UInt64Encoder => ts.uInt64Deserializer(path)
    case _ => __deserializerFor(enc, path, walkedTypePath)
  }

  private def __deserializerFor(enc: AgnosticEncoder[_],
                                path: Expression,
                                walkedTypePath: WalkedTypePath): Expression = ???

  @DecoratedMethod
  @Applicability(to = "3.4.4")
  private def encoderFor(tpe: `Type`, seenTypeSet: Set[`Type`], path: WalkedTypePath): AgnosticEncoder[_] =
    baseType(tpe) match {
      case t if isSubtype(t, ScalaReflection.localTypeOf[UInt64Long]) => UInt64Encoder
      case _ =>  __encoderFor(tpe, seenTypeSet, path)
    }

  private def __encoderFor(tpe: `Type`, seenTypeSet: Set[`Type`], path: WalkedTypePath): AgnosticEncoder[_] = ???

  @DecoratedMethod
  @Applicability(from = "3.5.0")
  private def encoderFor(tpe: `Type`,
                         seenTypeSet: Set[`Type`],
                         path: WalkedTypePath,
                         isRowEncoderSupported: Boolean): AgnosticEncoder[_] = {
    baseType(tpe) match {
      case t if isSubtype(t, ScalaReflection.localTypeOf[UInt64Long]) => UInt64Encoder
      case _ =>  __encoderFor(tpe, seenTypeSet, path, isRowEncoderSupported)
    }
  }

  private def __encoderFor(tpe: `Type`,
                           seenTypeSet: Set[`Type`],
                           path: WalkedTypePath,
                           isRowEncoderSupported: Boolean): AgnosticEncoder[_] = ???

  private def baseType(tpe: `Type`): `Type` = ???
  private[catalyst] def isSubtype(tpe1: `Type`, tpe2: `Type`): Boolean = ???
}
