package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, UInt64Encoder}
import org.apache.spark.sql.catalyst.expressions.Expression
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.DeserializerBuildHelper$")
@Applicability(from = "4.0.2")
object DeserializerBuildHelperDecarators402 {

  @DecoratedMethod
  private def createDeserializer(
    enc: AgnosticEncoder[_],
    path: Expression,
    walkedTypePath: WalkedTypePath,
    isTopLevel: Boolean): Expression = enc match {
    case UInt64Encoder => ts.uInt64Deserializer(path)
    case _ => __createDeserializer(enc, path, walkedTypePath, isTopLevel)
  }

  private def __createDeserializer(
    enc: AgnosticEncoder[_],
    path: Expression,
    walkedTypePath: WalkedTypePath,
    isTopLevel: Boolean): Expression = ???
}
