package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, UInt64Encoder}
import org.apache.spark.sql.catalyst.expressions.Expression
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.SerializerBuildHelper$")
@Applicability(from = "3.5.0")
object SerializerBuildHelperDecorators {

  @DecoratedMethod
  private def createSerializer(enc: AgnosticEncoder[_], input: Expression): Expression = enc match {
    case UInt64Encoder => ts.uInt64Serializer(input)
    case _ => __createSerializer(enc, input)
  }

  private def __createSerializer(enc: AgnosticEncoder[_], input: Expression): Expression = ???
}
