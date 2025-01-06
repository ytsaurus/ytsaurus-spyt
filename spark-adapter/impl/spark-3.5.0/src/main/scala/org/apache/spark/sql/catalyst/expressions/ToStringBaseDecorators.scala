package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodegenContext, ExprValue}
import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.ToStringBase")
@Applicability(from = "3.5.0")
trait ToStringBaseDecorators {

  @DecoratedMethod
  protected final def castToString(from: DataType): Any => Any = from match {
    case ts.uInt64DataType => ts.uInt64CastToString
    case _ => __castToString(from)
  }

  protected final def __castToString(from: DataType): Any => Any = ???

  @DecoratedMethod
  protected final def castToStringCode(from: DataType, ctx: CodegenContext): (ExprValue, ExprValue) => Block = from match {
    case ts.uInt64DataType => ToStringBaseDecorators.castToStringCodeUInt64
    case _ => __castToStringCode(from, ctx)
  }

  protected final def __castToStringCode(from: DataType, ctx: CodegenContext): (ExprValue, ExprValue) => Block = ???

}

object ToStringBaseDecorators {
  val castToStringCodeUInt64: (ExprValue, ExprValue) => Block = ts.uInt64CastToStringCode.apply(_, _, null)
}
