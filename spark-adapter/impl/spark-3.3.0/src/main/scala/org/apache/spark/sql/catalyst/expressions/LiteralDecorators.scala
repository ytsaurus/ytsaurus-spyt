package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode}
import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}
import tech.ytsaurus.spyt.types.UInt64Long

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.Literal")
class LiteralDecorators {

  private val value: Any = ???
  private val dataType: DataType = ???

  @DecoratedMethod
  def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (dataType == ts.uInt64DataType && value != null) {
      ExprCode.forNonNullValue(JavaCode.literal(LiteralDecorators.uint64Literal(value), dataType))
    } else {
      __doGenCode(ctx, ev)
    }
  }

  def __doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???
}

object LiteralDecorators {
  def uint64Literal(value: Any) = s"${value}L"
}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.Literal$")
@Applicability(to = "3.4.0")
object LiteralObjectDecorators {

  @DecoratedMethod
  private[expressions] def validateLiteralValue(value: Any, dataType: DataType): Unit = {
    if (dataType == ts.uInt64DataType && value != null) {
      require(value.isInstanceOf[Long] || value.isInstanceOf[UInt64Long])
    } else {
      __validateLiteralValue(value, dataType)
    }
  }

  private[expressions] def __validateLiteralValue(value: Any, dataType: DataType): Unit = ???
}
