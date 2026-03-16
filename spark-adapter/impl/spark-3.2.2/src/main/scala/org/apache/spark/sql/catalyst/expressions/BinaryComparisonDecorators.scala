package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.BinaryComparison")
class BinaryComparisonDecorators {

  @DecoratedMethod
  def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dataType = left.asInstanceOf[Expression].dataType
    if (dataType == ts.uInt64DataType) {
      defineCodeGen(ctx, ev, BinaryComparisonDecorators.generate(ctx, dataType, symbol))
    } else {
      __doGenCode(ctx, ev)
    }
  }

  def __doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???

  def left: TreeNode[_] = ???
  def symbol: String = ???
  protected def defineCodeGen(ctx: CodegenContext, ev: ExprCode, f: (String, String) => String): ExprCode = ???
}

object BinaryComparisonDecorators {
  def generate(ctx: CodegenContext, dt: DataType, symbol: String): (String, String) => String = {
    (c1, c2) => s"${ctx.genComp(dt, c1, c2)} $symbol 0"
  }
}
