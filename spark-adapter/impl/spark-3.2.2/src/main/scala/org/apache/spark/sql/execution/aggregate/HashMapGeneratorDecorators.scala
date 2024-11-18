package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.aggregate.HashMapGenerator")
class HashMapGeneratorDecorators {

  @DecoratedMethod
  protected final def genComputeHash(ctx: CodegenContext,
                                     input: String,
                                     dataType: DataType,
                                     result: String): String = dataType match {
    case ts.uInt64DataType => s"long $result = $input;"
    case _ => __genComputeHash(ctx, input, dataType, result)
  }


  protected final def __genComputeHash(ctx: CodegenContext,
                                       input: String,
                                       dataType: DataType,
                                       result: String): String = ???

}
