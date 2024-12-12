package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext")
class CodegenContextDecorators {

  @DecoratedMethod
  def genComp(dataType: DataType, c1: String, c2: String): String = dataType match {
    case ts.uInt64DataType => s"java.lang.Long.compareUnsigned($c1, $c2)"
    case _ => __genComp(dataType, c1, c2)
  }

  def __genComp(dataType: DataType, c1: String, c2: String): String = ???

}
