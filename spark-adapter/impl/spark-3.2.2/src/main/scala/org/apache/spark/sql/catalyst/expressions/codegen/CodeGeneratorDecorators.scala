package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator.JAVA_LONG
import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator$")
object CodeGeneratorDecorators {

  @DecoratedMethod
  def javaType(dt: DataType): String = dt match {
    case ts.uInt64DataType => JAVA_LONG
    case _ => __javaType(dt)
  }

  def __javaType(dt: DataType): String = ???

  @DecoratedMethod
  def javaClass(dt: DataType): Class[_] = dt match {
    case ts.uInt64DataType => java.lang.Long.TYPE
    case _ => __javaClass(dt)
  }

  def __javaClass(dt: DataType): Class[_] = ???
}
