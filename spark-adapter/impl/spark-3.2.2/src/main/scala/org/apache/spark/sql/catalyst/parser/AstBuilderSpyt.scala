package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.adapter.TypeSupport
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.parser.AstBuilder")
class AstBuilderSpyt {

  @DecoratedMethod
  def visitPrimitiveDataType(ctx: SqlBaseParser.PrimitiveDataTypeContext): DataType = {
    val uint64Opt = TypeSupport.instance.extractUint64Opt(ctx)

    if (uint64Opt.isDefined) {
      uint64Opt.get
    } else {
      __visitPrimitiveDataType(ctx)
    }
  }

  def __visitPrimitiveDataType(ctx: SqlBaseParser.PrimitiveDataTypeContext): DataType = ???
}
