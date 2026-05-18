package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.parser.DataTypeAstBuilderDecorators.extractUint64Opt
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.PrimitiveDataTypeContext
import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.adapter.TypeSupport
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.parser.DataTypeAstBuilder")
@Applicability(from = "3.5.0")
class DataTypeAstBuilderDecorators {

  @DecoratedMethod
  def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = {
    val uint64Opt = extractUint64Opt(ctx)
    if (uint64Opt.isDefined) {
      uint64Opt.get
    } else {
      __visitPrimitiveDataType(ctx)
    }
  }


  def __visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = ???

}

object DataTypeAstBuilderDecorators {
  def extractUint64Opt(ctx: PrimitiveDataTypeContext): Option[DataType] =
    ParserUtils.withOrigin(ctx) {
      // We should dynamically load IDENTIFIER value here, otherwise it will be embedded on the compilation stage
      // with the value from Spark 3.5.x
      val identifierId = Class.forName("org.apache.spark.sql.catalyst.parser.SqlBaseParser")
        .getDeclaredField("IDENTIFIER")
        .getInt(null)
      if (SparkAdapter.instance.isUint64DataTypeContext(ctx, identifierId)) {
        Some(TypeSupport.instance.uInt64DataType)
      } else {
        None
      }
    }
}
