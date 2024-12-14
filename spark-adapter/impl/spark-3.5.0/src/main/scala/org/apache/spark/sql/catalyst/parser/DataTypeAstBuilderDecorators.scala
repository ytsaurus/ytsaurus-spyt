package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.parser.DataTypeAstBuilderDecorators.extractUint64Opt
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.PrimitiveDataTypeContext
import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.adapter.TypeSupport
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

import java.util.Locale

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
  def extractUint64Opt(ctx: SqlBaseParser.PrimitiveDataTypeContext): Option[DataType] =
    SparkAdapter.instance.parserUtilsWithOrigin(ctx) {
      if (
        ctx.`type`.start.getType == SqlBaseParser.IDENTIFIER &&
        ctx.INTEGER_VALUE().isEmpty &&
        ctx.`type`.start.getText.toLowerCase(Locale.ROOT) == "uint64"
      ) {
        Some(TypeSupport.instance.uInt64DataType)
      } else {
        None
      }
    }
}
