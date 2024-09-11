package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.parser.AstBuilderSpyt.extractUint64Opt
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.spyt.types.UInt64Type
import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

import java.util.Locale

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.parser.AstBuilder")
class AstBuilderSpyt {

  @DecoratedMethod
  def visitPrimitiveDataType(ctx: SqlBaseParser.PrimitiveDataTypeContext): DataType = {
    val uint64Opt = extractUint64Opt(ctx)

    if (uint64Opt.isDefined) {
      uint64Opt.get
    } else {
      __visitPrimitiveDataType(ctx)
    }
  }

  def __visitPrimitiveDataType(ctx: SqlBaseParser.PrimitiveDataTypeContext): DataType = ???
}

object AstBuilderSpyt {
  def extractUint64Opt(ctx: SqlBaseParser.PrimitiveDataTypeContext): Option[UInt64Type] =
    SparkAdapter.instance.parserUtilsWithOrigin(ctx) {
      val dataType = ctx.identifier.getText.toLowerCase(Locale.ROOT)
      dataType match {
        case "uint64" => Some(UInt64Type)
        case _ => None
      }
  }
}
