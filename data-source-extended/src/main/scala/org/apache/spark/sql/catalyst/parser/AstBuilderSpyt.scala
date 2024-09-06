package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.parser.ParserUtils.{operationNotAllowed, withOrigin}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.TablePropertyListContext
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.spyt.types.UInt64Type
import tech.ytsaurus.spyt.patch.annotations.{OriginClass, Subclass}

import java.util.Locale

@Subclass
@OriginClass("org.apache.spark.sql.catalyst.parser.AstBuilder")
class AstBuilderSpyt extends AstBuilder {

  // Hotfix: To prevent
  // java.lang.NoSuchMethodError: 'void org.apache.spark.sql.catalyst.parser.AstBuilder$$anonfun$2.<init>
  // (org.apache.spark.sql.catalyst.parser.AstBuilderBase)'
  // In test YtSparkSQLTest::create table with custom attributes
  override def visitPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = {
    val props = visitTablePropertyList(ctx)
    for ((k, v) <- props) {
      if (v == null) operationNotAllowed(s"Values must be specified for key: $k", ctx)
    }
    props
  }

  override def visitPrimitiveDataType(ctx: SqlBaseParser.PrimitiveDataTypeContext): DataType = {

    val uint64Opt = withOrigin(ctx) {
      val dataType = ctx.identifier.getText.toLowerCase(Locale.ROOT)
      dataType match {
        case "uint64" => Some(UInt64Type)
        case _ => None
      }
    }

    if (uint64Opt.isDefined) {
      uint64Opt.get
    } else {
      super.visitPrimitiveDataType(ctx)
    }
  }

}
