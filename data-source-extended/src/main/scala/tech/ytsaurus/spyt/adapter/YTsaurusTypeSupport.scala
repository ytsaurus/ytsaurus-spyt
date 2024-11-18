package tech.ytsaurus.spyt.adapter

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.SqlBaseParser
import org.apache.spark.sql.spyt.types._
import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.adapter.TypeSupport.CastFunction

import java.util.Locale

class YTsaurusTypeSupport extends TypeSupport {

  override val uInt64DataType: DataType = UInt64Type
  override def uInt64Serializer(inputObject: Expression): Expression = UInt64Support.createSerializer(inputObject)
  override def uInt64Deserializer(path: Expression): Expression = UInt64Support.createDeserializer(path)

  override def uInt64Cast(from: DataType): Any => Any = UInt64Support.cast(from)
  override val uInt64CastToString: Any => Any = UInt64CastToString
  override val uInt64CastToStringCode: CastFunction = UInt64CastToStringCode

  override def extractUint64Opt(ctx: SqlBaseParser.PrimitiveDataTypeContext): Option[DataType] = {
    SparkAdapter.instance.parserUtilsWithOrigin(ctx) {
      val dataType = ctx.identifier.getText.toLowerCase(Locale.ROOT)
      dataType match {
        case "uint64" => Some(UInt64Type)
        case _ => None
      }
    }
  }

  override val ysonDataType: DataType = YsonType
  override def ysonCast(from: DataType): Any => Any = YsonBinary.cast(from)
  override val ysonCastToBinary: Any => Any = YsonCastToBinary
  override val ysonCastToBinaryCode: CastFunction = YsonCastToBinaryCode
  override val binaryCastToYsonCode: CastFunction = BinaryCastToYsonCode
}
