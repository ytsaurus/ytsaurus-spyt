package org.apache.spark.sql.spyt.types

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, ExprValue}
import org.apache.spark.sql.types.{DataType, LongType, SQLUserDefinedType, UserDefinedType}
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import tech.ytsaurus.spyt.common.utils.DateTimeTypesConverter.localDateTimeToSeconds

import java.time.{LocalDateTime, ZoneOffset}


class DatetimeType extends UserDefinedType[Datetime] {
  override def pyUDT: String = "spyt.types.DatetimeType"

  override def sqlType: DataType = LongType

  override def serialize(d: Datetime): Any = {
    localDateTimeToSeconds(d.datetime)
  }

  override def deserialize(datum: Any): Datetime = {
    datum match {
      case t: java.lang.Long => new Datetime(LocalDateTime.ofEpochSecond(t, 0, ZoneOffset.UTC))
      case _ => throw new AnalysisException(
        "Deserialization error: Expected java.lang.Long but got datum of type "
          + datum.getClass
      )
    }
  }

  override def userClass: Class[Datetime] = classOf[Datetime]

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("pyClass" -> pyUDT) ~
      ("serializedClass" -> serializedPyClass) ~
      ("sqlType" -> sqlType.jsonValue)
  }

  override def catalogString: String = "datetime"
}

@SQLUserDefinedType(udt = classOf[DatetimeType])
case class Datetime(datetime: LocalDateTime) extends Serializable {
  def toLong: Long = {
    val instant = datetime.toInstant(ZoneOffset.UTC)
    instant.getEpochSecond
  }

  override def toString: String = datetime.toString
}

object Datetime {
  def apply(value: Long): Datetime = {
    Datetime(LocalDateTime.ofEpochSecond(value, 0, ZoneOffset.UTC))
  }
}

object DatetimeCastToTimestamp extends (Any => Any) {
  override def apply(v: Any): Any = {
    v.asInstanceOf[Long] * 1000000
  }
}

object DatetimeCastToTimestampCode extends ((ExprValue, ExprValue, ExprValue) => Block) {
  def apply(c: ExprValue, evPrim: ExprValue, evNull: ExprValue): Block = code"""$evPrim = $c * 1000000;"""
}
