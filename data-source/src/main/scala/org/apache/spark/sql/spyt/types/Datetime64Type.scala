package org.apache.spark.sql.spyt.types

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.spyt.types.Datetime64.{MAX_DATETIME64, MIN_DATETIME64}
import org.apache.spark.sql.types.{DataType, LongType, SQLUserDefinedType, UserDefinedType}
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import tech.ytsaurus.spyt.common.utils.DateTimeTypesConverter.{localDateTimeToSeconds, longToDatetime, toLocalDatetime}

import java.time.LocalDateTime


class Datetime64Type extends UserDefinedType[Datetime64] {
  override def pyUDT: String = "spyt.types.Datetime64Type"

  override def sqlType: DataType = LongType

  override def serialize(d: Datetime64): Any = {
    d.seconds
  }

  override def deserialize(datum: Any): Datetime64 = {
    datum match {
      case v: java.lang.Long =>
        if (isValid(v)) {
          new Datetime64(v)
        } else {
          throw new IllegalArgumentException(s"Number $v is out of range [$MIN_DATETIME64 to $MAX_DATETIME64]")
        }
      case _ => throw new AnalysisException(
        "Datetime64 deserialization error: Expected java.lang.Long but got datum of type "
          + datum.getClass
      )
    }
  }

  private def isValid(value: Long): Boolean = {
    MIN_DATETIME64 <= value && value <= MAX_DATETIME64
  }

  override def userClass: Class[Datetime64] = classOf[Datetime64]

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("pyClass" -> pyUDT) ~
      ("serializedClass" -> serializedPyClass) ~
      ("sqlType" -> sqlType.jsonValue)
  }

  override def catalogString: String = "datetime64"
}

@SQLUserDefinedType(udt = classOf[Datetime64Type])
case class Datetime64(seconds: Long){
  def toLong: Long = seconds

  override def toString: String = longToDatetime(seconds)

  def toDatetime: LocalDateTime = toLocalDatetime(toString)
}

object Datetime64 {
  val MIN_DATETIME64: Long = -4611669897600L
  val MAX_DATETIME64: Long = 4611669811199L
  val MIN_DATETIME64_STR: String = "-144168-01-01T00:00:00Z"
  val MAX_DATETIME64_STR: String = "+148107-12-31T23:59:59Z"

  def apply(localDatetime: LocalDateTime): Datetime64 = new Datetime64(localDateTimeToSeconds(localDatetime))
}
