package org.apache.spark.sql.spyt.types

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.spyt.types.Timestamp64.{MAX_TIMESTAMP64, MIN_TIMESTAMP64}
import org.apache.spark.sql.types.{DataType, LongType, SQLUserDefinedType, UserDefinedType}
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import tech.ytsaurus.spyt.common.utils.DateTimeTypesConverter.{convertUTCtoLocal, localDateTimeToMicros, longToZonedTimestamp, timestampToLong}

import java.sql.Timestamp
import java.time.LocalDateTime


class Timestamp64Type extends UserDefinedType[Timestamp64] {
  override def pyUDT: String = "spyt.types.Timestamp64Type"

  override def sqlType: DataType = LongType

  override def serialize(d: Timestamp64): Any = {
    d.microseconds
  }

  override def deserialize(datum: Any): Timestamp64 = {
    datum match {
      case v: java.lang.Long =>
        if (isValid(v)) {
          new Timestamp64(v)
        } else {
          throw new IllegalArgumentException(s"Number $v is out of range [$MIN_TIMESTAMP64 to $MAX_TIMESTAMP64]")
        }
      case _ => throw new AnalysisException(
        "Timestamp64 deserialization error: Expected java.lang.Long but got datum of type "
          + datum.getClass
      )
    }
  }

  private def isValid(value: Long): Boolean = {
    MIN_TIMESTAMP64 <= value && value <= MAX_TIMESTAMP64
  }

  override def userClass: Class[Timestamp64] = classOf[Timestamp64]

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("pyClass" -> pyUDT) ~
      ("serializedClass" -> serializedPyClass) ~
      ("sqlType" -> sqlType.jsonValue)
  }

  override def catalogString: String = "timestamp64"
}

@SQLUserDefinedType(udt = classOf[Timestamp64Type])
case class Timestamp64(microseconds: Long) {
  def toLong: Long = microseconds

  override def toString: String = longToZonedTimestamp(microseconds)

  def toTimestamp: Timestamp = convertUTCtoLocal(toString, 0)
}

object Timestamp64 {
  val MIN_TIMESTAMP64: Long = -4611669897600000000L
  val MAX_TIMESTAMP64: Long = 4611669811199999999L
  val MIN_TIMESTAMP64_STR: String = "-144168-01-01T00:00:00.000000Z"
  val MAX_TIMESTAMP64_STR: String = "+148107-12-31T23:59:59.999999Z"

  @deprecated(
    message = "Use apply(localDatetime: LocalDateTime) instead",
    since = "2.6.0"
  )
  def apply(timestamp: Timestamp): Timestamp64 = Timestamp64(timestampToLong(timestamp))

  def apply(localDatetime: LocalDateTime): Timestamp64 = Timestamp64(localDateTimeToMicros(localDatetime))

}
