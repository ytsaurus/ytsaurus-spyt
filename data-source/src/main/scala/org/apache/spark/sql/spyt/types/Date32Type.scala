package org.apache.spark.sql.spyt.types

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.spyt.types.Date32.{MAX_DATE32, MIN_DATE32}
import org.apache.spark.sql.types.{DataType, IntegerType, SQLUserDefinedType, UserDefinedType}
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import tech.ytsaurus.spyt.common.utils.DateTimeTypesConverter.dateToLong

import java.time.LocalDate


class Date32Type extends UserDefinedType[Date32] {
  override def pyUDT: String = "spyt.types.Date32Type"

  override def sqlType: DataType = IntegerType

  override def serialize(d: Date32): Any = d.days

  override def deserialize(datum: Any): Date32 = {
    datum match {
      case v: java.lang.Integer =>
        if (isValid(v)) {
          new Date32(v)
        } else {
          throw new IllegalArgumentException(s"Number $v is out of range [$MIN_DATE32 to $MAX_DATE32]")
        }
      case _ => throw new AnalysisException(
        "Date32 deserialization error: Expected java.lang.Integer but got datum of type "
          + datum.getClass
      )
    }
  }

  private def isValid(value: Int): Boolean = {
    MIN_DATE32 <= value && value <= MAX_DATE32
  }

  override def userClass: Class[Date32] = classOf[Date32]

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("pyClass" -> pyUDT) ~
      ("serializedClass" -> serializedPyClass) ~
      ("sqlType" -> sqlType.jsonValue)
  }

  override def catalogString: String = "date32"
}

@SQLUserDefinedType(udt = classOf[Date32Type])
case class Date32(days: Int) {
  def toInt: Int = days

  override def toString: String = toDate.toString


  def toDate: LocalDate = LocalDate.ofEpochDay(days)
}

object Date32 {
  val MIN_DATE32: Int = -53375809
  val MAX_DATE32: Int = 53375807
  val MIN_DATE32_STR: String = "-144168-01-01"
  val MAX_DATE32_STR: String = "+148107-12-31"

  def apply(localDate: LocalDate): Date32 = new Date32(dateToLong(localDate.toString).toInt)
}
