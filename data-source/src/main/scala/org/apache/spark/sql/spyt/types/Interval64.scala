package org.apache.spark.sql.spyt.types

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.spyt.types.Interval64.{MAX_INTERVAL64, MIN_INTERVAL64}
import org.apache.spark.sql.spyt.types.Timestamp64.{MAX_TIMESTAMP64, MIN_TIMESTAMP64}
import org.apache.spark.sql.types.{DataType, LongType, SQLUserDefinedType, UserDefinedType}
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._


class Interval64Type extends UserDefinedType[Interval64] {
  override def pyUDT: String = "spyt.types.Interval64Type"

  override def sqlType: DataType = LongType

  override def serialize(d: Interval64): Any = {
    d.interval64
  }

  override def deserialize(datum: Any): Interval64 = {
    datum match {
      case v: java.lang.Long =>
        if (isValid(v)) {
          new Interval64(v)
        } else {
          throw new IllegalArgumentException(s"Number $v is out of range ($MIN_INTERVAL64 to $MAX_INTERVAL64)")
        }
      case _ => throw new AnalysisException(
        "Interval64 deserialization error: Expected java.lang.Long but got datum of type "
          + datum.getClass
      )
    }
  }

  private def isValid(value: Long): Boolean = {
    MIN_INTERVAL64 <= value && value <= MAX_INTERVAL64
  }

  override def userClass: Class[Interval64] = classOf[Interval64]

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("pyClass" -> pyUDT) ~
      ("serializedClass" -> serializedPyClass) ~
      ("sqlType" -> sqlType.jsonValue)
  }

  override def catalogString: String = "interval64"
}

@SQLUserDefinedType(udt = classOf[Interval64Type])
case class Interval64(interval64: Long) {
  def toLong: Long = interval64

  override def toString: String = interval64.toString
}

object Interval64 {
  val MAX_INTERVAL64: Long = MAX_TIMESTAMP64 - MIN_TIMESTAMP64
  val MIN_INTERVAL64: Long = - MAX_INTERVAL64
}
