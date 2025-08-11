package tech.ytsaurus.spyt.types

import tech.ytsaurus.spyt.types.UInt64Long.J_DECIMAL_2_POW_64

case class UInt64Long(value: Long) extends Ordered[UInt64Long] {
  def toLong: Long = value

  def toBigDecimal: java.math.BigDecimal = {
    val bdValue = new java.math.BigDecimal(value)
    if (value >= 0) bdValue else J_DECIMAL_2_POW_64.add(bdValue)
  }

  override def toString: String = UInt64Long.toString(value)

  override def hashCode(): Int = value.toInt

  override def compare(that: UInt64Long): Int = java.lang.Long.compareUnsigned(value, that.value)
}

object UInt64Long {

  def apply(number: String): UInt64Long = {
    UInt64Long(fromString(number))
  }

  private val J_DECIMAL_2_POW_64 = new java.math.BigDecimal(2).pow(64)

  def apply(decimal: java.math.BigDecimal): UInt64Long = {
    if (decimal.compareTo(java.math.BigDecimal.ZERO) == -1 ||
      decimal.compareTo(J_DECIMAL_2_POW_64) >= 0 ||
      decimal.scale() > 0) {
      throw new IllegalArgumentException("Decimal must be a positive integer that is less than 2^64")
    }
    UInt64Long(decimal.longValue())
  }

  def fromString(number: String): Long = java.lang.Long.parseUnsignedLong(number)

  def toString(value: Long): String = java.lang.Long.toUnsignedString(value)
}
