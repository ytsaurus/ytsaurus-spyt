package tech.ytsaurus.spyt.types

case class UInt64Long(value: Long) {
  def toLong: Long = value

  override def toString: String = UInt64Long.toString(value)

  override def hashCode(): Int = value.toInt
}

object UInt64Long {

  def apply(number: String): UInt64Long = {
    UInt64Long(fromString(number))
  }

  def fromString(number: String): Long = java.lang.Long.parseUnsignedLong(number)

  def toString(value: Long): String = java.lang.Long.toUnsignedString(value)
}
