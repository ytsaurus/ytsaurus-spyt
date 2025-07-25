package tech.ytsaurus.spyt.common.utils

import java.math.{BigDecimal, BigInteger}

object ULongUtils {
  private val UNSIGNED_LONG_MASK = BigInteger.ONE.shiftLeft(java.lang.Long.SIZE).subtract(BigInteger.ONE)

  def toBigDecimal(long: Long): BigDecimal = new BigDecimal(BigInteger.valueOf(long).and(UNSIGNED_LONG_MASK))

  def tryToUnsignedLong(bigDecimal: BigDecimal): Option[Long] =
    if (bigDecimal.scale() == 0 && bigDecimal.signum() != -1) {
      val bigInteger = bigDecimal.toBigInteger
      if (bigInteger.bitLength() <= java.lang.Long.SIZE) Some(bigInteger.longValue()) else None
    } else {
      None
    }
}
