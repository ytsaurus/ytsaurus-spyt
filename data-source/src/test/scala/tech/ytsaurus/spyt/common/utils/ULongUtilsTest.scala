package tech.ytsaurus.spyt.common.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.funsuite.AnyFunSuiteLike

class ULongUtilsTest extends AnyFunSuiteLike with Matchers {
  test("testTryToUnsignedLong") {
    ULongUtils.tryToUnsignedLong(ULongUtils.toBigDecimal(Long.MaxValue)) shouldBe Some(Long.MaxValue)
    ULongUtils.tryToUnsignedLong(ULongUtils.toBigDecimal(Long.MinValue)) shouldBe Some(Long.MinValue)
    ULongUtils.tryToUnsignedLong(ULongUtils.toBigDecimal(Long.MaxValue).add(java.math.BigDecimal.ONE)) shouldBe Some(Long.MinValue)
    ULongUtils.tryToUnsignedLong(ULongUtils.toBigDecimal(-1)) shouldBe Some(-1)
    ULongUtils.tryToUnsignedLong(ULongUtils.toBigDecimal(-1).add(java.math.BigDecimal.ONE)) shouldBe None
  }
}
