package tech.ytsaurus.spyt.format

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.cypress.{Range, RangeLimit, YPath}
import tech.ytsaurus.ysontree.{YTree, YTreeNode}

import java.util

class YPathUtilsTest extends AnyFlatSpec with Matchers {

  "YPathUtils.rowCount" should "return None for YPath without ranges" in {
    val ypath = YPath.simple("//some/table")

    YPathUtils.rowCount(ypath) shouldBe None
  }

  it should "return None when range is defined by keys instead of indices" in {
    val key = new util.ArrayList[YTreeNode]()
    key.add(YTree.builder.value("key").build())

    val range = new Range(
      RangeLimit.builder().setKey(key).build(),
      RangeLimit.builder().setKey(key).build()
    )

    val ypath = YPath.simple("//some/table").plusRange(range)

    YPathUtils.rowCount(ypath) shouldBe None
  }

  it should "calculate row count for single range with indices" in {
    val lower = RangeLimit.builder().setRowIndex(5L).build()
    val upper = RangeLimit.builder().setRowIndex(15L).build()
    val range = new Range(lower, upper)

    val ypath = YPath.simple("//some/table").plusRange(range)

    YPathUtils.rowCount(ypath) shouldBe Some(10L)
  }

  it should "calculate row count for multiple ranges with indices" in {
    val range1 = new Range(
      RangeLimit.builder().setRowIndex(5L).build(),
      RangeLimit.builder().setRowIndex(15L).build()
    )

    val range2 = new Range(
      RangeLimit.builder().setRowIndex(20L).build(),
      RangeLimit.builder().setRowIndex(30L).build()
    )

    val ypath = YPath.simple("//some/table")
      .plusRange(range1)
      .plusRange(range2)

    YPathUtils.rowCount(ypath) shouldBe Some(20L) // (15-5) + (30-20) = 10 + 10 = 20
  }

  it should "return None when one of ranges is defined by keys" in {
    val range1 = new Range(
      RangeLimit.builder().setRowIndex(5L).build(),
      RangeLimit.builder().setRowIndex(15L).build()
    )

    val key = new util.ArrayList[YTreeNode]()
    key.add(YTree.builder.value("key").build())

    val range2 = new Range(
      RangeLimit.builder().setKey(key).build(),
      RangeLimit.builder().setKey(key).build()
    )

    val ypath = YPath.simple("//some/table")
      .plusRange(range1)
      .plusRange(range2)

    YPathUtils.rowCount(ypath) shouldBe None
  }
}