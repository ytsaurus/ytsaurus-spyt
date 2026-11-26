package org.apache.spark.sql.v2

import org.apache.spark.sql.sources.{And, EqualTo, GreaterThanOrEqual, In, LessThanOrEqual, Or}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.spyt.common.utils.{ExpressionTransformer, MInfinity, PInfinity, RealValue, Segment, SegmentSet}
import tech.ytsaurus.spyt.types.UInt64Long

import java.util.{Map => JMap}
import scala.util.Random

class YtBucketIntersectionTest extends AnyFlatSpec with Matchers {
  private val HASH_KEY = "hash"

  private def bucket(value: Long): Segment.Segment = Segment(RealValue(UInt64Long(value)))

  private def check(filters: SegmentSet, bucketValue: Long): Unit = {
    val normalized = SegmentSet.intercept(filters)
    val point = bucket(bucketValue)
    val expected = SegmentSet.intercept(normalized, SegmentSet(HASH_KEY, point))
    val actual = YtHashBucketing.intersectBucket(normalized, HASH_KEY, point)
    actual shouldEqual expected
    normalized shouldEqual SegmentSet.intercept(filters)
  }

  behavior of "bucket filter intersection"

  it should "keep nulls, boundary points and columns without min/max" in {
    val points = Seq(Segment(RealValue(null)), Segment(RealValue(-1L)), Segment(RealValue(1L)), Segment(RealValue(24L)))
    val filters = SegmentSet("date", points: _*)
    YtHashBucketing.clipToBounds(filters, Map.empty) shouldEqual filters
    val bounds = Map("date" -> (RealValue(1L), RealValue(24L)))
    YtHashBucketing.clipToBounds(filters, bounds) shouldEqual SegmentSet("date", points.head, points(2), points.last)
  }

  it should "reuse normalized segments for unaffected columns" in {
    val filters = SegmentSet.intercept(SegmentSet("item", Segment(RealValue(3L)), Segment(RealValue(5L))))
    val actual = YtHashBucketing.intersectBucket(filters, HASH_KEY, bucket(1))
    (actual.map.get("item") eq filters.map.get("item")) shouldBe true
  }

  it should "match full intersection for absent, empty, null, range, OR and mixed columns" in {
    val hashPoint = bucket(3)
    val data = Seq(
      SegmentSet(),
      SegmentSet(HASH_KEY),
      SegmentSet(HASH_KEY, hashPoint),
      SegmentSet(HASH_KEY, hashPoint, Segment(RealValue(3L)), Segment(RealValue(7L))),
      SegmentSet(HASH_KEY, Segment(MInfinity(), PInfinity())),
      SegmentSet(JMap.of(HASH_KEY, Seq(hashPoint), "empty", Seq.empty[Segment.Segment])),
      ExpressionTransformer.filtersToSegmentSet(Seq(
        Or(EqualTo("item", 3L), EqualTo("item", 7L)),
        In("dates", Array[Any](null, 1L, 2L, 2L)))),
      ExpressionTransformer.filtersToSegmentSet(Seq(
        And(GreaterThanOrEqual("item", 4L), LessThanOrEqual("item", 12L)),
        Or(EqualTo("flag", true), EqualTo("flag", false)))),
      ExpressionTransformer.filtersToSegmentSet(Seq(
        Or(EqualTo("text", "alpha"), EqualTo("text", "omega")),
        In("number", Array[Any](1.5, 3.0, 1.5))))
    )
    data.foreach(filters => Seq(0L, 3L, 255L).foreach(check(filters, _)))
  }

  it should "match full intersection over randomized overlapping and empty segment lists" in {
    val random = new Random(1203L)
    for (_ <- 0 until 500) {
      def ranges(): Seq[Segment.Segment] = {
        (0 until random.nextInt(12)).map { _ =>
          val start = random.nextInt(20).toLong
          val end = start + random.nextInt(8)
          Segment(RealValue(start), RealValue(end))
        }
      }
      val values = new java.util.HashMap[String, Seq[Segment.Segment]]()
      if (random.nextBoolean()) {
        values.put("item", ranges())
      }
      if (random.nextBoolean()) {
        values.put("other", ranges())
      }
      if (random.nextBoolean()) {
        values.put(HASH_KEY, (0 until random.nextInt(8)).map(_ => bucket(random.nextInt(20))))
      }
      check(SegmentSet(values), random.nextInt(20))
    }
  }
}
