package tech.ytsaurus.spyt.common.utils

import org.apache.spark.sql.sources._
import org.mockito.scalatest.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import AbstractSegment.SegmentSide
import Segment._
import tech.ytsaurus.spyt.test.TestUtils
import tech.ytsaurus.spyt.utils.CollectionUtils
import tech.ytsaurus.spyt.utils.CollectionUtils.concatMaps

import java.util.{Map => JMap}


class SegmentSetTest extends AnyFlatSpec with Matchers
  with TestUtils with MockitoSugar with TableDrivenPropertyChecks {
  behavior of "SegmentSet"

  private val variableName = "a"
  private val segmentMInfTo5 = Segment(MInfinity(), RealValue(5))
  private val segment2To20 = Segment(RealValue(2), RealValue(20))
  private val segment10To30 = Segment(RealValue(10), RealValue(30))
  private val segment15ToPInf = Segment(RealValue(15), PInfinity())
  private val point7 = Segment(RealValue(7), RealValue(7))

  private val exampleSet1 = SegmentSet(JMap.of("a", Seq(segmentMInfTo5, segment15ToPInf), "b", Seq(segment2To20)))
  private val exampleSet2 = SegmentSet(JMap.of("a", Seq(segment2To20), "b", Seq(segment10To30), "c", Seq(segment10To30)))
  private val exampleSet3 = SegmentSet(JMap.of("b", Seq(point7, segment15ToPInf)))

  it should "create SegmentSet" in {
    val segment = segmentMInfTo5
    val res = SegmentSet(variableName, segment)

    res shouldBe SegmentSet(JMap.of(variableName, Seq(segment)))
  }

  it should "merge and union same segment sets" in {
    val segment1 = segmentMInfTo5
    val segment2 = segment2To20
    val segment3 = segment15ToPInf
    val set = SegmentSet(JMap.of("a", Seq(segment1, segment3), "b", Seq(segment2)))

    SegmentSet.union(set, set) shouldBe set
    SegmentSet.intercept(set, set) shouldBe set
  }

  it should "merge" in {
    val segment1 = segmentMInfTo5
    val segment2 = segment2To20
    val segment3 = segment15ToPInf

    val set1 = SegmentSet(variableName, segment1)
    val set2 = SegmentSet(variableName, segment2)
    val set3 = SegmentSet(variableName, segment3)

    List(set1, set2, set3).foreach {
      set => SegmentSet.intercept(set) shouldBe set
    }
    SegmentSet.intercept(set1, set2) shouldBe SegmentSet(variableName, Segment(RealValue(2), RealValue(5)))
    SegmentSet.intercept(set1, set3) shouldBe SegmentSet(variableName)
    SegmentSet.intercept(set2, set3) shouldBe SegmentSet(variableName, Segment(RealValue(15), RealValue(20)))
    SegmentSet.intercept(set1, set2, set3) shouldBe SegmentSet(variableName)
  }

  it should "union" in {
    val segment1 = segmentMInfTo5
    val segment2 = segment2To20
    val segment3 = segment15ToPInf

    val set1 = SegmentSet(variableName, segment1)
    val set2 = SegmentSet(variableName, segment2)
    val set3 = SegmentSet(variableName, segment3)

    List(set1, set2, set3).foreach {
      set => SegmentSet.union(set) shouldBe set
    }
    SegmentSet.union(set1, set2) shouldBe SegmentSet(variableName, Segment(MInfinity(), RealValue(20)))
    SegmentSet.union(set1, set3) shouldBe SegmentSet(variableName, Segment(MInfinity(), RealValue(5)), Segment(RealValue(15), PInfinity()))
    SegmentSet.union(set2, set3) shouldBe SegmentSet(variableName, Segment(RealValue(2), PInfinity()))
    SegmentSet.union(set1, set2, set3) shouldBe SegmentSet(variableName, Segment(MInfinity(), PInfinity()))
  }

  it should "merge different variables" in {
    val map1 = JMap.of("a", Seq(segmentMInfTo5))
    val map2 = JMap.of("b", Seq(segment2To20))
    val map3 = JMap.of("c", Seq(segment15ToPInf))

    val set1 = SegmentSet(map1)
    val set2 = SegmentSet(map2)
    val set3 = SegmentSet(map3)

    SegmentSet.intercept(set1, set2) shouldBe SegmentSet(concatMaps(map1, map2))
    SegmentSet.intercept(set1, set3) shouldBe SegmentSet(concatMaps(map1, map3))
    SegmentSet.intercept(set2, set3) shouldBe SegmentSet(concatMaps(map2, map3))
    SegmentSet.intercept(set1, set2, set3) shouldBe SegmentSet(concatMaps(concatMaps(map1, map2), map3))
  }

  it should "merge hard cases" in {
    SegmentSet.intercept(exampleSet1, exampleSet2) shouldBe SegmentSet(JMap.of(
      "a", Seq(Segment(RealValue(2), RealValue(5)), Segment(RealValue(15), RealValue(20))),
      "b", Seq(Segment(RealValue(10), RealValue(20))),
      "c", Seq(segment10To30)
    ))
    SegmentSet.intercept(exampleSet1, exampleSet3) shouldBe SegmentSet(JMap.of(
      "a", Seq(segmentMInfTo5, segment15ToPInf),
      "b", Seq(point7, Segment(RealValue(15), RealValue(20)))
    ))
    SegmentSet.intercept(exampleSet2, exampleSet3) shouldBe SegmentSet(JMap.of(
      "a", Seq(segment2To20),
      "b", Seq(Segment(RealValue(15), RealValue(30))),
      "c", Seq(segment10To30)
    ))
    SegmentSet.intercept(exampleSet1, exampleSet2, exampleSet3) shouldBe SegmentSet(JMap.of(
      "a", Seq(Segment(RealValue(2), RealValue(5)), Segment(RealValue(15), RealValue(20))),
      "b", Seq(Segment(RealValue(15), RealValue(20))),
      "c", Seq(segment10To30)
    ))
  }

  it should "transform segment to filter" in {
    segmentToFilter("a",
      Segment(MInfinity(), PInfinity())) shouldBe None
    segmentToFilter("b",
      segmentMInfTo5) shouldBe Some(LessThanOrEqual("b", 5))
    segmentToFilter("c",
      segment15ToPInf) shouldBe Some(GreaterThanOrEqual("c", 15))
    segmentToFilter("d",
      segment2To20) shouldBe
      Some(And(GreaterThanOrEqual("d", 2), LessThanOrEqual("d", 20)))
  }

  it should "transform set to filters" in {
    exampleSet1.toFilters should contain theSameElementsAs Seq(
      Or(LessThanOrEqual("a", 5), GreaterThanOrEqual("a", 15)),
      And(GreaterThanOrEqual("b", 2), LessThanOrEqual("b", 20))
    )
    exampleSet2.toFilters should contain theSameElementsAs Seq(
      And(GreaterThanOrEqual("a", 2), LessThanOrEqual("a", 20)),
      And(GreaterThanOrEqual("b", 10), LessThanOrEqual("b", 30)),
      And(GreaterThanOrEqual("c", 10), LessThanOrEqual("c", 30))
    )
    exampleSet3.toFilters should contain theSameElementsAs Seq(
      Or(In("b", Array(7)), GreaterThanOrEqual("b", 15))
    )
  }

  it should "get points from segment seq" in {
    val data = List(
      List(segmentMInfTo5, segment10To30), List(segment15ToPInf),
      Nil, List(segment2To20, segment10To30), List(point7)
    )

    AbstractSegment.getAllPoints(data) shouldBe Map(
      (MInfinity(), Map(SegmentSide.Begin -> 1)),
      (RealValue(2), Map(SegmentSide.Begin -> 1)),
      (RealValue(5), Map(SegmentSide.End -> 1)),
      (RealValue(7), Map(SegmentSide.Begin -> 1, SegmentSide.End -> 1)),
      (RealValue(10), Map(SegmentSide.Begin -> 2)),
      (RealValue(15), Map(SegmentSide.Begin -> 1)),
      (RealValue(20), Map(SegmentSide.End -> 1)),
      (RealValue(30), Map(SegmentSide.End -> 2)),
      (PInfinity(), Map(SegmentSide.End -> 1))
    )
  }

  it should "calculate coverage from segment seq" in {
    val res = List(
      List(segmentMInfTo5, segment10To30), List(segment15ToPInf),
      Nil, List(segment2To20, segment10To30, point7)
    )

    AbstractSegment.calculateCoverage(res) shouldBe Seq(
      (Segment(MInfinity(), MInfinity()), 1),
      (Segment(MInfinity(), RealValue(2)), 1),
      (Segment(RealValue(2), RealValue(2)), 2),
      (Segment(RealValue(2), RealValue(5)), 2),
      (Segment(RealValue(5), RealValue(5)), 2),
      (Segment(RealValue(5), RealValue(7)), 1),
      (Segment(RealValue(7), RealValue(7)), 2),
      (Segment(RealValue(7), RealValue(10)), 1),
      (Segment(RealValue(10), RealValue(10)), 3),
      (Segment(RealValue(10), RealValue(15)), 3),
      (Segment(RealValue(15), RealValue(15)), 4),
      (Segment(RealValue(15), RealValue(20)), 4),
      (Segment(RealValue(20), RealValue(20)), 4),
      (Segment(RealValue(20), RealValue(30)), 3),
      (Segment(RealValue(30), RealValue(30)), 3),
      (Segment(RealValue(30), PInfinity()), 1),
      (Segment(PInfinity(), PInfinity()), 1)
    )
  }

  it should "union neighbour segments" in {
    val test = List(
      Segment(MInfinity(), RealValue(2)),
      Segment(RealValue(2), RealValue(2)),
      Segment(RealValue(5), RealValue(10)),
      Segment(RealValue(10), RealValue(15)),
      Segment(RealValue(20), RealValue(20)),
      Segment(RealValue(20), RealValue(30)))

    AbstractSegment.unionNeighbourSegments(test) shouldBe Seq(
      Segment(MInfinity(), RealValue(2)),
      Segment(RealValue(5), RealValue(15)),
      Segment(RealValue(20), RealValue(30))
    )
  }

  it should "simplify segment set" in {
    exampleSet1.simplifySegments shouldBe SegmentSet(
      JMap.of(
        "a", Seq(Segment(MInfinity(), PInfinity())),
        "b", Seq(segment2To20)
      )
    )
    exampleSet2.simplifySegments shouldBe exampleSet2
    exampleSet3.simplifySegments shouldBe SegmentSet(
      JMap.of(
        "b", Seq(Segment(RealValue(7), PInfinity()))
      )
    )
  }
}
