package tech.ytsaurus.spyt.common.utils

import org.apache.spark.sql.sources.{And, Filter, GreaterThanOrEqual, In, LessThanOrEqual, Or}
import Segment.Segment
import tech.ytsaurus.spyt.utils.CollectionUtils

import java.util.function.{Function => JFunction}
import java.util.stream.Collectors
import java.util.{Map => JMap, Set => JSet}
import scala.jdk.CollectionConverters._

object Segment {
  type Segment = AbstractSegment[Point]

  val full: Segment = Segment(MInfinity(), PInfinity())

  def unapply(segment: Segment): Option[(Point, Point)] = Some((segment.left, segment.right))

  def apply(left: Point, right: Point): Segment = new Segment(left, right)

  def apply(point: Point): Segment = Segment(point, point)

  private [utils]def toFilters(varName: String, segmentsAndPoints: Seq[Segment]): List[Filter] = {
    val (points, segments) = segmentsAndPoints.partition { segment => segment.left == segment.right }
    val pointFilters = if (points.nonEmpty) {
      List(In(varName, points.map { case Segment(_, rv: RealValue[_]) => rv.canonicalValue }.toArray))
    } else {
      Nil
    }
    val segmentFilters = segments.flatMap(segmentToFilter(varName, _))
    pointFilters ++ segmentFilters
  }

  private[utils] def segmentToFilter(varName: String, segment: Segment): Option[Filter] = segment match {
    case Segment(MInfinity(), PInfinity()) => None
    case Segment(MInfinity(), rv: RealValue[_]) => Some(LessThanOrEqual(varName, rv.canonicalValue))
    case Segment(rv: RealValue[_], PInfinity()) => Some(GreaterThanOrEqual(varName, rv.canonicalValue))
    case Segment(leftRv: RealValue[_], rightRv: RealValue[_]) =>
      Some(And(GreaterThanOrEqual(varName, leftRv.canonicalValue), LessThanOrEqual(varName, rightRv.canonicalValue)))
    case _ => throw new IllegalArgumentException("Invalid segment: left value must be less than or equal to right")
  }
}

case class SegmentSet(map: JMap[String, Seq[Segment]]) {
  def simplifySegments: SegmentSet = {
    val mapped =
      CollectionUtils.mapValues(map, (segments: Seq[Segment]) => Seq(Segment(segments.head.left, segments.last.right)))
    val filtered = CollectionUtils.filterMap(mapped,
      (entry: JMap.Entry[String, Seq[Segment]]) => entry.getValue != Seq(MInfinity(), PInfinity())
    )
    SegmentSet(filtered)
  }

  def toFilters: Array[Filter] = {
    map.asScala.flatMap {
      case (varName, segments) =>
        Segment.toFilters(varName, segments).reduceOption(Or)
    }.toArray
  }
}

object SegmentSet {
  def apply(): SegmentSet = new SegmentSet(JMap.of())

  def apply(columnName: String, segments: Segment*): SegmentSet = new SegmentSet(JMap.of(columnName, segments.toSeq))

  def union(array: SegmentSet*): SegmentSet = merge(array, AbstractSegment.union)

  def intercept(array: SegmentSet*): SegmentSet = merge(array, AbstractSegment.intercept)

  private def merge(array: Seq[SegmentSet], operation: Seq[Seq[Segment]] => Seq[Segment]): SegmentSet = {
    val mergedMap = mergeByColumn(array)
    val segmentMap: JMap[String, Seq[Segment]] =
      CollectionUtils.mapValues(mergedMap, (values: Seq[Seq[Segment]]) => operation(values))
    SegmentSet(segmentMap)
  }

  private def mergeByColumn(array: Seq[SegmentSet]): JMap[String, Seq[Seq[Segment]]] = {
    val arrayKeys = array.map(x => x.map.keySet).asJava
    val keys: JSet[String] = arrayKeys.stream().flatMap(s => s.stream()).collect(Collectors.toSet())
    val keyMapper: JFunction[String, String] = (key: String) => key
    val valueMapper: JFunction[String, Seq[Seq[Segment]]] = (key: String) => array.asJava
      .stream()
      .filter(_.map.containsKey(key))
      .map[Seq[Segment]](a => a.map.get(key))
      .collect(Collectors.toList())
      .asScala
      .toSeq

    keys.stream().collect(Collectors.toMap[String, String, Seq[Seq[Segment]]](keyMapper, valueMapper))
  }
}
