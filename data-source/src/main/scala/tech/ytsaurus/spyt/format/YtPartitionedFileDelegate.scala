package tech.ytsaurus.spyt.format

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import tech.ytsaurus.spyt.common.utils.{TuplePoint, TupleSegment}
import YtPartitionedFileDelegate._
import tech.ytsaurus.spyt.serializers.PivotKeysConverter
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.core.cypress.{Range, RangeCriteria, RangeLimit, YPath}
import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.fs.YtHadoopPath
import tech.ytsaurus.ysontree.{YTreeBinarySerializer, YTreeNode}

import java.io.ByteArrayInputStream

// At most one range supported inside ypath.
class YtPartitionedFileDelegate(val serializedYPath: Array[Byte],
                                override val byteLength: Long,
                                override val partitionValues: InternalRow,
                                val hadoopPath: YtHadoopPath) extends YtPartitioningDelegate {

  override val filePath: String = getPath(serializedYPath)
  override val start: Long = getNormalizedStart(serializedYPath)

  def isDynamic: Boolean = hadoopPath.meta.isDynamic

  def cluster: Option[String] = hadoopPath.ypath.cluster

  def copy(newBeginKey: Array[Byte], newEndKey: Array[Byte]): YtPartitionedFile = {
    import scala.collection.JavaConverters._
    withNewRangeCriteria(
      new Range(
        RangeLimit.key(PivotKeysConverter.toList(newBeginKey).asJava),
        RangeLimit.key(PivotKeysConverter.toList(newEndKey).asJava)
      )
    )
  }

  def ypath: YPath = deserializeYPath(serializedYPath)

  private def getAttributeFromSelf[T](attributeGetter: YPath => T): T = {
    attributeGetter(ypath)
  }

  def beginKey: Seq[YTreeNode] = getAttributeFromSelf(YPathUtils.beginKey)

  def beginPoint: Option[TuplePoint] = {
    if (beginKey.isEmpty) {
      Some(TupleSegment.mInfinity)
    } else {
      PivotKeysConverter.toPoint(beginKey)
    }
  }

  def endKey: Seq[YTreeNode] = getAttributeFromSelf(YPathUtils.endKey)

  def endPoint: Option[TuplePoint] = {
    if (endKey.isEmpty) {
      Some(TupleSegment.pInfinity)
    } else {
      PivotKeysConverter.toPoint(endKey)
    }
  }

  def beginRow: Long = getAttributeFromSelf(getStart)

  def endRow: Long = getAttributeFromSelf(getEnd)

  private def withNewRangeCriteria(rangeCriteria: RangeCriteria): YtPartitionedFile = {
    YtPartitionedFileDelegate(ypath.ranges(rangeCriteria), byteLength, partitionValues, hadoopPath)
  }
}

object YtPartitionedFileDelegate {
  type YtPartitionedFile = YtPartitioningSupport.YtPartitionedFileBase[YtPartitionedFileDelegate]
  type YtPartitionedFileExt = YtPartitioningSupport[YtPartitionedFileDelegate]

  val emptyInternalRow = new GenericInternalRow(new Array[Any](0))

  val fullRange: RangeCriteria = new Range(RangeLimit.key(), RangeLimit.key())

  private def toSimpleYPath(path: String): YPath = {
    YPath.simple(YtWrapper.formatPath(path))
  }

  def static(path: String, beginRow: Long, endRow: Long, byteLength: Long,
             partitionValues: InternalRow = emptyInternalRow,
             hadoopPath: YtHadoopPath = null): YtPartitionedFile = {
    static(toSimpleYPath(path), beginRow, endRow, byteLength, partitionValues, hadoopPath)
  }

  def static(path: YPath, beginRow: Long, endRow: Long, byteLength: Long,
             partitionValues: InternalRow, hadoopPath: YtHadoopPath): YtPartitionedFile = {
    val ypath = path.ranges(new Range(RangeLimit.row(beginRow), RangeLimit.row(endRow)))
    apply(ypath, byteLength, partitionValues, hadoopPath)
  }

  def dynamic(path: String, range: RangeCriteria, byteLength: Long,
              partitionValues: InternalRow, hadoopPath: YtHadoopPath = null): YtPartitionedFile = {
    dynamic(toSimpleYPath(path), range, byteLength, partitionValues, hadoopPath)
  }

  def dynamic(path: YPath, range: RangeCriteria, byteLength: Long, partitionValues: InternalRow,
              hadoopPath: YtHadoopPath): YtPartitionedFile = {
    val ypath = path.ranges(range)
    apply(ypath, byteLength, partitionValues, hadoopPath)
  }

  def apply(yPath: YPath, byteLength: Long, partitionValues: InternalRow,
            hadoopPath: YtHadoopPath): YtPartitionedFile = {
    val serializedYPath: Array[Byte] = serializeYPath(yPath)
    val delegate = new YtPartitionedFileDelegate(serializedYPath, byteLength, partitionValues, hadoopPath)
    SparkAdapter.instance.createYtPartitionedFile(delegate)
  }

  private def getAttributeFromYPath[T](attributeGetter: YPath => T)(serializedPath: Array[Byte]): T = {
    attributeGetter(deserializeYPath(serializedPath))
  }

  private def serializeYPath(ypath: YPath): Array[Byte] = {
    ypath.toTree.toBinary
  }

  private def deserializeYPath(serializedYPath: Array[Byte]): YPath = {
    val input = new ByteArrayInputStream(serializedYPath)
    val treeNode = YTreeBinarySerializer.deserialize(input)
    YPath.fromTree(treeNode)
  }

  private def getStartOption(ypath: YPath): Option[Long] = {
    YPathUtils.beginRowOption(ypath)
  }

  private def getStart(ypath: YPath): Long = {
    getStartOption(ypath).getOrElse(0)
  }

  private def getEnd(ypath: YPath): Long = {
    YPathUtils.endRowOption(ypath).getOrElse(0)
  }

  private def getPath: Array[Byte] => String = {
    getAttributeFromYPath(YPathUtils.getPath)
  }

  private def getStart: Array[Byte] => Long = {
    getAttributeFromYPath(getStart)
  }

  private def getNormalizedStart(serializedYPath: Array[Byte]): Long = {
    val start = getStart(serializedYPath)
    if (start == -1) {
      0
    } else {
      start
    }
  }
}
