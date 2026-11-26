package org.apache.spark.sql.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.expressions.{FieldReference, LogicalExpressions, NamedReference, Transform}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, LongType, ShortType, StructField, StructType}
import org.slf4j.LoggerFactory

import tech.ytsaurus.core.cypress.Range
import tech.ytsaurus.spyt.common.utils.{AbstractSegment, PInfinity, Point, RealValue, Segment, SegmentSet, TuplePoint}
import tech.ytsaurus.spyt.format.{YtInputSplit, YtPartitionedFileDelegate}
import tech.ytsaurus.spyt.format.YtPartitionedFileDelegate.emptyInternalRow
import tech.ytsaurus.spyt.format.bucketing.{HashFunctionParser, YtBucketingCatalog}
import tech.ytsaurus.spyt.format.conf.{FilterPushdownConfig, SparkYtConfiguration, SparkYtInternalConfiguration}
import tech.ytsaurus.spyt.fs.YtHadoopPath
import tech.ytsaurus.spyt.serializers.SchemaConverter.MetadataFields
import tech.ytsaurus.spyt.serializers.{PivotKeysConverter, SchemaConverter, YtLogicalType}
import tech.ytsaurus.spyt.types.{UInt64Long, YTsaurusTypes}
import tech.ytsaurus.spyt.wrapper.YtJavaConverters.toOption
import tech.ytsaurus.spyt.wrapper.config.{OptionsConf, SparkYtSparkSession, sparkConfigKeys}
import tech.ytsaurus.typeinfo.TiType

import java.util.{HashMap, Locale}

import scala.jdk.CollectionConverters._

final class YtHashBucketing private (
  val transform: Transform,
  val numBuckets: Int,
  hashKey: String,
  path: YtHadoopPath,
  sparkSession: SparkSession,
  schema: StructType,
  hintedColumns: Set[String],
  loadBounds: Seq[String] => Map[String, (Point, Point)]) {

  private val keys = SchemaConverter.filterPushdownKeys(schema)
  private lazy val statisticsColumns = keys.drop(1).flatten.filter { name =>
    !hintedColumns.contains(name.toLowerCase(Locale.ROOT)) && YtHashBucketing.findField(schema, name).exists { field =>
      Seq(ByteType, ShortType, IntegerType, LongType, YtHashBucketing.uint64SparkType).contains(field.dataType)
    }
  }

  // Without a transaction that pins the same snapshot for statistics and reads, concurrent table
  // changes can make cached bounds stale and cause matching rows to be skipped.
  private lazy val bounds = loadBounds(statisticsColumns)

  def partitions(pushedFilters: SegmentSet, config: FilterPushdownConfig): Seq[FilePartition] = {
    val tablePath = path.toYPath
    val byteLength = Math.max(path.meta.size / numBuckets, 1L)
    val clip = config.enabled && sparkSession.ytConf(SparkYtConfiguration.Read.HashBucketing.ClipRangesByStatistics) &&
      statisticsColumns.exists(k => Option(pushedFilters.map.get(k)).exists(_.size > 1))
    val filters = if (clip) YtHashBucketing.clipToBounds(pushedFilters, bounds) else pushedFilters
    val pushdownEnabled = config.enabled && !filters.map.isEmpty
    val emptyScan = pushdownEnabled && filters.map.values().asScala.exists(_.isEmpty)

    (0 until numBuckets).map { index =>
      val point = RealValue(UInt64Long(index.toLong))
      val range = YtHashBucketing.bucketRange(point)
      val ranges = if (emptyScan) {
        Iterator.empty
      } else if (pushdownEnabled) {
        val bucketFilters = YtHashBucketing.intersectBucket(filters, hashKey, Segment(point))
        if (bucketFilters.map.get(hashKey).isEmpty) {
          Iterator.empty
        } else {
          val filtered = YtInputSplit.applyPushdownFilters(
            tablePath.ranges(range),
            keys,
            bucketFilters,
            config,
            union = false,
            fromPlanning = true)
          filtered.getRanges.iterator().asScala
        }
      } else {
        Iterator.single(range)
      }
      val files = ranges.map { fileRange =>
        YtPartitionedFileDelegate(tablePath.ranges(fileRange), byteLength, emptyInternalRow, path)
      }.toArray[PartitionedFile]
      new YtKeyedFilePartition(index, files, new GenericInternalRow(Array[Any](index.toLong)))
    }
  }
}

/**
 * Detects a static YT table read through YtBucketingCatalog whose first key column is a uint64 computed column
 * `farm_hash(column) % N` and splits its scan into N partitions keyed by the bucket number, each reading the key
 * range of one bucket, so Spark can report KeyGroupedPartitioning bucket(N, column). Anything else (a read by path,
 * dynamic or several tables, no modulo, a hash column not read as uint64, several or unread arguments, an argument
 * Spark cannot hash as YT did or with a schema hint, too many buckets) leaves the scan unbucketed.
 */
object YtHashBucketing {
  private val log = LoggerFactory.getLogger(getClass)

  private lazy val uint64SparkType: DataType = YTsaurusTypes.instance.sparkTypeFor(TiType.uint64())

  private val maxBucketsKeys: String = sparkConfigKeys(SparkYtConfiguration.Read.HashBucketing.MaxBuckets)

  def tryBuild(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    fullDataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: Map[String, String],
    loadBounds: (YtHadoopPath, Seq[String]) => Map[String, (Point, Point)]): Option[YtHashBucketing] = {
    if (!applicable(sparkSession, fileIndex, readPartitionSchema, options)) {
      None
    } else {
      for {
        columns <- bucketColumns(fullDataSchema, readDataSchema)
        path <- singleStaticTable(fileIndex)
        bucketing <- build(sparkSession, path, columns, fullDataSchema, SchemaConverter.hintedColumns(options), loadBounds)
      } yield bucketing
    }
  }

  private def applicable(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    readPartitionSchema: StructType,
    options: Map[String, String]): Boolean = {
    options.getYtConf(SparkYtInternalConfiguration.HashBucketingCatalogRead).exists(identity) &&
      sparkSession.ytConf(SparkYtConfiguration.Read.HashBucketing.Enabled) &&
      sparkSession.sessionState.conf.v2BucketingEnabled &&
      readPartitionSchema.isEmpty &&
      fileIndex.partitionSchema.isEmpty &&
      !sparkSession.ytConf(SparkYtConfiguration.Read.YtDistributedReadingEnabled)
  }

  private def maxBucketCount(sparkSession: SparkSession): Int = {
    val maxBuckets = try {
      sparkSession.ytConf(SparkYtConfiguration.Read.HashBucketing.MaxBuckets)
    } catch {
      case e: NumberFormatException =>
        throw new IllegalArgumentException(s"$maxBucketsKeys must be a positive integer", e)
    }
    if (maxBuckets < 1) {
      throw new IllegalArgumentException(s"$maxBucketsKeys must be a positive integer, got $maxBuckets")
    }
    maxBuckets
  }

  private def bucketColumns(fullDataSchema: StructType, readDataSchema: StructType): Option[BucketColumns] = {
    for {
      hashColumn <- SchemaConverter.filterPushdownKeys(fullDataSchema).headOption.flatten
      hashField <- findField(fullDataSchema, hashColumn)
      if hashField.dataType == uint64SparkType
      expression <- MetadataFields.getExpression(hashField)
      call <- toOption(HashFunctionParser.parse(expression))
      buckets <- toOption(call.buckets()).map(_.intValue())
      arguments = call.arguments().asScala.toSeq
      if !arguments.contains(hashColumn)
      argumentFields = arguments.flatMap(argument => findField(readDataSchema, argument))
      if argumentFields.length == arguments.length
    } yield BucketColumns(hashField, argumentFields, buckets)
  }

  private def unbucketableArgument(field: StructField, hintedColumns: Set[String]): Option[String] = {
    val isUuid = field.metadata.contains(MetadataFields.YT_LOGICAL_TYPE) &&
      field.metadata.getString(MetadataFields.YT_LOGICAL_TYPE) == YtLogicalType.Uuid.getNameV3(true)
    if (hintedColumns.contains(field.name.toLowerCase(Locale.ROOT))) {
      Some(s"${field.name} with a schema hint")
    } else if (isUuid || !YtBucketingCatalog.isSupportedArgumentType(field.dataType)) {
      Some(s"${field.name} of type ${if (isUuid) "uuid" else field.dataType.catalogString}")
    } else {
      None
    }
  }

  private def findField(schema: StructType, column: String): Option[StructField] = {
    schema.fields.find(field => MetadataFields.getOriginalName(field) == column)
  }

  private def singleStaticTable(fileIndex: PartitioningAwareFileIndex): Option[YtHadoopPath] = {
    fileIndex.allFiles() match {
      case Seq(status) =>
        YtHadoopPath.fromPath(status.getPath) match {
          case path: YtHadoopPath if !path.meta.isDynamic => Some(path)
          case _ => None
        }
      case _ => None
    }
  }

  private def build(
    sparkSession: SparkSession,
    path: YtHadoopPath,
    columns: BucketColumns,
    schema: StructType,
    hintedColumns: Set[String],
    loadBounds: (YtHadoopPath, Seq[String]) => Map[String, (Point, Point)]): Option[YtHashBucketing] = {
    val maxBuckets = maxBucketCount(sparkSession)
    val refusal = if (columns.buckets > maxBuckets) {
      Some(s"is split into ${columns.buckets} buckets, more than $maxBuckets allowed by $maxBucketsKeys")
    } else if (columns.arguments.length != 1) {
      Some(s"hashes ${columns.arguments.length} columns, Spark joins bucketed tables on a single bucket column only")
    } else {
      unbucketableArgument(columns.arguments.head, hintedColumns)
        .map(argument => s"hashes $argument, whose Spark values may not reproduce the YTsaurus farm_hash")
    }
    val hashColumn = s"Hash column ${columns.hashField.name} of ${path.toStringPath}"
    refusal match {
      case Some(reason) =>
        log.info(s"$hashColumn $reason, bucketing is not applied")
        None
      case None =>
        log.debug(s"$hashColumn splits the scan into ${columns.buckets} bucketed partitions")
        val argument = FieldReference.column(columns.arguments.head.name)
        val transform = LogicalExpressions.bucket(columns.buckets, Array[NamedReference](argument))
        Some(new YtHashBucketing(transform, columns.buckets, MetadataFields.getOriginalName(columns.hashField),
          path, sparkSession, schema, hintedColumns, names => loadBounds(path, names)))
    }
  }

  private[v2] def clipToBounds(filters: SegmentSet, bounds: Map[String, (Point, Point)]): SegmentSet = {
    val clipped = filters.map.asScala.map { case (column, segments) =>
      column -> bounds.get(column).fold(segments) { case (lo, hi) =>
        // YT min/max only describe non-null values.
        segments.filter(s => s.left <= RealValue(null) || (s.right >= lo && s.left <= hi))
      }
    }
    SegmentSet(clipped.toMap.asJava)
  }

  private[v2] def intersectBucket(
    normalizedFilters: SegmentSet,
    hashKey: String,
    bucket: Segment.Segment): SegmentSet = {
    val point = Seq(bucket)
    val hashIntersection = Option(normalizedFilters.map.get(hashKey)).fold(point) { segments =>
      AbstractSegment.intercept(Seq(segments, point))
    }
    val result = new HashMap[String, Seq[Segment.Segment]](normalizedFilters.map)
    result.put(hashKey, hashIntersection)
    SegmentSet(result)
  }

  private def bucketRange(point: RealValue[UInt64Long]): Range = {
    new Range(
      PivotKeysConverter.toRangeLimit(TuplePoint(Seq(point))),
      PivotKeysConverter.toRangeLimit(TuplePoint(Seq(point, PInfinity()))))
  }

  private case class BucketColumns(hashField: StructField, arguments: Seq[StructField], buckets: Int)
}
