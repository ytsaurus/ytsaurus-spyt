package org.apache.spark.sql.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.expressions.{FieldReference, LogicalExpressions, NamedReference, Transform}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.slf4j.LoggerFactory

import tech.ytsaurus.core.cypress.{Range, YPath}
import tech.ytsaurus.spyt.common.utils.{PInfinity, RealValue, TuplePoint}
import tech.ytsaurus.spyt.format.YtPartitionedFileDelegate
import tech.ytsaurus.spyt.format.bucketing.{HashFunctionParser, YtBucketingCatalog}
import tech.ytsaurus.spyt.format.conf.{SparkYtConfiguration, SparkYtInternalConfiguration}
import tech.ytsaurus.spyt.fs.YtHadoopPath
import tech.ytsaurus.spyt.serializers.SchemaConverter.MetadataFields
import tech.ytsaurus.spyt.serializers.{PivotKeysConverter, SchemaConverter, YtLogicalType}
import tech.ytsaurus.spyt.types.{UInt64Long, YTsaurusTypes}
import tech.ytsaurus.spyt.wrapper.YtJavaConverters.toOption
import tech.ytsaurus.spyt.wrapper.config.{OptionsConf, SparkYtSparkSession, sparkConfigKeys}
import tech.ytsaurus.typeinfo.TiType

import java.util.Locale

import scala.jdk.CollectionConverters._

/** The bucket transform a hash-bucketed scan reports and its partitions, one key range per bucket. */
case class YtHashBucketing(transform: Transform, partitions: Seq[FilePartition])

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
    options: Map[String, String]): Option[YtHashBucketing] = {
    if (!applicable(sparkSession, fileIndex, readPartitionSchema, options)) {
      None
    } else {
      for {
        columns <- bucketColumns(fullDataSchema, readDataSchema)
        path <- singleStaticTable(fileIndex)
        bucketing <- build(path, columns, maxBucketCount(sparkSession), SchemaConverter.hintedColumns(options))
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
      hashColumn <- SchemaConverter.prefixKeys(fullDataSchema).headOption
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
    path: YtHadoopPath,
    columns: BucketColumns,
    maxBuckets: Int,
    hintedColumns: Set[String]): Option[YtHashBucketing] = {
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
        val byteLength = Math.max(path.meta.size / columns.buckets, 1L)
        val yPath = path.toYPath
        val partitions = (0 until columns.buckets).map { bucket =>
          val file = YtPartitionedFileDelegate(
            bucketRange(yPath, bucket),
            byteLength,
            YtPartitionedFileDelegate.emptyInternalRow,
            path)
          new YtKeyedFilePartition(bucket, Array(file), new GenericInternalRow(Array[Any](bucket.toLong)))
        }
        log.debug(s"$hashColumn splits the scan into ${partitions.length} bucketed partitions")
        val argument = FieldReference.column(columns.arguments.head.name)
        Some(YtHashBucketing(LogicalExpressions.bucket(columns.buckets, Array[NamedReference](argument)), partitions))
    }
  }

  private def bucketRange(yPath: YPath, bucket: Long): YPath = {
    val point = RealValue(UInt64Long(bucket))
    val range = new Range(
      PivotKeysConverter.toRangeLimit(TuplePoint(Seq(point))),
      PivotKeysConverter.toRangeLimit(TuplePoint(Seq(point, PInfinity()))))
    yPath.ranges(range)
  }

  private case class BucketColumns(hashField: StructField, arguments: Seq[StructField], buckets: Int)
}
