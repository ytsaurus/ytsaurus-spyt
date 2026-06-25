package org.apache.spark.sql.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.read.partitioning.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, Statistics, SupportsReportPartitioning, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionDirectory, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.YtFilePartition.tryGetKeyPartitions
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration
import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.common.utils.{ExpressionTransformer, SegmentSet}
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.YtReadSettingsFactory
import tech.ytsaurus.spyt.format.conf.{FilterPushdownConfig, KeyPartitioningConfig, SparkYtConfiguration, YtTableSparkSettings}
import tech.ytsaurus.spyt.fs.YtHadoopPath
import tech.ytsaurus.spyt.logger.{YtDynTableLoggerConfig, YtLogger}
import tech.ytsaurus.spyt.serializers.SchemaConverter
import tech.ytsaurus.spyt.wrapper.client.YtThrottle
import tech.ytsaurus.spyt.wrapper.config.SparkYtSparkSession

import java.util.concurrent.CompletableFuture
import java.util.{Locale, OptionalLong}
import scala.jdk.CollectionConverters._

case class YtScan(sparkSession: SparkSession,
                  hadoopConf: Configuration,
                  fileIndex: PartitioningAwareFileIndex,
                  dataSchema: StructType,
                  readDataSchema: StructType,
                  readPartitionSchema: StructType,
                  options: CaseInsensitiveStringMap,
                  partitionFilters: Seq[Expression],
                  dataFilters: Seq[Expression],
                  pushedFilterSegments: SegmentSet = SegmentSet(),
                  keyPartitionsHint: Option[Seq[FilePartition]] = None) extends FileScan
  with SupportsReportPartitioning
  with SupportsRuntimeFiltering {

  private val filterPushdownConf = FilterPushdownConfig(sparkSession)
  private val keyPartitioningConf = KeyPartitioningConfig(sparkSession)

  @transient private var runtimeFilterSegments: SegmentSet = SegmentSet()

  @transient private var cachedPartitions: Option[Seq[FilePartition]] = None

  private def effectiveFilterSegments: SegmentSet = {
    SegmentSet.intercept(pushedFilterSegments, runtimeFilterSegments)
  }

  private val pushedFiltersStr: String = pushedFilterSegments.toFilters.mkString("[", ", ", "]")

  override def filterAttributes(): Array[NamedReference] = {
    val keyColumns = SchemaConverter.keys(dataSchema).flatten
    val result = keyColumns.map(name => FieldReference.column(name)).toArray
    result
  }

  override def filter(filters: Array[Filter]): Unit = {
    implicit val ytLog: YtLogger = YtLogger.noop
    val newRuntimeSegments = ExpressionTransformer.filtersToSegmentSet(filters.toSeq)
    runtimeFilterSegments = newRuntimeSegments
    cachedPartitions = None
  }

  def supportsKeyPartitioning: Boolean = {
    keyPartitionsHint.isDefined
  }

  override def isSplitable(path: Path): Boolean = true

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val keyPartitionedOptions = Map(YtTableSparkSettings.KeyPartitioned.name -> supportsKeyPartitioning.toString)

    val adapter = YtPartitionReaderFactoryAdapter(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema,
      options.asScala.toMap ++ keyPartitionedOptions,
      effectiveFilterSegments, filterPushdownConf, YtDynTableLoggerConfig.fromSpark(sparkSession),
      YtReadSettingsFactory.fromSpark(sparkSession)
    )
    SparkAdapter.instance.createYtPartitionReaderFactory(adapter)
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: YtScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    val runtimeFiltersStr = if (!runtimeFilterSegments.map.isEmpty) {
      s", RuntimeFilters: ${runtimeFilterSegments.toFilters.mkString("[", ", ", "]")}"
    } else {
      ""
    }
    super.description() +
      ", PushedFilters: " + pushedFiltersStr +
      runtimeFiltersStr +
      ", filter pushdown enabled: " + filterPushdownConf.enabled +
      ", key partitioned: " + supportsKeyPartitioning
  }

  // Left for backward compatibility with Spark 3.2.x
  def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan = {
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
  }

  private val maybeReadParallelism = Option(options.get("readParallelism")).map(_.toInt)

  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }

  // for tests
  def getPartitions: Seq[FilePartition] = partitions

  private def tryGetKeyPartitioning(columns: Option[Seq[String]] = None): Option[Seq[FilePartition]] = {
    val splitFiles = preparePartitioning()
    tryGetKeyPartitions(sparkSession, splitFiles, readDataSchema, keyPartitioningConf, columns)
  }

  private def addKeyPartitioningHint(partitions: Seq[FilePartition]): YtScan = {
    copy(keyPartitionsHint = Some(partitions))
  }

  def tryKeyPartitioning(columns: Option[Seq[String]] = None): Option[YtScan] = {
    tryGetKeyPartitioning(columns).map(addKeyPartitioningHint)
  }

  override protected def partitions: Seq[FilePartition] = {
    cachedPartitions.getOrElse {
      val computed = keyPartitionsHint.getOrElse {
        val splitFiles = preparePartitioning()
        YtFilePartition.getFilePartitions(splitFiles)
      }
      cachedPartitions = Some(computed)
      computed
    }
  }

  private def preparePartitioning(): Seq[PartitionedFile] = {
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val maxSplitBytes = YtFilePartition.maxSplitBytes(sparkSession, selectedPartitions, maybeReadParallelism)
    getSplitFiles(selectedPartitions, maxSplitBytes)
  }

  private def getSplitFiles(selectedPartitions: Seq[PartitionDirectory], maxSplitBytes: Long): Seq[PartitionedFile] = {
    val partitionAttributes = SparkAdapter.instance.schemaToAttributes(fileIndex.partitionSchema)
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.getOrElse(
        normalizeName(readField.name),
        throw SparkAdapter.instance.createAnalysisException(s"Can't find required partition column ${readField.name} " +
          s"in partition schema ${fileIndex.partitionSchema}")
      )
    }
    lazy val partitionValueProject = GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
    val throttle = YtThrottle(sparkSession.ytConf(SparkYtConfiguration.Throttling.MaxConcurrency))
    val splitFilesFutures = selectedPartitions.map { partition =>
      // Prune partition values if part of the partition columns are not required.
      val partitionValues = if (readPartitionAttributes != partitionAttributes) {
        partitionValueProject(partition.values).copy()
      } else {
        partition.values
      }
      val fileFutures = SparkAdapter.instance.getPartitionFileStatuses(partition).map { file =>
        throttle.gate {
          YtFilePartition.splitFilesAsync(
            sparkSession = sparkSession,
            file = file,
            filePath = file.getPath,
            maxSplitBytes = maxSplitBytes,
            partitionValues = partitionValues,
            pushedFilterSegments = effectiveFilterSegments,
            readDataSchema = Some(readDataSchema)
          )
        }
      }

      fileFutures.foldLeft(CompletableFuture.completedFuture(Seq.empty[PartitionedFile])) { (acc, fileFuture) =>
        acc.thenCombine[Seq[PartitionedFile], Seq[PartitionedFile]](
          fileFuture, (accFiles: Seq[PartitionedFile], files: Seq[PartitionedFile]) => accFiles ++ files)
      }
    }

    CompletableFuture.allOf(splitFilesFutures: _*).join()
    splitFilesFutures.flatMap(_.get())
  }

  // This method is intended to support YTsaurus native partitioning and should help to avoid shuffle at spark side
  override def outputPartitioning(): Partitioning = {
    new UnknownPartitioning(partitions.length)
  }

  override def estimateStatistics(): Statistics = new Statistics {
    override val sizeInBytes: OptionalLong = OptionalLong.of(fileIndex.sizeInBytes)

    override val numRows: OptionalLong = {
      val rowCounts = fileIndex.allFiles().map { status =>
        YtHadoopPath.fromPath(status.getPath) match {
          case yp: YtHadoopPath => Some(yp.meta.rowCount)
          case _ => None
        }
      }
      if (rowCounts.forall(_.isDefined)) {
        OptionalLong.of(rowCounts.map(_.get).sum)
      } else {
        OptionalLong.empty()
      }
    }
  }
}

object YtScan {
  type ScanDescription = (YtScan, Seq[String])

  def trySyncKeyPartitioning(leftScanDescO: Option[ScanDescription], rightScanDescO: Option[ScanDescription]
                            ): (Option[ScanDescription], Option[ScanDescription]) = {
    def singleKeyPartitioning(scanDescO: Option[ScanDescription]): Option[ScanDescription] = {
      scanDescO.flatMap { case (scan, vars) => scan.tryKeyPartitioning(Some(vars)).map((_, vars)) }
    }

    (leftScanDescO, rightScanDescO) match {
      case (Some(leftDesc), Some(rightDesc)) =>
        trySyncKeyPartitioning(leftDesc, rightDesc)
      case _ =>
        (singleKeyPartitioning(leftScanDescO), singleKeyPartitioning(rightScanDescO))
    }
  }

  def trySyncKeyPartitioning(leftYtScanDesc: ScanDescription, rightYtScanDesc: ScanDescription
                            ): (Option[ScanDescription], Option[ScanDescription]) = {
    val (leftYtScan, leftVars) = leftYtScanDesc
    val (rightYtScan, rightVars) = rightYtScanDesc
    val leftPartitioningO = leftYtScan.tryGetKeyPartitioning(Some(leftVars))
    val rightPartitioningO = rightYtScan.tryGetKeyPartitioning(Some(rightVars))
    val (leftPartitioning, rightPartitioning) = (leftPartitioningO, rightPartitioningO) match {
      case (Some(leftPartitioning), Some(rightPartitioning)) =>
        val leftPivots = YtFilePartition.getPivotFromHintFiles(leftVars, leftPartitioning)
        val rightPivots = YtFilePartition.getPivotFromHintFiles(rightVars, rightPartitioning)
        val newLeftPartitioning = YtFilePartition.addPivots(leftPartitioning, leftVars, rightPivots)
        val newRightPartitioning = YtFilePartition.addPivots(rightPartitioning, rightVars, leftPivots)
        (Some(newLeftPartitioning), Some(newRightPartitioning))
      case p =>
        p
    }
    (
      leftPartitioning.map(leftYtScan.addKeyPartitioningHint).map((_, leftVars)),
      rightPartitioning.map(rightYtScan.addKeyPartitioningHint).map((_, rightVars))
    )
  }
}
