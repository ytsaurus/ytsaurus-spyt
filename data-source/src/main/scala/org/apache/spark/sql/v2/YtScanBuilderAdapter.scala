package org.apache.spark.sql.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownFilters}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.{Filter, IsNotNull}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import tech.ytsaurus.spyt.common.utils.ExpressionTransformer.filtersToSegmentSet
import tech.ytsaurus.spyt.common.utils.SegmentSet
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.KeyColumnsFilterPushdown
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings
import tech.ytsaurus.spyt.fs.YtHadoopPath
import tech.ytsaurus.spyt.logger.{YtDynTableLogger, YtLogger}
import tech.ytsaurus.spyt.serializers.SchemaConverter
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode

import scala.collection.JavaConverters._

case class YtScanBuilderAdapter(sparkSession: SparkSession,
                                fileIndex: PartitioningAwareFileIndex,
                                schema: StructType,
                                dataSchema: StructType,
                                options: CaseInsensitiveStringMap) extends ScanBuilderAdapter {
  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }
  lazy val optimizedForScan: Boolean = fileIndex.allFiles().forall { fileStatus =>
    YtHadoopPath.fromPath(fileStatus.getPath) match {
      case yp: YtHadoopPath => !yp.meta.isDynamic && yp.meta.optimizeMode == OptimizeMode.Scan
      case _ => false
    }
  }

  private var pushedFilterSegments: SegmentSet = SegmentSet()
  private var filters: Array[Filter] = Array.empty
  private var partitionFilters = Seq.empty[Expression]
  private var dataFilters = Seq.empty[Expression]

  def setPartitionFilters(partitionFilters: Seq[Expression]): Unit = {
    this.partitionFilters = partitionFilters
  }

  def setDataFilters(dataFilters: Seq[Expression]): Unit = {
    this.dataFilters = dataFilters
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    implicit val ytLog: YtLogger = YtDynTableLogger.pushdown(sparkSession)

    this.filters = filters
    this.pushedFilterSegments = filtersToSegmentSet(filters)

    logPushdownDetails()

    this.filters
  }

  private def logPushdownDetails()(implicit ytLog: YtLogger): Unit = {
    import tech.ytsaurus.spyt.fs.conf._

    val pushdownEnabled = sparkSession.ytConf(KeyColumnsFilterPushdown.Enabled)
    val keyColumns = SchemaConverter.keys(schema)
    val keySet = keyColumns.flatten.toSet

    val logInfo = Map(
      "filters" -> filters.mkString(", "),
      "keyColumns" -> keyColumns.mkString(", "),
      "segments" -> pushedFilterSegments.toString,
      "pushdownEnabled" -> pushdownEnabled.toString,
      "paths" -> options.get("paths")
    )

    val importantFilters = filters.filter(!_.isInstanceOf[IsNotNull])

    if (importantFilters.exists(_.references.exists(keySet.contains))) {
      if (pushedFilterSegments.map.nonEmpty) {
        ytLog.info("Pushing filters in YtScanBuilder, filters contain some key columns", logInfo)
      } else {
        ytLog.debug("Pushing filters in YtScanBuilder, filters contain some key columns", logInfo)
      }
    } else if (importantFilters.nonEmpty) {
      ytLog.trace("Pushing filters in YtScanBuilder, filters don't contain key columns", logInfo)
    }
  }

  override def pushedFilters(): Array[Filter] = {
    pushedFilterSegments.toFilters
  }

  override def build(dataSchema: StructType, partitionSchema: StructType): Scan = {
    var opts = options.asScala
    opts = opts + (YtTableSparkSettings.OptimizedForScan.name -> optimizedForScan.toString)
    YtScan(sparkSession, hadoopConf, fileIndex, dataSchema, dataSchema, partitionSchema,
      new CaseInsensitiveStringMap(opts.asJava), partitionFilters, dataFilters,
      pushedFilterSegments = pushedFilterSegments)
  }
}

