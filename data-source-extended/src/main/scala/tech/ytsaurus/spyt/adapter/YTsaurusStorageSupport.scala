package tech.ytsaurus.spyt.adapter

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.YTsaurusExternalCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.v2.YtFilePartition
import org.apache.spark.sql.yt.YtSourceScanExec
import tech.ytsaurus.spyt.format.optimizer.YtSortedTableMarkerRule

class YTsaurusStorageSupport extends StorageSupport {
  override def shouldUseYtSplitFiles(): Boolean = {
    YtSourceScanExec.currentThreadInstance.get() != null
  }

  override def splitFiles(sparkSession: SparkSession, file: FileStatus, filePath: Path,
                          maxSplitBytes: Long, partitionValues: InternalRow): Seq[PartitionedFile] = {
    YtFilePartition.splitFiles(sparkSession, file, filePath, maxSplitBytes, partitionValues)
  }

  override def maxSplitBytes(sparkSession: SparkSession, selectedPartitions: Seq[PartitionDirectory]): Long = {
    val ytSourceScanExec = YtSourceScanExec.currentThreadInstance.get()
    YtFilePartition.maxSplitBytes(sparkSession, selectedPartitions, ytSourceScanExec.maybeReadParallelism)
  }

  override def createExtraOptimizations(spark: SparkSession): Seq[Rule[LogicalPlan]] = {
    Seq(new YtSortedTableMarkerRule(spark))
  }

  override val ytsaurusExternalCatalogName: String = classOf[YTsaurusExternalCatalog].getCanonicalName
}
