package tech.ytsaurus.spyt.adapter

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionedFile}

import java.util.ServiceLoader

trait StorageSupport {
  def shouldUseYtSplitFiles(): Boolean
  def splitFiles(sparkSession: SparkSession, file: FileStatus, filePath: Path,
                 maxSplitBytes: Long, partitionValues: InternalRow): Seq[PartitionedFile]
  def maxSplitBytes(sparkSession: SparkSession, selectedPartitions: Seq[PartitionDirectory]): Long
  def createExtraOptimizations(spark: SparkSession): Seq[Rule[LogicalPlan]]
  val ytsaurusExternalCatalogName: String
}

object StorageSupport {
  lazy val instance: StorageSupport = ServiceLoader.load(classOf[StorageSupport]).findFirst().get()
}
