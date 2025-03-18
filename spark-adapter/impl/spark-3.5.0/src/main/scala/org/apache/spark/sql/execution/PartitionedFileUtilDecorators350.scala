package org.apache.spark.sql.execution

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileStatusWithMetadata, PartitionedFile}
import tech.ytsaurus.spyt.adapter.StorageSupport.{instance => ssi}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.PartitionedFileUtil$")
@Applicability(from = "3.5.0")
object PartitionedFileUtilDecorators350 {

  @DecoratedMethod
  @Applicability(to = "3.5.4")
  def splitFiles(sparkSession: SparkSession,
                 file: FileStatusWithMetadata,
                 isSplitable: Boolean,
                 maxSplitBytes: Long,
                 partitionValues: InternalRow): Seq[PartitionedFile] = {
    if (ssi.shouldUseYtSplitFiles()) {
      ssi.splitFiles(sparkSession, file.fileStatus, file.getPath, maxSplitBytes, partitionValues)
    } else {
      __splitFiles(sparkSession, file, isSplitable, maxSplitBytes, partitionValues)
    }
  }

  def __splitFiles(sparkSession: SparkSession,
                   file: FileStatusWithMetadata,
                   isSplitable: Boolean,
                   maxSplitBytes: Long,
                   partitionValues: InternalRow): Seq[PartitionedFile] = ???

  @DecoratedMethod
  @Applicability(from = "3.5.5")
  def splitFiles(sparkSession: SparkSession,
                 file: FileStatusWithMetadata,
                 filePath: Path,
                 isSplitable: Boolean,
                 maxSplitBytes: Long,
                 partitionValues: InternalRow): Seq[PartitionedFile] = {
    if (ssi.shouldUseYtSplitFiles()) {
      ssi.splitFiles(sparkSession, file.fileStatus, filePath, maxSplitBytes, partitionValues)
    } else {
      __splitFiles(sparkSession, file, filePath, isSplitable, maxSplitBytes, partitionValues)
    }
  }

  def __splitFiles(sparkSession: SparkSession,
                   file: FileStatusWithMetadata,
                   filePath: Path,
                   isSplitable: Boolean,
                   maxSplitBytes: Long,
                   partitionValues: InternalRow): Seq[PartitionedFile] = ???
}
