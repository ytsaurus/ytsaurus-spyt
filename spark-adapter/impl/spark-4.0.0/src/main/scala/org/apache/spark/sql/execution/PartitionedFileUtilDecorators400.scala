package org.apache.spark.sql.execution

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileStatusWithMetadata, PartitionedFile}
import tech.ytsaurus.spyt.adapter.StorageSupport.{instance => ssi}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.PartitionedFileUtil$")
@Applicability(from = "4.0.0")
object PartitionedFileUtilDecorators400 {

  @DecoratedMethod
  def splitFiles(
    file: FileStatusWithMetadata,
    filePath: Path,
    isSplitable: Boolean,
    maxSplitBytes: Long,
    partitionValues: InternalRow): Seq[PartitionedFile] = {
    if (ssi.shouldUseYtSplitFiles()) {
      val sparkSession = SparkSession.getActiveSession.get
      ssi.splitFiles(sparkSession, file.fileStatus, filePath, maxSplitBytes, partitionValues)
    } else {
      __splitFiles(file, filePath, isSplitable, maxSplitBytes, partitionValues)
    }
  }

  def __splitFiles(
    file: FileStatusWithMetadata,
    filePath: Path,
    isSplitable: Boolean,
    maxSplitBytes: Long,
    partitionValues: InternalRow): Seq[PartitionedFile] = ???
}
