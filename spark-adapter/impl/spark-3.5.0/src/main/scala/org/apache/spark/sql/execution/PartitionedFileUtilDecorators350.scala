package org.apache.spark.sql.execution

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
}
