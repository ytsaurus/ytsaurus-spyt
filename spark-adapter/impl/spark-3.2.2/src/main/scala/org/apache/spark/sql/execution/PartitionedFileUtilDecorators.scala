package org.apache.spark.sql.execution

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import tech.ytsaurus.spyt.adapter.StorageSupport.{instance => ssi}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.PartitionedFileUtil$")
@Applicability(to = "3.4.4")
object PartitionedFileUtilDecorators {

  @DecoratedMethod
  def splitFiles(sparkSession: SparkSession,
                 file: FileStatus,
                 filePath: Path,
                 isSplitable: Boolean,
                 maxSplitBytes: Long,
                 partitionValues: InternalRow): Seq[PartitionedFile] = {
    if (ssi.shouldUseYtSplitFiles()) {
      ssi.splitFiles(sparkSession, file, filePath, maxSplitBytes, partitionValues)
    } else {
      __splitFiles(sparkSession, file, filePath, isSplitable, maxSplitBytes, partitionValues)
    }
  }

  def __splitFiles(sparkSession: SparkSession,
                 file: FileStatus,
                 filePath: Path,
                 isSplitable: Boolean,
                 maxSplitBytes: Long,
                 partitionValues: InternalRow): Seq[PartitionedFile] = ???

}
