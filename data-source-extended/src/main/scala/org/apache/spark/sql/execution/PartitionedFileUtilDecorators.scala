package org.apache.spark.sql.execution

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.v2.YtFilePartition
import org.apache.spark.sql.yt.YtSourceScanExec
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.PartitionedFileUtil")
object PartitionedFileUtilDecorators {

  @DecoratedMethod
  def splitFiles(sparkSession: SparkSession,
                 file: FileStatus,
                 filePath: Path,
                 isSplitable: Boolean,
                 maxSplitBytes: Long,
                 partitionValues: InternalRow): Seq[PartitionedFile] = {
    if (YtSourceScanExec.currentThreadInstance.get() != null) {
      YtFilePartition.splitFiles(sparkSession, file, filePath, isSplitable, maxSplitBytes, partitionValues)
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
