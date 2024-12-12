package tech.ytsaurus.spyt

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import tech.ytsaurus.spyt.format.{YtPartitionedFile330, YtPartitioningDelegate}
import tech.ytsaurus.spyt.format.YtPartitioningSupport.YtPartitionedFileBase

@MinSparkVersion("3.3.0")
class PartitionedFileAdapter330 extends PartitionedFileAdapter {

  override def createPartitionedFile(partitionValues: InternalRow, filePath: String,
                                     start: Long, length: Long): PartitionedFile = {
    PartitionedFile(partitionValues, filePath, start, length)
  }

  override def createYtPartitionedFile[T <: YtPartitioningDelegate](delegate: T): YtPartitionedFileBase[T] = {
    new YtPartitionedFile330(delegate)
  }
}
