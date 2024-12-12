package tech.ytsaurus.spyt
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import tech.ytsaurus.spyt.format.{YtPartitionedFile322, YtPartitioningDelegate}
import tech.ytsaurus.spyt.format.YtPartitioningSupport.YtPartitionedFileBase

@MinSparkVersion("3.2.2")
class PartitionedFileAdapter322 extends PartitionedFileAdapter {
  override def createPartitionedFile(partitionValues: InternalRow, filePath: String,
                                     start: Long, length: Long): PartitionedFile = {
    PartitionedFile(partitionValues, filePath, start, length)
  }

  override def createYtPartitionedFile[T <: YtPartitioningDelegate](delegate: T): YtPartitionedFileBase[T] = {
    new YtPartitionedFile322(delegate)
  }
}
