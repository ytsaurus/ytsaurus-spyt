package tech.ytsaurus.spyt.format

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile

trait YtPartitioningSupport[T <: YtPartitioningDelegate] { this: PartitionedFile =>
  val delegate: T
  def path: String = filePath
}

object YtPartitioningSupport {
  type YtPartitionedFileBase[T <: YtPartitioningDelegate] = PartitionedFile with YtPartitioningSupport[T]
}

trait YtPartitioningDelegate extends Serializable {
  val partitionValues: InternalRow
  val filePath: String
  val start: Long
  val byteLength: Long
}
