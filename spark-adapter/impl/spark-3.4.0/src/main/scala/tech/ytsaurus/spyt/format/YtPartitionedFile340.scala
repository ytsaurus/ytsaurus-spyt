package tech.ytsaurus.spyt.format

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.execution.datasources.PartitionedFile

class YtPartitionedFile340[T <: YtPartitioningDelegate](override val delegate: T)
  extends PartitionedFile(delegate.partitionValues, SparkPath.fromUrlString(delegate.filePath),
    delegate.start, delegate.byteLength) with YtPartitioningSupport[T]
