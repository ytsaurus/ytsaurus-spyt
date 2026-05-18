package tech.ytsaurus.spyt.format

import org.apache.spark.sql.execution.datasources.PartitionedFile

class YtPartitionedFile330[T <: YtPartitioningDelegate](override val delegate: T)
  extends PartitionedFile(delegate.partitionValues, delegate.filePath, delegate.start, delegate.byteLength)
    with YtPartitioningSupport[T]
