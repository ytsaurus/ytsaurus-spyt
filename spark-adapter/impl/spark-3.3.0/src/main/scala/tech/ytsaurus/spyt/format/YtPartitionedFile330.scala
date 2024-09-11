package tech.ytsaurus.spyt.format

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile

// This class seems identical to YtPartitionedFile322, but it uses 7-arg constructor of the superclass instead of
// 5-arg constructor in 3.2.2. Thus we were forced to create two classes that look identical.
class YtPartitionedFile330[T <: YtPartitioningDelegate](override val delegate: T)
  extends PartitionedFile(delegate.partitionValues, delegate.filePath, delegate.start, delegate.byteLength)
    with YtPartitioningSupport[T]
