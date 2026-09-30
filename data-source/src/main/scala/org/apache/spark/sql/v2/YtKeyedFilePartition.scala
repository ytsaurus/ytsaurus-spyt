package org.apache.spark.sql.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.HasPartitionKey
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}

/** A file partition that reports its partition key to Spark, e.g. the bucket number of a hash-bucketed scan. */
class YtKeyedFilePartition(partitionIndex: Int, partitionFiles: Array[PartitionedFile], key: InternalRow)
  extends FilePartition(partitionIndex, partitionFiles) with HasPartitionKey {

  override def partitionKey(): InternalRow = key

  override def copy(index: Int, files: Array[PartitionedFile]): FilePartition = {
    new YtKeyedFilePartition(index, files, key)
  }
}
