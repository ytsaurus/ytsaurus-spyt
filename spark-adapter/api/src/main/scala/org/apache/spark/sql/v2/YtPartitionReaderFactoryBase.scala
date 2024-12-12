package org.apache.spark.sql.v2

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class YtPartitionReaderFactoryBase(adapter: PartitionReaderFactoryAdapter)
  extends FilePartitionReaderFactory with Logging {

  override def supportColumnarReads(partition: InputPartition): Boolean =
    adapter.supportColumnarReads(partition)

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] =
    adapter.buildReader(file)

  override def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch] =
    adapter.buildColumnarReader(file)
}
