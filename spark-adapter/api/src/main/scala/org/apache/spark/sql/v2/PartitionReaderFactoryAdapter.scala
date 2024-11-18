package org.apache.spark.sql.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.vectorized.ColumnarBatch

trait PartitionReaderFactoryAdapter {
  def supportColumnarReads(partition: InputPartition): Boolean
  def buildReader(file: PartitionedFile): PartitionReader[InternalRow]
  def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch]
  def options: Map[String, String]
}
