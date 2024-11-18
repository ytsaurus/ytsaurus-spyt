package org.apache.spark.sql.execution

import org.apache.spark.Partitioner

object ShuffleSupport {
  def createShufflePartitioner(numPartitions: Int): Partitioner =
    new PartitionIdPassthrough(numPartitions)
}
