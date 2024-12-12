package org.apache.spark

object ShuffleSupport {
  def createShufflePartitioner(numPartitions: Int): Partitioner =
    new PartitionIdPassthrough(numPartitions)
}
