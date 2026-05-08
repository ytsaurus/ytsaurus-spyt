package tech.ytsaurus.spyt

import org.apache.spark.{Partitioner, ShuffleSupport}

@MinSparkVersion("3.4.0")
class ShuffleAdapter340 extends ShuffleAdapter {

  override def createShufflePartitioner(numPartitions: Int): Partitioner =
    ShuffleSupport.createShufflePartitioner(numPartitions)
}
