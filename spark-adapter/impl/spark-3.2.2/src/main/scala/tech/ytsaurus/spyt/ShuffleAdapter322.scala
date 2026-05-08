package tech.ytsaurus.spyt

import org.apache.spark.Partitioner
import org.apache.spark.sql.execution.ShuffleSupport

@MinSparkVersion("3.2.2")
class ShuffleAdapter322 extends ShuffleAdapter {

  override def createShufflePartitioner(numPartitions: Int): Partitioner =
    ShuffleSupport.createShufflePartitioner(numPartitions)
}
