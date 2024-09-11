package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.v2.YtFilePartition
import org.apache.spark.sql.yt.YtSourceScanExec
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.datasources.FilePartition")
object FilePartitionDecorators {

  @DecoratedMethod
  def maxSplitBytes(sparkSession: SparkSession, selectedPartitions: Seq[PartitionDirectory]): Long = {
    val ytSourceScanExec = YtSourceScanExec.currentThreadInstance.get()
    if (ytSourceScanExec != null) {
      YtFilePartition.maxSplitBytes(sparkSession, selectedPartitions, ytSourceScanExec.maybeReadParallelism)
    } else {
      __maxSplitBytes(sparkSession, selectedPartitions)
    }
  }

  def __maxSplitBytes(sparkSession: SparkSession, selectedPartitions: Seq[PartitionDirectory]): Long = ???

}
