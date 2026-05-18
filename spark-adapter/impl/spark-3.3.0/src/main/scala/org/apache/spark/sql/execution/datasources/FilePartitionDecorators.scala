package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.SparkSession
import tech.ytsaurus.spyt.adapter.StorageSupport.{instance => ssi}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.datasources.FilePartition$")
object FilePartitionDecorators {

  @DecoratedMethod
  def maxSplitBytes(sparkSession: SparkSession, selectedPartitions: Seq[PartitionDirectory]): Long = {
    if (ssi.shouldUseYtSplitFiles()) {
      ssi.maxSplitBytes(sparkSession, selectedPartitions)
    } else {
      __maxSplitBytes(sparkSession, selectedPartitions)
    }
  }

  def __maxSplitBytes(sparkSession: SparkSession, selectedPartitions: Seq[PartitionDirectory]): Long = ???

}
