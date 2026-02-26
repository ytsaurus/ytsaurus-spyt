package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import tech.ytsaurus.spyt.adapter.CommitProtocolSupport
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand")
class InsertIntoHadoopFsRelationCommandDecorators {

  val mode: SaveMode = ???

  @DecoratedMethod
  def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    CommitProtocolSupport.instance.setSaveMode(mode)
    val result = __run(sparkSession, child)
    CommitProtocolSupport.instance.clearSaveMode()
    result
  }

  def __run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = ???
}
