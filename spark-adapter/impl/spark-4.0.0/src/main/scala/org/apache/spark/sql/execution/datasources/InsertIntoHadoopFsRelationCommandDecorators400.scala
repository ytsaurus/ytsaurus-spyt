package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{Row, SaveMode}
import tech.ytsaurus.spyt.adapter.CommitProtocolSupport
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand")
@Applicability(from = "4.0.0")
class InsertIntoHadoopFsRelationCommandDecorators400 {
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
