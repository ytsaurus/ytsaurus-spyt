package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import tech.ytsaurus.spyt.adapter.CommitProtocolSupport
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand")
@Applicability(to = "4.0.0")
class InsertIntoHadoopFsRelationCommandDecorators {

  val mode: SaveMode = ???
  val fileFormat: FileFormat = ???
  val options: Map[String, String] = ???

  @DecoratedMethod
  def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    CommitProtocolSupport.instance.validateWrite(sparkSession, fileFormat, options)
    CommitProtocolSupport.instance.setSaveMode(mode)
    try {
      __run(sparkSession, child)
    } finally {
      CommitProtocolSupport.instance.clearSaveMode()
    }
  }

  def __run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = ???
}
