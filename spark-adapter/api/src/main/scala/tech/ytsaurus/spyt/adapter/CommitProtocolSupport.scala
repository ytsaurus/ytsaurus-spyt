package tech.ytsaurus.spyt.adapter

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.FileFormat

import java.util.ServiceLoader

trait CommitProtocolSupport {
  def validateWrite(sparkSession: SparkSession, fileFormat: FileFormat, options: Map[String, String]): Unit

  def setSaveMode(mode: SaveMode): Unit
  def clearSaveMode(): Unit
}

object CommitProtocolSupport {
  lazy val instance: CommitProtocolSupport = ServiceLoader.load(classOf[CommitProtocolSupport]).findFirst().get()
}
