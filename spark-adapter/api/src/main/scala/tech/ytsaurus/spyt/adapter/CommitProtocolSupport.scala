package tech.ytsaurus.spyt.adapter

import org.apache.spark.sql.SaveMode

import java.util.ServiceLoader

trait CommitProtocolSupport {
  def setSaveMode(mode: SaveMode): Unit
  def clearSaveMode(): Unit
}

object CommitProtocolSupport {
  lazy val instance: CommitProtocolSupport = ServiceLoader.load(classOf[CommitProtocolSupport]).findFirst().get()
}
