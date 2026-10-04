package tech.ytsaurus.spyt.adapter

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.v2.YtDataSourceV2
import org.apache.spark.sql.vectorized.YtFileFormat

import tech.ytsaurus.spyt.format.YtOutputCommitProtocol

class YTsaurusCommitProtocolSupport extends CommitProtocolSupport {

  override def validateWrite(
    sparkSession: SparkSession,
    fileFormat: FileFormat,
    options: Map[String, String]): Unit = {
    if (fileFormat.isInstanceOf[YtFileFormat]) {
      YtDataSourceV2.validateWriteTransactionDefault(sparkSession.sessionState.conf, options)
    }
  }

  override def setSaveMode(mode: SaveMode): Unit = {
    YtOutputCommitProtocol.saveModeTL.set(mode)
  }

  override def clearSaveMode(): Unit = {
    YtOutputCommitProtocol.saveModeTL.remove()
  }
}
