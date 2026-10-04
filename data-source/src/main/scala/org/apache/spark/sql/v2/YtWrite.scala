package org.apache.spark.sql.v2

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo}
import org.apache.spark.sql.execution.datasources.v2.FileWrite
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

import tech.ytsaurus.spyt.format.YtOutputWriterFactory
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration
import tech.ytsaurus.spyt.logging.Logging
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration

import scala.jdk.CollectionConverters._

case class YtWrite(paths: Seq[String],
                   formatName: String,
                   supportsDataType: DataType => Boolean,
                   info: LogicalWriteInfo)
  extends FileWrite with Logging {

  override def toBatch: BatchWrite = {
    YtDataSourceV2.validateWriteTransactionDefault(
      SparkSession.active.sessionState.conf,
      info.options().asCaseSensitiveMap().asScala.toMap)
    super.toBatch
  }

  override def prepareWrite(
    sqlConf: SQLConf,
    job: Job,
    options: Map[String, String],
    dataSchema: StructType): OutputWriterFactory = {
    YtOutputWriterFactory.create(
      SparkYtWriteConfiguration(sqlConf),
      ytClientConfiguration(sqlConf),
      options,
      dataSchema,
      job.getConfiguration)
  }
}
