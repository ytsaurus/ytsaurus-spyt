package tech.ytsaurus.spyt.format

import com.google.common.base.Charsets
import com.google.protobuf.ByteString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request.DistributedWriteCookie
import tech.ytsaurus.spyt.format.conf.{SparkYtWriteConfiguration, YtTableSparkSettings}
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.serializers.{SchemaConverter, WriteSchemaConverter}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.{YtClientConfiguration, YtClientProvider}

class YtOutputWriterFactory(ytClientConf: YtClientConfiguration,
                            writeConfiguration: SparkYtWriteConfiguration,
                            options: Map[String, String]) extends OutputWriterFactory {
  import YtOutputWriterFactory._

  override def getFileExtension(context: TaskAttemptContext): String = ""

  override def newInstance(f: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
    log.debug(s"[${Thread.currentThread().getName}] Creating new output writer for ${context.getTaskAttemptID.getTaskID} at path $f")

    implicit val ytClient: CompoundClient = YtClientProvider.ytClient(ytClientConf, Some(WriteStatisticsReporter))

    if (writeConfiguration.distributedWrite) {
      val cookie = YtOutputCommitProtocol.getCookieForTask(context)
      return new YtDistributedOutputWriter(f, cookie, dataSchema, writeConfiguration, options)
    }

    val path = YPathEnriched.fromPath(new Path(f))

    if (YtWrapper.isDynamicTable(path.toStringYPath)) {
      new YtDynamicTableWriter(path, dataSchema, writeConfiguration, options)
    } else {
      val transaction = YtOutputCommitProtocol.getWriteTransaction(context.getConfiguration)
      val richPath = path.withTransaction(transaction)
      new YtOutputWriter(richPath, dataSchema, writeConfiguration, options)
    }
  }
}

object YtOutputWriterFactory {
  private val log = LoggerFactory.getLogger(getClass)

  def create(writeConfiguration: SparkYtWriteConfiguration,
             ytClientConf: YtClientConfiguration,
             options: Map[String, String],
             dataSchema: StructType,
             jobConfiguration: Configuration): YtOutputWriterFactory = {
    SchemaConverter.checkSchema(dataSchema, options)

    val updatedOptions = addWriteOptions(options, writeConfiguration)
    YtTableSparkSettings.serialize(
      updatedOptions, WriteSchemaConverter(updatedOptions).ytLogicalTypeStruct(dataSchema), jobConfiguration
    )

    new YtOutputWriterFactory(ytClientConf, writeConfiguration, updatedOptions)
  }

  private def addWriteOptions(options: Map[String, String],
                              writeConfiguration: SparkYtWriteConfiguration): Map[String, String] = {
    import YtTableSparkSettings.WriteTypeV3
    if (options.contains(WriteTypeV3.name)) options
    else options + (WriteTypeV3.name -> writeConfiguration.typeV3Format.toString)
  }
}
