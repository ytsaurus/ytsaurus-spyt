package tech.ytsaurus.spyt.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.StreamingUtils
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.v2.YtUtils
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.core.cypress.{RichYPath, YPath}
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

class YtStreamingProviderImpl extends YtStreamingProvider {

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String,
    parameters: Map[String, String]): (String, StructType) = {
    val caseInsensitiveParameters = CaseInsensitiveMap(parameters)
    val ypath = RichYPath.fromString(caseInsensitiveParameters(YtUtils.Options.QUEUE_PATH))
    val resultSchema = StreamingUtils.getStreamingSourceSchema(
      sqlContext.sparkSession, ypath, None, None, caseInsensitiveParameters)
    (providerName, resultSchema)
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType],
    providerName: String, parameters: Map[String, String]): Source = {
    val caseInsensitiveParameters = CaseInsensitiveMap(parameters)
    val consumerPath = caseInsensitiveParameters(YtUtils.Options.CONSUMER_PATH)
    val queuePath = caseInsensitiveParameters(YtUtils.Options.QUEUE_PATH)
    val requiredSchema = schema.getOrElse {
      StreamingUtils.getStreamingSourceSchema(
        sqlContext.sparkSession, YPath.simple(queuePath), None, None, caseInsensitiveParameters)
    }
    implicit val yt: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(sqlContext.sparkSession))
    new YtStreamingSource(sqlContext, consumerPath, queuePath, requiredSchema, caseInsensitiveParameters)
  }

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String],
    outputMode: OutputMode): Sink = {
    require(outputMode == OutputMode.Append(), "Only append mode is supported now")
    val caseInsensitiveParameters = CaseInsensitiveMap(parameters)
    val richQueuePath = RichYPath.fromString(caseInsensitiveParameters(YtUtils.Options.QUEUE_PATH))
    new YtStreamingSink(sqlContext, richQueuePath.justPath().toStableString, caseInsensitiveParameters)
  }
}
