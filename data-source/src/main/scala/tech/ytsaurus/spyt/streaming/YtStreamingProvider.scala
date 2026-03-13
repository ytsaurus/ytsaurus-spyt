package tech.ytsaurus.spyt.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

trait YtStreamingProvider {
  def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String,
    parameters: Map[String, String]): (String, StructType)

  def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType],
    providerName: String, parameters: Map[String, String]): Source

  def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String],
    outputMode: OutputMode): Sink
}
