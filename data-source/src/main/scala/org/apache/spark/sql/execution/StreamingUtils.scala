package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.v2.YtUtils
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import tech.ytsaurus.core.cypress.YPath

object StreamingUtils {
  val STREAMING_SERVICE_KEY_COLUMNS_PREFIX = "__spyt_streaming_src_"

  private val additionalKeysSchemaForStreaming = StructType(Seq(
    StructField(s"${STREAMING_SERVICE_KEY_COLUMNS_PREFIX}tablet_index", LongType, nullable = true),
    StructField(s"${STREAMING_SERVICE_KEY_COLUMNS_PREFIX}row_index", LongType, nullable = true)
  ))

  def createStreamingDataFrame(sqlContext: SQLContext, rdd: RDD[InternalRow], schema: StructType): DataFrame = {
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  def getStreamingSourceSchema(sparkSession: SparkSession, path: YPath, transaction: Option[String], proxy: Option[String],
                               parameters: Map[String, String]): StructType = {
    val valuesSchema = YtUtils.getSchema(sparkSession, path, transaction, proxy, parameters)
    val includeServiceColumns = parameters.get("include_service_columns").exists(_.toBoolean)
    if (includeServiceColumns) createExtendedStreamingSchema(valuesSchema) else valuesSchema
  }

  def createExtendedStreamingSchema(valuesSchema: StructType): StructType = {
    StructType(additionalKeysSchemaForStreaming.fields ++ valuesSchema.fields)
  }
}
