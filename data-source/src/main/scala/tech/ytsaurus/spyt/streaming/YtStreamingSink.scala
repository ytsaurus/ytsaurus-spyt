package tech.ytsaurus.spyt.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}
import tech.ytsaurus.spyt.format.YtDynamicTableWriter
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.wrapper.client.{YtClientConfigurationConverter, YtClientProvider}

import java.util.UUID

class YtStreamingSink(sqlContext: SQLContext,
                      queuePath: String,
                      parameters: Map[String, String]) extends Sink with Logging {
  @volatile private var latestBatchId = -1L

  override def toString: String = "YtStreamingSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val sparkContext = sqlContext.sparkSession.sparkContext

      val ytClientConfiguration = YtClientConfigurationConverter.ytClientConfiguration(sparkContext.getConf)
      val bcYtClientConfiguration = sparkContext.broadcast(ytClientConfiguration)

      val wConfig = SparkYtWriteConfiguration(sqlContext).copy(dynBatchSize = Int.MaxValue)
      val bcWriterConfig = sparkContext.broadcast(wConfig)

      val path = YPathEnriched.fromPath(new Path(queuePath))
      val bcPath = sparkContext.broadcast(path)
      val bcParameters = sparkContext.broadcast(parameters)
      val bcSchema = sparkContext.broadcast(data.schema)

      data.queryExecution.toRdd.foreachPartition { partitionIterator =>
        val partitionYtClient = YtClientProvider.ytClient(bcYtClientConfiguration.value,
          s"YtStreamingPartition-${UUID.randomUUID()}")
        val dynamicTableWriter = new YtDynamicTableWriter(bcPath.value, bcSchema.value, bcWriterConfig.value,
          bcParameters.value)(partitionYtClient)

        try {
          partitionIterator.foreach { row => dynamicTableWriter.write(row) }
        } finally {
          dynamicTableWriter.close()
        }
      }

      latestBatchId = batchId
    }
  }
}
