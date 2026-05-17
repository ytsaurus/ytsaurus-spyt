package tech.ytsaurus.spyt.streaming

import org.apache.hadoop.fs.Path
import tech.ytsaurus.spyt.logging.Logging
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.format.YtDynamicTableWriter
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.{YtClientConfigurationConverter, YtClientProvider}

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

      val wConfig = SparkYtWriteConfiguration(sqlContext)
      val bcWriterConfig = sparkContext.broadcast(wConfig)

      val path = YPathEnriched.fromPath(new Path(queuePath))
      val bcPath = sparkContext.broadcast(path)
      val bcParameters = sparkContext.broadcast(parameters)
      val bcSchema = sparkContext.broadcast(data.schema)

      val txContext = YtStreamingTransactionContext.get
      val bcParentTransactionId = txContext.map(ctx => sparkContext.broadcast(ctx.txId))
      val bcStickyProxyAddress = txContext.flatMap(_.stickyAddress).map(sparkContext.broadcast)

      data.queryExecution.toRdd.foreachPartition { partitionIterator =>
        if (partitionIterator.hasNext) {
          val txId = bcParentTransactionId.map(_.value)
          val stickyAddr = bcStickyProxyAddress.map(_.value)
          implicit val partitionYtClient: CompoundClient = stickyAddr match {
            case Some(addr) => YtClientProvider.ytClient(bcYtClientConfiguration.value.copy(fixedProxyAddress = Some(addr)))
            case None => YtClientProvider.ytClient(bcYtClientConfiguration.value)
          }
          val attachedTx = txId.map(id => YtWrapper.attachTransaction(id))
          val dynamicTableWriter = new YtDynamicTableWriter(bcPath.value, bcSchema.value, bcWriterConfig.value,
            bcParameters.value, attachedTx)
          try {
            partitionIterator.foreach { row => dynamicTableWriter.write(row) }
          } finally {
            dynamicTableWriter.close()
          }
        }
      }
      latestBatchId = batchId
    }
  }
}
