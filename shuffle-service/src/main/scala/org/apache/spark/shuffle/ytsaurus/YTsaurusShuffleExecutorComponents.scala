package org.apache.spark.shuffle.ytsaurus

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter}
import org.apache.spark.shuffle.ytsaurus.Config._
import org.apache.spark.shuffle.ytsaurus.YTsaurusShuffleExecutorComponents.{currentHandle, currentMapIndex}
import tech.ytsaurus.client.request.CreateShuffleWriter
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.shuffle.YTsaurusShuffleMapOutputWriter
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider
import tech.ytsaurus.ysontree.YTreeNode

import java.util

class YTsaurusShuffleExecutorComponents(sparkConf: SparkConf) extends ShuffleExecutorComponents {

  private val ytClientConf = ytClientConfiguration(sparkConf)
  private implicit val ytsaurusClient: CompoundClient = YtClientProvider.ytClient(ytClientConf)

  private val writeConfigOpt: Option[YTreeNode] = ShuffleUtils.parseConfig(sparkConf, YTSAURUS_SHUFFLE_WRITE_CONFIG)

  override def initializeExecutor(appId: String, execId: String, extraConfigs: util.Map[String, String]): Unit = ()

  override def createMapOutputWriter(shuffleId: Int, mapTaskId: Long, numPartitions: Int): ShuffleMapOutputWriter = {
    val handle = currentHandle.get()
    currentHandle.remove()
    val mapIndex = currentMapIndex.get()
    currentMapIndex.remove()

    val reqBuilder = CreateShuffleWriter.builder()
      .setHandle(handle.ytHandle)
      .setPartitionColumn(sparkConf.get(YTSAURUS_SHUFFLE_PARTITION_COLUMN))
      .setWriterIndex(mapIndex)
      .setOverwriteExistingWriterData(true)

    writeConfigOpt.foreach(reqBuilder.setConfig)

    val rowSize = sparkConf.get(YTSAURUS_SHUFFLE_WRITE_ROW_SIZE).toInt
    val bufferSize = sparkConf.get(YTSAURUS_SHUFFLE_WRITE_BUFFER_SIZE)

    val ytsaurusWriter = ytsaurusClient.createShuffleWriter(reqBuilder.build).join
    new YTsaurusShuffleMapOutputWriter(ytsaurusWriter, shuffleId, mapTaskId, numPartitions, rowSize, bufferSize)
  }
}

object YTsaurusShuffleExecutorComponents {
  // These two variables are needed to pass parameters from YTsaurusShuffleManager to createMapOutputWriter method
  val currentHandle: ThreadLocal[CompoundShuffleHandle[_, _, _]] = new ThreadLocal()
  val currentMapIndex: ThreadLocal[Int] = new ThreadLocal()
}