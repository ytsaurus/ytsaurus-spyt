package org.apache.spark.shuffle.ytsaurus

import org.apache.commons.codec.binary.Hex
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.OptionalConfigEntry
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.ytsaurus.Config._
import org.apache.spark.shuffle.{BaseShuffleHandle, MetadataFetchFailedException, ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReadMetricsReporter, ShuffleReader, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId, ShuffleDataBlockId, ShuffleIndexBlockId, ShuffleMergedBlockId}
import tech.ytsaurus.client.{AsyncReader, CompoundClient}
import tech.ytsaurus.client.request.{CreateShuffleReader, CreateShuffleWriter, StartShuffle, ShuffleHandle => YTsaurusShuffleHandle}
import tech.ytsaurus.client.rows.UnversionedRow
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.ysontree.{YTreeNode, YTreeTextSerializer}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.concurrent.duration.Duration

class YTsaurusShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  private val ytClientProvider: YtClientProvider =
    conf.getOption("spark.ytsaurus.client.provider.class").map { className =>
      Class.forName(className).getDeclaredConstructor().newInstance().asInstanceOf[YtClientProvider]
    }.getOrElse(YtClientProvider)

  private val ytClientConf = ytClientConfiguration(conf)
  private implicit val ytClient: CompoundClient = ytClientProvider.ytClient(ytClientConf)
  private val delegate = new SortShuffleManager(conf)

  private val readConfigOpt: Option[YTreeNode] = ShuffleUtils.parseConfig(conf, YTSAURUS_SHUFFLE_READ_CONFIG)

  override def registerShuffle[K, V, C](shuffleId: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // IS CALLED ON DRIVER
    val baseHandle = delegate.registerShuffle(shuffleId, dependency).asInstanceOf[BaseShuffleHandle[K, V, C]]
    val shuffleTransactionTimeout = Duration(conf.get(YTSAURUS_SHUFFLE_TRANSACTION_TIMEOUT), TimeUnit.MILLISECONDS)
    val shuffleTransaction = YtWrapper.createTransaction(None, shuffleTransactionTimeout)
    val partitionCount = dependency.partitioner.numPartitions

    val reqBuilder = StartShuffle.builder()
      .setAccount(conf.get(YTSAURUS_SHUFFLE_ACCOUNT))
      .setPartitionCount(partitionCount)
      .setParentTransactionId(shuffleTransaction.getId)

    conf.get(YTSAURUS_SHUFFLE_MEDIUM).foreach(reqBuilder.setMedium)
    conf.get(YTSAURUS_SHUFFLE_REPLICATION_FACTOR).foreach(rf => reqBuilder.setReplicationFactor(rf))
    val ytShuffleHandle = ytClient.startShuffle(reqBuilder.build()).join()

    if (isTraceEnabled()) {
      logTrace(s"REGISTERED SHUFFLE: $shuffleId, DESCRIPTOR: ${ytShuffleHandle.getPayload}")
    }

    val handle = CompoundShuffleHandle(baseHandle, ytShuffleHandle, partitionCount)

    conf.set(shuffleTransactionId(shuffleId), shuffleTransaction.getId.toString)
    handle
  }

  override def getWriter[K, V](
    handle: ShuffleHandle,
    mapId: Long,
    context: TaskContext,
    metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    // IS CALLED ON EXECUTOR
    // mapId is task attempt id
    if (isTraceEnabled()) {
      val info =
        s"""
           |CREATE SHUFFLE WRITER
           |=======================
           |shuffleId = ${handle.shuffleId}
           |mapId = ${mapId}
           |stageId=${context.stageId()}
           |stageAttempNumber=${context.stageAttemptNumber()}
           |partitionId=${context.partitionId()}
           |numPartitions=${context.numPartitions()}
           |attemptNumber=${context.attemptNumber()}
           |taskAttemptId=${context.taskAttemptId()}
           |props=${context.getLocalProperties}
           |""".stripMargin
      logTrace(info)
    }

    val compoundHandle = handle.asInstanceOf[CompoundShuffleHandle[K, V, _]]
    YTsaurusShuffleExecutorComponents.currentHandle.set(compoundHandle)
    YTsaurusShuffleExecutorComponents.currentMapIndex.set(context.partitionId())
    delegate.getWriter[K, V](compoundHandle.baseHandle, mapId, context, metrics)
  }

  override def getReader[K, C](
    handle: ShuffleHandle,
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    // IS CALLED ON EXECUTOR
    val compoundHandle = handle.asInstanceOf[CompoundShuffleHandle[K, _, C]]
    // startPartition <= p < endPartition
    if (isTraceEnabled()) {
      val info = s"CREATING SHUFFLE READER: ${handle.shuffleId}, " +
        s"MAP: ($startMapIndex - $endMapIndex), " +
        s"PART: ($startPartition - $endPartition) " +
        s"STAGE: id=${context.stageId()} attempt=${context.stageAttemptNumber()} " +
        s"METRICS CLASS: ${metrics.getClass}"
      logTrace(info)
    }

    new YTsaurusShuffleReader(
      compoundHandle,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      context,
      metrics,
      ytClient,
      readConfigOpt
    )
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    logDebug(s"Unregistering shuffle: ${shuffleId}")
    try {
      val transactionId = conf.get(shuffleTransactionId(shuffleId))
      ytClient.abortTransaction(GUID.valueOf(transactionId)).join()
      conf.remove(shuffleTransactionId(shuffleId))
    } catch {
      case e: Exception =>
        logWarning("An exception was thrown while trying to abort shuffle transaction", e)
    }
    delegate.unregisterShuffle(shuffleId)
  }

  override def stop(): Unit = {
    try {
      ytClient.close()
    } finally {
      delegate.stop()
    }
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = delegate.shuffleBlockResolver
}
