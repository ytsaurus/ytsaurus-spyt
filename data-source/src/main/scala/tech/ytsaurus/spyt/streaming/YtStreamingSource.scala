package tech.ytsaurus.spyt.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{CompositeReadLimit, ReadLimit, ReadMaxRows, SupportsAdmissionControl}
import org.apache.spark.sql.execution.StreamingUtils
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider


class YtStreamingSource(sqlContext: SQLContext,
                        consumerPath: String,
                        queuePath: String,
                        val schema: StructType,
                        parameters: Map[String, String]) extends Source with Logging with SupportsAdmissionControl {

  private implicit val yt: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(sqlContext.sparkSession))
  private val cluster = YtWrapper.clusterName()
  private val includeServiceColumns = parameters.get("include_service_columns").exists(_.toBoolean)

  override def getDefaultReadLimit: ReadLimit = parameters.get("max_rows_per_partition") match {
    case Some(maxRows) => ReadLimit.compositeLimit(Array(ReadLimit.maxRows(maxRows.toLong)))
    case None => ReadLimit.allAvailable()
  }

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    val maxRows = limit match {
      case c: CompositeReadLimit => c.getReadLimits.collectFirst { case r: ReadMaxRows => r.maxRows() }
      case r: ReadMaxRows => Some(r.maxRows())
      case _ => None
    }

    val maxOffset = YtQueueOffset.getMaxOffset(cluster, queuePath).get

    if (maxRows.isEmpty) {return maxOffset}

    val startOffsetParsed = Option(startOffset).map(YtQueueOffset.apply)
      .getOrElse(YtQueueOffset.getCurrentOffset(cluster, consumerPath, queuePath))

    val partitions = maxOffset.partitions.map { case (i, upper) =>
      val start = startOffsetParsed.partitions.getOrElse(i, -1L)
      i -> math.min(start + maxRows.get, upper)
    }
    YtQueueOffset(cluster, queuePath, partitions)
  }

  override def getOffset: Option[Offset] = None

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val rdd = if (start.isDefined && start.get == end) {
      sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty")
    } else {
      val currentOffset = YtQueueOffset.getCurrentOffset(cluster, consumerPath, queuePath)
      val preparedStart = start.map(YtQueueOffset.apply).getOrElse(currentOffset)
      require(preparedStart >= currentOffset, "Committed offset was queried")
      val preparedEnd = YtQueueOffset(end)
      require(preparedEnd >= preparedStart, "Batch end is less than batch start")
      val ranges = YtQueueOffset.getRanges(preparedStart, preparedEnd)
      new YtQueueRDD(sqlContext.sparkContext, schema, consumerPath, queuePath, ranges, includeServiceColumns).setName("yt")
    }
    StreamingUtils.createStreamingDataFrame(sqlContext, rdd, schema)
  }

  override def commit(end: Offset): Unit = {
    try {
      YtQueueOffset.advance(consumerPath, YtQueueOffset(end))
    } catch {
      case e: Throwable =>
        logWarning("Error in committing new offset", e)
    }
  }

  override def stop(): Unit = {
    logDebug("Close YtStreamingSource")
  }
}
