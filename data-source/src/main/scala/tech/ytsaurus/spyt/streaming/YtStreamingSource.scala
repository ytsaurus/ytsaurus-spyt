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

import java.util.UUID
import scala.collection.SortedMap
import scala.util.{Failure, Success}


class YtStreamingSource(sqlContext: SQLContext,
                        consumerPath: String,
                        queuePath: String,
                        val schema: StructType,
                        parameters: Map[String, String]) extends Source with Logging with SupportsAdmissionControl {

  private val id: String = s"YtStreamingSource-${UUID.randomUUID()}"
  private implicit val yt: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(sqlContext.sparkSession), id)
  private val cluster = YtWrapper.clusterName()

  private var latestOffset: Option[YtQueueOffset] = None

  override def getDefaultReadLimit: ReadLimit = {
    parameters.get("max_rows_per_partition") match {
      case Some(maxRows) => ReadLimit.compositeLimit(Array(ReadLimit.maxRows(maxRows.toLong)))
      case None => ReadLimit.allAvailable()
    }
  }

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    val limits: Seq[ReadLimit] = limit match {
      case rows: CompositeReadLimit => rows.getReadLimits
      case rows => Seq(rows)
    }

    val maxOffsetInQueue = getOffset.get.asInstanceOf[YtQueueOffset]

    val maxRowsOpt = limits.collectFirst { case rmr: ReadMaxRows => rmr }
    if (maxRowsOpt.isDefined) {
      val maxRows = maxRowsOpt.get.maxRows()
      val partitionSeq = maxOffsetInQueue.partitions.toSeq.map { case (i, upperIndex) =>
        val realStartIndex = startOffset match {
          case YtQueueOffset(_, _, partitions) => partitions.getOrElse(i, -1L)
          case _ => -1L
        }

        val endOffset = math.min(realStartIndex + maxRows, upperIndex)
        (i, endOffset)
      }
      YtQueueOffset(cluster, queuePath, SortedMap(partitionSeq: _*))
    } else {
      maxOffsetInQueue
    }
  }

  override def getOffset: Option[Offset] = {
    logDebug(s"Get offset for $queuePath")
    val newOffset = YtQueueOffset.getMaxOffset(cluster, queuePath)
    newOffset match {
      case Success(value) =>
        latestOffset = Some(value)
        latestOffset
      case Failure(exception) =>
        // Fault-tolerance for temporal table issues.
        logWarning("Error while getting new offset", exception)
        latestOffset.orElse(throw new IllegalStateException("Latest and new offsets are lost"))
    }
  }

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
      new YtQueueRDD(sqlContext.sparkContext, schema, consumerPath, queuePath, ranges).setName("yt")
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
    YtClientProvider.close(id)
  }


}
