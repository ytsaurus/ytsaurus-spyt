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

import scala.util.{Failure, Success}

class YtStreamingSource(sqlContext: SQLContext,
                        consumerPath: String,
                        queuePath: String,
                        val schema: StructType,
                        parameters: Map[String, String],
                        offsetProvider: YtQueueOffsetProviderTrait = YtQueueOffsetProvider)
                       (implicit val yt: CompoundClient) extends Source with Logging with SupportsAdmissionControl {

  protected[streaming] lazy val cluster: String = YtWrapper.clusterName()
  private val includeServiceColumns = parameters.get("include_service_columns").exists(_.toBoolean)

  private var lastCommittedOffset: YtQueueOffset = getLastCommittedOffset
  private var maxOffset: Option[YtQueueOffset] = None

  protected[streaming] def getMaxOffset: Option[YtQueueOffset] = {
    logDebug(s"Get offset for $queuePath")

    offsetProvider.getMaxOffset(cluster, queuePath) match {
      case Success(newMaxOffset) =>
        if (!(newMaxOffset >= lastCommittedOffset)) {
          return None // to show that there is no data to process
        } else {
          if (maxOffset.isDefined && !(newMaxOffset >= maxOffset.get)) {
            logWarning(s"New upper index < old upper index: $newMaxOffset < ${maxOffset.get}. Using cached value.")
          } else {
            maxOffset = Some(newMaxOffset)
          }
        }
      case Failure(exception) =>
        logWarning(s"Failed to get new max offset from provider, using cached value. Error: ${exception.getMessage}", exception)

    }
    maxOffset
  }

  protected[streaming] def getLastCommittedOffset: YtQueueOffset = {
    logDebug(s"Get current offset for $consumerPath")
    lastCommittedOffset = offsetProvider.getCurrentOffset(cluster, consumerPath, queuePath)
    lastCommittedOffset
  }

  override def getDefaultReadLimit: ReadLimit = parameters.get("max_rows_per_partition") match {
    case Some(maxRows) => ReadLimit.compositeLimit(Array(ReadLimit.maxRows(maxRows.toLong)))
    case None => ReadLimit.allAvailable()
  }

  override def getOffset: Option[Offset] = throw new UnsupportedOperationException(
    "latestOffset(Offset, ReadLimit) should be called instead of this method")

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    val lastOffsetCachedBySpark: Option[YtQueueOffset] = Option(startOffset).map(YtQueueOffset.apply)

    val startOffsetParsed = if (lastOffsetCachedBySpark.isEmpty) {
      getLastCommittedOffset
    } else if (lastOffsetCachedBySpark.get >= lastCommittedOffset) {
      lastOffsetCachedBySpark.get
    } else {
      logWarning(s"Offset cached by Spark is less than offset from consumer: " +
        s"${lastOffsetCachedBySpark.get.partitions} < ${lastCommittedOffset.partitions}. Using last committed offset.")
      lastCommittedOffset
    }

    val newMaxOffsetOpt = getMaxOffset
    if (newMaxOffsetOpt.isEmpty) {
      logWarning("No data to process")
      return null // to show that there is no data to process
    }

    val maxOffset = newMaxOffsetOpt.get

    val maxRows = limit match {
      case c: CompositeReadLimit => c.getReadLimits.collectFirst { case r: ReadMaxRows => r.maxRows() }
      case r: ReadMaxRows => Some(r.maxRows())
      case _ => None
    }

    if (maxRows.isEmpty) {
      return maxOffset
    }

    val partitions = maxOffset.partitions.map { case (i, upper) =>
      val start = startOffsetParsed.partitions.getOrElse(i, -1L)
      i -> math.min(start + maxRows.get, upper)
    }
    YtQueueOffset(cluster, queuePath, partitions)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val rdd = if (start.isDefined && start.get == end) {
      logInfo("return empty RDD")
      sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty")
    } else {
      val currentOffset = getLastCommittedOffset
      val preparedStart = start.map(YtQueueOffset.apply).filter(_ >= currentOffset).getOrElse(currentOffset)
      val preparedEnd = YtQueueOffset(end)

      if (!(preparedEnd >= preparedStart)) {
        logWarning(f"Batch end is less than batch start. " +
          f"${preparedEnd.partitions} < ${preparedStart.partitions} => return emptyRDD")
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty")
      } else {
        val ranges = YtQueueOffset.getRanges(preparedStart, preparedEnd)
        new YtQueueRDD(sqlContext.sparkContext, schema, consumerPath, queuePath, ranges, includeServiceColumns).setName("yt")
      }
    }
    StreamingUtils.createStreamingDataFrame(sqlContext, rdd, schema)
  }

  override def commit(end: Offset): Unit = {
    try {
      offsetProvider.advance(consumerPath, YtQueueOffset(end), lastCommittedOffset, maxOffset)
    } catch {
      case e: Throwable =>
        logWarning("Error in committing new offset", e)
    }
  }

  override def stop(): Unit = {
    logDebug("Close YtStreamingSource")
  }
}
