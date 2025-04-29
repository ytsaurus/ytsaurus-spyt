package tech.ytsaurus.spyt.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, SparkContext, TaskContext}
import tech.ytsaurus.client.rows.QueueRowset
import tech.ytsaurus.spyt.serializers.UnversionedRowsetDeserializer
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider


class YtQueueRDD(sc: SparkContext,
                 schema: StructType,
                 consumerPath: String,
                 queuePath: String,
                 ranges: Seq[YtQueueRange],
                 val includeServiceColumns: Boolean = false) extends RDD[InternalRow](sc, Nil) {
  private val configuration = ytClientConfiguration(sc.hadoopConfiguration)

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val partition = split.asInstanceOf[YtQueueRange]
    val size = partition.upperIndex - partition.lowerIndex
    if (size > 0) {
      val yt = YtClientProvider.ytClient(configuration)
      val rowsets: Seq[QueueRowset] =
        YtWrapper.pullConsumerStrict(consumerPath, queuePath, partition.tabletIndex, partition.lowerIndex, size)(yt)
      val deserializer = new UnversionedRowsetDeserializer(schema)
      val proj = UnsafeProjection.create(schema)

      val deserializedRowset = if (includeServiceColumns) {
        rowsets.map(rowset => deserializer.deserializeQueueRowsetWithServiceColumns(rowset))
      } else {
        rowsets.map(rowset => deserializer.deserializeRowset(rowset))
      }

      deserializedRowset
        .foldLeft(Iterator[InternalRow]())(_ ++ _)
        .map(proj)
    } else {
      Seq.empty.iterator
    }

  }

  override protected def getPartitions: Array[Partition] = ranges.sortBy(_.tabletIndex).toArray
}
