package tech.ytsaurus.spyt.common.utils

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.v2.YtUtils.bytesReadReporter
import org.apache.spark.util.SerializableConfiguration
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.format.YtInputSplit
import tech.ytsaurus.spyt.serializers.InternalRowDeserializer
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientConfiguration
import tech.ytsaurus.spyt.wrapper.table.TableIterator

object YtReadingUtils {
  def createRowBaseReader(split: YtInputSplit, transaction: Option[String] = None, resultSchema: StructType,
                          ytClientConf: YtClientConfiguration, broadcastedConf: Broadcast[SerializableConfiguration],
                          distributedReadingEnabled: Boolean)(implicit yt: CompoundClient): TableIterator[InternalRow] = {
    val deserializer = InternalRowDeserializer.getOrCreate(resultSchema)
    if (distributedReadingEnabled) {
      YtWrapper.createTablePartitionReader(
        split.file.delegate.cookie.get,
        deserializer
      )
    } else {
      YtWrapper.readTable(
        split.ytPathWithFiltersDetailed,
        deserializer,
        ytClientConf.timeout, transaction,
        bytesReadReporter(broadcastedConf)
      )
    }
  }
}
