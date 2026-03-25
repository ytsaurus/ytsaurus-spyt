package tech.ytsaurus.spyt.common.utils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import tech.ytsaurus.spyt.format.YtInputSplit
import tech.ytsaurus.spyt.serializers.InternalRowDeserializer
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientConfiguration
import tech.ytsaurus.spyt.wrapper.table.{TableIterator, YtReadContext}

object YtReadingUtils {
  def createRowBaseReader(split: YtInputSplit, transaction: Option[String] = None, resultSchema: StructType,
    ytClientConf: YtClientConfiguration)
    (implicit ytReadContext: YtReadContext): TableIterator[InternalRow] = {
    val deserializer = InternalRowDeserializer.getOrCreate(resultSchema)
    if (ytReadContext.settings.distributedReadingEnabled) {
      YtWrapper.createTablePartitionReader(
        split.file.delegate.cookie.get,
        deserializer
      )
    } else {
      YtWrapper.readTable(
        split.ytPathWithFiltersDetailed,
        deserializer,
        ytClientConf.timeout,
        transaction
      )
    }
  }
}
