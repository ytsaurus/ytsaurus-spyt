package tech.ytsaurus.spyt.common.utils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import tech.ytsaurus.client.rows.WireRowDeserializer
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt.format.YtInputSplit
import tech.ytsaurus.spyt.serializers.InternalRowDeserializer
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientConfiguration
import tech.ytsaurus.spyt.wrapper.table.{TableIterator, YtArrowInputStream, YtReadContext}

import java.time.Duration

/** Reads a split from its distributed-read partition when the file carries a cookie, otherwise from the path. */
object YtReadingUtils {
  def createRowIterator[T](
    split: YtInputSplit,
    path: YPath,
    deserializer: WireRowDeserializer[T],
    timeout: Duration,
    transaction: Option[String])(implicit ytReadContext: YtReadContext): TableIterator[T] = {
    split.file.delegate.cookie match {
      case Some(cookie) => YtWrapper.createTablePartitionReader(cookie, deserializer)
      case None => YtWrapper.readTable(path, deserializer, timeout, transaction)
    }
  }

  def createArrowStream(
    split: YtInputSplit,
    path: YPath,
    transaction: Option[String])(implicit ytReadContext: YtReadContext): YtArrowInputStream = {
    split.file.delegate.cookie match {
      case Some(cookie) => YtWrapper.createTablePartitionArrowStream(cookie)
      case None => YtWrapper.readTableArrowStream(path, transaction)
    }
  }

  def createRowBaseReader(split: YtInputSplit, transaction: Option[String] = None, resultSchema: StructType,
    ytClientConf: YtClientConfiguration)
    (implicit ytReadContext: YtReadContext): TableIterator[InternalRow] = {
    val deserializer = InternalRowDeserializer.getOrCreate(resultSchema)
    createRowIterator(split, split.ytPathWithFiltersDetailed, deserializer, ytClientConf.timeout, transaction)
  }
}
