package org.apache.spark.sql.v2

import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

case class YtPartitionReaderFactory340(adapter: PartitionReaderFactoryAdapter)
  extends YtPartitionReaderFactoryBase(adapter) {
  protected override def options: FileSourceOptions =
    new FileSourceOptions(CaseInsensitiveMap[String](adapter.options))
}
