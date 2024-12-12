package tech.ytsaurus.spyt
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.v2.{PartitionReaderFactoryAdapter, YtPartitionReaderFactory340}

@MinSparkVersion("3.4.0")
class YtPartitionReaderFactoryCreator340 extends YtPartitionReaderFactoryCreator {

  override def createYtPartitionReaderFactory(adapter: PartitionReaderFactoryAdapter): PartitionReaderFactory = {
    YtPartitionReaderFactory340(adapter)
  }
}
