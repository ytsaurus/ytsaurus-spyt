package tech.ytsaurus.spyt
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.v2.{PartitionReaderFactoryAdapter, YtPartitionReaderFactory322}

@MinSparkVersion("3.2.2")
class YtPartitionReaderFactoryCreator322 extends YtPartitionReaderFactoryCreator {

  override def createYtPartitionReaderFactory(adapter: PartitionReaderFactoryAdapter): PartitionReaderFactory =
    YtPartitionReaderFactory322(adapter)
}
