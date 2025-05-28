package org.apache.spark.shuffle.ytsaurus

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit

import java.util.concurrent.TimeUnit

object Config {

  val YTSAURUS_SHUFFLE_TRANSACTION_TIMEOUT = ConfigBuilder("spark.ytsaurus.shuffle.transaction.timeout")
    .doc("Timeout for parent YTsaurus shuffle transaction")
    .version("2.7.0")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("5m")

  val YTSAURUS_SHUFFLE_ACCOUNT = ConfigBuilder("spark.ytsaurus.shuffle.account")
    .doc("YTsaurus account to be used for storing shuffle data on cypress.")
    .version("2.7.0")
    .stringConf
    .createWithDefault("intermediate")

  val YTSAURUS_SHUFFLE_MEDIUM = ConfigBuilder("spark.ytsaurus.shuffle.medium")
    .doc("YTsaurus medium to be used for storing shuffle data on cypress.")
    .version("2.7.0")
    .stringConf
    .createOptional

  val YTSAURUS_SHUFFLE_REPLICATION_FACTOR = ConfigBuilder("spark.ytsaurus.shuffle.replication.factor")
    .doc("Replication factor for YTsaurus shuffle data, default is system default")
    .version("2.7.0")
    .intConf
    .createOptional

  val YTSAURUS_SHUFFLE_PARTITION_COLUMN = ConfigBuilder("spark.ytsaurus.shuffle.partition.column")
    .doc("The name of a column used for storing target partition number")
    .version("2.7.0")
    .stringConf
    .createWithDefault("partition")

  val YTSAURUS_SHUFFLE_WRITE_ROW_SIZE = ConfigBuilder("spark.ytsaurus.shuffle.write.row.size")
    .doc("The maximum size of a single row that is written to ytsaurus shuffle")
    .version("2.7.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("8m")

  val YTSAURUS_SHUFFLE_WRITE_BUFFER_SIZE = ConfigBuilder("spark.ytsaurus.shuffle.write.buffer.size")
    .doc("The size of a buffer (in rows) used to write shuffle data to YTsaurus. This should be set" +
      "along with spark.ytsaurus.shuffle.write.row.size property to prevent OOM")
    .version("2.7.0")
    .intConf
    .createWithDefault(10)

  val YTSAURUS_SHUFFLE_WRITE_CONFIG = ConfigBuilder("spark.ytsaurus.shuffle.write.config")
    .doc("YSON-serialized config for writing shuffle data to YTsaurus")
    .version("2.7.0")
    .stringConf
    .createOptional

  val YTSAURUS_SHUFFLE_READ_CONFIG = ConfigBuilder("spark.ytsaurus.shuffle.read.config")
    .doc("YSON-serialized config for reading shuffle data from YTsaurus")
    .version("2.7.0")
    .stringConf
    .createOptional

  def shuffleTransactionId(shuffleId: Int) = s"spark.ytsaurus.shuffle.internal.$shuffleId.transaction_id"
}
