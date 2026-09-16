package org.apache.spark.shuffle.ytsaurus;

import org.apache.spark.internal.config.ConfigBuilder;
import org.apache.spark.internal.config.ConfigEntry;
import org.apache.spark.internal.config.OptionalConfigEntry;
import org.apache.spark.network.util.ByteUnit;

import java.util.concurrent.TimeUnit;

public final class Config {
    private Config() { }

    public static final ConfigEntry<Object> YTSAURUS_SHUFFLE_TRANSACTION_TIMEOUT =
            new ConfigBuilder("spark.ytsaurus.shuffle.transaction.timeout")
            .doc("Timeout for parent YTsaurus shuffle transaction")
            .version("2.7.0")
            .timeConf(TimeUnit.MILLISECONDS)
            .createWithDefaultString("5m");

    public static final ConfigEntry<String> YTSAURUS_SHUFFLE_ACCOUNT =
            new ConfigBuilder("spark.ytsaurus.shuffle.account")
            .doc("YTsaurus account to be used for storing shuffle data on cypress.")
            .version("2.7.0")
            .stringConf()
            .createWithDefault("intermediate");

    public static final OptionalConfigEntry<String> YTSAURUS_SHUFFLE_MEDIUM =
            new ConfigBuilder("spark.ytsaurus.shuffle.medium")
            .doc("YTsaurus medium to be used for storing shuffle data on cypress.")
            .version("2.7.0")
            .stringConf()
            .createOptional();

    public static final OptionalConfigEntry<Object> YTSAURUS_SHUFFLE_REPLICATION_FACTOR =
            new ConfigBuilder("spark.ytsaurus.shuffle.replication.factor")
            .doc("Replication factor for YTsaurus shuffle data, default is system default")
            .version("2.7.0")
            .intConf()
            .createOptional();

    public static final ConfigEntry<String> YTSAURUS_SHUFFLE_PARTITION_COLUMN =
            new ConfigBuilder("spark.ytsaurus.shuffle.partition.column")
            .doc("The name of a column used for storing target partition number")
            .version("2.7.0")
            .stringConf()
            .createWithDefault("partition");

    public static final ConfigEntry<Object> YTSAURUS_SHUFFLE_WRITE_ROW_SIZE =
            new ConfigBuilder("spark.ytsaurus.shuffle.write.row.size")
            .doc("The maximum size of a single row that is written to ytsaurus shuffle")
            .version("2.7.0")
            .bytesConf(ByteUnit.BYTE)
            .createWithDefaultString("8m");

    public static final ConfigEntry<Object> YTSAURUS_SHUFFLE_WRITE_BUFFER_SIZE =
            new ConfigBuilder("spark.ytsaurus.shuffle.write.buffer.size")
            .doc("The size of a buffer (in rows) used to write shuffle data to YTsaurus. This should be set" +
                "along with spark.ytsaurus.shuffle.write.row.size property to prevent OOM")
            .version("2.7.0")
            .intConf()
            .createWithDefault(10);

    public static final ConfigEntry<Object> YTSAURUS_SHUFFLE_PUSH_BASED_ENABLED =
            new ConfigBuilder("spark.ytsaurus.shuffle.push.enabled")
            .doc("Enables push-based model for YTSaurus shuffle")
            .version("2.11.0")
            .booleanConf()
            .createWithDefault(false);

    public static final OptionalConfigEntry<String> YTSAURUS_SHUFFLE_CONFIG =
            new ConfigBuilder("spark.ytsaurus.shuffle.config")
            .doc("YSON-serialized YTsaurus shuffle config. It is passed once when a shuffle is started and is shared " +
                "by all writers and readers of that shuffle. May contain only the section of the used shuffle mode: " +
                "{pull={reader=...;writer=...}} when spark.ytsaurus.shuffle.push.enabled is false or " +
                "{push={writer=...;reader=...;journal_writer=...;session_pool=...}} when it is true")
            .version("2.12.0")
            .stringConf()
            .createOptional();

    public static String shuffleTransactionId(int shuffleId) {
        return "spark.ytsaurus.shuffle.internal." + shuffleId + ".transaction_id";
    }
}
