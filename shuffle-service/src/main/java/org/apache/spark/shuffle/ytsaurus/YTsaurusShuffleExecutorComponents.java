package org.apache.spark.shuffle.ytsaurus;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;

import tech.ytsaurus.client.CompoundClient;
import tech.ytsaurus.client.request.CreateShuffleWriter;
import tech.ytsaurus.spyt.shuffle.YTsaurusShuffleMapOutputWriter;
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter;
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider$;

import java.util.Map;

import static org.apache.spark.shuffle.ytsaurus.Config.*;

public class YTsaurusShuffleExecutorComponents implements ShuffleExecutorComponents {
    // These two variables are needed to pass parameters from YTsaurusShuffleManager to createMapOutputWriter method
    private static final ThreadLocal<CompoundShuffleHandle<?, ?, ?>> currentHandle = new ThreadLocal<>();
    private static final ThreadLocal<Integer> currentMapIndex = new ThreadLocal<>();

    private final SparkConf sparkConf;
    private final CompoundClient ytsaurusClient;

    public YTsaurusShuffleExecutorComponents(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
        this.ytsaurusClient = YtClientProvider$.MODULE$.ytClient(
                YtClientConfigurationConverter.ytClientConfiguration(sparkConf));
    }

    public static ThreadLocal<CompoundShuffleHandle<?, ?, ?>> currentHandle() {
        return currentHandle;
    }

    public static ThreadLocal<Integer> currentMapIndex() {
        return currentMapIndex;
    }

    @Override
    public void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs) { }

    @Override
    public ShuffleMapOutputWriter createMapOutputWriter(int shuffleId, long mapTaskId, int numPartitions) {
        CompoundShuffleHandle<?, ?, ?> handle = currentHandle.get();
        currentHandle.remove();
        Integer mapIndex = currentMapIndex.get();
        currentMapIndex.remove();

        var reqBuilder = CreateShuffleWriter.builder()
                .setHandle(handle.ytHandle())
                .setPartitionColumn(sparkConf.get(YTSAURUS_SHUFFLE_PARTITION_COLUMN))
                .setWriterIndex(mapIndex)
                .setOverwriteExistingWriterData(true);
        int rowSize = ((Number) sparkConf.get(YTSAURUS_SHUFFLE_WRITE_ROW_SIZE)).intValue();
        int bufferSize = (Integer) sparkConf.get(YTSAURUS_SHUFFLE_WRITE_BUFFER_SIZE);
        var writer = ytsaurusClient.createShuffleWriter(reqBuilder.build()).join();
        return new YTsaurusShuffleMapOutputWriter(writer, shuffleId, mapTaskId, numPartitions, rowSize, bufferSize);
    }
}
