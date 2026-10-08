package org.apache.spark.shuffle.ytsaurus;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;

import tech.ytsaurus.client.CompoundClient;
import tech.ytsaurus.client.request.CreateShuffleWriter;
import tech.ytsaurus.spyt.shuffle.YTsaurusShuffleMapOutputWriter;
import tech.ytsaurus.spyt.shuffle.YTsaurusSingleSpillMapOutputWriter;
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter;
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider$;

import java.util.Map;
import java.util.Optional;

import static org.apache.spark.shuffle.ytsaurus.Config.*;

public class YTsaurusShuffleExecutorComponents implements ShuffleExecutorComponents {
    // These variables pass the parameters of the current map task from YTsaurusShuffleManager.getWriter to
    // createMapOutputWriter and createSingleFileMapOutputWriter, which read and clear what they consume.
    static final ThreadLocal<CompoundShuffleHandle<?, ?, ?>> currentHandle = new ThreadLocal<>();
    static final ThreadLocal<Integer> currentMapIndex = new ThreadLocal<>();
    static final ThreadLocal<ShuffleWriteMetricsReporter> currentWriteMetrics = new ThreadLocal<>();

    private final SparkConf sparkConf;
    private final CompoundClient ytsaurusClient;

    public YTsaurusShuffleExecutorComponents(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
        this.ytsaurusClient = YtClientProvider$.MODULE$.ytClient(
                YtClientConfigurationConverter.ytClientConfiguration(sparkConf));
    }

    @Override
    public void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs) { }

    @Override
    public ShuffleMapOutputWriter createMapOutputWriter(int shuffleId, long mapTaskId, int numPartitions) {
        CompoundShuffleHandle<?, ?, ?> handle = currentHandle.get();
        currentHandle.remove();
        Integer mapIndex = currentMapIndex.get();
        currentMapIndex.remove();
        currentWriteMetrics.remove();

        var reqBuilder = CreateShuffleWriter.builder()
                .setHandle(handle.ytHandle())
                .setPartitionColumn(sparkConf.get(YTSAURUS_SHUFFLE_PARTITION_COLUMN))
                .setWriterIndex(mapIndex)
                .setOverwriteExistingWriterData(true);
        int bufferSize = (Integer) sparkConf.get(YTSAURUS_SHUFFLE_WRITE_BUFFER_SIZE);
        var writer = ytsaurusClient.createShuffleWriter(reqBuilder.build()).join();
        return new YTsaurusShuffleMapOutputWriter(writer, shuffleId, mapTaskId, numPartitions, rowSize(), bufferSize);
    }

    /**
     * Spark calls this before {@link #createMapOutputWriter} when a map task produced exactly one spill file.
     * Without it Spark merges that file through the standard writer and reports its bytes as written twice:
     * once while spilling and once while merging.
     */
    @Override
    public Optional<SingleSpillShuffleMapOutputWriter> createSingleFileMapOutputWriter(int shuffleId, long mapId) {
        ShuffleWriteMetricsReporter writeMetrics = currentWriteMetrics.get();
        currentWriteMetrics.remove();
        return Optional.of(new YTsaurusSingleSpillMapOutputWriter(
                numPartitions -> createMapOutputWriter(shuffleId, mapId, numPartitions), rowSize(), writeMetrics));
    }

    private int rowSize() {
        return ((Number) sparkConf.get(YTSAURUS_SHUFFLE_WRITE_ROW_SIZE)).intValue();
    }
}
