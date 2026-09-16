package org.apache.spark.shuffle.ytsaurus;

import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.BaseShuffleHandle;
import org.apache.spark.shuffle.ShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.ShuffleManager;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.shuffle.ShuffleReader;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.sort.SortShuffleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.CompoundClient;
import tech.ytsaurus.client.request.StartShuffle;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.spyt.wrapper.YtWrapper;
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter;
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider;
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider$;
import tech.ytsaurus.ysontree.YTreeMapNode;

import java.time.Duration;
import java.util.Optional;

import static org.apache.spark.shuffle.ytsaurus.Config.*;
import static org.apache.spark.shuffle.ytsaurus.ShuffleUtils.toJavaOptional;

public class YTsaurusShuffleManager implements ShuffleManager {
    private static final Logger log = LoggerFactory.getLogger(YTsaurusShuffleManager.class);
    private static final TableSchema SHUFFLE_SCHEMA = TableSchema.builder()
            .setStrict(true)
            .addValue("partition", ColumnValueType.INT64)
            .addValue("data", ColumnValueType.STRING)
            .build();

    private final SparkConf conf;
    private final CompoundClient ytClient;
    private final SortShuffleManager delegate;
    private final boolean pushBasedEnabled;
    private Optional<YTreeMapNode> shuffleConfigOpt;

    public YTsaurusShuffleManager(SparkConf conf) {
        this(conf, createClient(conf), new SortShuffleManager(conf));
    }

    YTsaurusShuffleManager(SparkConf conf, CompoundClient ytClient, SortShuffleManager delegate) {
        this.conf = conf;
        this.ytClient = ytClient;
        this.delegate = delegate;
        pushBasedEnabled = (Boolean) conf.get(YTSAURUS_SHUFFLE_PUSH_BASED_ENABLED);
        log.info("YTsaurus shuffle service is in use (account={}, pushBased={})",
                conf.get(YTSAURUS_SHUFFLE_ACCOUNT), pushBasedEnabled);
    }

    private static CompoundClient createClient(SparkConf conf) {
        Optional<String> providerClass = toJavaOptional(conf.getOption("spark.ytsaurus.client.provider.class"));
        YtClientProvider provider;
        try {
            provider = providerClass.isPresent()
                    ? (YtClientProvider) Class.forName(providerClass.get()).getDeclaredConstructor().newInstance()
                    : YtClientProvider$.MODULE$;
        } catch (ReflectiveOperationException e) {
            throw propagate(e);
        }
        return provider.ytClient(YtClientConfigurationConverter.ytClientConfiguration(conf));
    }

    private synchronized Optional<YTreeMapNode> shuffleConfig() {
        if (shuffleConfigOpt == null) {
            shuffleConfigOpt = ShuffleUtils.shuffleConfig(conf, pushBasedEnabled);
        }
        return shuffleConfigOpt;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V, C> ShuffleHandle registerShuffle(int shuffleId, ShuffleDependency<K, V, C> dependency) {
        Duration timeout = Duration.ofMillis((Long) conf.get(YTSAURUS_SHUFFLE_TRANSACTION_TIMEOUT));
        int partitionCount = dependency.partitioner().numPartitions();
        var request = StartShuffle.builder()
                .setAccount(conf.get(YTSAURUS_SHUFFLE_ACCOUNT))
                .setPartitionCount(partitionCount)
                .setUsePushBasedShuffle(pushBasedEnabled)
                .setSchema(SHUFFLE_SCHEMA);
        shuffleConfig().ifPresent(request::setConfig);
        toJavaOptional(conf.get(YTSAURUS_SHUFFLE_MEDIUM)).ifPresent(request::setMedium);
        toJavaOptional(conf.get(YTSAURUS_SHUFFLE_REPLICATION_FACTOR))
                .ifPresent(replicationFactor -> request.setReplicationFactor((Integer) replicationFactor));

        BaseShuffleHandle<K, V, C> baseHandle =
                (BaseShuffleHandle<K, V, C>) delegate.registerShuffle(shuffleId, dependency);
        ApiServiceTransaction transaction = null;
        try {
            transaction = YtWrapper.createTransaction(scala.Option.empty(), timeout,
                    YtWrapper.createTransaction$default$3(), YtWrapper.createTransaction$default$4(),
                    YtWrapper.createTransaction$default$5(), ytClient);
            request.setParentTransactionId(transaction.getId());
            var ytHandle = ytClient.startShuffle(request.build()).join();
            if (log.isTraceEnabled()) {
                log.trace("REGISTERED SHUFFLE: {}, DESCRIPTOR: {}", shuffleId, ytHandle.getPayload());
            }
            CompoundShuffleHandle<K, V, C> handle = new CompoundShuffleHandle<>(baseHandle, ytHandle, partitionCount);
            conf.set(shuffleTransactionId(shuffleId), transaction.getId().toString());
            return handle;
        } catch (RuntimeException | Error failure) {
            if (transaction != null) {
                try {
                    transaction.abort().join();
                } catch (RuntimeException | Error cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            try {
                delegate.unregisterShuffle(shuffleId);
            } catch (RuntimeException | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> ShuffleWriter<K, V> getWriter(
            ShuffleHandle handle,
            long mapId,
            TaskContext context,
            ShuffleWriteMetricsReporter metrics) {
        if (log.isTraceEnabled()) {
            log.trace("CREATE SHUFFLE WRITER shuffleId={} mapId={} stageId={} stageAttemptNumber={} "
                            + "partitionId={} numPartitions={} attemptNumber={} taskAttemptId={} props={}",
                    handle.shuffleId(), mapId, context.stageId(), context.stageAttemptNumber(),
                    context.partitionId(), context.numPartitions(), context.attemptNumber(),
                    context.taskAttemptId(), context.getLocalProperties());
        }
        CompoundShuffleHandle<K, V, ?> compoundHandle = (CompoundShuffleHandle<K, V, ?>) handle;
        YTsaurusShuffleExecutorComponents.currentHandle().set(compoundHandle);
        YTsaurusShuffleExecutorComponents.currentMapIndex().set(context.partitionId());
        return delegate.getWriter(compoundHandle.baseHandle(), mapId, context, metrics);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, C> ShuffleReader<K, C> getReader(
            ShuffleHandle handle,
            int startMapIndex,
            int endMapIndex,
            int startPartition,
            int endPartition,
            TaskContext context,
            ShuffleReadMetricsReporter metrics) {
        if (log.isTraceEnabled()) {
            log.trace("CREATING SHUFFLE READER: {}, MAP: ({} - {}), PART: ({} - {}) "
                            + "STAGE: id={} attempt={} METRICS CLASS: {}",
                    handle.shuffleId(), startMapIndex, endMapIndex, startPartition, endPartition,
                    context.stageId(), context.stageAttemptNumber(), metrics.getClass());
        }
        return new YTsaurusShuffleReader<>((CompoundShuffleHandle<K, ?, C>) handle,
                startMapIndex, endMapIndex, startPartition, endPartition, context, metrics, ytClient);
    }

    @Override
    public boolean unregisterShuffle(int shuffleId) {
        log.debug("Unregistering shuffle: {}", shuffleId);
        try {
            String transactionId = conf.get(shuffleTransactionId(shuffleId));
            ytClient.abortTransaction(GUID.valueOf(transactionId)).join();
            conf.remove(shuffleTransactionId(shuffleId));
        } catch (Exception e) {
            log.warn("An exception was thrown while trying to abort shuffle transaction", e);
        }
        return delegate.unregisterShuffle(shuffleId);
    }

    @Override
    public void stop() {
        try {
            ytClient.close();
        } catch (java.io.IOException e) {
            throw propagate(e);
        } finally {
            delegate.stop();
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> RuntimeException propagate(Throwable error) throws E {
        throw (E) error;
    }

    @Override
    public ShuffleBlockResolver shuffleBlockResolver() {
        return delegate.shuffleBlockResolver();
    }
}
