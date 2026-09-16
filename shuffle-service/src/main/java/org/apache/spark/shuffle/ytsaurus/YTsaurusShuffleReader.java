package org.apache.spark.shuffle.ytsaurus;

import org.apache.spark.InterruptibleIterator;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.shuffle.ShuffleReader;
import org.apache.spark.storage.ShuffleBlockId;
import org.apache.spark.util.CompletionIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ytsaurus.client.CompoundClient;
import tech.ytsaurus.client.request.CreateShuffleReader;

import java.io.InputStream;

import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

public class YTsaurusShuffleReader<K, C> implements ShuffleReader<K, C> {
    private static final Logger log = LoggerFactory.getLogger(YTsaurusShuffleReader.class);

    private final CompoundShuffleHandle<K, ?, C> compoundHandle;
    private final int startMapIndex;
    private final int endMapIndex;
    private final int startPartition;
    private final int endPartition;
    private final TaskContext context;
    private final ShuffleReadMetricsReporter readMetrics;
    private final CompoundClient ytClient;
    private final SerializerInstance serializer;
    private final ShuffleBlockId blockId;

    public YTsaurusShuffleReader(
            CompoundShuffleHandle<K, ?, C> compoundHandle,
            int startMapIndex,
            int endMapIndex,
            int startPartition,
            int endPartition,
            TaskContext context,
            ShuffleReadMetricsReporter readMetrics,
            CompoundClient ytClient) {
        this.compoundHandle = compoundHandle;
        this.startMapIndex = startMapIndex;
        this.endMapIndex = endMapIndex;
        this.startPartition = startPartition;
        this.endPartition = endPartition;
        this.context = context;
        this.readMetrics = readMetrics;
        this.ytClient = ytClient;
        serializer = compoundHandle.baseHandle().dependency().serializer().newInstance();
        blockId = new ShuffleBlockId(compoundHandle.shuffleId(), startMapIndex, startPartition);
    }

    @SuppressWarnings("unchecked")
    private Iterator<Tuple2<K, C>> deserialize(InputStream input) {
        InputStream wrapped = SparkEnv.get().serializerManager().wrapStream(blockId, input);
        return (Iterator<Tuple2<K, C>>) (Iterator<?>) serializer.deserializeStream(wrapped).asKeyValueIterator();
    }

    @Override
    public Iterator<Product2<K, C>> read() {
        ShuffleRecordIterator<K, C> records = new ShuffleRecordIterator<>(startPartition, endPartition, partition -> {
            log.trace("SHUFFLE {} READING PARTITION: {}", compoundHandle.shuffleId(), partition);
            var request = CreateShuffleReader.builder()
                    .setHandle(compoundHandle.ytHandle())
                    .setPartitionIndex((Integer) partition)
                    .setRange(new CreateShuffleReader.Range(startMapIndex, endMapIndex));
            return ytClient.createShuffleReader(request.build());
        }, this::deserialize, readMetrics);
        Iterator<Product2<K, C>> counted = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return records.hasNext();
            }

            @Override
            public Product2<K, C> next() {
                Tuple2<K, C> record = records.next();
                readMetrics.incRecordsRead(1);
                return record;
            }
        };
        CompletionIterator<Product2<K, C>, Iterator<Product2<K, C>>> completed = new CompletionIterator<>(counted) {
            @Override
            public void completion() {
                context.taskMetrics().mergeShuffleReadMetrics();
            }
        };
        return new InterruptibleIterator<>(context, completed);
    }
}
