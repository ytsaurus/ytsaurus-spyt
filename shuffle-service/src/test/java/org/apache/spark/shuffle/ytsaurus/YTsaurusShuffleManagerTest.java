package org.apache.spark.shuffle.ytsaurus;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.ShuffleManager;
import org.apache.spark.shuffle.ShuffleWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.spyt.test.LocalYt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.Product2;
import scala.Tuple2;
import scala.collection.JavaConverters;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class YTsaurusShuffleManagerTest {
    private JavaSparkContext sc;
    private ShuffleManager shuffleManager;
    private ShuffleHandle handle;

    @AfterEach
    void tearDown() {
        try {
            if (shuffleManager != null && handle != null) {
                shuffleManager.unregisterShuffle(handle.shuffleId());
            }
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }

    private TaskContext taskContext(int partitionId) {
        TaskContext context = mock(TaskContext.class);
        when(context.taskMetrics()).thenReturn(TaskMetrics.empty());
        when(context.partitionId()).thenReturn(partitionId);
        return context;
    }

    @SuppressWarnings("unchecked")
    private void testShuffle(Map<String, String> extraConf, Set<Integer> skippedPartitions) throws Exception {
        int shuffleId = 1;
        int mapTasks = 4;
        int outputPartitions = 10;
        SparkConf conf = new SparkConf(false)
                .setMaster("local")
                .setAppName("test")
                .set("spark.shuffle.manager", "org.apache.spark.shuffle.ytsaurus.YTsaurusShuffleManager")
                .set("spark.shuffle.sort.io.plugin.class", "tech.ytsaurus.spyt.shuffle.YTsaurusShuffleDataIO")
                .set("spark.hadoop.yt.proxy", LocalYt.proxy())
                .set("spark.hadoop.yt.user", "root")
                .set("spark.hadoop.yt.token", "")
                .set("spark.ytsaurus.shuffle.replication.factor", "1");
        extraConf.forEach(conf::set);
        sc = new JavaSparkContext(conf);
        shuffleManager = SparkEnv.get().shuffleManager();
        assertInstanceOf(YTsaurusShuffleManager.class, shuffleManager);

        var rdd = sc.parallelizePairs(Collections.singletonList(new Tuple2<>(1, "Value 1")))
                .partitionBy(new IntPartitioner(outputPartitions));
        ShuffleDependency<Integer, String, String> dependency =
                (ShuffleDependency<Integer, String, String>) rdd.rdd().dependencies().head();
        handle = shuffleManager.registerShuffle(shuffleId, dependency);

        for (int mapId = 0; mapId < mapTasks; mapId++) {
            TaskContext context = taskContext(mapId);
            List<Product2<Integer, String>> data = new ArrayList<>();
            for (int pId = 0; pId < outputPartitions * 3; pId++) {
                if (!skippedPartitions.contains(pId / 3)) {
                    data.add(new Tuple2<>(pId / 3, "M" + mapId + " P" + pId));
                }
            }
            ShuffleWriter<Integer, String> writer = shuffleManager.getWriter(
                    handle, mapId, context, context.taskMetrics().shuffleWriteMetrics());
            writer.write(JavaConverters.asScalaIteratorConverter(data.iterator()).asScala());
            assertNull(YTsaurusShuffleExecutorComponents.currentHandle().get());
            assertNull(YTsaurusShuffleExecutorComponents.currentMapIndex().get());
            writer.stop(true);
        }

        for (int reduceId = 0; reduceId < outputPartitions; reduceId++) {
            TaskContext context = taskContext(reduceId);
            var metrics = context.taskMetrics().createTempShuffleReadMetrics();
            var reader = shuffleManager.<Integer, String>getReader(handle, reduceId, reduceId + 1, context, metrics);
            List<Tuple2<Integer, String>> result = new ArrayList<>();
            var records = reader.read();
            while (records.hasNext()) {
                Product2<Integer, String> record = records.next();
                result.add(new Tuple2<>(record._1(), record._2()));
            }
            List<Tuple2<Integer, String>> expected = new ArrayList<>();
            if (!skippedPartitions.contains(reduceId)) {
                for (int mapId = 0; mapId < mapTasks; mapId++) {
                    for (int itemId = 0; itemId < 3; itemId++) {
                        expected.add(new Tuple2<>(reduceId, "M" + mapId + " P" + (reduceId * 3 + itemId)));
                    }
                }
            }
            Comparator<Tuple2<Integer, String>> order = Comparator.comparing(Tuple2::_2);
            result.sort(order);
            expected.sort(order);
            assertEquals(expected, result);
            assertEquals(expected.size(), metrics.recordsRead());
        }
    }

    @Test
    void readsEmptyPartitionsWithoutCompression() throws Exception {
        testShuffle(Map.of("spark.shuffle.compress", "false"), Set.of(3, 5));
    }

    @Test
    void readsEmptyPartitionsWithCompression() throws Exception {
        testShuffle(Map.of("spark.shuffle.compress", "true"), Set.of(3, 5));
    }

    @Test
    void writesAndReadsPushBasedShuffle() throws Exception {
        testShuffle(Map.of("spark.ytsaurus.shuffle.push.enabled", "true"), Set.of(3, 5));
    }

    private static class IntPartitioner extends Partitioner {
        private final int numPartitions;

        IntPartitioner(int numPartitions) {
            this.numPartitions = numPartitions;
        }

        @Override
        public int numPartitions() {
            return numPartitions;
        }

        @Override
        public int getPartition(Object key) {
            return (Integer) key;
        }
    }
}
