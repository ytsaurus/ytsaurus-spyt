package tech.ytsaurus.spyt.shuffle;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;

import java.util.Map;

public class YTsaurusShuffleDriverComponents implements ShuffleDriverComponents {

    @Override
    public Map<String, String> initializeApplication() {
        return Map.of();
    }

    @Override
    public void cleanupApplication() {}

    @Override
    public boolean supportsReliableStorage() {
        return true;
    }
}
