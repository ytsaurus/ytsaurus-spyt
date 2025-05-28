package tech.ytsaurus.spyt.shuffle;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.ytsaurus.YTsaurusShuffleExecutorComponents;

public class YTsaurusShuffleDataIO implements ShuffleDataIO {

    private final SparkConf sparkConf;

    public YTsaurusShuffleDataIO(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }

    @Override
    public ShuffleExecutorComponents executor() {
        return new YTsaurusShuffleExecutorComponents(sparkConf);
    }

    @Override
    public ShuffleDriverComponents driver() {
        return new YTsaurusShuffleDriverComponents();
    }
}
