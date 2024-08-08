package tech.ytsaurus.spyt.example;

import org.apache.spark.sql.SparkSession;

public class YtCloseTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.read().format("yt").load("//home/spark/examples/tables/example_1").show();
    }
}
