package tech.ytsaurus.spyt.example;

import org.apache.spark.sql.SparkSession;

public class YtCloseExceptionTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.read().format("yt").load("//home/spark/examples/tables/example_1").show();
        throw new RuntimeException("This is fine");
    }
}
