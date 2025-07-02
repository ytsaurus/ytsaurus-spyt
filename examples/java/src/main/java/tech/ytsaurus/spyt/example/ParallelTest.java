package tech.ytsaurus.spyt.example;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.sql.SparkSession;

import tech.ytsaurus.client.CompoundClient;
import tech.ytsaurus.spyt.SparkAppJava;

public class ParallelTest extends SparkAppJava {
    private static ExecutorService pool = Executors.newFixedThreadPool(4);

    @Override
    protected void doRun(String[] args, SparkSession spark, CompoundClient yt) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try {
            CompletableFuture.supplyAsync(() -> {
                Thread.currentThread().setContextClassLoader(loader);
                spark.read().format("yt").load("//home/spark/examples/tables/example_1").show();
                return 1;
            }, pool).join();
        } finally {
            pool.shutdown();
        }
    }

    public static void main(String[] args) {
        new ParallelTest().run(args);
    }
}
