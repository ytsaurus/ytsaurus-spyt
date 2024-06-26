import argparse
import spyt

from pyspark import SparkConf


def run_job(yt_proxy: str, gpu_pool: str, gpu_tree: str):
    conf = SparkConf()
    conf.set("spark.app.name", "GPU example")
    conf.set("spark.executor.instances", "1")
    conf.set("spark.executor.cores", "1")
    conf.set("spark.executor.memory", "8G")

    conf.set("spark.executor.resource.gpu.amount", "1")
    conf.set("spark.task.resource.gpu.amount", "0.5")
    conf.set("spark.rapids.memory.pinnedPool.size", "2G")
    conf.set("spark.ytsaurus.executor.operation.parameters", "{{pool={};pool_trees=[{}]}}".format(gpu_pool, gpu_tree))
    conf.set("spark.plugins", "com.nvidia.spark.SQLPlugin")
    conf.set("spark.jars", "yt:///home/spark/lib/rapids-4-spark_2.12-23.12.2-cuda11.jar")

    with spyt.direct_spark_session(yt_proxy, conf) as spark:
        df = spark.read.yt("//home/spark/examples/example_1")
        df.filter("`id` > 50").show()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('proxy', help="YT proxy address")
    parser.add_argument('gpu_pool', help="GPU pool for executors")
    parser.add_argument('gpu_tree', help="GPU tree for executors")
    args, _ = parser.parse_known_args()
    run_job(args.proxy, args.gpu_pool, args.gpu_tree)


if __name__ == '__main__':
    main()
