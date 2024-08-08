import argparse

import spyt  # noqa
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def run_job(table_in_path, table_out_path, spyt_version, driver_max_failures, executor_max_failures, is_python,
            python_version):
    print(f"Spark direct submit example.\nSPYT version: {spyt_version}\n"
          f"This job reads the table at {table_in_path}, \n"
          "selecting the 'id' column, renaming it to 'host', \n"
          "groups by host, \n"
          "counts the number of occurrences of each unique host, \n"
          "show first 7 rows in console\n"
          f"and write result to {table_out_path} in yt")

    conf = SparkConf()
    conf.set("spark.app.name", "Direct submit example")
    conf.set("spark.ytsaurus.driver.maxFailures", driver_max_failures)
    conf.set("spark.ytsaurus.executor.maxFailures", executor_max_failures)
    conf.set("spark.ytsaurus.isPython", is_python)
    conf.set("spark.ytsaurus.python.version", python_version)
    if spyt_version:
        conf.set("spark.ytsaurus.spyt.version", spyt_version)
        if spyt_version.endswith("-SNAPSHOT"):
            conf.set("spark.ytsaurus.config.releases.path", "//home/spark/conf/snapshots")
            conf.set("spark.ytsaurus.spyt.releases.path", "//home/spark/spyt/snapshots")

    try:
        spark = SparkSession.builder.config(conf=conf).appName('spark direct submit example').getOrCreate()
        read_df = spark.read.yt(table_in_path).limit(1000000)
        result_df = read_df.select((col("id")).alias("host")) \
            .groupBy("host") \
            .count()
        result_df.show(n=7, truncate=False, vertical=True)
        result_df.write.mode("overwrite").yt(table_out_path)

        print("Direct spark session was successfully stopped, the result is printed to the console.")
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--table-in-path', required=True)
    parser.add_argument('--table-out-path', required=True)
    parser.add_argument('--spyt-version', help="Custom SPYT version", default=None)
    parser.add_argument('--spark-ytsaurus-driver-max-failures', required=False, default=5)
    parser.add_argument('--spark-ytsaurus-executor-max-failures', required=False, default=10)
    parser.add_argument('--is-python', required=False, default=False)
    parser.add_argument('--python-version', required=False)
    args, _ = parser.parse_known_args()
    run_job(args.table_in_path, args.table_out_path, args.spyt_version, args.spark_ytsaurus_driver_max_failures,
            args.spark_ytsaurus_executor_max_failures, args.is_python, args.python_version)


if __name__ == '__main__':
    main()
