import spyt

from pyspark.sql import SparkSession
from yt.wrapper import YtClient
import sys

out_path = sys.argv[1]

spark = SparkSession.builder.getOrCreate()
try:
    shuffle_manager = spark.conf.get("spark.shuffle.manager", "sort")
    client = YtClient(proxy=spark.conf.get("spark.hadoop.yt.proxy"),
                      token=spark.conf.get("spark.hadoop.yt.token", ""))
    client.write_file(out_path, shuffle_manager.encode())
finally:
    spark.stop()
