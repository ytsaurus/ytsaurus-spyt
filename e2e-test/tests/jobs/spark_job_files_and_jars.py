import spyt

import os
import sys
import json

from pyspark.sql import SparkSession
from yt.wrapper import YtClient

out_path = sys.argv[1]

spark = SparkSession.builder.getOrCreate()
try:
    result = {
        "file_present": os.path.exists('id.py'),
        "jar_value": spark.sparkContext._jvm.org.example.JarDep.value(),
    }
    client = YtClient(proxy=spark.conf.get("spark.hadoop.yt.proxy"),
                      token=spark.conf.get("spark.hadoop.yt.token", ""))
    client.create("file", out_path, recursive=True, ignore_existing=True)
    client.write_file(out_path, json.dumps(result).encode())
finally:
    spark.stop()
