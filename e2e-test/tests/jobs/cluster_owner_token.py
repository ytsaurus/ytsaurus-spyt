import spyt

from pyspark.sql import SparkSession

import os


spark = SparkSession.builder.getOrCreate()
try:
    print(f"driver_token={os.environ.get('YT_SECURE_VAULT_YT_TOKEN')}", flush=True)
    executor_tokens = spark.sparkContext.parallelize([0], 1).map(
        lambda _: os.environ.get("YT_SECURE_VAULT_YT_TOKEN")
    ).collect()
    for token in executor_tokens:
        print(f"executor_token={token}", flush=True)
finally:
    spark.stop()
