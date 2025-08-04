import spyt

from pyspark.sql import SparkSession
import sys
import os

table_out = sys.argv[1]

spark = SparkSession.builder.getOrCreate()

try:
    result = {
        "secret": spark.conf.get("spark.some.secret.key"),
        "password": spark.conf.get("spark.some.password"),
        "token": spark.conf.get("spark.external.service.token"),
        "secret_env": os.environ["YT_SECURE_VAULT_SPARK_SOME_SECRET_KEY"],
        "password_env": os.environ["YT_SECURE_VAULT_SPARK_SOME_PASSWORD"],
        "token_env": os.environ["YT_SECURE_VAULT_SPARK_EXTERNAL_SERVICE_TOKEN"],
    }
    spark.createDataFrame([result]).write.yt(table_out)
finally:
    spark.stop()
