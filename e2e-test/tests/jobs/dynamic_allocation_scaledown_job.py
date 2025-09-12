import spyt

from pyspark.sql import SparkSession
from datetime import datetime
from time import sleep

spark = SparkSession.builder.getOrCreate()
try:
    print(datetime.now(), spark.sql("SELECT 1").count())
    sleep(15)
finally:
    spark.stop()
