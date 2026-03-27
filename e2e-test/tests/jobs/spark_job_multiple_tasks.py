import spyt

from pyspark.sql import SparkSession
import sys

table_out = sys.argv[1]

spark = SparkSession.builder.getOrCreate()
try:
    df = spark.range(10000000, numPartitions=3000)
    df.write.yt(table_out)
finally:
    spark.stop()
