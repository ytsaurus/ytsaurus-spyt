import spyt

from pyspark.sql import SparkSession
import sys

table_in = sys.argv[1]
table_out = sys.argv[2]

spark = SparkSession.builder.getOrCreate()

df = spark.read.yt(table_in)
df.write.yt(table_out)

spark.stop()
