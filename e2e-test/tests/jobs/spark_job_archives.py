import spyt

from pyspark.sql import SparkSession
import sys

table_out = sys.argv[1]

spark = SparkSession.builder.getOrCreate()

with open('arcdep/key.txt', 'r') as f:
    key = f.read()

with open('deps2.zip/key2.txt', 'r') as f:
    key2 = f.read()

df = spark.createDataFrame([(key, key2)])
df.write.yt(table_out)

spark.stop()
