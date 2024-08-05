import spyt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from dependencies import key_column_name, id_mapper
import sys

table_in = sys.argv[1]
table_out = sys.argv[2]

idmapUDF = udf(lambda x: id_mapper(x), StringType())

spark = SparkSession.builder.getOrCreate()

df = spark.read.yt(table_in)
result = df.select(idmapUDF(col("num")).alias(key_column_name()))
result.write.yt(table_out)

spark.stop()
