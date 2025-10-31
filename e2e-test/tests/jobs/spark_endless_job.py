import spyt # noqa
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

spark = SparkSession.builder.getOrCreate()
try:
    @udf
    def infinite_loop(x):
        while True:
            x = x + 0

    spark.range(10).select(infinite_loop("id")).collect()
finally:
    spark.stop()
