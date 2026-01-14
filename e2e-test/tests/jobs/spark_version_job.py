import sys
import spyt  # noqa
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
try:
    print(f"SPARK_VERSION={spark.version}", file=sys.stderr)
finally:
    spark.stop()

