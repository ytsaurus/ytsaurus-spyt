import spyt

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
import sys


spark = SparkSession.builder.getOrCreate()
try:
    df1 = spark.range(50000).withColumn("key", (rand() * 100).cast("int"))
    df2 = spark.range(50000).withColumn("key", (rand() * 100).cast("int"))

    df1.repartition(10).createOrReplaceTempView("table1")
    df2.repartition(10).createOrReplaceTempView("table2")

    heavy_query = """
                SELECT
                  t1.id,
                  t1.key,
                  SUM(t2.id) AS sum_t2_id,
                  COUNT(*) OVER (PARTITION BY t1.key) AS count_by_key
                FROM table1 t1
                JOIN table2 t2 ON t1.key = t2.key
                GROUP BY t1.id, t1.key
            """
    spark.sql(heavy_query).show(20)
finally:
    spark.stop()
