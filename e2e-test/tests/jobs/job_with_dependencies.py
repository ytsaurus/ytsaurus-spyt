from spyt import spark_session
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from dependencies import key_column_name, id_mapper
import sys

table_in = sys.argv[1]
table_out = sys.argv[2]

idmapUDF = udf(lambda x: id_mapper(x), StringType())

with spark_session() as spark:
    df = spark.read.yt(table_in)
    rem_df = df.withColumn("a_mod_10", col("a") % 10).groupBy("a_mod_10").count()
    result = rem_df.select(idmapUDF(col("a_mod_10")).alias(key_column_name()), col("count"))
    result.write.yt(table_out)
