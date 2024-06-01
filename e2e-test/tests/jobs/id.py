from spyt import spark_session
import sys

table_in = sys.argv[1]
table_out = sys.argv[2]

with spark_session() as spark:
    df = spark.read.yt(table_in)
    df.write.yt(table_out)
