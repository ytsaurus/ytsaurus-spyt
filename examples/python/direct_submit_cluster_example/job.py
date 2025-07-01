from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import sys
from spyt_direct_submit_cluster_example.dependencies import my_function, verbalizer


def main():
    print("Hello world")
    print(f"Python version: {sys.version}")
    print("my_function call result:", my_function())
    print("This is written to stderr", file=sys.stderr)

    input_table = sys.argv[1]
    output_table = sys.argv[2]
    print(f"Input table: {input_table}")
    print(f"Output table: {output_table}")

    verbUDF = udf(lambda x: verbalizer(x), StringType())

    spark = SparkSession.builder.appName('Direct submit cluster example').getOrCreate()

    try:
        print("PyFiles:", spark.conf.get("spark.submit.pyFiles"))
        print("Spark context pythonVer", spark._sc.pythonVer)
        print("Spark context pythonExec", spark._sc.pythonExec)

        spark.read.yt(input_table) \
            .withColumn("id_mod_10", col("id") % 10) \
            .groupBy("id_mod_10") \
            .count() \
            .select(verbUDF(col("id_mod_10")).alias("key"), col("count")) \
            .write.mode("overwrite").yt(output_table)

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
