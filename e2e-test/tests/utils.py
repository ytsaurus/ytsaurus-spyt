import spyt

import logging
from pyspark.conf import SparkConf
import os


YT_PROXY = "127.0.0.1:8000"
DRIVER_HOST = "172.17.0.1"

DRIVER_CLIENT_CONF = {
    "spark.driver.host": DRIVER_HOST,
    "spark.driver.port": "27010",
    "spark.ui.port": "27015",
    "spark.blockManager.port": "27018",
}

SPARK_CONF = SparkConf() \
    .setMaster("local[4]") \
    .set("spark.hadoop.yt.proxy", YT_PROXY) \
    .set("spark.hadoop.yt.user", "root") \
    .set("spark.hadoop.yt.token", "") \
    .set("spark.yt.log.enabled", "false") \
    .set("spark.driver.cores", "1") \
    .set("spark.driver.memory", "768M") \
    .set("spark.executor.instances", "1") \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "768M") \
    .setAll(DRIVER_CLIENT_CONF.items())


def upload_file(yt_client, source_path, remote_path):
    logging.debug(f"Uploading {source_path} to {remote_path}")
    yt_client.create("file", remote_path)
    full_source_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), source_path)
    with open(full_source_path, 'rb') as file:
        yt_client.write_file(remote_path, file)
