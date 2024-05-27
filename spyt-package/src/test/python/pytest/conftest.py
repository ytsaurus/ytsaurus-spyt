import spyt

import logging
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pytest
import uuid
from yt.wrapper import YtClient


DOCKER_HOST = "localhost"
DOCKER_PROXY_PORT = 8000

YT_PROXY = f"{DOCKER_HOST}:{DOCKER_PROXY_PORT}"

SPARK_CONF = SparkConf()\
    .setMaster("local[4]")\
    .set("spark.hadoop.yt.proxy", YT_PROXY)\
    .set("spark.hadoop.yt.user", "root")\
    .set("spark.hadoop.yt.token", "")\
    .set("spark.yt.log.enabled", "false")\
    .set("spark.driver.memory", "768M")\
    .set("spark.executor.cores", "1")\
    .set("spark.executor.memory", "768M")


@pytest.fixture(scope="module")
def yt_client():
    client = YtClient(proxy=YT_PROXY, token="token")
    logging.info(f"Created YTsaurus client for {YT_PROXY}")
    return client


@pytest.fixture(scope="function")
def tmp_dir(yt_client):
    unique_dir = f"//tmp/{uuid.uuid4()}"
    yt_client.create("map_node", unique_dir)
    logging.debug(f"Created temp directory {unique_dir}")
    yield unique_dir
    yt_client.remove(unique_dir, recursive=True, force=True)
    logging.debug(f"Cleaned temp directory {unique_dir}")


@pytest.fixture(scope="module")
def spark_session():
    session = SparkSession.builder.config(conf=SPARK_CONF).getOrCreate()
    logging.info("Created local spark session")
    yield session
    session.stop()
