import spyt

from common.cluster import SpytCluster
import logging
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pytest
import spyt.client
import uuid
from yt.wrapper import YtClient


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


@pytest.fixture(scope="module")
def yt_client():
    client = YtClient(proxy=YT_PROXY, token="token")
    logging.info(f"Created YTsaurus client for {YT_PROXY}")
    return client


@pytest.fixture(scope="function")
def spyt_cluster():
    with SpytCluster(proxy=YT_PROXY) as cluster:
        yield cluster


@pytest.fixture(scope="function")
def tmp_dir(yt_client):
    unique_dir = f"//tmp/{uuid.uuid4()}"
    yt_client.create("map_node", unique_dir)
    logging.debug(f"Created temp directory {unique_dir}")
    yield unique_dir
    yt_client.remove(unique_dir, recursive=True, force=True)
    logging.debug(f"Cleaned temp directory {unique_dir}")


@pytest.fixture(scope="function")
def local_session():
    session = SparkSession.builder.config(conf=SPARK_CONF).getOrCreate()
    logging.debug("Created local spark session")
    try:
        yield session
    finally:
        spyt.client.stop(session)
        logging.debug("Stopped local spark session")


@pytest.fixture(scope="function")
def direct_session():
    with spyt.direct_spark_session(YT_PROXY, SPARK_CONF) as session:
        logging.debug("Created direct spark session")
        yield session
    logging.debug("Stopped direct spark session")


@pytest.fixture(scope="function")
def cluster_session(spyt_cluster):
    with spyt_cluster.spark_session(spark_conf_args=DRIVER_CLIENT_CONF) as spark:
        yield spark
