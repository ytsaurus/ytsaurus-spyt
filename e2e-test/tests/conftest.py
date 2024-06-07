import spyt

from common.cluster import HistoryServer, SpytCluster
import logging
from pyspark.sql import SparkSession
import pytest
import spyt.client
from utils import DRIVER_CLIENT_CONF, SPARK_CONF, YT_PROXY
import uuid
from yt.wrapper import YtClient


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
def history_server():
    with HistoryServer(proxy=YT_PROXY) as server:
        yield server


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
