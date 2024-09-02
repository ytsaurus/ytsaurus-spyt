import spyt

from common.cluster import DirectSubmitter, HistoryServer, LivyServer, SpytCluster, direct_spark_session
from common.cluster_utils import default_conf
import logging
import os
from pyspark.sql import SparkSession
import pytest
import shutil
import spyt.client
from utils import DRIVER_CLIENT_CONF, SPARK_CONF, YT_PROXY
import uuid
from yt.wrapper import YtClient


def test_directory(request):
    dir_path = os.path.join(os.getcwd(), "test-results",
                            os.environ['TOX_ENV_NAME'], request.node.parent.name + "__" + request.node.name)
    shutil.rmtree(dir_path, ignore_errors=True)
    os.makedirs(dir_path)
    return dir_path


@pytest.fixture(scope="module")
def yt_client():
    client = YtClient(proxy=YT_PROXY, token="token")
    logging.info(f"Created YTsaurus client for {YT_PROXY}")
    return client


@pytest.fixture(scope="function")
def spyt_cluster(request):
    with SpytCluster(proxy=YT_PROXY, dump_dir=test_directory(request)) as cluster:
        yield cluster


@pytest.fixture(scope="function")
def history_server():
    with HistoryServer(proxy=YT_PROXY) as server:
        yield server


@pytest.fixture(scope="function")
def livy_server(request):
    with LivyServer(proxy=YT_PROXY, master_address="ytsaurus://" + YT_PROXY, dump_dir=test_directory(request)) as server:
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
    session = SparkSession.builder.config(conf=default_conf().setAll(SPARK_CONF.items())).getOrCreate()
    logging.debug("Created local spark session")
    try:
        yield session
    finally:
        spyt.client.stop(session)
        logging.debug("Stopped local spark session")


@pytest.fixture(scope="function")
def direct_session():
    with direct_spark_session(YT_PROXY, extra_conf=SPARK_CONF) as session:
        logging.debug("Created direct spark session")
        yield session
    logging.debug("Stopped direct spark session")


@pytest.fixture(scope="function")
def direct_submitter(request):
    with DirectSubmitter(YT_PROXY, extra_conf=SPARK_CONF, dump_dir=test_directory(request)) as submitter:
        logging.debug("Created direct submitter")
        yield submitter
    logging.debug("Stopped direct submitter")


@pytest.fixture(scope="function")
def cluster_session(spyt_cluster):
    with spyt_cluster.spark_session(spark_conf_args=DRIVER_CLIENT_CONF) as spark:
        yield spark
