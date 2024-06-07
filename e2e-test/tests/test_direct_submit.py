import spyt

from common.helpers import assert_items_equal
from pyspark.conf import SparkConf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, udf
import requests
import time
from utils import SPARK_CONF, YT_PROXY
import yt.yson as yson


def test_client_mode(yt_client, tmp_dir, direct_session):
    table_in = f"{tmp_dir}/direct_client_in"
    table_out = f"{tmp_dir}/direct_client_out"
    yt_client.create("table", table_in, attributes={"schema": [{"name": "id", "type": "int64"}]})
    rows = [{"id": i} for i in range(95)]
    yt_client.write_table(table_in, rows)

    df = direct_session.read.yt(table_in)
    df.groupBy((col("id") % 10).alias("rem")).count().write.yt(table_out)

    result = yt_client.read_table(table_out)
    assert_items_equal(result, [{'rem': 0, 'count': 10},
                                {'rem': 1, 'count': 10},
                                {'rem': 2, 'count': 10},
                                {'rem': 3, 'count': 10},
                                {'rem': 4, 'count': 10},
                                {'rem': 5, 'count': 9},
                                {'rem': 6, 'count': 9},
                                {'rem': 7, 'count': 9},
                                {'rem': 8, 'count': 9},
                                {'rem': 9, 'count': 9}])


def test_spyt_types_in_lambda(yt_client, tmp_dir, direct_session):
    table_in = f"{tmp_dir}/direct_yson_in"
    table_out = f"{tmp_dir}/direct_yson_out"
    schema = [{"name": "id", "type": "int64"}, {"name": "data", "type_v3": {"type_name": "optional", "item": "yson"}}]
    yt_client.create("table", table_in, attributes={"schema": schema})
    rows = [{"id": i, "data": [] if i % 6 == 0 else [hex(k) for k in range(i)]} for i in range(50)]
    expected = [{"id": row["id"], "data_length": len(row["data"])} for row in rows]
    yt_client.write_table(table_in, rows)

    df = direct_session.read.yt(table_in)
    yson_len = udf(lambda yson_bytes: len(yson.loads(bytes(yson_bytes))) if yson_bytes else 0, IntegerType())
    df.select("id", yson_len("data").alias("data_length")).write.yt(table_out)

    result = yt_client.read_table(table_out)
    assert_items_equal(result, expected)


def test_history_server(history_server):
    log_path = f"ytEventLog:/{history_server.discovery_path}/logs/event_log_table"
    conf_patch = {"spark.eventLog.enabled": "true", "spark.eventLog.dir": log_path}
    spark_conf = SparkConf().setAll(SPARK_CONF.getAll()).setAll(conf_patch.items())
    with spyt.direct_spark_session(YT_PROXY, spark_conf) as direct_session:
        app_id = direct_session.conf.get("spark.app.id")
    applications = requests.get(f"http://{history_server.rest()}/api/v1/applications").json()
    assert len(applications) == 1
    assert applications[0]['id'] == app_id


# TODO write tests for cluster submit and cluster submit with dependencies
