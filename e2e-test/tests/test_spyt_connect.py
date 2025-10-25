from spyt.connect import start_connect_server

from common.helpers import assert_items_equal
from contextlib import contextmanager
from hashlib import sha256
from itertools import chain
import time
from functools import reduce
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import Row
from utils import wait_for_operation
from yt.wrapper.http_helpers import get_token
import yt.yson as yt_yson


def _wait_for_spark_connect_endpoint(yt_client, operation_id):
    spark_connect_endpoint = None
    while not spark_connect_endpoint:
        operation = yt_client.get_operation(operation_id)
        spark_connect_endpoint = (reduce(lambda map, key: map[key] if map and key in map else None,
                                         ['runtime_parameters', 'annotations', 'spark_connect_endpoint'],
                                         operation))
        if spark_connect_endpoint:
            return str(spark_connect_endpoint)
        time.sleep(1)


@contextmanager
def _spark_connect_session(yt_client):
    operation = start_connect_server(yt_client)
    spark = None

    try:
        spark_connect_endpoint = _wait_for_spark_connect_endpoint(yt_client, operation.id)
        spark = (SparkSession
                 .builder
                 .remote(f"sc://{spark_connect_endpoint}")
                 .getOrCreate())
        yield spark
    finally:
        if spark:
            spark.stop()
        yt_client.complete_operation(operation.id)


def test_idle_shutdown(yt_client):
    idle_timeout_seconds = 30
    spark_conf = {"spark.ytsaurus.connect.idle.timeout": f"{idle_timeout_seconds}s"}
    operation = start_connect_server(yt_client, spark_conf=spark_conf)
    start = time.time()
    wait_for_operation(yt_client, operation.id)
    finish = time.time()
    assert finish - start > idle_timeout_seconds


def test_two_servers(yt_client):
    grpc_port = 27080
    op1, op2 = None, None
    try:
        op1 = start_connect_server(yt_client, grpc_port_start=grpc_port)
        endpoint_1 = _wait_for_spark_connect_endpoint(yt_client, op1.id)
        assert endpoint_1 == f"localhost:{grpc_port}"

        op2 = start_connect_server(yt_client, grpc_port_start=grpc_port)
        endpoint_2 = _wait_for_spark_connect_endpoint(yt_client, op2.id)
        assert endpoint_2 == f"localhost:{grpc_port + 1}"
    finally:
        for op in [op1, op2]:
            if op:
                yt_client.complete_operation(op.id)


def test_base_request(yt_client):
    with _spark_connect_session(yt_client) as spark:
        df = spark.range(0, 93)
        result = df.groupBy((f.col("id") % 4).alias("rem")).count().collect()
        expected = [
            Row(rem=0, count=24),
            Row(rem=1, count=23),
            Row(rem=2, count=23),
            Row(rem=3, count=23),
        ]
        assert_items_equal(result, expected)


def test_refresh_token(yt_client):
    token_hash = sha256(get_token(client=yt_client).encode()).hexdigest()
    token_path = f"//sys/cypress_tokens/{token_hash}"
    counter_before = yt_client.get(f"{token_path}/@access_counter")
    spark_conf = {
        "spark.ytsaurus.connect.idle.timeout": f"30s",
        "spark.ytsaurus.connect.token.refresh.period": f"10s",
    }
    operation = start_connect_server(yt_client, spark_conf=spark_conf)
    wait_for_operation(yt_client, operation.id)
    counter_after = yt_client.get(f"{token_path}/@access_counter")
    assert counter_after - counter_before >= 3, "Should be at least 3 pings to token"


def test_custom_types(yt_client, tmp_dir):
    path = f"{tmp_dir}/table_with_custom_types"
    yt_client.create("table", path, attributes={"schema": [
        {"name": "id", "type": "uint64"},
        {"name": "json_field", "type_v3": "json"},
        {"name": "uuid_field", "type_v3": "uuid"},
        {"name": "yson_field", "type_v3": {"type_name": "optional", "item": "yson"}}
    ]})

    yt_yson_rows = [
        {"string": "string1", "int": 1234567890},
        {"string": "string2", "short": 321, "long": 6347568734657887},
        None
    ]

    def generate_row(id):
        return {
            "id": id,
            "json_field": "{" + ",".join([f'"key_{x}": {x*x}' for x in range(1, (id % 10) + 1)]) + "}",
            "uuid_field": b'\x16m\xca\xbd\xec\xcbO\xa3\xa1\x19\xd1A\xceaG*',
            "yson_field": yt_yson_rows[id % 3]
        }

    rows = [generate_row(id) for id in chain(range(1, 11), range(1 << 63, (1 << 63) + 5))]
    yt_client.write_table(path, rows)

    expected_rows = [
        Row(id=row["id"],
            json_field=row["json_field"],
            uuid_field="bdca6d16-cbec-a34f-a119-d141ce61472a",
            yson_field= bytearray(yt_yson.dumps(row["yson_field"], "binary")) if row["yson_field"] else None)
        for row in rows
    ]

    with _spark_connect_session(yt_client) as spark:
        df = spark.read.format("yt").load(f"yt:/{path}")
        result = df.collect()
        assert_items_equal(result, expected_rows)
