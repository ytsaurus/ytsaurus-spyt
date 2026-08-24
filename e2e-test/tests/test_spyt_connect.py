from spyt.connect import start_connect_server, start_connect_server_inner_cluster, \
    list_active_connect_servers_inner_cluster, wait_for_spark_connect_endpoint

from common.helpers import assert_items_equal, assert_sequences_equal, wait_for_operation
from functools import reduce
from itertools import chain
import time
from pyspark.sql import SparkSession
import pyspark.sql.connect.functions as f
from pyspark.sql.types import Row, StringType
from spyt.types import UInt64Type
import yt.yson as yt_yson


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
        endpoint_1 = wait_for_spark_connect_endpoint(yt_client, op1.id)
        assert endpoint_1 == f"localhost:{grpc_port}"

        op2 = start_connect_server(yt_client, grpc_port_start=grpc_port)
        endpoint_2 = wait_for_spark_connect_endpoint(yt_client, op2.id)
        assert endpoint_2 == f"localhost:{grpc_port + 1}"
    finally:
        for op in [op1, op2]:
            if op:
                yt_client.complete_operation(op.id)


def test_web_ui_endpoint(yt_client):
    operation = start_connect_server(yt_client)
    try:
        web_ui_endpoint = None
        timeout = 30
        while not web_ui_endpoint and timeout > 0:
            operation_data = yt_client.get_operation(operation.id)
            web_ui_endpoint = (reduce(lambda map, key: map[key] if map and key in map else None,
                                      ["runtime_parameters", "annotations", "description", "Web UI"], operation_data))
            time.sleep(1)
            timeout -= 1

        assert web_ui_endpoint, "Web UI endpoint not set in Spark Connect driver operation"
    finally:
        yt_client.complete_operation(operation.id)


def _test_base_request(spark):
    df = spark.range(0, 93)
    result = df.groupBy((f.col("id") % 4).alias("rem")).count().collect()
    expected = [
        Row(rem=0, count=24),
        Row(rem=1, count=23),
        Row(rem=2, count=23),
        Row(rem=3, count=23),
    ]
    assert_items_equal(result, expected)


def test_base_request(spark_connect_session_factory):
    with spark_connect_session_factory() as spark:
        _test_base_request(spark)


def test_base_request_inner_cluster(yt_client, spyt_cluster, spark_connect_session_factory):
    endpoint = start_connect_server_inner_cluster(yt_client, spyt_cluster.discovery_path)
    with spark_connect_session_factory(endpoint=endpoint) as spark:
        _test_base_request(spark)


def test_custom_types(yt_client, tmp_dir, spark_connect_session_factory):
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

    with spark_connect_session_factory() as spark:
        df = spark.read.yt(path)
        result = df.collect()
        assert_items_equal(result, expected_rows)


def test_sql_mixed_sort_orders(yt_client, tmp_dir, spark_connect_session_factory):
    path = f"{tmp_dir}/mixed_sort_orders"

    with spark_connect_session_factory() as spark:
        test_data = [
            (2023, "Electronics", 2.5, "Laptop X1"),
            (2023, "Electronics", 1.2, "Tablet Pro"),
            (2023, "Clothing", 0.8, "Jacket Winter"),
            (2022, "Electronics", 3.1, "Desktop Gamer"),
            (2022, "Clothing", 0.5, "T-Shirt Summer"),
            (2022, "Books", 1.0, "Novel BestSeller"),
            (2021, "Electronics", 2.8, "Laptop Old"),
            (2021, "Books", 0.9, "Science Physics")
        ]

        expected = [
            {'year': 2023, 'category': 'Clothing', 'weight_kg': 0.8, 'product_name': 'Jacket Winter'},
            {'year': 2023, 'category': 'Electronics', 'weight_kg': 2.5, 'product_name': 'Laptop X1'},
            {'year': 2023, 'category': 'Electronics', 'weight_kg': 1.2, 'product_name': 'Tablet Pro'},
            {'year': 2022, 'category': 'Books', 'weight_kg': 1.0, 'product_name': 'Novel BestSeller'},
            {'year': 2022, 'category': 'Clothing', 'weight_kg': 0.5, 'product_name': 'T-Shirt Summer'},
            {'year': 2022, 'category': 'Electronics', 'weight_kg': 3.1, 'product_name': 'Desktop Gamer'},
            {'year': 2021, 'category': 'Books', 'weight_kg': 0.9, 'product_name': 'Science Physics'},
            {'year': 2021, 'category': 'Electronics', 'weight_kg': 2.8, 'product_name': 'Laptop Old'}
        ]

        df = spark.createDataFrame(
            test_data,
            ["year", "category", "weight_kg", "product_name"]
        )

        df.createOrReplaceTempView("products")

        spark.sql(f"""
            CREATE TABLE yt.`{path}`
            USING yt
            OPTIONS (
                sort_columns '["year","category","weight_kg"]',
                sort_orders '["desc","asc","desc"]'
            )
            AS SELECT * FROM products ORDER BY year DESC, category ASC, weight_kg DESC
        """)

    yt_schema = yt_client.get_table_schema(path)

    for column_schema in yt_schema.to_yson_type():
        if column_schema["name"] == "year":
            assert column_schema["sort_order"] == "descending"
        elif column_schema["name"] == "category":
            assert column_schema["sort_order"] == "ascending"
        elif column_schema["name"] == "weight_kg":
            assert column_schema["sort_order"] == "descending"
        elif column_schema["name"] == "product_name":
            assert "sort_order" not in column_schema  # Not sorted column

    result = list(yt_client.read_table(path))
    assert_sequences_equal(result, expected)


def test_list_active_connect_servers_inner_clusters(yt_client, spyt_cluster):
    spark_conf = {"spark.ytsaurus.connect.settings.hash": "some hash"}
    endpoint = start_connect_server_inner_cluster(yt_client, spyt_cluster.discovery_path, spark_conf=spark_conf)
    active_servers = list_active_connect_servers_inner_cluster(yt_client, spyt_cluster.discovery_path)
    assert len(active_servers) == 1
    assert active_servers[0]["endpoint"] == endpoint
    assert active_servers[0]["settingsHash"] == "some hash"
    assert active_servers[0]["driverId"] is not None


def test_string_as_binary(yt_client, tmp_dir, spark_connect_session_factory):
    path = f"{tmp_dir}/table_with_strings"
    yt_client.create("table", path, attributes={"schema": [
        {"name": "id", "type": "int64"},
        {"name": "value", "type": "string"}
    ]})
    rows = [{"id": id, "value": f"value {id}"} for id in range(1, 5)]
    yt_client.write_table(path, rows)

    spark_conf = {"spark.ytsaurus.arrow.stringToBinary": "true"}
    with spark_connect_session_factory(spark_conf=spark_conf) as spark:
        df = spark.read.yt(path)
        assert type(df.collect()[0]["value"]) == bytes


def test_python_udf(yt_client, tmp_dir, spark_connect_session_factory):
    path_in = f"{tmp_dir}/table_with_strings"
    path_out = f"{tmp_dir}/table_with_hashes"

    yt_client.create("table", path_in, attributes={"schema": [
        {"name": "id", "type": "int64"},
        {"name": "value", "type": "string"}
    ]})
    rows = [{"id": id, "value": f"value {id}"} for id in range(1, 10)]
    yt_client.write_table(path_in, rows)

    reverse_udf = f.udf(lambda x: x[::-1], StringType())
    with spark_connect_session_factory() as spark:
        df = spark.read.yt(path_in)
        df.withColumn("v_reversed", reverse_udf("value")).drop("value").write.yt(path_out)

    expected = [{"id": id, "v_reversed": f"value {id}"[::-1]} for id in range(1, 10)]
    actual = [{k: v for k, v in row.items()} for row in yt_client.read_table(path_out)]
    assert_items_equal(actual, expected)


def test_uint64_deserialization(yt_client, tmp_dir, spark_connect_session_factory):
    table_path = f"{tmp_dir}/table_with_uint64"
    yt_client.create("table", table_path, attributes={"schema": [
        {"name": "id", "type": "uint64"},
        {"name": "value", "type": "string"}
    ]})
    rows = [
        {"id": 1, "value": "value 1"},
        {"id": 2, "value": "value 2"},
        {"id": 3, "value": "value 3"},
        {"id": 9223372036854775816, "value": "value 4"},
        {"id": 9223372036854775813, "value": "value 5"},
        {"id": 18446744073709551615, "value": "value 6"},
    ]
    yt_client.write_table(table_path, rows)

    expected = [1, 2, 3, 9223372036854775816, 9223372036854775813, 18446744073709551615]

    with spark_connect_session_factory() as spark:
        df = spark.read.yt(table_path)

        collected = [row.id for row in df.collect()]
        assert_items_equal(collected, expected)

        collected_explicit_cast = [row.id for row in df.select(f.col("id").cast(UInt64Type())).collect()]
        assert_items_equal(collected_explicit_cast, expected)

        pandas_list = df.toPandas()["id"].tolist()
        assert_items_equal(pandas_list, expected)
