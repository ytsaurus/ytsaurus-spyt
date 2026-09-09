import spyt.conf as spyt_conf
import spyt.connect as spyt_connect
import spyt.spec as spyt_spec
from spyt.connect import start_connect_server, start_connect_server_inner_cluster, \
    list_active_connect_servers_inner_cluster, wait_for_spark_connect_endpoint

from common.helpers import assert_items_equal, assert_sequences_equal, wait_for_operation
from functools import reduce
from itertools import chain
from types import SimpleNamespace
import os
import shlex
import subprocess
import time
import pytest
from pyspark.sql import SparkSession
import pyspark.sql.connect.functions as f
from pyspark.sql.types import Row, StringType
from spyt.types import UInt64Type
import yt.yson as yt_yson
from utils import upload_file


@pytest.fixture
def connect_server_spec_client(yt_client, monkeypatch):
    def read_version_config(path, client):
        version = str(path).split("/")[-2]
        return {
            "spark_conf": {},
            "environment": {},
            "layer_paths": [],
            "squashfs_layer_paths": [],
            "default_cluster_java_home": "/opt/jdk",
            "spark_yt_base_path": f"//home/spark/spyt/releases/{version}",
            "file_paths": [f"//home/spark/spyt/releases/{version}/spyt-package.zip"],
            "enablers": {"enable_squashfs": True},
        }

    monkeypatch.setattr(spyt_connect, "read_global_conf", lambda client: {})
    monkeypatch.setattr(spyt_conf, "get", read_version_config)
    monkeypatch.setattr(
        spyt_conf, "yt_list",
        lambda path, client: ["spark.tgz", "spark.squashfs", "extra.jar"],
    )
    monkeypatch.setattr(spyt_connect, "get_user_name", lambda client: "test-user")
    monkeypatch.setattr(spyt_spec, "get_user_name", lambda client: "test-user")
    monkeypatch.setattr(
        spyt_connect, "run_operation", lambda builder, sync, client: builder.build(),
    )
    return yt_client


@pytest.mark.parametrize("spyt_override", [{}, {"spyt_version": None}, {"spyt_version": "2.11.0"}])
@pytest.mark.parametrize("spark_override", [{}, {"spark_version": None}, {"spark_version": "4.1.2"}])
@pytest.mark.parametrize("enable_squashfs", [False, True])
def test_connect_server_spec_versions(
    connect_server_spec_client, spyt_override, spark_override, enable_squashfs,
):
    spec = spyt_connect.start_connect_server(
        connect_server_spec_client,
        spark_conf={"spark.ytsaurus.squashfs.enabled": str(enable_squashfs)},
        **spyt_override, **spark_override,
    )
    spyt_version = spyt_override.get("spyt_version") or spyt_connect.default_spyt_version
    spark_version = spark_override.get("spark_version") or spyt_connect.default_spark_version
    spark_root = str(spyt_conf.DISTRIB_BASE_PATH.join(spark_version.replace(".", "/")))
    spyt_root = f"//home/spark/spyt/releases/{spyt_version}"
    driver = spec["tasks"]["driver"]

    if enable_squashfs:
        assert driver["layer_paths"] == [
            f"{spyt_root}/spyt-package.squashfs", f"{spark_root}/spark.squashfs",
        ]
        assert driver["file_paths"] == []
        assert "--use-squashfs" in driver["command"]
    else:
        assert driver["file_paths"] == [
            f"{spyt_root}/spyt-package.zip", f"{spark_root}/spark.tgz",
            f"{spark_root}/extra.jar",
        ]
        assert "--spark-distributive spark.tgz" in driver["command"]


def test_connect_server_spec_versions_affect_reuse(connect_server_spec_client):
    def settings_hash(**kwargs):
        spec = spyt_connect.start_connect_server(connect_server_spec_client, **kwargs)
        return spec["annotations"]["settings_hash"]

    default_hash = settings_hash()
    assert settings_hash(
        spyt_version=spyt_connect.default_spyt_version,
        spark_version=spyt_connect.default_spark_version,
    ) == default_hash
    assert settings_hash(spyt_version=None, spark_version=None) == default_hash
    assert len({
        default_hash,
        settings_hash(spyt_version="2.11.0"),
        settings_hash(spark_version="4.1.2"),
        settings_hash(spyt_version="2.11.0", spark_version="4.1.2"),
    }) == (
        len({spyt_connect.default_spyt_version, "2.11.0"})
        * len({spyt_connect.default_spark_version, "4.1.2"})
    )


@pytest.mark.parametrize("files", [None, [], ["//tmp/config file", "yt:///tmp/file's-$name"]])
@pytest.mark.parametrize("jars", [None, [], ["//tmp/first.jar", "yt:///tmp/second jar.jar"]])
def test_connect_server_spec_files_and_jars(connect_server_spec_client, files, jars):
    spec = spyt_connect.start_connect_server(connect_server_spec_client, files=files, jars=jars)
    command = shlex.split(spec["tasks"]["driver"]["command"])

    for flag, paths, expected in [
        ("--files", files, "yt:///tmp/config file,yt:///tmp/file's-$name"),
        ("--jars", jars, "yt:///tmp/first.jar,yt:///tmp/second jar.jar"),
    ]:
        if paths:
            assert command.count(flag) == 1
            index = command.index(flag)
            assert command[index + 1] == expected
            assert index < command.index("spark-internal")
        else:
            assert flag not in command


@pytest.mark.parametrize("option", ["files", "jars"])
@pytest.mark.parametrize("path", ["//tmp/first,second", "yt:///tmp/first,second"])
def test_connect_server_rejects_commas(option, path):
    with pytest.raises(ValueError, match=f"{option} path .* contains a comma"):
        start_connect_server(None, **{option: [path]})


def test_connect_server_command_quoting(connect_server_spec_client, tmp_path):
    title = "Connect \"server\" 'name' $HOME $(false) `false`"
    conf_value = "value with spaces; $HOME $(false) 'quotes'"
    files = ["//tmp/file's name", "yt:///tmp/$HOME"]
    spec = start_connect_server(
        connect_server_spec_client, title=title, files=files,
        spark_conf={"spark.test.value": conf_value},
    )
    command = spec["tasks"]["driver"]["command"].split(" && ", 1)[1]
    spark_submit = tmp_path / "spark" / "bin" / "spark-submit"
    spark_submit.parent.mkdir(parents=True)
    spark_submit.write_text("#!/bin/sh\nprintf '%s\\0' \"$@\"\n")
    spark_submit.chmod(0o755)

    result = subprocess.run(
        ["/bin/sh", "-c", command], cwd=tmp_path,
        env={**os.environ, "YT_OPERATION_ID": "test-operation-id"},
        capture_output=True, text=True, check=True,
    )
    args = result.stdout.rstrip("\0").split("\0")
    assert args[args.index("--name") + 1] == title
    assert args[args.index("--files") + 1] == "yt:///tmp/file's name,yt:///tmp/$HOME"
    conf = [args[index + 1] for index, arg in enumerate(args) if arg == "--conf"]
    assert f"spark.test.value={conf_value}" in conf
    assert "spark.ytsaurus.driver.operation.id=test-operation-id" in conf
    assert args[-1] == "spark-internal"


def test_connect_server_spec_dependencies_affect_reuse(connect_server_spec_client):
    def settings_hash(**kwargs):
        spec = spyt_connect.start_connect_server(connect_server_spec_client, **kwargs)
        return spec["annotations"]["settings_hash"]

    default_hash = settings_hash()
    assert settings_hash(files=None, jars=None) == default_hash
    assert settings_hash(files=[], jars=[]) == default_hash
    assert settings_hash(files=["//tmp/file"], jars=["//tmp/dep.jar"]) == settings_hash(
        files=["yt:///tmp/file"], jars=["yt:///tmp/dep.jar"],
    )
    assert len({
        default_hash,
        settings_hash(files=["//tmp/file"]),
        settings_hash(files=["//tmp/other"]),
        settings_hash(jars=["//tmp/dep.jar"]),
        settings_hash(jars=["//tmp/other.jar"]),
        settings_hash(files=["//tmp/file"], jars=["//tmp/dep.jar"]),
    }) == 6


@pytest.mark.parametrize("reverse_files", [False, True])
@pytest.mark.parametrize("reverse_jars", [False, True])
def test_connect_server_reuses_reordered_dependencies(
    connect_server_spec_client, monkeypatch, reverse_files, reverse_jars,
):
    files = ["//tmp/first.txt", "//tmp/second.txt"]
    jars = ["//tmp/first.jar", "//tmp/second.jar"]
    spec = start_connect_server(connect_server_spec_client, files=files, jars=jars)
    settings_hash = spec["annotations"]["settings_hash"]
    reordered_files = files[::-1] if reverse_files else files
    reordered_jars = jars[::-1] if reverse_jars else jars
    reordered_spec = start_connect_server(
        connect_server_spec_client, files=reordered_files, jars=reordered_jars,
    )
    assert reordered_spec["annotations"]["settings_hash"] == settings_hash

    operation_id = "1-2-3-4"
    monkeypatch.setattr(
        spyt_connect, "Operation", lambda id, type, client: SimpleNamespace(id=id),
    )
    monkeypatch.setattr(connect_server_spec_client, "list_operations", lambda **kwargs: {
        "operations": [{
            "id": operation_id,
            "type": "vanilla",
            "runtime_parameters": {"annotations": {"settings_hash": settings_hash}},
        }],
    })
    reused_operation = start_connect_server(
        connect_server_spec_client, files=reordered_files, jars=reordered_jars,
        reuse_existing=True,
    )
    assert reused_operation.id == operation_id


def test_connect_server_files_and_jars(yt_client, tmp_dir, spark_connect_session_factory):
    file_path = f"{tmp_dir}/message.txt"
    jar_path = f"{tmp_dir}/deps.jar"
    yt_client.create("file", file_path)
    yt_client.write_file(file_path, b"file-dep-loaded")
    upload_file(yt_client, "jobs/deps.jar", jar_path)

    operation = start_connect_server(yt_client, files=[file_path], jars=[jar_path])
    try:
        endpoint = wait_for_spark_connect_endpoint(yt_client, operation.id)
        with spark_connect_session_factory(endpoint=endpoint) as spark:
            @f.udf(StringType())
            def read_file():
                """Read server-wide files outside Connect's isolated session artifact directory."""
                import os

                with open(os.path.join(os.environ["HOME"], "message.txt")) as file:
                    return file.read()

            result = spark.range(1).select(
                read_file().alias("file_value"),
                f.expr("reflect('org.example.JarDep', 'value')").alias("jar_value"),
            ).collect()
            assert result == [Row(file_value="file-dep-loaded", jar_value="jar-dep-loaded")]
    finally:
        yt_client.complete_operation(operation.id)


def test_connect_server_settings_hash_is_deterministic():
    first_settings = {
        "nested": {"second": 2, "first": 1},
        "items": [{"second": 2, "first": 1}, "value"],
        "file": yt_yson.to_yson_type(
            "//tmp/file",
            attributes={"file_name": "file", "executable": True},
        ),
    }
    second_settings = {
        "file": yt_yson.to_yson_type(
            "//tmp/file",
            attributes={"executable": True, "file_name": "file"},
        ),
        "items": [{"first": 1, "second": 2}, "value"],
        "nested": {"first": 1, "second": 2},
    }

    assert spyt_connect._connect_server_settings_hash(first_settings) == \
        spyt_connect._connect_server_settings_hash(second_settings)
    second_settings["items"].reverse()
    assert spyt_connect._connect_server_settings_hash(first_settings) != \
        spyt_connect._connect_server_settings_hash(second_settings)


def test_wait_for_spark_connect_endpoint_checks_reachability(monkeypatch):
    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            pass

    class Client:
        call_count = 0

        def get_operation(self, operation_id):
            assert operation_id == "operation-id"
            self.call_count += 1
            return {
                "runtime_parameters": {
                    "annotations": {
                        "spark_connect_endpoint": "[::1]:27080",
                    },
                },
            }

    connection_attempt_count = 0

    def create_connection(address, timeout):
        nonlocal connection_attempt_count
        connection_attempt_count += 1
        assert address == ("::1", 27080)
        assert timeout <= 1
        if connection_attempt_count == 1:
            raise TimeoutError
        return Connection()

    monkeypatch.setattr(spyt_connect.socket, "create_connection", create_connection)
    monkeypatch.setattr(spyt_connect.time, "sleep", lambda timeout: None)

    client = Client()
    endpoint = wait_for_spark_connect_endpoint(client, "operation-id")

    assert endpoint == "[::1]:27080"
    assert client.call_count == 2
    assert connection_attempt_count == 2


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


def test_reuse_existing_server(yt_client):
    title = "Spark connect reuse test"
    operations = {}
    try:
        operation = start_connect_server(yt_client, title=title)
        operations[operation.id] = operation
        wait_for_spark_connect_endpoint(yt_client, operation.id)

        reused_operation = start_connect_server(yt_client, title=title, reuse_existing=True)
        operations[reused_operation.id] = reused_operation
        assert reused_operation.id == operation.id

        different_operation = start_connect_server(
            yt_client,
            title=title,
            reuse_existing=True,
            executor_memory="3G",
        )
        operations[different_operation.id] = different_operation
        assert different_operation.id != operation.id
        wait_for_spark_connect_endpoint(yt_client, different_operation.id)
    finally:
        for operation in operations.values():
            yt_client.complete_operation(operation.id)


def test_abort_driver_job_stops_operation(yt_client):
    operation = start_connect_server(yt_client, fail_on_job_restart=True)
    try:
        wait_for_spark_connect_endpoint(yt_client, operation.id)
        driver_jobs = yt_client.list_jobs(operation.id, job_state="running")["jobs"]
        assert len(driver_jobs) == 1

        yt_client.abort_job(driver_jobs[0]["id"])

        operation_state = wait_for_operation(yt_client, operation.id)
        assert str(operation_state) == "failed"
    finally:
        if not yt_client.get_operation_state(operation.id).is_finished():
            yt_client.complete_operation(operation.id)


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
