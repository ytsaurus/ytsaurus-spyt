import spyt

from common.helpers import assert_items_equal
from pyspark.conf import SparkConf
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, Row
from pyspark.sql.functions import col, udf
import requests
from utils import SPARK_CONF, YT_PROXY, upload_file
from utils import wait_for_operation


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
    yson_len = udf(lambda yson_bytes: len(yson_bytes) if yson_bytes else 0, IntegerType())
    df.select("id", yson_len("data").alias("data_length")).write.yt(table_out)

    result = yt_client.read_table(table_out)
    assert_items_equal(result, expected)


def test_history_server(history_server):
    log_path = f"ytEventLog:/{history_server.discovery_path}/logs/event_log_table"
    conf_patch = {"spark.eventLog.enabled": "true", "spark.eventLog.dir": log_path}
    spark_conf = SparkConf().setAll(SPARK_CONF.items()).setAll(conf_patch.items())
    with spyt.direct_spark_session(YT_PROXY, spark_conf) as direct_session:
        app_id = direct_session.conf.get("spark.app.id")
    applications: list = requests.get(f"http://{history_server.rest()}/api/v1/applications").json()
    assert len(applications) == 1
    assert applications[0]['id'] == app_id


def prepare_in_table(tmp_dir, yt_client):
    table_in = f"{tmp_dir}/t_in"
    yt_client.create("table", table_in,
                     attributes={"schema": [{"name": "name", "type": "string"}, {"name": "price", "type": "uint32"}]})
    rows = [{'name': 'potato', 'price': 2}, {'name': 'milk', 'price': 5}, {'name': 'meat', 'price': 10}]
    yt_client.write_table(table_in, rows)
    return rows, table_in


def test_cluster_mode(yt_client, tmp_dir, direct_submitter):
    rows, table_in = prepare_in_table(tmp_dir, yt_client)

    table_out = f"{tmp_dir}/t_out"
    upload_file(yt_client, 'jobs/spark_id.py', f'{tmp_dir}/spark_id.py')
    operation_id = direct_submitter.submit(f'yt:/{tmp_dir}/spark_id.py', job_args=[table_in, table_out])

    assert operation_id is not None
    wait_for_operation(yt_client, operation_id)
    assert_items_equal(yt_client.read_table(table_out), rows)


def test_cluster_mode_multi_operations(yt_client, tmp_dir, direct_submitter):
    rows, table_in = prepare_in_table(tmp_dir, yt_client)
    upload_file(yt_client, 'jobs/spark_id.py', f'{tmp_dir}/spark_id.py')

    operation_ids = []
    table_outs = []
    for i in range(3):
        table_out = f"{tmp_dir}/t_out_{i}"
        table_outs.append(table_out)
        op_id = direct_submitter.submit(
            f'yt:/{tmp_dir}/spark_id.py',
            job_args=[table_in, table_out]
        )
        assert op_id is not None
        operation_ids.append(op_id)

    assert len(set(operation_ids)) == 3, f"Operation IDs are not unique: {operation_ids}"

    for op_id in operation_ids:
        wait_for_operation(yt_client, op_id)

    for table_out in table_outs:
        assert_items_equal(yt_client.read_table(table_out), rows)


def test_cluster_mode_broken_spark_conf(yt_client, tmp_dir, direct_submitter):
    rows, table_in = prepare_in_table(tmp_dir, yt_client)

    table_out = f"{tmp_dir}/t_out"
    upload_file(yt_client, 'jobs/spark_id.py', f'{tmp_dir}/spark_id.py')
    invalid_spark_conf = {"spark.executor.memory": "0M"}
    try:
        direct_submitter.submit(f'yt:/{tmp_dir}/spark_id.py',
                                           job_args=[table_in, table_out],
                                           conf=invalid_spark_conf)
        assert False, "Exception was not raised"
    except Exception as e:
        assert "Failed to submit Spark job" in str(e)



def test_cluster_mode_with_dependencies(yt_client, tmp_dir, direct_submitter):
    table_in = f"{tmp_dir}/t_dep_in"
    table_out = f"{tmp_dir}/t_dep_out"
    yt_client.create("table", table_in, attributes={"schema": [{"name": "num", "type": "int64"}]})
    rows = [{"num": i} for i in range(3)]
    yt_client.write_table(table_in, rows)

    for script in ['spark_job_with_dependencies.py', 'dependencies.py']:
        upload_file(yt_client, f'jobs/{script}', f'{tmp_dir}/{script}')

    operation_id = direct_submitter.submit(f'yt:/{tmp_dir}/spark_job_with_dependencies.py',
                                           spark_base_args=['--py-files', f'yt:/{tmp_dir}/dependencies.py'],
                                           job_args=[table_in, table_out])

    assert operation_id is not None
    wait_for_operation(yt_client, operation_id)

    result_rows = [
        {"key": "Reminder count for 0"},
        {"key": "Reminder count for 1"},
        {"key": "Reminder count for 2"},
    ]
    assert_items_equal(yt_client.read_table(table_out), result_rows)


def test_cluster_mode_with_secret_parameters(yt_client, tmp_dir, direct_submitter):
    table_out = f"{tmp_dir}/t_secret_out"
    upload_file(yt_client, "jobs/spark_secrets.py", f"{tmp_dir}/spark_secrets.py")

    operation_id = direct_submitter.submit(f"yt:/{tmp_dir}/spark_secrets.py", job_args=[table_out], conf={
        "spark.some.secret.key": "aKeyToHide",
        "spark.some.password": "p@ssw0rd",
        "spark.external.service.token": "t0k3n",
    })

    assert operation_id is not None

    opeartion = yt_client.get_operation(operation_id)
    command = opeartion['spec']['tasks']['driver']['command']
    assert "spark.some.secret.key" not in command
    assert "spark.some.password" not in command
    assert "spark.external.service.token" not in command

    final_state = wait_for_operation(yt_client, operation_id)
    assert not final_state.is_unsuccessfully_finished()

    result_rows = [{
        "secret": "aKeyToHide",
        "password": "p@ssw0rd",
        "token": "t0k3n",
        "secret_env": "aKeyToHide",
        "password_env": "p@ssw0rd",
        "token_env": "t0k3n",
    }]
    assert_items_equal(yt_client.read_table(table_out), result_rows)


def test_archives(yt_client, tmp_dir, direct_submitter):
    table_out = f"{tmp_dir}/t_arc_out"

    for script in ['spark_job_archives.py', 'deps.tar', 'deps2.zip']:
        upload_file(yt_client, f'jobs/{script}', f'{tmp_dir}/{script}')

    operation_id = direct_submitter.submit(f'yt:/{tmp_dir}/spark_job_archives.py',
                                           spark_base_args=['--archives',
                                                            f'yt:/{tmp_dir}/deps.tar#arcdep,yt:/{tmp_dir}/deps2.zip'],
                                           job_args=[table_out])
    assert operation_id is not None
    wait_for_operation(yt_client, operation_id)

    result_rows = [{"_1": "Sp4rK", "_2": "YTsaaaurus"}]
    assert_items_equal(yt_client.read_table(table_out), result_rows)


csv_data = """id,name,value
1,Name 1,10
2,Name 2,20
3,Name 3,30
4,Name 4,40
5,Name 5,50
6,Name 6,60
"""

csv_schema = [
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('value', IntegerType(), True)
]

csv_rows = [
    Row(id=1, name='Name 1', value=10),
    Row(id=2, name='Name 2', value=20),
    Row(id=3, name='Name 3', value=30),
    Row(id=4, name='Name 4', value=40),
    Row(id=5, name='Name 5', value=50),
    Row(id=6, name='Name 6', value=60)
]


def test_reading_csv_file(yt_client, tmp_dir, direct_session):
    csv_file_in = f"{tmp_dir}/data.csv"
    yt_client.write_file(csv_file_in, csv_data.encode("utf-8"))

    df = direct_session.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(f"yt:/{csv_file_in}")

    assert_items_equal(df.schema.fields, csv_schema)
    assert_items_equal(df.collect(), csv_rows)


def test_writing_csv_file(yt_client, tmp_dir, direct_session):
    df = direct_session.createDataFrame(data=csv_rows, schema=StructType(csv_schema))
    csv_dir_out = f"{tmp_dir}/result.csv"
    df.coalesce(1).write.format("csv").option("header", "true").save(f"yt:/{csv_dir_out}")

    result_file = yt_client.list(csv_dir_out)[0]
    result_csv = yt_client.read_file(f"{csv_dir_out}/{result_file}").read().decode("utf-8")
    assert result_csv == csv_data
