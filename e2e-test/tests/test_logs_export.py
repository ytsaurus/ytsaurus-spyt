import pytest
from utils import upload_file,get_executors_operation_id
from spyt.standalone import find_spark_cluster
from pyspark.sql.functions import rand

input_config_with_tvm = {"spark.ytsaurus.logs.export.enabled": "true",
                         "spark.ytsaurus.driver.operation.parameters" : "{secure_vault={tvm_logs=\"secret\"}}",
                         "spark.ytsaurus.executor.operation.parameters" : "{secure_vault={tvm_logs=\"secret\"}}"}

@pytest.mark.yandex_internal
@pytest.mark.parametrize("spyt_cluster", [{"tvm_secret": "secret",
                                           "enable_monium_logs_export" : "True"}], indirect=True)
def test_logs_export_for_standalone_cluster_with_tvm(yt_client, spyt_cluster):
    cluster_info = find_spark_cluster(spyt_cluster.discovery_path, yt_client)

    assert_logs(yt_client=yt_client, op_id=cluster_info.operation_id)

@pytest.mark.yandex_internal
@pytest.mark.parametrize("spyt_cluster", [{"enable_monium_logs_export" : "True"}], indirect=True)
def test_logs_export_for_standalone_cluster_no_tvm(yt_client, spyt_cluster):
    cluster_info = find_spark_cluster(spyt_cluster.discovery_path, yt_client)

    assert_no_logs(yt_client=yt_client, op_id=cluster_info.operation_id)

@pytest.mark.yandex_internal
def test_logs_export_for_direct_submit_cluster_mode_with_tvm(yt_client, tmp_dir, direct_submitter):
    config = input_config_with_tvm
    op_id = spark_job_for_cluster(yt_client, tmp_dir, direct_submitter, config)
    executor_op_id = get_executors_operation_id(yt_client, op_id, retries=80)

    assert_logs(yt_client, op_id)
    assert_logs(yt_client, executor_op_id)

@pytest.mark.yandex_internal
def test_logs_export_for_direct_submit_cluster_mode_no_tvm(yt_client, tmp_dir, direct_submitter):
    config = {"spark.ytsaurus.logs.export.enabled": "true"}
    op_id = spark_job_for_cluster(yt_client, tmp_dir, direct_submitter, config)
    executor_op_id = get_executors_operation_id(yt_client, op_id, retries=80)

    assert_no_logs(yt_client, op_id)
    assert_no_logs(yt_client, executor_op_id)

@pytest.mark.yandex_internal
@pytest.mark.parametrize("direct_session", [input_config_with_tvm], indirect=True)
def test_logs_export_for_direct_submit_client_mode_with_tvm(yt_client, direct_session):
    op_id = spark_job_for_client(direct_session)

    assert_logs(yt_client, op_id)

@pytest.mark.yandex_internal
@pytest.mark.parametrize("direct_session", [{"spark.ytsaurus.logs.export.enabled": "true"}], indirect=True)
def test_logs_export_for_direct_submit_client_mode_no_tvm(yt_client, direct_session):
    op_id = spark_job_for_client(direct_session)

    assert_no_logs(yt_client, op_id)

def assert_logs(yt_client, op_id=None):
    tasks_spec = yt_client.get_operation_attributes(op_id)["spec"]["tasks"]
    for tasks in tasks_spec:
        env_variables = tasks_spec[tasks]["environment"]
        assert env_variables["ENABLE_MONIUM_LOGS_EXPORT"] == "true", "Logs export must be enabled"
        assert env_variables["YT_APPENDER_TITLE"] == "file", "No appender was found"

def assert_no_logs(yt_client, op_id=None):
    tasks_spec = yt_client.get_operation_attributes(op_id)["spec"]["tasks"]
    for tasks in tasks_spec:
        env_variables = tasks_spec[tasks]["environment"]
        assert "ENABLE_MONIUM_LOGS_EXPORT" not in env_variables, "Logs must be disabled"
        assert "YT_APPENDER_TITLE" not in env_variables, "Appender must not be initialized"

def spark_job_for_cluster(yt_client, tmp_dir, direct_submitter, config):
    upload_file(yt_client, 'jobs/spark_heavy_job.py', f'{tmp_dir}/spark_heavy_job.py')
    operation_id = direct_submitter.submit(f'yt:/{tmp_dir}/spark_heavy_job.py',
                                           conf=config)

    assert operation_id is not None
    return operation_id

def spark_job_for_client(direct_session):
    query = """
            SELECT 1
        """
    direct_session.sql(query)
    return direct_session.conf.get("spark.ytsaurus.executor.operation.id")
