import spyt
import pytest
import requests
from time import sleep
from utils import upload_file


spark_conf_dynamic_allocation = {
    "spark.ytsaurus.rpc.job.proxy.enabled": "true",
    "spark.ytsaurus.shuffle.enabled": "true",
    "spark.executor.cores": "1",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "3",
    "spark.dynamicAllocation.executorIdleTimeout": "5s"
}

spark_conf_scale_down = spark_conf_dynamic_allocation | {
    "spark.dynamicAllocation.initialExecutors": "3"
}

spark_conf_scale_up = spark_conf_dynamic_allocation | {
    "spark.dynamicAllocation.initialExecutors": "1"
}


def get_running_jobs_count(yt_client, executors_operation_id):
    operation = yt_client.get_operation(executors_operation_id)
    if 'brief_progress' not in operation:
        return 0
    running = operation['brief_progress']['jobs'].get('running')
    return int(running) if running else 0


def get_executors_operation_id(yt_client, driver_operation_id, retries=30):
    for _ in range(retries):
        val = (
            yt_client.get_operation(driver_operation_id)
            .get("runtime_parameters", {})
            .get("annotations", {})
            .get("description", {})
            .get("Executors operation ID")
        )
        if val:
            return val
        sleep(1)
    raise TimeoutError("Executors operation ID not found")


def check_min_max_jobs(yt_client, executors_operation_id, spark_conf, poll_iterations=10, poll_delay=2):
    min_jobs = int(spark_conf["spark.dynamicAllocation.maxExecutors"])
    max_jobs = int(spark_conf["spark.dynamicAllocation.minExecutors"])

    for _ in range(poll_iterations):
        current_state = yt_client.get_operation_state(executors_operation_id)
        if current_state.is_finished():
            break
        if current_state.is_running():
            current_jobs = get_running_jobs_count(yt_client, executors_operation_id)
            if current_jobs > 0:
                min_jobs = min(min_jobs, current_jobs)
                max_jobs = max(max_jobs, current_jobs)
        sleep(poll_delay)
    assert min_jobs == int(spark_conf["spark.dynamicAllocation.minExecutors"]), f"min jobs {min_jobs}"
    assert max_jobs == int(spark_conf["spark.dynamicAllocation.maxExecutors"]), f"max jobs {max_jobs}"


def test_dynamic_allocation_scale_down_cluster_mode(yt_client, tmp_dir, direct_submitter):
    file_name = 'dynamic_allocation_scaledown_job.py'
    upload_file(yt_client, f'jobs/{file_name}', f'{tmp_dir}/{file_name}')
    submit_result = direct_submitter.submit(f'yt:/{tmp_dir}/{file_name}', conf=spark_conf_scale_down)
    operation_id = submit_result["operation_id"] if isinstance(submit_result, dict) else submit_result
    assert operation_id is not None

    executors_operation_id = get_executors_operation_id(yt_client, operation_id)
    assert executors_operation_id is not None

    check_min_max_jobs(yt_client, executors_operation_id, spark_conf_scale_down, poll_iterations=20)


def test_dynamic_allocation_scale_up_cluster_mode(yt_client, tmp_dir, direct_submitter):
    file_name = 'spark_heavy_job.py'
    upload_file(yt_client, f'jobs/{file_name}', f'{tmp_dir}/{file_name}')
    submit_result = direct_submitter.submit(f'yt:/{tmp_dir}/{file_name}', conf=spark_conf_scale_up)
    operation_id = submit_result["operation_id"] if isinstance(submit_result, dict) else submit_result
    assert operation_id is not None

    executors_operation_id = get_executors_operation_id(yt_client, operation_id)
    assert executors_operation_id is not None

    check_min_max_jobs(yt_client, executors_operation_id, spark_conf_scale_up, poll_iterations=20)


@pytest.mark.parametrize("direct_session", [spark_conf_scale_down], indirect=True)
def test_dynamic_allocation_scale_down_client_mode(yt_client, direct_session):
    direct_session.sql("SELECT 1").count()
    executors_operation_id = direct_session.conf.get("spark.ytsaurus.executor.operation.id")
    assert executors_operation_id is not None

    check_min_max_jobs(yt_client, executors_operation_id, spark_conf_scale_down)
