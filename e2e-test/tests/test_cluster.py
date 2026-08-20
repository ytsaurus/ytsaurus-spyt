from common.helpers import assert_items_equal
from utils import upload_file
from spyt.conf import read_global_conf, read_remote_conf, read_spark_defaults_conf
import requests
import pytest
import time

def test_spyt_root_existence(yt_client):
    assert_items_equal(yt_client.list("//home/spark"), ["conf", "distrib", "spyt"])


def test_cluster_startup(yt_client, spyt_cluster):
    assert_items_equal(yt_client.list(spyt_cluster.discovery_path),
                       ["discovery", "logs"])
    assert_items_equal(yt_client.list(spyt_cluster.discovery_path + "/discovery"),
                       ["conf", "operation", "rest", "spark_address", "version", "webui", "master_jobs"])


def test_reverse_proxy_cluster_startup(yt_client, reverse_proxy_spyt_cluster):
    assert_items_equal(yt_client.list(reverse_proxy_spyt_cluster.discovery_path),
                       ["discovery", "logs"])
    assert_items_equal(yt_client.list(reverse_proxy_spyt_cluster.discovery_path + "/discovery"),
                       ["conf", "operation", "rest", "spark_address", "version", "webui", "master_jobs"])
    job_id = yt_client.list(reverse_proxy_spyt_cluster.discovery_path + "/discovery/master_jobs")[0]
    assert yt_client.get(reverse_proxy_spyt_cluster.discovery_path + f"/discovery/master_jobs/{job_id}") == {
        "webui_url": "https://some-host/some-path/"
    }


def test_prometheus_endpoint(yt_client, spyt_cluster):
    webui_endpoint = yt_client.list(spyt_cluster.discovery_path + "/discovery/webui")[0]
    master_metrics_endpoint = f'http://{webui_endpoint}/metrics/master/prometheus'
    response = requests.get(master_metrics_endpoint)
    response_body = response.text
    assert not response_body.startswith('<!DOCTYPE html>')
    response_lines = response_body.splitlines()
    for line in response_lines:
        assert line.startswith('metrics_')


@pytest.mark.parametrize("spyt_cluster", [{"enable_multi_operation_mode" : True}], indirect=True)
def test_multi_operation_mode(yt_client, spyt_cluster):
    assert_multi_operation_mode(yt_client, spyt_cluster)


@pytest.mark.parametrize("spyt_cluster", [{"enable_multi_operation_mode" : True,
                                           "operation_alias" : "*alias"}], indirect=True)
def test_multi_operation_mode_with_alias(yt_client, spyt_cluster):
    op_ids = assert_multi_operation_mode(yt_client, spyt_cluster)

    operation_aliases = []

    for op_id in op_ids:
        operation = yt_client.get_operation(op_id)
        operation_aliases.append(operation["brief_spec"]["alias"])

    assert_items_equal(operation_aliases, ["*alias_workers", "*alias"])


def assert_multi_operation_mode(yt_client, spyt_cluster):
    assert_items_equal(yt_client.list(spyt_cluster.discovery_path + "/discovery"),
                       ["conf", "operation", "rest", "spark_address",
                        "version", "webui", "master_jobs", "children_operations"])
    op_ids = yt_client.list(f"{spyt_cluster.discovery_path}/discovery/operation") + \
            yt_client.list(f"{spyt_cluster.discovery_path}/discovery/children_operations")

    job_types = []

    for op_id in op_ids:
        operation = yt_client.get_operation(op_id)
        job_types.append(
            operation["runtime_parameters"]["annotations"]["description"]["Spark over YT"]["job_types"][0]
        )

    assert_items_equal(job_types, ["worker", "master"])
    return op_ids


@pytest.mark.parametrize("spyt_cluster", [{"enable_ytsaurus_shuffle": True, "rpc_job_proxy": True}], indirect=True)
@pytest.mark.parametrize("shuffle_enabled", [True, False])
def test_per_app_ytsaurus_shuffle(yt_client, tmp_dir, spyt_cluster, shuffle_enabled):
    from spyt.submit import SubmissionStatus
    upload_file(yt_client, 'jobs/ytsaurus_shuffle_job.py', f'{tmp_dir}/ytsaurus_shuffle_job.py')
    out_path = f'{tmp_dir}/shuffle_manager'

    conf = {"spark.ytsaurus.shuffle.enabled": "true"} if shuffle_enabled else {}
    status = spyt_cluster.submit_cluster_job(
        f'{tmp_dir}/ytsaurus_shuffle_job.py',
        args=[out_path],
        conf=conf)
    assert status is SubmissionStatus.FINISHED

    shuffle_manager = yt_client.read_file(out_path).read().decode()
    ytsaurus_manager = "org.apache.spark.shuffle.ytsaurus.YTsaurusShuffleManager"
    if shuffle_enabled:
        assert shuffle_manager == ytsaurus_manager, \
            f"Expected {ytsaurus_manager} to be the active shuffle manager, got {shuffle_manager}"
    else:
        assert shuffle_manager != ytsaurus_manager, \
            f"Expected a non-YTsaurus shuffle manager, got {shuffle_manager}"


@pytest.mark.timeout(180)
@pytest.mark.parametrize("spyt_cluster", [{"enable_ytsaurus_shuffle": True, "rpc_job_proxy": True}], indirect=True)
def test_ytsaurus_shuffle_rest_submit(yt_client, tmp_dir, spyt_cluster):
    upload_file(yt_client, 'jobs/ytsaurus_shuffle_job.py', f'{tmp_dir}/ytsaurus_shuffle_job.py')
    out_path = f'{tmp_dir}/shuffle_manager'
    app_resource = f'yt:/{tmp_dir}/ytsaurus_shuffle_job.py'

    rest_endpoint = yt_client.list(f'{spyt_cluster.discovery_path}/discovery/rest')[0]
    cluster_version = yt_client.list(f'{spyt_cluster.discovery_path}/discovery/version')[0]
    remote_conf = read_remote_conf(read_global_conf(client=yt_client), cluster_version, client=yt_client)
    cluster_conf = yt_client.get(f'{spyt_cluster.discovery_path}/discovery/conf')['spark_conf']

    # Clients older than 2.11.0 don't expand spark.ytsaurus.shuffle.enabled into shuffle manager
    # settings, so the request contains the flag only and the master has to do the wiring
    spark_properties = read_spark_defaults_conf() | remote_conf['spark_conf'] | cluster_conf | {
        "spark.app.name": "test_ytsaurus_shuffle_rest_submit",
        "spark.submit.deployMode": "cluster",
        "spark.hadoop.yt.proxy": spyt_cluster.proxy,
        "spark.hadoop.yt.user": spyt_cluster.user,
        "spark.hadoop.yt.token": spyt_cluster.token,
        "spark.ytsaurus.shuffle.enabled": "true",
    }
    assert not any(key.startswith("spark.shuffle.manager") for key in spark_properties)

    request = {
        "action": "CreateSubmissionRequest",
        "appResource": app_resource,
        "mainClass": "org.apache.spark.deploy.PythonRunner",
        "appArgs": ["{{USER_JAR}}", "{{PY_FILES}}", out_path],
        "environmentVariables": {},
        "sparkProperties": spark_properties,
        "clientSparkVersion": "",
    }
    response = requests.post(f'http://{rest_endpoint}/v1/submissions/create', json=request)
    assert response.status_code == 200, response.text
    submission_id = response.json()["submissionId"]

    driver_state = wait_submission_final_state(rest_endpoint, submission_id)
    assert driver_state == "FINISHED", f"Driver {submission_id} finished with state {driver_state}"

    shuffle_manager = yt_client.read_file(out_path).read().decode()
    ytsaurus_manager = "org.apache.spark.shuffle.ytsaurus.YTsaurusShuffleManager"
    assert shuffle_manager == ytsaurus_manager, \
        f"Expected {ytsaurus_manager} to be wired by the master, got {shuffle_manager}"


def wait_submission_final_state(rest_endpoint, submission_id, timeout=120, ping_period=3):
    final_states = {"FINISHED", "FAILED", "ERROR", "KILLED"}
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = requests.get(f'http://{rest_endpoint}/v1/submissions/status/{submission_id}')
        assert response.status_code == 200, response.text
        driver_state = response.json().get("driverState")
        if driver_state in final_states:
            return driver_state
        time.sleep(ping_period)
    raise TimeoutError(f"Submission {submission_id} has not finished in {timeout} seconds")
