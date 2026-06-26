from common.helpers import assert_items_equal
from utils import upload_file
import requests
import pytest

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
