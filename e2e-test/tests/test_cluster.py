from common.helpers import assert_items_equal
import requests


def test_spyt_root_existence(yt_client):
    assert_items_equal(yt_client.list("//home/spark"), ["conf", "distrib", "livy", "spyt"])


def test_cluster_startup(yt_client, spyt_cluster):
    assert_items_equal(yt_client.list(spyt_cluster.discovery_path),
                       ["discovery", "logs"])
    assert_items_equal(yt_client.list(spyt_cluster.discovery_path + "/discovery"),
                       ["conf", "master_wrapper", "operation", "rest", "spark_address", "version", "webui", "master_jobs"])


def test_reverse_proxy_cluster_startup(yt_client, reverse_proxy_spyt_cluster):
    assert_items_equal(yt_client.list(reverse_proxy_spyt_cluster.discovery_path),
                       ["discovery", "logs"])
    assert_items_equal(yt_client.list(reverse_proxy_spyt_cluster.discovery_path + "/discovery"),
                       ["conf", "master_wrapper", "operation", "rest", "spark_address", "version", "webui", "master_jobs"])
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
