from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from spyt.submit import (
    ApplicationStatus,
    RestClusterEndpoints,
    RetryConfig,
    SparkRestClient,
    SparkSubmissionClient,
    SubmissionStatus,
)


@contextmanager
def _patch_discovery(values):
    paths = {"master_spark": "//d/spark_address", "master_rest": "//d/rest",
             "spark_cluster_version": "//d/version"}
    p2v = {paths[k]: values[k] for k in paths}
    with patch("spyt.submit.SparkDiscovery") as cls, \
         patch("spyt.submit.SparkDiscovery.get",
               side_effect=lambda path, client=None: p2v[path]):
        for m, p in paths.items():
            getattr(cls.return_value, m).return_value = p
        yield


class _Resp:
    def __init__(self, b): self._b = b
    ok = True
    def json(self): return self._b
    def raise_for_status(self): return None


def _rest(headers=None):
    session = MagicMock()
    return SparkRestClient(RestClusterEndpoints("m:7077", "m:6066", "2.10.0"),
                           extra_headers=headers, session=session,
                           timeout_sec=5.0), session


def _client(rest_mock=None):
    eps = RestClusterEndpoints(master_host_port="m:7077", rest_host_port="m:6066",
                                version="2.10.0")
    rest = rest_mock or MagicMock()
    rest.endpoints = eps
    with patch("spyt.submit.YtClient", return_value=MagicMock()):
        client = SparkSubmissionClient(gateway=None, proxy="test-proxy",
                                       discovery_path="//disc", user="alice", token="t0k")
    client._rest_cache = rest
    return client, rest


def _stub_submit_env(monkeypatch):
    monkeypatch.setattr("spyt.submit.pyspark_version", "3.5.0")
    monkeypatch.setattr("spyt.submit.time.sleep", lambda s: None)


def test_endpoints_discover():
    with _patch_discovery({"master_spark": "h:7077", "master_rest": "h:6066",
                           "spark_cluster_version": "2.10.0"}):
        eps = RestClusterEndpoints.discover("//disc", MagicMock())
    assert (eps.master_host_port, eps.rest_host_port, eps.version) == ("h:7077", "h:6066", "2.10.0")
    assert eps.rest_base_url == "http://h:6066"


@pytest.mark.parametrize("missing,msg", [
    ("master_spark", "Spark master address not found"),
    ("master_rest", "Spark REST address not found"),
    ("spark_cluster_version", "Cluster version not found"),
])
def test_endpoints_discover_missing(missing, msg):
    values = {"master_spark": "h:1", "master_rest": "h:2", "spark_cluster_version": "v"}
    values[missing] = None
    with _patch_discovery(values), pytest.raises(RuntimeError, match=msg):
        RestClusterEndpoints.discover("//disc", MagicMock())


_BASE = "http://m:6066/v1/submissions"


@pytest.mark.parametrize("method,verb,path,body,arg", [
    ("status", "GET", "status/s1", {"success": True, "driverState": "RUNNING"}, "s1"),
    ("kill", "POST", "kill/s1", {"success": True}, "s1"),
    ("app_id", "GET", "getAppId/s1", {"success": True, "appId": "a"}, "s1"),
    ("app_status", "GET", "getAppStatus/a",
        {"success": True, "appState": "FINISHED"}, "a"),
    ("all_drivers", "GET", "status",
        {"statuses": [{"driverId": "d", "status": "RUNNING"}]}, None),
    ("submit", "POST", "submit", {"success": True, "submissionId": "d1"}, {"action": "X"}),
])
def test_rest_client_endpoints(method, verb, path, body, arg):
    client, session = _rest()
    session.request.return_value = _Resp(body)
    getattr(client, method)(arg) if arg is not None else getattr(client, method)()
    args, _ = session.request.call_args
    assert (args[0], args[1]) == (verb, f"{_BASE}/{path}")


def test_client_requires_proxy_and_discovery_path():
    with pytest.raises(TypeError, match="proxy and discovery_path are required"):
        SparkSubmissionClient(gateway=None, user="u", token="t")


@pytest.mark.parametrize("response,expected", [
    ({"success": True, "driverState": "RUNNING"}, "RUNNING"),
    ({"success": True, "driverState": "FINISHED"}, "FINISHED"),
    ({"success": False}, "UNKNOWN"),
])
def test_get_status_branches(response, expected):
    rest = MagicMock()
    rest.status.return_value = response
    client, _ = _client(rest)
    assert client.get_status("s1") is SubmissionStatus(expected)


def test_get_status_returns_undefined_on_network_error():
    import requests
    rest = MagicMock()
    rest.status.side_effect = requests.exceptions.ConnectionError("net")
    client, _ = _client(rest)
    with patch.object(client, "_force_cluster_update") as forced:
        assert client.get_status("s1") is SubmissionStatus.UNDEFINED
        forced.assert_called_once()


def test_get_app_status_chain_and_unknown():
    rest = MagicMock()
    rest.app_id.return_value = {"success": True, "appId": "a99"}
    rest.app_status.return_value = {"success": True, "appState": "RUNNING"}
    client, _ = _client(rest)
    assert client.get_app_status("s1") is ApplicationStatus.RUNNING
    rest.app_id.assert_called_once_with("s1")
    rest.app_status.assert_called_once_with("a99")
    rest.app_id.return_value = {"success": False}
    rest.app_status.reset_mock()
    assert client.get_app_status("s2") is ApplicationStatus.UNKNOWN
    rest.app_status.assert_not_called()


def test_kill_and_drivers_filter():
    rest = MagicMock()
    rest.kill.return_value = {"success": True}
    rest.all_drivers.return_value = [
        {"driverId": "d-1", "status": "RUNNING"},
        {"driverId": "d-2", "status": "FINISHED"},
    ]
    client, _ = _client(rest)
    assert client.kill("s1") is True
    assert (client.get_active_drivers(), client.get_completed_drivers()) == (["d-1"], ["d-2"])
    assert sorted(client.get_all_drivers()) == ["d-1", "d-2"]


def test_submit_python_app(monkeypatch):
    rest = MagicMock()
    rest.submit.return_value = {"success": True, "submissionId": "sub-1"}
    _stub_submit_env(monkeypatch)
    client, _ = _client(rest)
    launcher = (client.new_launcher()
                .set_app_resource("yt:///j/main.py")
                .add_app_args("--in", "x")
                .set_conf("spark.executor.instances", "5")
                .set_conf("spark.hadoop.yt.token", "USER_TRIED"))
    assert client.submit(launcher) == "sub-1"
    body = rest.submit.call_args[0][0]
    assert body["action"] == "SpytSubmitRequest"
    assert body["appResource"] == "yt:///j/main.py"
    assert body["mainClass"] is None
    assert body["appArgs"] == ["--in", "x"]
    sp = body["sparkProperties"]
    assert sp["spark.executor.instances"] == "5"
    assert sp["spark.app.name"] == "main.py"
    assert sp["spark.hadoop.yt.proxy"] == "test-proxy"
    assert sp["spark.hadoop.yt.user"] == "alice"
    assert sp["spark.hadoop.yt.token"] == "t0k"


def test_submit_jar(monkeypatch):
    rest = MagicMock()
    rest.submit.return_value = {"success": True, "submissionId": "sub-2"}
    _stub_submit_env(monkeypatch)
    client, _ = _client(rest)
    launcher = (client.new_launcher().set_app_resource("yt:///j/my.jar")
                .set_main_class("com.X").add_app_args("a", "b"))
    assert client.submit(launcher) == "sub-2"
    body = rest.submit.call_args[0][0]
    assert body["action"] == "SpytSubmitRequest"
    assert body["mainClass"] == "com.X"
    assert body["appArgs"] == ["a", "b"]


def test_wait_final_bounded_on_persistent_undefined(monkeypatch):
    rest = MagicMock()
    rest.status.side_effect = __import__("requests").exceptions.ConnectionError("down")
    client, _ = _client(rest)
    monkeypatch.setattr("spyt.submit.time.sleep", lambda s: None)
    with patch.object(client, "_force_cluster_update"):
        with pytest.raises(RuntimeError, match="consecutive polls"):
            client.wait_final("s1", ping_period=0, max_undefined_in_a_row=3)


@pytest.mark.parametrize("enable_retry,attempts,limit,match", [
    (True, 2, 5, None),
    (False, 1, 5, "retry is disabled"),
    (True, 3, 2, "retry limit 2 exceeded"),
])
def test_submit_retry_behavior(monkeypatch, enable_retry, attempts, limit, match):
    rest = MagicMock()
    rest.submit.side_effect = (
        [RuntimeError("temporary network error"), {"success": True, "submissionId": "ok"}]
        if match is None else RuntimeError("persistent server error"))
    _stub_submit_env(monkeypatch)
    client, _ = _client(rest)
    monkeypatch.setattr(client, "_force_cluster_update", lambda: None)
    launcher = client.new_launcher().set_app_resource("yt:///j.py")
    cfg = RetryConfig(enable_retry=enable_retry, retry_limit=limit,
                     retry_interval=timedelta(seconds=0))
    if match is None:
        assert client.submit(launcher, cfg) == "ok"
    else:
        with pytest.raises(RuntimeError, match=match):
            client.submit(launcher, cfg)
    assert rest.submit.call_count == attempts


def _http_error(status, text="err"):
    import requests
    return requests.exceptions.HTTPError(response=SimpleNamespace(status_code=status, text=text))


@pytest.mark.parametrize("side_effect,return_value,raises_match,returns,expected_calls", [
    pytest.param(_http_error(400, "missing field"), None,
                 r"rejected by master \(status 400\)", None, 1, id="4xx-rejected"),
    pytest.param([_http_error(503, "down"), {"success": True, "submissionId": "ok"}], None,
                 None, "ok", 2, id="5xx-retried"),
    pytest.param(None, {"success": False, "message": "duplicate submissionId"},
                 "Submission failed", None, 1, id="success-false-rejected"),
])
def test_submit_master_response_handling(monkeypatch, side_effect, return_value,
                                          raises_match, returns, expected_calls):
    rest = MagicMock()
    if side_effect is not None:
        rest.submit.side_effect = side_effect
    else:
        rest.submit.return_value = return_value
    _stub_submit_env(monkeypatch)
    client, _ = _client(rest)
    monkeypatch.setattr(client, "_force_cluster_update", lambda: None)
    launcher = client.new_launcher().set_app_resource("yt:///j.py")
    cfg = RetryConfig(enable_retry=True, retry_limit=5,
                     retry_interval=timedelta(seconds=0))
    if raises_match is not None:
        with pytest.raises(RuntimeError, match=raises_match):
            client.submit(launcher, cfg)
    else:
        assert client.submit(launcher, cfg) == returns
    assert rest.submit.call_count == expected_calls
