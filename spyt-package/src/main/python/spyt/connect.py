import requests

import hashlib
import os
import socket
import sys
import time
from functools import reduce
from spyt.dependency_utils import require_yt_client
from spyt.enabler import SpytEnablers

require_yt_client()

from yt.wrapper.file_commands import upload_file_to_cache  # noqa: E402
from yt.wrapper.http_helpers import get_token, get_user_name  # noqa: E402
from yt.wrapper.operation_commands import Operation  # noqa: E402
from yt.wrapper.run_operation_commands import run_operation  # noqa: E402
import yt.yson as yson  # noqa: E402
from .conf import read_global_conf, read_remote_conf  # noqa: E402
from .spec import build_spark_connect_server_spec, CommonConnectParams  # noqa: E402
from .utils import parse_bool, SparkDiscovery  # noqa: E402
from .version import __scala_version__ as spyt_version  # noqa: E402


def _connect_server_settings_hash(settings: dict) -> str:
    serialized_settings = yson.dumps(settings, yson_format="binary", sort_keys=True)
    return hashlib.sha256(serialized_settings).hexdigest()


def _find_existing_connect_server(client, user: str, title: str, settings_hash: str):
    operations = client.list_operations(
        user=user,
        state="running",
        filter=title,
        attributes=["id", "type", "runtime_parameters"],
    )["operations"]
    for operation in operations:
        annotations = operation.get("runtime_parameters", {}).get("annotations", {})
        if annotations.get("settings_hash") == settings_hash:
            return Operation(operation["id"], type=operation.get("type"), client=client)
    return None


def start_connect_server(client, enablers: SpytEnablers = None, prefer_ipv6: bool = False,
                         pool: str = None, java_home: str = None, operation_alias: str = None, title: str = None,
                         python_executable: str = None, self_upload: bool = False, reuse_existing: bool = False,
                         **kwargs):
    params = CommonConnectParams(**kwargs)
    global_conf = read_global_conf(client=client)
    version_config = read_remote_conf(global_conf, spyt_version, client)
    java_home = java_home or version_config.get('default_cluster_java_home')
    extra_files = []

    if self_upload:
        cached_binary = upload_file_to_cache(sys.executable, client=client)
        binary_file_name = os.path.basename(sys.executable)
        extra_files.append(yson.to_yson_type(cached_binary, attributes={
            "executable": True,
            "file_name": binary_file_name,
        }))
        params.spark_conf["spark.ytsaurus.isPythonBinary"] = "true"
        params.spark_conf["spark.ytsaurus.python.binary.file"] = binary_file_name
    else:
        python_executable = python_executable or f"python{sys.version_info.major}.{sys.version_info.minor}"
        params.spark_conf["spark.ytsaurus.python.executable"] = python_executable

    enable_squashfs = parse_bool(params.spark_conf.get("spark.ytsaurus.squashfs.enabled"))
    enablers = enablers or SpytEnablers(enable_squashfs=enable_squashfs)
    enablers.apply_config(version_config)

    user = get_user_name(client=client)
    title = title or f"Spark connect driver for {user}"
    settings_hash = _connect_server_settings_hash({
        "enablers": vars(enablers),
        "extra_files": extra_files,
        "java_home": java_home,
        "operation_alias": operation_alias,
        "params": vars(params),
        "pool": pool,
        "prefer_ipv6": prefer_ipv6,
        "python_executable": python_executable,
        "self_upload": self_upload,
        "spyt_version": spyt_version,
        "title": title,
        "user": user,
    })
    if reuse_existing:
        operation = _find_existing_connect_server(client, user, title, settings_hash)
        if operation:
            return operation

    spec = build_spark_connect_server_spec(client, version_config, enablers, java_home,
                                           prefer_ipv6, pool, operation_alias, title, extra_files,
                                           params, settings_hash)
    return run_operation(spec, sync=False, client=client)


def _is_spark_connect_endpoint_reachable(endpoint: str, timeout: float) -> bool:
    try:
        host, port = endpoint.rsplit(":", 1)
        if host.startswith("[") and host.endswith("]"):
            host = host[1:-1]
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except (OSError, ValueError):
        return False


def wait_for_spark_connect_endpoint(client, operation_id: str, timeout: int = 60):
    spark_connect_endpoint = None
    deadline = time.monotonic() + timeout
    while (remaining_timeout := deadline - time.monotonic()) > 0:
        operation = client.get_operation(operation_id)
        spark_connect_endpoint = (reduce(lambda map, key: map[key] if map and key in map else None,
                                         ['runtime_parameters', 'annotations', 'spark_connect_endpoint'],
                                         operation))
        if spark_connect_endpoint and _is_spark_connect_endpoint_reachable(
                str(spark_connect_endpoint), min(1, remaining_timeout)):
            return str(spark_connect_endpoint)
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    if spark_connect_endpoint:
        raise TimeoutError(
            f"Spark connect endpoint {spark_connect_endpoint} is not reachable in {timeout} seconds"
        )
    raise TimeoutError(f"Spark connect endpoint not found in {timeout} seconds")


def _spyt_connect_server_inner_cluster_endpoint(client, discovery_path: str):
    discovery = SparkDiscovery(discovery_path=discovery_path)
    master_rest_endpoint = SparkDiscovery.getOption(discovery.master_rest(), client=client)
    return f"http://{master_rest_endpoint}/v1/submissions/spytConnectServer"


def start_connect_server_inner_cluster(client, discovery_path: str, **kwargs):
    params = CommonConnectParams(**kwargs)
    user = get_user_name(client=client)
    token = get_token(client=client)
    spark_conf = {}
    spark_conf |= params.spark_conf
    spark_conf |= {
        "spark.hadoop.yt.user": user,
        "spark.hadoop.yt.token": token,
        "spark.app.name": f"Spyt connect server for {user}",
    }

    request_body = {
        "action": "StartConnectServerRequest",
        "driverMemory": params.driver_memory,
        "numExecutors": params.num_executors,
        "executorCores": params.executor_cores,
        "executorMemory": params.executor_memory,
        "grpcPortStart": params.grpc_port_start,
        "sparkConf": spark_conf
    }

    result = requests.post(_spyt_connect_server_inner_cluster_endpoint(client, discovery_path), json=request_body)
    result.raise_for_status()
    return result.json()["endpoint"]


def list_active_connect_servers_inner_cluster(client, discovery_path: str):
    user = get_user_name(client=client)
    result = requests.get(_spyt_connect_server_inner_cluster_endpoint(client, discovery_path), params={"user": user})
    result.raise_for_status()
    return result.json()["apps"]
