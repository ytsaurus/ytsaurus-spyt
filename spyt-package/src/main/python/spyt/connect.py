import requests
from spyt.dependency_utils import require_yt_client
from spyt.enabler import SpytEnablers

require_yt_client()

from yt.wrapper.http_helpers import get_token, get_user_name  # noqa: E402
from yt.wrapper.run_operation_commands import run_operation  # noqa: E402
from .conf import read_global_conf, read_remote_conf  # noqa: E402
from .spec import build_spark_connect_server_spec, CommonConnectParams  # noqa: E402
from .utils import parse_bool, SparkDiscovery  # noqa: E402
from .version import __scala_version__ as spyt_version  # noqa: E402


def start_connect_server(client, enablers: SpytEnablers = None, prefer_ipv6: bool = False,
                         pool: str = None, java_home: str = None, operation_alias: str = None, **kwargs):
    params = CommonConnectParams(**kwargs)
    global_conf = read_global_conf(client=client)
    version_config = read_remote_conf(global_conf, spyt_version, client)
    java_home = java_home or version_config.get('default_cluster_java_home')

    enable_squashfs = parse_bool(params.spark_conf.get("spark.ytsaurus.squashfs.enabled"))
    enablers = enablers or SpytEnablers(enable_squashfs=enable_squashfs)
    enablers.apply_config(version_config)

    spec = build_spark_connect_server_spec(client, version_config, enablers, java_home,
                                           prefer_ipv6, pool, operation_alias, params)
    return run_operation(spec, sync=False, client=client)


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
