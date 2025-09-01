from spyt.dependency_utils import require_yt_client
from spyt.enabler import SpytEnablers

require_yt_client()

from yt.wrapper.run_operation_commands import run_operation  # noqa: E402
from .conf import read_global_conf, read_remote_conf  # noqa: E402
from .spec import build_spark_connect_server_spec  # noqa: E402
from .utils import parse_bool  # noqa: E402
from .version import __scala_version__ as spyt_version  # noqa: E402


def start_connect_server(client, spark_conf: dict = {}, enablers: SpytEnablers = None, prefer_ipv6: bool = False,
                         driver_memory: str = "1G", num_executors: int = 1, executor_cores: int = 1,
                         executor_memory: str = "2G", pool: str = None, grpc_port_start: int = 27080,
                         java_home: str = None):
    global_conf = read_global_conf(client=client)
    version_config = read_remote_conf(global_conf, spyt_version, client)
    java_home = java_home or version_config.get('default_cluster_java_home')

    enable_squashfs = parse_bool(spark_conf.get("spark.ytsaurus.squashfs.enabled"))
    enablers = enablers or SpytEnablers(enable_squashfs=enable_squashfs)
    enablers.apply_config(version_config)

    spec = build_spark_connect_server_spec(client, version_config, spark_conf, enablers, java_home,
                                           driver_memory, num_executors, executor_cores, executor_memory,
                                           prefer_ipv6, pool, grpc_port_start)
    return run_operation(spec, sync=False, client=client)
