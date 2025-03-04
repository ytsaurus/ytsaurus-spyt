from spyt.enabler import SpytEnablers
from spyt.spec import CommonComponentConfig, CommonSpecParams, WorkerConfig, WorkerResources, build_worker_spec
from spyt.utils import SparkDiscovery, format_memory, parse_memory
from yt.wrapper.common import update
from yt.wrapper.spec_builders import VanillaSpecBuilder


def test_parse_memory():
    assert parse_memory(128) == 128
    assert parse_memory("128") == 128
    assert parse_memory("256") == 256
    assert parse_memory("256b") == 256
    assert parse_memory("256B") == 256
    assert parse_memory("128k") == 128 * 1024
    assert parse_memory("256k") == 256 * 1024
    assert parse_memory("256K") == 256 * 1024
    assert parse_memory("128kb") == 128 * 1024
    assert parse_memory("256kb") == 256 * 1024
    assert parse_memory("256Kb") == 256 * 1024
    assert parse_memory("256KB") == 256 * 1024
    assert parse_memory("256m") == 256 * 1024 * 1024
    assert parse_memory("256M") == 256 * 1024 * 1024
    assert parse_memory("256mb") == 256 * 1024 * 1024
    assert parse_memory("256Mb") == 256 * 1024 * 1024
    assert parse_memory("256MB") == 256 * 1024 * 1024
    assert parse_memory("256g") == 256 * 1024 * 1024 * 1024
    assert parse_memory("256G") == 256 * 1024 * 1024 * 1024
    assert parse_memory("256gb") == 256 * 1024 * 1024 * 1024
    assert parse_memory("256Gb") == 256 * 1024 * 1024 * 1024
    assert parse_memory("256GB") == 256 * 1024 * 1024 * 1024


def test_format_memory():
    assert format_memory(128) == "128B"
    assert format_memory(256) == "256B"
    assert format_memory(256 * 1024) == "256K"
    assert format_memory(128 * 1024) == "128K"
    assert format_memory(256 * 1024 * 1024) == "256M"
    assert format_memory(256 * 1024 * 1024 * 1024) == "256G"


def build_spec(enable_tmpfs=False):
    enablers = SpytEnablers(enable_byop=False, enable_profiling=False)
    discovery = SparkDiscovery("//home/cluster")
    common_config = CommonComponentConfig(
        enable_tmpfs=enable_tmpfs, enablers=enablers, rpc_job_proxy_thread_pool_size=6, spark_discovery=discovery)
    common_params = CommonSpecParams(
        container_home="./spark", spyt_home="$HOME/./spark/spark",spark_home="$HOME/./spark/spyt-package",
        spark_distributive="spark-3.2.2-bin-hadoop3.2.tgz", java_home="/opt/jdk",
        extra_java_opts=["-Dtest=true"], environment={"TEST_ENV": "True"}, spark_conf={"spark.yt.option": "2024"},
        task_spec={"file_paths": ["//home/job.jar"]}, config=common_config)
    resources = WorkerResources(cores=2, memory="8Gb", num=4, cores_overhead=1, timeout="1m", memory_overhead="1Gb")
    worker_config = WorkerConfig(
        tmpfs_limit="1G", res=resources, worker_port=27072, driver_op_discovery_script=None, extra_metrics_enabled=True,
        autoscaler_enabled=False, worker_log_transfer=False, worker_log_json_mode=False,
        worker_log_update_interval="5m", worker_log_table_ttl="5d", worker_disk_name="default", worker_gpu_limit=1,
        cuda_toolkit_version="11.0")

    builder = VanillaSpecBuilder()
    build_worker_spec(builder, "workers", None, False, common_params, worker_config)
    return builder.build()


def test_worker_spec_builder():
    spec = build_spec()
    expected_command = \
        './setup-spyt-env.sh --spark-home ./spark --spark-distributive spark-3.2.2-bin-hadoop3.2.tgz && ' \
        '/opt/jdk/bin/java -Xmx2g ' \
        '-cp $HOME/./spark/spark/conf/:$HOME/./spark/spark/jars/*:$HOME/./spark/spyt-package/jars/* ' \
        '-Dtest=true -Dspark.yt.option=2024 -Dspark.workerLog.tablePath=yt:///home/cluster/logs/worker_log ' \
        '-Dspark.ui.prometheus.enabled=true -Dspark.worker.resource.gpu.amount=1 ' \
        '-Dspark.worker.resource.gpu.discoveryScript=./spark/spyt-package/bin/getGpusResources.sh ' \
        'tech.ytsaurus.spark.launcher.WorkerLauncher --cores 2 --memory 8Gb --wait-master-timeout 1m ' \
        '--wlog-service-enabled False --wlog-enable-json False --wlog-update-interval 5m --wlog-table-ttl 5d '
    expected_spec = {
        'tasks': {
            'workers': {
                'job_count': 4,
                'command': expected_command,
                'memory_limit': 9 * 1024 * 1024 * 1024,
                'cpu_limit': 3,
                'file_paths': ['//home/job.jar'],
                'environment': {
                    'TEST_ENV': 'True',
                    'SPARK_YT_BYOP_ENABLED': 'False',
                    'SPARK_WORKER_PORT': '27072',
                    'SPARK_YT_CLUSTER_CONF_PATH': '//home/cluster/discovery/conf',
                    'SPARK_LOCAL_DIRS': '.'},
                'rpc_proxy_worker_thread_pool_size': 6,
                'cuda_toolkit_version': '11.0',
                'gpu_limit': 1
            }
        }
    }

    assert update(spec, expected_spec) == spec, f"{update(spec, expected_spec)} != {spec}"


def test_worker_spec_builder_enable_tmpfs():
    spec = build_spec(enable_tmpfs=True)
    expected_command = \
        './setup-spyt-env.sh --spark-home ./spark --spark-distributive spark-3.2.2-bin-hadoop3.2.tgz && ' \
        '/opt/jdk/bin/java -Xmx2g ' \
        '-cp $HOME/./spark/spark/conf/:$HOME/./spark/spark/jars/*:$HOME/./spark/spyt-package/jars/* ' \
        '-Dtest=true -Dspark.yt.option=2024 -Dspark.workerLog.tablePath=yt:///home/cluster/logs/worker_log ' \
        '-Dspark.ui.prometheus.enabled=true -Dspark.worker.resource.gpu.amount=1 ' \
        '-Dspark.worker.resource.gpu.discoveryScript=./spark/spyt-package/bin/getGpusResources.sh ' \
        'tech.ytsaurus.spark.launcher.WorkerLauncher --cores 2 --memory 8Gb --wait-master-timeout 1m ' \
        '--wlog-service-enabled False --wlog-enable-json False --wlog-update-interval 5m --wlog-table-ttl 5d '
    expected_spec = {
        'tasks': {
            'workers': {
                'job_count': 4,
                'command': expected_command,
                'memory_limit': 10 * 1024 * 1024 * 1024,
                'tmpfs_size': 1024 * 1024 * 1024,
                'cpu_limit': 3,
                'file_paths': ['//home/job.jar'],
                'environment': {
                    'TEST_ENV': 'True',
                    'SPARK_YT_BYOP_ENABLED': 'False',
                    'SPARK_WORKER_PORT': '27072',
                    'SPARK_YT_CLUSTER_CONF_PATH': '//home/cluster/discovery/conf',
                    'SPARK_LOCAL_DIRS': './tmpfs'},
                'rpc_proxy_worker_thread_pool_size': 6,
                'cuda_toolkit_version': '11.0',
                'gpu_limit': 1
            }
        }
    }

    assert update(spec, expected_spec) == spec, f"{update(spec, expected_spec)} != {spec}"
