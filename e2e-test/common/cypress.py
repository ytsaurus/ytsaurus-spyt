import logging
import os

logger = logging.getLogger(__name__)


def patch_conf(yt_client, python_path, java_home, direct_submit=False):
    if not direct_submit:
        set_python_path(python_path, yt_client)
    set_java_home(java_home, yt_client)
    set_executor_conf(yt_client)
    set_another_shuffle_port(yt_client)
    disable_rpc_job_proxy(yt_client)


def get_spark_conf(yt_client):
    return yt_client.get("//home/spark/conf/global/spark_conf")


def set_spark_conf(options, yt_client):
    spark_conf = get_spark_conf(yt_client)
    spark_conf.update(options)
    yt_client.set("//home/spark/conf/global/spark_conf", spark_conf)


def set_python_path(python_path, yt_client):
    set_spark_conf({'spark.pyspark.python': python_path}, yt_client)


def set_java_home(java_home, yt_client):
    logger.info(f"Set java home: {java_home}")
    yt_client.set("//home/spark/conf/global/default_cluster_java_home", java_home)


def set_executor_conf(yt_client, executor_cores=1, executor_memory='1g'):
    set_spark_conf({'spark.executor.cores': str(executor_cores), 'spark.executor.memory': executor_memory}, yt_client)


def set_another_shuffle_port(yt_client):
    set_spark_conf({'spark.shuffle.service.port': str(27120 + (os.getpid() % 80))}, yt_client)


def disable_rpc_job_proxy(yt_client):
    # TODO: fix rpc job proxy authentication in yt_local, write tests for it and remove this patch
    set_spark_conf({"spark.ytsaurus.rpc.job.proxy.enabled": "false"}, yt_client)
