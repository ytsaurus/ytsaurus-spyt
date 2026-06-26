"""JVM-dependent direct submit helpers shared by spyt.submit variants.

This module hosts direct_submit and direct_submit_binary, which still drive
a local JVM (py4j + InProcessLauncher + DirectSubmitter). The submit-without-
py4j work in SPYT-1101 targets the REST SparkSubmissionClient path; direct
submit keeps the legacy py4j flow until that, too, can be migrated. Pulling
this code into its own file keeps spyt.submit free of JVM-specific glue and
makes the future jvm-free rewrite of direct_submit a single-file change.
"""
import logging
import sys

from .jvm import java_gateway
from ._submit_common import _retry_with_backoff


def direct_submit(yt_proxy, num_executors, main_file, deploy_mode="cluster", pool=None,
                  spark_base_args=[], job_args=[], spark_conf={}, java_home=None, prefer_ipv6=False,
                  timeout_sec=30, max_attempts=1, base_delay=10, max_delay=300):
    """
    Submits a Spark job directly to YTsaurus using the provided parameters.

    :param yt_proxy: YTsaurus proxy address (e.g., "hume.yt.yandex.net").
    :param num_executors: Number of Spark executors to use for the job.
    :param main_file: path to the main Spark application file (e.g., Python script).
    :param deploy_mode: deployment mode for Spark ("client" or "cluster", default: "cluster",
    "client" is not recommended, use direct_spark_session instead)
    :param pool: YTsaurus pool to execute this job (default: None)
    :param spark_base_args: additional Spark arguments
    :param job_args: job arguments
    :param spark_conf: additional Spark configuration as Python dict
    :param java_home: path to the Java home directory (default: None)
    :param prefer_ipv6:prefer IPv6 addresses (internal Yandex users must enable ipv6 option by default)
    :param timeout_sec: timeout for submitting the job in seconds (default: 30 sec)
    :param max_attempts: maximum number of attempts to submit the job (default: 1)
    :param base_delay: base delay between attempts in seconds, doubles for every new attempt (default: 10 sec)
    :param max_delay: maximum delay between attempts in seconds (default: 300 sec)
    :return: operation ID of the submitted Spark job.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be greater than 0")
    if base_delay < 0:
        raise ValueError("base_delay must be non-negative")
    if max_delay < base_delay:
        raise ValueError("max_delay must be greater than or equal to base_delay")

    spark_args = []
    spark_args.extend(["--num-executors", str(num_executors)])
    if pool:
        spark_args.extend(["--queue", pool])
    spark_args.extend(spark_base_args)

    with (java_gateway(java_home=java_home, prefer_ipv6=prefer_ipv6) as gateway):
        j_launcher = gateway.jvm.org.apache.spark.launcher.InProcessLauncher()
        j_launcher.setMaster("ytsaurus://" + yt_proxy)
        j_launcher.setDeployMode(deploy_mode)
        j_launcher.setAppResource(main_file)
        for k, v in spark_conf.items():
            j_launcher.setConf(k, str(v))
        j_utils = gateway.jvm.tech.ytsaurus.spyt.submit.InProcessLauncherPythonUtils
        for arg in job_args:
            j_utils.addAppArg(j_launcher, arg)
        for i in range(0, len(spark_args), 2):
            j_launcher.addSparkArg(spark_args[i], spark_args[i + 1])

        j_directsubmitter = gateway.jvm.tech.ytsaurus.spyt.submit.DirectSubmitter()

        def _submit_once():
            try_op = j_directsubmitter.submit(j_launcher, timeout_sec)
            if try_op.isSuccess():
                return try_op.get()
            failure = try_op.failed().get()
            raise RuntimeError(
                f"Failed to submit Spark job: {failure.toString()}, {failure.getMessage()}")

        return _retry_with_backoff(
            _submit_once,
            max_attempts=max_attempts,
            base_delay_sec=base_delay,
            max_delay_sec=max_delay,
            classify_transient=lambda _: True,
            on_retry=lambda attempt, e, delay: logging.warning(
                f"Attempt {attempt}. {e}. Retrying in {delay} sec ..."),
        )


def direct_submit_binary(yt_proxy, num_executors, spyt_version, driver_entry_point=None, executable=sys.executable,
                         deploy_mode="cluster", pool=None, spark_base_args=[], job_args=[], spark_conf={}):
    """
    Submits Spark job compiled as single binary executable file. By default, it submits itself assuming
    that this method is called from inside such binary

    :param yt_proxy: YTsaurus proxy address
    :param num_executors: number of Spark executors
    :param spyt_version: SPYT version to be used on cluster
    :param driver_entry_point: Spark driver entry point (default: None)
    :param executable: (default: sys.executable)
    :param deploy_mode: Spark deploy mode, client or cluster (default: cluster)
    :param pool: YTsaurus pool to execute this job (default: None)
    :param spark_base_args: additional Spark arguments
    :param job_args: job arguments
    :param spark_conf: additional Spark configuration as Python dict
    :return:
    """
    conf = dict(spark_conf)
    conf["spark.ytsaurus.spyt.version"] = spyt_version
    if driver_entry_point:
        conf["spark.ytsaurus.python.binary.entry.point"] = driver_entry_point

    return direct_submit(yt_proxy, num_executors, executable, deploy_mode, pool,
                         spark_base_args, job_args, conf)
