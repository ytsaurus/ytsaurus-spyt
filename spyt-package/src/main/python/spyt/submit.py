import logging
import os
import sys
import shutil
import signal
import tempfile
import time
from contextlib import contextmanager
from py4j.java_gateway import JavaGateway, GatewayParameters
from pyspark.serializers import read_int, UTF8Deserializer
from subprocess import Popen, PIPE
from py4j.protocol import Py4JJavaError
from enum import Enum
from datetime import timedelta
from .utils import scala_buffer_to_list, get_spark_home, get_spyt_home


logger = logging.getLogger(__name__)


def launch_gateway(memory="512m",
                   java_home=None,
                   java_opts=None,
                   additional_jars=None,
                   additional_environ=None,
                   prefer_ipv6=False):  # Internal Yandex users must enable ipv6 option by default
    spark_home = get_spark_home()
    spyt_home = get_spyt_home()
    java = os.path.join(java_home, "bin", "java") if java_home else "java"
    additional_jars = additional_jars or []

    command = [java, "-Xmx{}".format(memory)]
    command += java_opts or []
    if prefer_ipv6:
        command.append('-Djava.net.preferIPv6Addresses=true')
    spark_patch = [
        os.path.join(spyt_home, 'jars', jar)
        for jar in os.listdir(os.path.join(spyt_home, 'jars'))
        if 'spark-yt-spark-patch' in jar][0]
    command += [
        f"-javaagent:{spark_patch}",
        "-XX:+IgnoreUnrecognizedVMOptions",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "-cp", ":".join(additional_jars + _submit_classpath(spark_home)),
        "tech.ytsaurus.spyt.submit.PythonGatewayServer"
    ]

    conn_info_dir = tempfile.mkdtemp()
    try:
        fd, conn_info_file = tempfile.mkstemp(dir=conn_info_dir)
        os.close(fd)
        os.unlink(conn_info_file)

        env = dict(os.environ)
        env.update(additional_environ or {})
        env["_SPYT_SUBMIT_CONN_INFO_PATH"] = conn_info_file
        env["SPARK_HOME"] = spark_home
        env["SPARK_CONF_DIR"] = os.path.join(get_spyt_home(), "conf")

        # Launch the Java gateway.
        popen_kwargs = {'stdin': PIPE, 'env': env}

        # Don't send ctrl-c / SIGINT to the Java gateway:
        def preexec_func():
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        popen_kwargs['preexec_fn'] = preexec_func
        logger.debug(f"Starting JVM process. Path to bin: {java}")
        proc = Popen(command, **popen_kwargs)

        # Wait for the file to appear, or for the process to exit, whichever happens first.
        while not proc.poll() and not os.path.isfile(conn_info_file):
            time.sleep(0.1)

        if not os.path.isfile(conn_info_file):
            raise Exception("Java gateway process exited before sending its port number")

        logger.debug("Process started. Reading gateway data")
        with open(conn_info_file, "rb") as info:
            gateway_port = read_int(info)
            gateway_secret = UTF8Deserializer().loads(info)
    finally:
        shutil.rmtree(conn_info_dir)

    # Connect to the gateway (or client server to pin the thread between JVM and Python)
    address = '::1' if prefer_ipv6 else '127.0.0.1'
    logger.debug(f"Connecting to created gateway on {address}:{gateway_port}")
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(address=address, port=gateway_port,
                                             auth_token=gateway_secret,
                                             auto_convert=True))

    logger.debug("Gateway connection established")
    # Store a reference to the Popen object for use by the caller (e.g., in reading stdout/stderr)
    gateway.proc = proc
    return gateway


def _submit_classpath(spark_home=None):
    spark_home = spark_home or get_spark_home()
    spyt_home = get_spyt_home()
    jars_cp = [os.path.join(home, "jars/*") for home in [spyt_home, spark_home]]
    return [os.path.join(spyt_home, "conf")] + jars_cp


def shutdown_gateway(gateway):
    gateway.shutdown()
    gateway.proc.stdin.close()
    logger.debug("Gateway stopped")


@contextmanager
def java_gateway(kill_jvm_on_shutdown=True, *args, **kwargs):
    logger.debug("Launching java gateway")
    gateway = launch_gateway(*args, **kwargs)
    try:
        yield gateway
    except Py4JJavaError as e:
        raise RuntimeError(str(e))
    finally:
        shutdown_gateway(gateway)
        if kill_jvm_on_shutdown:
            gateway.proc.kill()
            logger.debug("Java process stopped")


class RetryConfig(object):
    def __new__(cls, *args, **kwargs):
        instance = super(RetryConfig, cls).__new__(cls)
        return instance

    def __init__(self, enable_retry=True, retry_limit=10, retry_interval=timedelta(minutes=1)):
        self.enable_retry = enable_retry
        self.retry_limit = retry_limit
        self.retry_interval = retry_interval
        self.wait_submission_id_retry_limit = None  # Every retry will be 5 seconds

    def _to_java(self, gateway):
        jduration = gateway.jvm.tech.ytsaurus.spyt.submit.RetryConfig.durationFromSeconds(
            int(self.retry_interval.total_seconds()))
        if self.wait_submission_id_retry_limit:  # COMPAT(alex-shishkin)
            args = [self.enable_retry, self.retry_limit, jduration, self.wait_submission_id_retry_limit]
        else:
            args = [self.enable_retry, self.retry_limit, jduration]
        return gateway.jvm.tech.ytsaurus.spyt.submit.RetryConfig(*args)


class SparkSubmissionClient(object):
    def __new__(cls, *args, **kwargs):
        instance = super(SparkSubmissionClient, cls).__new__(cls)
        return instance

    def __init__(self, gateway, proxy, discovery_path, user, token):
        self._jclient = gateway.jvm.tech.ytsaurus.spyt.submit.SubmissionClient(proxy, discovery_path, user, token)
        self.gateway = gateway

    def new_launcher(self):
        jlauncher = self._jclient.newLauncher()
        return SparkLauncher(jlauncher, self.gateway)

    def submit(self, launcher, retry_config=None):
        if retry_config is None:
            retry_config = RetryConfig()
        logger.debug("Job submit")
        jresult = self._jclient.submit(launcher._jlauncher, retry_config._to_java(self.gateway))
        if jresult.isFailure():
            raise RuntimeError(jresult.failed().get().getMessage())
        return jresult.get()

    def kill(self, submission_id):
        return self._jclient.kill(submission_id)

    def get_active_drivers(self):
        return scala_buffer_to_list(self._jclient.getActiveDrivers())

    def get_completed_drivers(self):
        return scala_buffer_to_list(self._jclient.getCompletedDrivers())

    def get_all_drivers(self):
        return scala_buffer_to_list(self._jclient.getAllDrivers())

    def get_status(self, submission_id):
        # TODO refactor to use requests package
        return SubmissionStatus.from_string(self._jclient.getStringStatus(submission_id))

    def get_app_status(self, driver_id):
        # TODO refactor to use requests package
        return ApplicationStatus.from_string(self._jclient.getStringApplicationStatus(driver_id))

    def wait_final(self, app_id, ping_period=5):
        logger.debug(f"Waiting app {app_id} finishing")
        status = self.get_status(app_id)
        while not SubmissionStatus.is_final(status):
            status = self.get_status(app_id)
            logger.debug(f"Current app {app_id} status: {status}")
            time.sleep(ping_period)
        return status


class SubmissionStatus(Enum):
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    RELAUNCHING = "RELAUNCHING"
    UNKNOWN = "UNKNOWN"
    KILLED = "KILLED"
    FAILED = "FAILED"
    ERROR = "ERROR"
    UNDEFINED = "UNDEFINED"

    @staticmethod
    def from_string(name):
        return next(member for n, member in SubmissionStatus.__members__.items() if n == name)

    @staticmethod
    def is_final(status):
        return (
            status is SubmissionStatus.FINISHED or
            status is SubmissionStatus.UNKNOWN or
            status is SubmissionStatus.KILLED or
            status is SubmissionStatus.FAILED or
            status is SubmissionStatus.ERROR
        )

    @staticmethod
    def is_success(status):
        return (
            status is SubmissionStatus.FINISHED
        )

    @staticmethod
    def is_failure(status):
        return (
            status is SubmissionStatus.UNKNOWN or
            status is SubmissionStatus.KILLED or
            status is SubmissionStatus.FAILED or
            status is SubmissionStatus.ERROR
        )


class ApplicationStatus(Enum):
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"
    KILLED = "KILLED"
    UNKNOWN = "UNKNOWN"
    UNDEFINED = "UNDEFINED"

    @staticmethod
    def from_string(name):
        return next(member for n, member in ApplicationStatus.__members__.items() if n == name)

    @staticmethod
    def is_final(status):
        return (
            status is ApplicationStatus.FINISHED or
            status is ApplicationStatus.UNKNOWN or
            status is ApplicationStatus.KILLED or
            status is ApplicationStatus.FAILED
        )

    @staticmethod
    def is_success(status):
        return (
            status is ApplicationStatus.FINISHED
        )

    @staticmethod
    def is_failure(status):
        return (
            status is ApplicationStatus.UNKNOWN or
            status is ApplicationStatus.KILLED or
            status is ApplicationStatus.FAILED
        )

    @staticmethod
    def is_waiting(status):
        return (
            status is ApplicationStatus.WAITING
        )


# wraps methods from
# https://github.com/apache/spark/blob/master/launcher/src/main/java/org/apache/spark/launcher/AbstractLauncher.java
class SparkLauncher(object):
    def __new__(cls, *args, **kwargs):
        instance = super(SparkLauncher, cls).__new__(cls)
        return instance

    def __init__(self, jlauncher, gateway):
        self._jlauncher = jlauncher
        self._jutils = gateway.jvm.tech.ytsaurus.spyt.submit.InProcessLauncherPythonUtils

    def set_app_resource(self, resource: str):
        self._jlauncher.setAppResource(resource)
        return self

    def set_main_class(self, main_class: str):
        self._jlauncher.setMainClass(main_class)
        return self

    def set_conf(self, key: str, value: str):
        self._jlauncher.setConf(key, value)
        return self

    def set_properties_file(self, path: str):
        self._jlauncher.setPropertiesFile(path)
        return self

    def set_app_name(self, app_name: str):
        self._jlauncher.setAppName(app_name)
        return self

    def add_spark_arg(self, arg: str, value: str = None):
        if value:
            self._jlauncher.addSparkArg(arg, value)
        else:
            self._jlauncher.addSparkArg(arg)
        return self

    def add_app_args(self, *args):
        for arg in args:
            self._jutils.addAppArg(self._jlauncher, arg)
        return self

    def add_jar(self, jar: str):
        self._jlauncher.addJar(jar)
        return self

    def add_file(self, file: str):
        self._jlauncher.addFile(file)
        return self

    def add_py_file(self, file: str):
        self._jlauncher.addPyFile(file)
        return self

    def set_verbose(self, verbose: bool):
        self._jlauncher.setVerbose(verbose)
        return self

    def add_spark_args(self, *spark_args):
        for i in range(0, len(spark_args), 2):
            self.add_spark_arg(spark_args[i], spark_args[i+1])
        return self

    def set_spark_conf(self, conf: dict):
        for key, value in conf.items():
            self.set_conf(key, str(value))
        return self

    def set_master(self, master: str):
        self._jlauncher.setMaster(master)
        return self

    def set_deploy_mode(self, deploy_mode: str):
        self._jlauncher.setDeployMode(deploy_mode)
        return self

    def launcher(self):
        return self._jlauncher


def create_base_spark_env(spark_home):
    spark_env = os.environ.copy()
    spark_env["SPARK_CONF_DIR"] = os.path.join(get_spyt_home(), "conf")
    if spark_home:
        spark_env["SPARK_HOME"] = spark_home
    return spark_env


def direct_submit(yt_proxy, num_executors, main_file, deploy_mode="cluster", pool=None,
                  spark_base_args=[], job_args=[], spark_conf={}, java_home=None, prefer_ipv6=False, timeout_sec=30):
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
    :return: operation ID of the submitted Spark job.
    """
    spark_args = []
    spark_args.extend(["--num-executors", str(num_executors)])
    if pool:
        spark_args.extend(["--queue", pool])
    spark_args.extend(spark_base_args)

    with (java_gateway(java_home=java_home, prefer_ipv6=prefer_ipv6) as gateway):
        j_launcher = gateway.jvm.org.apache.spark.launcher.InProcessLauncher()
        spark_launcher = (SparkLauncher(j_launcher, gateway)
                          .set_master("ytsaurus://" + yt_proxy)
                          .set_deploy_mode(deploy_mode)
                          .set_app_resource(main_file)
                          .set_spark_conf(spark_conf)
                          .add_app_args(*job_args)
                          .add_spark_args(*spark_args))

        j_directsubmitter = gateway.jvm.tech.ytsaurus.spyt.submit.DirectSubmitter()
        try_operation_id = j_directsubmitter.submit(spark_launcher.launcher(), timeout_sec)
        if try_operation_id.isSuccess():
            return try_operation_id.get()
        failure = try_operation_id.failed().get()
        raise Exception(f"Failed to submit Spark job: {failure.toString()}, {failure.getMessage()}")


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
