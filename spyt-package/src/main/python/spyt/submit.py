import logging
import time
from datetime import timedelta
from .jvm import launch_gateway, _submit_classpath, shutdown_gateway, java_gateway  # noqa: F401
from .utils import scala_buffer_to_list
from ._submit_common import SubmissionStatus, ApplicationStatus
from .direct_submit import direct_submit, direct_submit_binary  # noqa: F401


logger = logging.getLogger(__name__)


class RetryConfig(object):
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
