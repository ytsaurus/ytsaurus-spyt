import spyt

from spyt.client import spark_session
from spyt.enabler import SpytEnablers
from spyt.standalone import start_spark_cluster, SparkDefaultArguments, \
    find_spark_cluster, start_livy_server, start_history_server
from spyt.submit import java_gateway, SparkSubmissionClient, direct_submit

from .cluster_utils import DEFAULT_SPARK_CONF, default_conf, dump_debug_data, is_accessible
from .version import VERSION

from yt.common import YtError
from yt.wrapper import YtClient

from contextlib import contextmanager
import logging
import shutil
import tempfile
import time
import uuid


logger = logging.getLogger(__name__)


class ClusterBase(object):
    def __init__(self, proxy, discovery_path=None, group_id=None, yt_root_path=None, dump_dir=None):
        self.proxy = proxy
        self.group_id = group_id
        self.discovery_path = discovery_path or f"//home/cluster-{str(uuid.uuid4())}"
        self.user = "root"
        self.token = "token"
        self.yt_client = YtClient(proxy=self.proxy, token=self.token)
        self.yt_root_path = yt_root_path
        self.dump_dir = dump_dir
        self.op = None

    @staticmethod
    def get_params():
        params = SparkDefaultArguments.get_params()
        params["operation_spec"]["max_failed_job_count"] = 1
        return params

    @staticmethod
    def get_enablers():
        return SpytEnablers()

    def get_component_url(self, name):
        return getattr(find_spark_cluster(self.discovery_path, self.yt_client), name)

    def wait_component_startup(self, name):
        logger.debug("Waiting component startup")
        for _ in range(20):
            url = self.get_component_url(name)
            if is_accessible(url):
                logger.info(f"{name} address: {url}")
                return
            time.sleep(2)
        raise RuntimeError(f"{name} is not started successfully")

    def finish(self, exc_type, exc_val):
        try:
            op_id = self.op.id if self.op is not None else None
            dump_debug_data(self.dump_dir, op_id, self.yt_root_path, self.yt_client, self.discovery_path)
        except Exception:
            logger.warning("Fail in dumping debug data", exc_info=True)
        try:
            self.op.complete()
            self.yt_client.remove(self.discovery_path, recursive=True, force=True)
        except YtError as err:
            inner_errors = [err]
            if exc_type is not None:
                inner_errors.append(exc_val)
            raise YtError("Operation stopping failed", inner_errors=inner_errors)


class SpytCluster(ClusterBase):
    def __init__(self, proxy, discovery_path=None, group_id=None, java_home=None, yt_root_path=None, enable_livy=False,
                 dump_dir=None):
        super().__init__(proxy, discovery_path, group_id, yt_root_path, dump_dir)
        self.java_home = java_home
        self.enable_livy = enable_livy

    def __enter__(self):
        self.op = start_spark_cluster(
            worker_cores=2, worker_memory='3G', worker_num=1, worker_cores_overhead=None, worker_memory_overhead='512M',
            operation_alias='spark_cluster', discovery_path=self.discovery_path, master_memory_limit='3G',
            enable_history_server=False, params=self.get_params(), enable_tmpfs=False,
            enablers=self.get_enablers(), client=self.yt_client, spark_cluster_version=VERSION,
            enable_livy=self.enable_livy, livy_max_sessions=1, group_id=self.group_id)
        if self.op is None:
            raise YtError("Cluster starting failed")
        cluster_info = find_spark_cluster(self.discovery_path, self.yt_client)
        if self.enable_livy:
            self.wait_component_startup('livy_url')
        logger.info(f"Master webUI: {cluster_info.master_web_ui_url}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish(exc_type, exc_val)

    @contextmanager
    def submission_client(self):
        with java_gateway(java_home=self.java_home) as gw:
            yield SparkSubmissionClient(gw, self.proxy, self.discovery_path, self.user, self.token)

    def submit_cluster_job(self, job_path, conf={}, args=[], py_files=[]):
        with self.submission_client() as client:
            launcher = client.new_launcher()
            launcher.set_app_resource("yt:/" + job_path)
            launcher.add_app_args(*args)
            for key, value in conf.items():
                launcher.set_conf(key, value)
            for py_file in py_files:
                launcher.add_py_file(f"yt:/{py_file}")
            app_id = client.submit(launcher)
            status = client.wait_final(app_id)
            return status

    @contextmanager
    def spark_session(self, **kwargs):
        with spark_session(discovery_path=self.discovery_path, client=self.yt_client, cores_per_executor=1,
                           executor_memory_per_core='1G', spyt_version=VERSION, **kwargs) as session:
            yield session


class LivyServer(ClusterBase):
    def __init__(self, proxy, discovery_path=None, group_id=None, yt_root_path=None, master_address=None,
                 dump_dir=None):
        super().__init__(proxy, discovery_path, group_id, yt_root_path, dump_dir)
        self.master_address = master_address

    def __enter__(self):
        self.op = start_livy_server(
            operation_alias='livy_server', discovery_path=self.discovery_path,
            params=self.get_params(),
            enablers=self.get_enablers(), client=self.yt_client, spark_cluster_version=VERSION,
            livy_max_sessions=1, spark_master_address=self.master_address, group_id=self.group_id)
        if self.op is None:
            raise YtError("Server starting failed")
        self.wait_component_startup('livy_url')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish(exc_type, exc_val)

    def rest(self):
        return self.get_component_url('livy_url')


@contextmanager
def direct_spark_session(proxy, extra_conf=None, dump_dir=None):
    extra_conf = extra_conf or {}
    conf = (default_conf()
            .set("spark.hadoop.yt.proxy", proxy)
            .set("spark.ytsaurus.config.releases.path", "//home/spark/conf/releases")
            .set("spark.ytsaurus.spyt.version", VERSION)
            .setAll(extra_conf.items()))
    with spyt.direct_spark_session(proxy, conf) as session:
        op_id = session.conf.get("spark.ytsaurus.executor.operation.id")
        try:
            yield session
        finally:
            try:
                yt_client = YtClient(proxy=proxy, token="token")
                dump_debug_data(dump_dir, op_id=op_id, yt_client=yt_client)
            except Exception:
                logger.warning("Fail in dumping debug data", exc_info=True)


class DirectSubmitter:
    def __init__(self, proxy, extra_conf={}, dump_dir=None):
        self.proxy = proxy
        self.default_conf = DEFAULT_SPARK_CONF | {"spark.hadoop.yt.proxy": self.proxy} | extra_conf
        self.dump_dir = dump_dir
        self.op_ids = []
        self.token = "token"
        self.yt_client = YtClient(proxy=self.proxy, token=self.token)

    def __enter__(self):
        return self

    def submit(self, job_path, spark_base_args=[], job_args=[], conf={}):
        op_path = None
        spark_conf = self.default_conf | conf
        if self.dump_dir:
            _, op_path = tempfile.mkstemp()
            spark_conf |= {"spark.ytsaurus.driver.operation.dump.path": op_path}
        exit_code = direct_submit(self.proxy, num_executors=1, main_file=job_path, spark_base_args=spark_base_args,
                                  job_args=job_args, spark_conf=spark_conf)
        if op_path:
            with open(op_path, 'r') as fd:
                self.op_ids.append(fd.read())
            shutil.rmtree(op_path, ignore_errors=True)
        return exit_code

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            for op_id in self.op_ids:
                dump_debug_data(self.dump_dir, op_id=op_id, yt_client=self.yt_client)
        except Exception:
            logger.warning("Fail in dumping debug data", exc_info=True)


class HistoryServer(ClusterBase):
    def __init__(self, proxy, discovery_path=None):
        super().__init__(proxy, discovery_path)

    def get_params(self):
        params = super().get_params()
        params['spark_conf'] = {'spark.history.fs.update.interval': '1s'}
        return params

    def __enter__(self):
        self.op = start_history_server(
            operation_alias='integration_tests', discovery_path=self.discovery_path, history_server_cpu_limit=1,
            history_server_memory_limit='512m', history_server_memory_overhead='512m', params=self.get_params(),
            enablers=self.get_enablers(), client=self.yt_client, spark_cluster_version=VERSION)
        if self.op is None:
            raise YtError("Server starting failed")
        self.wait_component_startup('shs_url')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish(exc_type, exc_val)

    def rest(self):
        return self.get_component_url('shs_url')
