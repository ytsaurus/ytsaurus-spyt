import logging

from spyt.dependency_utils import require_yt_client
require_yt_client()

from yt.wrapper import get, YPath, list as yt_list, exists, YtClient  # noqa: E402
from yt.wrapper.common import update_inplace  # noqa: E402
from .version import __scala_version__  # noqa: E402
from pyspark import __version__ as spark_version  # noqa: E402

RELEASES_SUBDIR = "releases"
SNAPSHOTS_SUBDIR = "snapshots"

SELF_VERSION = __scala_version__


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class SpytDistributions:
    def __init__(self, client: YtClient, yt_root: str):
        self.client = client
        self.yt_root = YPath(yt_root)
        self.conf_base_path = self.yt_root.join("conf")
        self.global_conf = client.get(self.conf_base_path.join("global"))
        self.distrib_base_path = self.yt_root.join("distrib")
        self.spyt_base_path = self.yt_root.join("spyt")

    def read_remote_conf(self, cluster_version):
        version_conf = self.client.get(self._get_version_conf_path(cluster_version))
        version_conf["cluster_version"] = cluster_version
        return update_inplace(self.global_conf, version_conf)  # TODO(alex-shishkin): Might cause undefined behaviour

    def latest_ytserver_proxy_path(self, cluster_version):
        if cluster_version:
            return None
        symlink_path = self.global_conf.get("ytserver_proxy_path")
        if symlink_path is None:
            return None
        return self.client.get("{}&/@target_path".format(symlink_path))

    def validate_cluster_version(self, spark_cluster_version):
        if not exists(self._get_version_conf_path(spark_cluster_version), client=self.client):
            raise RuntimeError("Unknown SPYT cluster version: {}. Available release versions are: {}".format(
                spark_cluster_version, self.get_available_cluster_versions()
            ))
        spyt_minor_version = SpytVersion(SELF_VERSION).get_minor()
        cluster_minor_version = SpytVersion(spark_cluster_version).get_minor()
        if spyt_minor_version < cluster_minor_version:
            logger.warning("You required SPYT version {} which is older than your local ytsaurus-spyt version {}."
                           "Please update your local ytsaurus-spyt".format(spark_cluster_version, SELF_VERSION))

    def get_available_cluster_versions(self):
        subdirs = yt_list(self.conf_base_path.join(RELEASES_SUBDIR), client=self.client)
        return [x for x in subdirs if x != "spark-launch-conf"]

    def latest_compatible_spyt_version(self, version):
        minor_version = SpytVersion(version).get_minor()
        spyt_versions = self.get_available_spyt_versions()
        compatible_spyt_versions = [x for x in spyt_versions if SpytVersion(x).get_minor() == minor_version]
        if not compatible_spyt_versions:
            raise RuntimeError(f"No compatible SPYT versions found for specified version {version}")
        return max(compatible_spyt_versions, key=SpytVersion)

    def get_available_spyt_versions(self):
        return yt_list(self.spyt_base_path.join(RELEASES_SUBDIR), client=self.client)

    def get_spark_distributive(self):
        distrib_root = self.distrib_base_path.join(spark_version.replace('.', '/'))
        distrib_root_contents = yt_list(distrib_root, client=self.client)
        spark_tgz = [x for x in distrib_root_contents if x.endswith('.tgz')]
        if len(spark_tgz) == 0:
            raise RuntimeError(f"Spark {spark_version} tgz distributive doesn't exist "
                               f"at path {distrib_root} on cluster {self.client.config['proxy']['url']}")
        return (spark_tgz[0], distrib_root.join(spark_tgz[0]))

    def _get_version_conf_path(self, version):
        return self.conf_base_path.join(
            SNAPSHOTS_SUBDIR if "SNAPSHOT" in version or "beta" in version or "dev" in version else RELEASES_SUBDIR
        ).join(version).join("spark-launch-conf")


class SpytVersion:
    def __init__(self, version=None, major=0, minor=0, patch=0):
        if version is not None:
            self.major, self.minor, self.patch = map(int, version.split("-")[0].split("."))
        else:
            self.major, self.minor, self.patch = major, minor, patch

    def get_minor(self):
        return SpytVersion(major=self.major, minor=self.minor)

    def __gt__(self, other):
        return self.tuple() > other.tuple()

    def __ge__(self, other):
        return self.tuple() >= other.tuple()

    def __eq__(self, other):
        return self.tuple() == other.tuple()

    def __lt__(self, other):
        return self.tuple() < other.tuple()

    def __le__(self, other):
        return self.tuple() <= other.tuple()

    def tuple(self):
        return self.major, self.minor, self.patch

    def __str__(self):
        return f"{self.major}.{self.minor}.{self.patch}"


def validate_versions_compatibility(spyt_version, spark_cluster_version):
    spyt_minor_version = SpytVersion(spyt_version).get_minor()
    spark_cluster_minor_version = SpytVersion(spark_cluster_version).get_minor()
    if spyt_minor_version > spark_cluster_minor_version:
        logger.warning("Your SPYT library has version {} which is older than your cluster version {}. "
                       "Some new features may not work as expected. "
                       "Please update your cluster with spark-launch-yt utility".format(spyt_version, spark_cluster_version))


def validate_mtn_config(enablers, network_project, tvm_id, tvm_secret):
    if enablers.enable_mtn and not network_project:
        raise RuntimeError("When using MTN, network_project arg must be set.")


def python_bin_path(global_conf, version):
    return global_conf["python_cluster_paths"].get(version)


def worker_num_limit(global_conf):
    return global_conf.get("worker_num_limit", 1000)


def cuda_toolkit_version(global_conf):
    return global_conf.get("cuda_toolkit_version", "11.0")


def validate_worker_num(worker_num, worker_num_lim):
    if worker_num > worker_num_lim:
        raise RuntimeError("Number of workers ({0}) is more than limit ({1})".format(worker_num, worker_num_lim))


def validate_ssd_config(disk_limit, disk_account):
    if disk_limit is not None and disk_account is None:
        raise RuntimeError("Disk account must be provided to use disk limit, please add --worker-disk-account option")


def read_cluster_conf(path=None, client=None):
    if path is None:
        return {}
    return get(path, client=client)


def update_config_inplace(base, patch):
    file_paths = _get_or_else(patch, "file_paths", []) + _get_or_else(base, "file_paths", [])
    layer_paths = _get_or_else(patch, "layer_paths", []) + _get_or_else(base, "layer_paths", [])
    update_inplace(base, patch)
    base["file_paths"] = file_paths
    base["layer_paths"] = layer_paths
    return base


def validate_custom_params(params):
    if params and "enablers" in params:
        raise RuntimeError("Argument 'params' contains 'enablers' field, which is prohibited. "
                           "Use argument 'enablers' instead")


def ytserver_proxy_attributes(path, client=None):
    return get("{}/@user_attributes".format(path), client=client)


def _get_or_else(d, key, default):
    return d.get(key) or default
