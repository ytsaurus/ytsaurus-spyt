import logging
import os

from spyt.dependency_utils import require_yt_client
require_yt_client()

from yt.wrapper import get, YPath, list as yt_list, exists  # noqa: E402
from yt.wrapper.common import update_inplace  # noqa: E402
from .version import __scala_version__  # noqa: E402
from pyspark import __version__ as spark_version  # noqa: E402

SPARK_BASE_PATH = YPath(os.getenv('SPYT_ROOT_BASE_PATH', default="//home/spark"))

CONF_BASE_PATH = SPARK_BASE_PATH.join("conf")
GLOBAL_CONF_PATH = CONF_BASE_PATH.join("global")

SPYT_BASE_PATH = SPARK_BASE_PATH.join("spyt")
DISTRIB_BASE_PATH = SPARK_BASE_PATH.join("distrib")

RELEASES_SUBDIR = "releases"
PRE_RELEASES_SUBDIR = "pre-releases"
SNAPSHOTS_SUBDIR = "snapshots"

SELF_VERSION = __scala_version__

logger = logging.getLogger(__name__)


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


def validate_cluster_version(spark_cluster_version, client=None):
    if not check_cluster_version_exists(spark_cluster_version, client=client):
        raise RuntimeError("Unknown SPYT cluster version: {}. Available release versions are: {}".format(
            spark_cluster_version, get_available_cluster_versions(client=client)
        ))
    spyt_minor_version = SpytVersion(SELF_VERSION).get_minor()
    cluster_minor_version = SpytVersion(spark_cluster_version).get_minor()
    if spyt_minor_version < cluster_minor_version:
        logger.warning("You required SPYT version {} which is older than your local ytsaurus-spyt version {}."
                       "Please update your local ytsaurus-spyt".format(spark_cluster_version, SELF_VERSION))


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


def get_available_cluster_versions(client=None):
    subdirs = yt_list(CONF_BASE_PATH.join(RELEASES_SUBDIR), client=client)
    return [x for x in subdirs if x != "spark-launch-conf"]


def check_cluster_version_exists(cluster_version, client=None):
    return exists(_get_version_conf_path(cluster_version), client=client)


def read_global_conf(client=None):
    return client.get(GLOBAL_CONF_PATH)


def read_remote_conf(global_conf, cluster_version, client=None):
    version_conf_path = _get_version_conf_path(cluster_version)
    version_conf = get(version_conf_path, client=client)
    version_conf["cluster_version"] = cluster_version
    return update_inplace(global_conf, version_conf)  # TODO(alex-shishkin): Might cause undefined behaviour


def read_metrics_conf():
    return read_conf("metrics.properties", delimiter="=")


def read_spark_defaults_conf():
    return read_conf("spark-defaults.conf")


def read_conf(filename, delimiter=None):
    props = {}
    try:
        conf_dir = os.environ.get("SPARK_CONF_DIR")
        filepath = os.path.join(conf_dir, filename)
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key_value = line.split(delimiter, 1)
                    if len(key_value) != 2:
                        continue
                    key, value = key_value
                    props[key.strip()] = value.strip()
    except Exception as e:
        print(f"Failed to load conf from {filename}: {e}")
    return props


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


def get_available_spyt_versions(client=None):
    return yt_list(SPYT_BASE_PATH.join(RELEASES_SUBDIR), client=client)


def latest_ytserver_proxy_path(cluster_version, client=None):
    if cluster_version:
        return None
    global_conf = read_global_conf(client=client)
    symlink_path = global_conf.get("ytserver_proxy_path")
    if symlink_path is None:
        return None
    return get("{}&/@target_path".format(symlink_path), client=client)


def ytserver_proxy_attributes(path, client=None):
    return get("{}/@user_attributes".format(path), client=client)


def get_spark_distributive(client, enable_squashfs):
    distrib_root = DISTRIB_BASE_PATH.join(spark_version.replace('.', '/'))
    distrib_root_contents = yt_list(distrib_root, client=client)
    extension = '.squashfs' if enable_squashfs else '.tgz'
    spark_distributive = [x for x in distrib_root_contents if x.endswith(extension)]
    if len(spark_distributive) == 0:
        raise RuntimeError(f"Spark {spark_version} {extension} distributive doesn't exist "
                           f"at path {distrib_root} on cluster {client.config['proxy']['url']}")
    return (spark_distributive[0], distrib_root.join(spark_distributive[0]))


def _get_or_else(d, key, default):
    return d.get(key) or default


def _version_subdir(version):
    return SNAPSHOTS_SUBDIR if "SNAPSHOT" in version \
        else PRE_RELEASES_SUBDIR if any(r_type in version for r_type in ["alpha", "beta", "rc"]) \
        else RELEASES_SUBDIR


def _get_version_conf_path(cluster_version):
    return CONF_BASE_PATH.join(_version_subdir(cluster_version)).join(cluster_version).join("spark-launch-conf")
