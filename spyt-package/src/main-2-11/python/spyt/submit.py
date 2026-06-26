import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import timedelta

import requests
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout
from pyspark import __version__ as pyspark_version
from yt.wrapper import YtClient
from yt.wrapper.common import SECRET_PREFIXES_RE

from .jvm import launch_gateway, _submit_classpath, shutdown_gateway, java_gateway  # noqa: F401
from .utils import SparkDiscovery
from ._submit_common import SubmissionStatus, ApplicationStatus, _retry_with_backoff
from .direct_submit import direct_submit, direct_submit_binary  # noqa: F401


logger = logging.getLogger(__name__)


class RetryConfig(object):
    def __init__(self, enable_retry=True, retry_limit=10, retry_interval=timedelta(minutes=1)):
        self.enable_retry = enable_retry
        self.retry_limit = retry_limit
        self.retry_interval = retry_interval


@dataclass(frozen=True)
class RestClusterEndpoints:
    master_host_port: str
    rest_host_port: str
    version: str
    scheme: str = "http"

    @property
    def rest_base_url(self) -> str:
        return "{}://{}".format(self.scheme, self.rest_host_port)

    @staticmethod
    def discover(discovery_path, yt_client, scheme="http"):
        discovery = SparkDiscovery(discovery_path=discovery_path)
        lookups = [
            (discovery.master_spark(), "Spark master address"),
            (discovery.master_rest(), "Spark REST address"),
            (discovery.spark_cluster_version(), "Cluster version"),
        ]
        values = []
        for path, label in lookups:
            v = SparkDiscovery.get(path, client=yt_client)
            if v is None:
                raise RuntimeError("{} not found under {}.".format(label, path))
            values.append(v)
        return RestClusterEndpoints(*values, scheme=scheme)


class SparkRestClient:
    def __init__(self,
                 endpoints,
                 extra_headers=None,
                 session=None,
                 timeout_sec=30.0):
        self.endpoints = endpoints
        self.extra_headers = dict(extra_headers or {})
        self.session = session or requests.Session()
        self.timeout = timeout_sec

    def _rest_url(self, path):
        return "{}/v1/submissions/{}".format(self.endpoints.rest_base_url, path)

    SECRET_KEY_RE = re.compile(
        r'("?[^"\s]*(?:token|secret|password)[^"\s]*"?\s*[:=]\s*"?)([^",\s)}]+)',
        re.IGNORECASE,
    )

    @classmethod
    def _redact_secrets(cls, text):
        if not text:
            return text
        text = cls.SECRET_KEY_RE.sub(r'\1***', text)
        for pattern in SECRET_PREFIXES_RE:
            text = pattern.sub('***', text)
        return text

    def _request(self, method, url, json=None, extra_headers=None):
        headers = {**self.extra_headers, **(extra_headers or {})}
        response = self.session.request(
            method, url, json=json, headers=headers, timeout=self.timeout,
        )
        if not response.ok:
            logger.error("REST %s %s failed %s: %s", method, url, response.status_code,
                         self._redact_secrets(response.text))
        response.raise_for_status()
        return response.json()

    def submit(self, request_body):
        return self._request("POST", self._rest_url("submit"), json=request_body,
                             extra_headers={"Content-Type": "application/json"})

    def status(self, submission_id):
        return self._request("GET", self._rest_url("status/" + submission_id))

    def kill(self, submission_id):
        return self._request("POST", self._rest_url("kill/" + submission_id))

    def app_id(self, submission_id):
        return self._request("GET", self._rest_url("getAppId/" + submission_id))

    def app_status(self, application_id):
        return self._request("GET", self._rest_url("getAppStatus/" + application_id))

    def all_drivers(self):
        return self._request("GET", self._rest_url("status")).get("statuses", [])


def _is_final_driver_state(state_name):
    if state_name is None:
        return False
    try:
        return SubmissionStatus.is_final(SubmissionStatus.from_string(state_name))
    except StopIteration:
        return False


def _is_discovery_error(e):
    return isinstance(e, (ConnectionError, Timeout))


class _SubmitRejectedError(RuntimeError):
    pass


class SparkSubmissionClient(object):
    def __init__(self, gateway=None, proxy=None, discovery_path=None,
                 user=None, token=None):
        if proxy is None or discovery_path is None:
            raise TypeError("proxy and discovery_path are required")
        if gateway is not None:
            import warnings
            warnings.warn(
                "SparkSubmissionClient `gateway` parameter is deprecated and ignored "
                "(REST-based submit no longer requires a JVM gateway). "
                "Use SparkSubmissionClient.create(proxy=..., discovery_path=..., user=..., token=...) instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        self._proxy = proxy
        self._discovery_path = discovery_path
        self._user = user
        self._token = token
        self._yt_client = YtClient(proxy=self._proxy, token=self._token)
        self._rest_cache = None

    @classmethod
    def create(cls, proxy, discovery_path, user=None, token=None):
        return cls(gateway=None, proxy=proxy, discovery_path=discovery_path,
                   user=user, token=token)

    def new_launcher(self):
        return SparkLauncher()

    def submit(self, launcher, retry_config=None):
        if retry_config is None:
            retry_config = RetryConfig()
        if not launcher.app_resource:
            raise ValueError("launcher.set_app_resource(...) must be called before submit()")

        def _attempt():
            try:
                return self._do_submit_attempt(launcher)
            except HTTPError as e:
                resp = e.response
                if resp is not None and 400 <= resp.status_code < 500:
                    raise _SubmitRejectedError(
                        "Submission rejected by master (status {}): {}".format(
                            resp.status_code, SparkRestClient._redact_secrets(resp.text))
                    ) from e
                raise

        def _on_retry(attempt, e, delay):
            logger.warning("Submit attempt %d failed: %s; retrying in %ss",
                           attempt, e, delay)
            if _is_discovery_error(e):
                self._force_cluster_update()

        max_attempts = retry_config.retry_limit + 1 if retry_config.enable_retry else 1
        interval_sec = retry_config.retry_interval.total_seconds()
        try:
            return _retry_with_backoff(
                _attempt,
                max_attempts=max_attempts,
                base_delay_sec=interval_sec,
                max_delay_sec=interval_sec,
                classify_transient=lambda e: not isinstance(e, _SubmitRejectedError),
                on_retry=_on_retry,
            )
        except _SubmitRejectedError as e:
            raise RuntimeError(str(e)) from e.__cause__
        except Exception as e:
            if not retry_config.enable_retry:
                raise RuntimeError("Failed to submit job and retry is disabled") from e
            raise RuntimeError(
                f"Failed to submit job and retry limit {retry_config.retry_limit} exceeded"
            ) from e

    def kill(self, submission_id):
        try:
            body = self._rest.kill(submission_id)
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return False
            raise
        except (ConnectionError, Timeout):
            self._force_cluster_update()
            raise
        return bool(body.get("success"))

    def _driver_ids_where(self, predicate, log_warning=False):
        return [d["driverId"] for d in self._safe_all_drivers(log_warning=log_warning)
                if predicate(d)]

    def get_active_drivers(self):
        return self._driver_ids_where(lambda d: not _is_final_driver_state(d.get("status")))

    def get_completed_drivers(self):
        return self._driver_ids_where(lambda d: _is_final_driver_state(d.get("status")))

    def get_all_drivers(self):
        return self._driver_ids_where(lambda d: True, log_warning=True)

    def get_status(self, submission_id):
        try:
            body = self._rest.status(submission_id)
        except (RequestException, ValueError) as e:
            logger.warning("Failed to get status of submission %s: %s", submission_id, e)
            if _is_discovery_error(e):
                self._force_cluster_update()
            return SubmissionStatus.UNDEFINED
        if not body.get("success"):
            return SubmissionStatus.UNKNOWN
        state_name = body.get("driverState")
        try:
            return SubmissionStatus.from_string(state_name)
        except StopIteration:
            logger.warning("Unknown driver state %r returned by master for %s",
                           state_name, submission_id)
            return SubmissionStatus.UNKNOWN

    def get_app_status(self, submission_id):
        try:
            id_body = self._rest.app_id(submission_id)
            if not id_body.get("success"):
                return ApplicationStatus.UNKNOWN
            app_id = id_body.get("appId")
            if not app_id:
                return ApplicationStatus.UNKNOWN
            st_body = self._rest.app_status(app_id)
        except (RequestException, ValueError) as e:
            logger.warning("Failed to get app status of submission %s: %s", submission_id, e)
            if _is_discovery_error(e):
                self._force_cluster_update()
            return ApplicationStatus.UNDEFINED
        if not st_body.get("success"):
            return ApplicationStatus.UNKNOWN
        state_name = st_body.get("appState")
        try:
            return ApplicationStatus.from_string(state_name)
        except StopIteration:
            logger.warning("Unknown application state %r returned by master for %s",
                           state_name, submission_id)
            return ApplicationStatus.UNKNOWN

    def wait_final(self, app_id, ping_period=5, max_undefined_in_a_row=12):
        logger.debug("Waiting app %s finishing", app_id)
        status = self.get_status(app_id)
        undefined_streak = 0
        while not SubmissionStatus.is_final(status):
            if status is SubmissionStatus.UNDEFINED:
                undefined_streak += 1
                if undefined_streak >= max_undefined_in_a_row:
                    raise RuntimeError(
                        "Cluster status unavailable for {} consecutive polls (~{}s)".format(
                            undefined_streak, undefined_streak * ping_period))
            else:
                undefined_streak = 0
            time.sleep(ping_period)
            status = self.get_status(app_id)
            logger.debug("Current app %s status: %s", app_id, status)
        return status

    def _discover_endpoints(self):
        eps = RestClusterEndpoints.discover(
            discovery_path=self._discovery_path, yt_client=self._yt_client,
        )
        self._rest_cache = SparkRestClient(eps)

    @property
    def _rest(self):
        if self._rest_cache is None:
            self._discover_endpoints()
        return self._rest_cache

    def _force_cluster_update(self):
        try:
            self._discover_endpoints()
        except Exception as e:
            logger.warning("Failed to refresh cluster endpoints: %s", e)

    def _do_submit_attempt(self, launcher):
        conf = dict(launcher.conf)

        default_app_name = os.path.basename(launcher.app_resource) or launcher.app_resource
        conf.setdefault("spark.app.name", launcher.app_name or default_app_name)

        for key, items in (("spark.jars", launcher.jars),
                           ("spark.files", launcher.files),
                           ("spark.submit.pyFiles", launcher.py_files)):
            if items:
                existing = [x.strip() for x in conf.get(key, "").split(",") if x.strip()]
                conf[key] = ",".join(existing + items)

        conf["spark.hadoop.yt.proxy"] = self._proxy
        if self._user is not None:
            conf["spark.hadoop.yt.user"] = self._user
        if self._token is not None:
            conf["spark.hadoop.yt.token"] = self._token

        request_body = {
            "action": "SpytSubmitRequest",
            "appResource": launcher.app_resource,
            "mainClass": launcher.main_class,
            "appArgs": list(launcher.app_args),
            "sparkProperties": conf,
            "environmentVariables": {},
            "clientSparkVersion": pyspark_version,
        }

        response = self._rest.submit(request_body)
        if not response.get("success"):
            raise _SubmitRejectedError("Submission failed: {}".format(response))
        return response["submissionId"]

    def _safe_all_drivers(self, log_warning=False):
        try:
            return self._rest.all_drivers()
        except Exception as e:
            if log_warning:
                logger.warning("Failed to get list of drivers: %s", e)
            return []


class SparkLauncher(object):
    def __init__(self):
        self.app_resource = None
        self.main_class = None
        self.app_name = None
        self.conf = {}
        self.jars = []
        self.files = []
        self.py_files = []
        self.app_args = []

    def set_app_resource(self, resource: str):
        self.app_resource = resource
        return self

    def set_main_class(self, main_class: str):
        self.main_class = main_class
        return self

    def set_conf(self, key: str, value: str):
        self.conf[key] = str(value)
        return self

    def set_app_name(self, app_name: str):
        self.app_name = app_name
        return self

    def add_app_args(self, *args):
        self.app_args.extend(args)
        return self

    def add_jar(self, jar: str):
        self.jars.append(jar)
        return self

    def add_file(self, file: str):
        self.files.append(file)
        return self

    def add_py_file(self, file: str):
        self.py_files.append(file)
        return self

    def set_spark_conf(self, conf):
        for k, v in conf.items():
            self.set_conf(k, v)
        return self


