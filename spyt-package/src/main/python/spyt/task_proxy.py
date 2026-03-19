from typing import Optional

from spyt.dependency_utils import require_yt_client
require_yt_client()

from yt.wrapper import YtClient  # noqa: E402
from .enabler import SpytEnablers  # noqa: E402
from .utils import SparkDiscovery, parse_bool  # noqa: E402


class TaskProxyInfo:
    SERVICES = "//sys/task_proxies/services"

    def __init__(self, master_rest: Optional[str] = None, master_ui: Optional[str] = None,
                 history_ui: Optional[str] = None):
        self.master_rest = master_rest
        self.master_ui = master_ui
        self.history_ui = history_ui

    @staticmethod
    def maybe_task_proxy(discovery: SparkDiscovery, client: YtClient):
        if not client.exists(TaskProxyInfo.SERVICES):
            return None
        conf_parameter = discovery.conf().join("spark_conf").join(SpytEnablers.TASK_PROXY_KEY)
        if not client.exists(conf_parameter) or not parse_bool(client.get(conf_parameter)):
            return None
        operation_id = SparkDiscovery.get(discovery.operation(), client)
        services_reader = client.read_table(TaskProxyInfo.SERVICES)
        domains = {}
        for row in services_reader:
            if row["operation_id"] == operation_id:
                domains[f"{row['task_name']}_{row['service']}"] = row["domain"]
        return TaskProxyInfo(**domains)
