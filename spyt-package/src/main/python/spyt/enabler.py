import json
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class SpytEnablers(object):
    BYOP_KEY = "spark.hadoop.yt.byop.enabled"
    PROFILING_KEY = "spark.hadoop.yt.profiling.enabled"
    ARROW_KEY = "spark.hadoop.yt.read.arrow.enabled"
    MTN_KEY = "spark.hadoop.yt.mtn.enabled"
    SOLOMON_AGENT_KEY = "spark.hadoop.yt.solomonAgent.enabled"
    IPV6_KEY = "spark.hadoop.yt.preferenceIpv6.enabled"
    TCP_PROXY_KEY = "spark.hadoop.yt.tcpProxy.enabled"

    def __init__(self, enable_byop=False, enable_profiling=False, enable_arrow=True,
                 enable_mtn=False, enable_solomon_agent=True, enable_preference_ipv6=True,
                 enable_tcp_proxy=False):
        self.enable_byop = enable_byop
        self.enable_profiling = enable_profiling
        self.enable_arrow = enable_byop if enable_arrow is None else enable_arrow
        self.enable_mtn = enable_mtn
        self.enable_solomon_agent = enable_solomon_agent
        self.enable_preference_ipv6 = enable_preference_ipv6
        self.enable_tcp_proxy = enable_tcp_proxy
        self.config_enablers = {}

    def _get_enabler(self, enabler, old_enabler_name, enabler_name):
        config_enabler = False
        # COMPAT(alex-shishkin): Remove when there is no cluster under 1.76.0
        if old_enabler_name in self.config_enablers:
            config_enabler = self.config_enablers.get(old_enabler_name)
        elif enabler_name in self.config_enablers:
            config_enabler = self.config_enablers.get(enabler_name)
        if enabler and not config_enabler:
            logger.warning("{} is enabled in start arguments, but disabled for current cluster version. "
                           "It will be disabled".format(old_enabler_name))
        return enabler and config_enabler

    def apply_config(self, config):
        self.config_enablers = config.get("enablers") or {}
        self.enable_byop = self._get_enabler(self.enable_byop, "enable_byop", self.BYOP_KEY)
        self.enable_mtn = self._get_enabler(self.enable_mtn, "enable_mtn", self.MTN_KEY)
        self.enable_solomon_agent = self._get_enabler(self.enable_solomon_agent, "enable_solomon_agent",
                                                      self.SOLOMON_AGENT_KEY)
        self.enable_tcp_proxy = self._get_enabler(self.enable_tcp_proxy, "enable_tcp_proxy", self.TCP_PROXY_KEY)
        self.enable_preference_ipv6 = self._get_enabler(self.enable_preference_ipv6, "enable_preference_ipv6",
                                                        self.IPV6_KEY)

    def __str__(self):
        return json.dumps(self.get_conf())

    def get_conf(self):
        return {
            self.BYOP_KEY: str(self.enable_byop),
            self.ARROW_KEY: str(self.enable_arrow),
            self.IPV6_KEY: str(self.enable_preference_ipv6),
        }
