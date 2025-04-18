from spyt.dependency_utils import require_yt_client
require_yt_client()

from yt.wrapper.cli_helpers import ParseStructuredArgument  # noqa: E402
from spyt.enabler import SpytEnablers  # noqa: E402
from spyt.spec import SparkDefaultArguments  # noqa: E402


def add_parser_group(parser, enabler_arg, disabler_arg, dest_arg, default_value):
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(enabler_arg, dest=dest_arg, action='store_true')
    group.add_argument(disabler_arg, dest=dest_arg, action='store_false')
    parser.set_defaults(**{dest_arg: default_value})


def add_default_launch_options(parser):
    parser.add_argument("--pool", required=False)
    parser.add_argument("--operation-alias", required=False)
    parser.add_argument("--network-project", required=False)
    parser.add_argument("--params", required=False, action=ParseStructuredArgument, dest="params",
                        default=SparkDefaultArguments.get_params())
    parser.add_argument("--spyt-version", required=False)
    parser.add_argument("--preemption_mode", required=False, default="normal")
    parser.add_argument("--cluster-log-level", required=False, default="INFO")

    default_enablers = SpytEnablers()
    add_parser_group(parser, '--enable-mtn', '--disable-mtn', 'enable_mtn', default_enablers.enable_mtn)
    add_parser_group(parser, '--prefer-ipv6', '--prefer-ipv4', 'enable_preference_ipv6', None)

    add_parser_group(parser, '--enable-tmpfs', '--disable-tmpfs', 'enable_tmpfs', False)
    add_parser_group(parser, '--enable-stderr-table', '--disable-stderr-table', 'enable_stderr_table', False)

    add_parser_group(parser, '--enable-tcp-proxy', '--disable-tcp-proxy', 'enable_tcp_proxy',
                     default_enablers.enable_tcp_proxy)
    parser.add_argument('--tcp-proxy-range-start', required=False, default=30000, type=int)
    parser.add_argument('--tcp-proxy-range-size', required=False, default=100, type=int)

    add_parser_group(parser, '--enable-rpc-job-proxy', '--disable-rpc-job-proxy', 'rpc_job_proxy', True)
    parser.add_argument("--rpc-job-proxy-thread-pool-size", required=False, default=4, type=int)

    parser.add_argument('--group-id', required=False, type=str)

    add_parser_group(parser, '--enable-squashfs', '--disable-squashfs', 'enable_squashfs',
                     default_enablers.enable_squashfs)
    parser.add_argument('--cluster-java-home', required=False, type=str)


def add_livy_options(parser):
    parser.add_argument('--livy-driver-cores', required=False,
                        default=SparkDefaultArguments.LIVY_DRIVER_CORES, type=int)
    parser.add_argument('--livy-driver-memory', required=False, default=SparkDefaultArguments.LIVY_DRIVER_MEMORY)
    parser.add_argument('--livy-max-sessions', required=False,
                        default=SparkDefaultArguments.LIVY_MAX_SESSIONS, type=int)


def add_hs_options(parser):
    parser.add_argument("--history-server-memory-limit",
                        required=False, default=SparkDefaultArguments.SPARK_HISTORY_SERVER_MEMORY_LIMIT)
    parser.add_argument("--history-server-memory-overhead",
                        required=False, default=SparkDefaultArguments.SPARK_HISTORY_SERVER_MEMORY_OVERHEAD)
    parser.add_argument("--history-server-cpu-limit",
                        required=False, default=SparkDefaultArguments.SPARK_HISTORY_SERVER_CPU_LIMIT, type=int)
    parser.add_argument("--shs-location", required=False)
    add_parser_group(parser, '--enable-advanced-event-log', '--disable-advanced-event-log', 'advanced_event_log', True)
