"""JVM-specific utilities for SPYT.

This module contains all code that directly interacts with the JVM via Py4J:
- SparkContext / SparkConf (classic PySpark JVM bridge)
- _shutdown_jvm, jvm_process_pid, is_stopped
- _close_yt_client
- DataFrameReader/Writer schema hint helpers that call _jsparkSession / _jvm
- withYsonColumn that calls _jdf / _sc._jvm

All functions in this module require a classic pyspark installation with Py4J.
They will raise ImportError if imported under pyspark-client (JVM-free mode).
Guard call sites with is_classic_pyspark() from dependency_utils before importing
this module.
"""

from .dependency_utils import is_classic_pyspark

if not is_classic_pyspark():
    raise ImportError(
        "spyt.jvm requires classic pyspark (with Py4J/JVM). "
        "The currently installed pyspark distribution does not provide SparkContext. "
        "Do not import spyt.jvm when running under pyspark-client."
    )

import logging
import os
import shutil
import signal
import tempfile
import time
from contextlib import contextmanager
from py4j.java_gateway import JavaGateway, GatewayParameters
from py4j.protocol import Py4JJavaError
from subprocess import Popen, PIPE

from pyspark import SparkContext, SparkConf  # noqa: F401  (re-exported)
from pyspark.serializers import read_int, UTF8Deserializer
from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField

from .utils import check_spark_version, get_spark_home, get_spyt_home, get_spyt_conf_dir, get_scala_version

if check_spark_version(less_than="4.0.0"):
    from pyspark.sql.column import _to_java_column
else:
    from pyspark.sql.classic.column import _to_java_column


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# spark-submit-yt JVM gateway helpers
# ---------------------------------------------------------------------------

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
    jars_root = os.path.join(spyt_home, 'jars', 'common')
    spark_patch = [os.path.join(jars_root, jar) for jar in os.listdir(jars_root) if 'spyt-patch-agent' in jar][0]
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
        env["SPARK_CONF_DIR"] = get_spyt_conf_dir()

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
    scala_version = get_scala_version()

    return [os.path.join(spyt_home, "conf"),
            os.path.join(spyt_home, f"jars/scala-{scala_version}/*"),
            os.path.join(spyt_home, "jars/common/*"),
            os.path.join(spark_home, "jars/*")]


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


# ---------------------------------------------------------------------------
# SparkContext extensions
# ---------------------------------------------------------------------------

def register_whl_package_extension():
    """Add '.whl' to SparkContext.PACKAGE_EXTENSIONS."""
    pyspark_context = __import__("pyspark.context", fromlist=["SparkContext"])
    pyspark_context.SparkContext.PACKAGE_EXTENSIONS += ('.whl',)


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------

def spark_session_exists():
    """Return True if a classic SparkSession already exists."""
    return SparkSession._instantiatedSession is not None


def is_stopped(spark):
    """Return True if the underlying Spark context has been stopped."""
    return spark._jsc.sc().isStopped()


def jvm_process_pid():
    """Return the PID of the Py4J gateway JVM process."""
    return SparkContext._gateway.proc.pid


# ---------------------------------------------------------------------------
# JVM shutdown / cleanup
# ---------------------------------------------------------------------------

def close_yt_client(spark):
    """Close the YtClient held in the JVM-side YtClientProvider."""
    spark._jvm.tech.ytsaurus.spyt.fs.YtClientProvider.close()


def shutdown_jvm(spark):
    """Shut down the Py4J gateway process attached to *spark*."""
    from subprocess import Popen
    proc = SparkContext._gateway.proc
    if not isinstance(proc, Popen):
        import logging
        logging.getLogger(__name__).warning(
            "SparkSession cannot be closed properly, "
            "please update ytsaurus-spyt and Spark cluster"
        )
        return
    proc.stdin.close()
    SparkContext._gateway.shutdown()
    proc.wait(timeout=15)
    SparkContext._gateway = None
    SparkContext._jvm = None
    spark._jvm = None
    SparkSession.builder._options = {}


# ---------------------------------------------------------------------------
# DataFrameReader / DataFrameWriter schema hint helpers
# ---------------------------------------------------------------------------

def apply_read_schema_hint(reader, fields):
    """Apply a YT schema hint to *reader* (DataFrameReader) via the JVM.

    Returns the modified reader.
    """
    spark = SparkSession.builder.getOrCreate()
    struct_fields = []
    for name, data_type in fields.items():
        if isinstance(data_type, dict):
            data_type = StructType([StructField(k, v) for k, v in data_type.items()])
        struct_fields.append(StructField(name, data_type))
    schema = StructType(struct_fields)

    jschema = spark._jsparkSession.parseDataType(schema.json())
    reader._jreader = spark._jvm.tech.ytsaurus.spyt.PythonUtils.schemaHint(reader._jreader, jschema)
    return reader


def apply_write_schema_hint(writer, fields):
    """Apply a YT schema hint to *writer* (DataFrameWriter) via the JVM.

    Returns the modified writer.
    """
    spark = SparkSession.builder.getOrCreate()
    jschema = spark._jvm.java.util.HashMap()
    for key, value in fields.items():
        jschema.put(key, value)
    writer._jwrite = spark._jvm.tech.ytsaurus.spyt.PythonUtils.schemaHint(writer._jwrite, jschema)
    return writer


# ---------------------------------------------------------------------------
# DataFrame JVM helpers
# ---------------------------------------------------------------------------

def with_yson_column(df, col_name, col):
    """Add a YSON-typed column to *df* via the JVM helper PythonUtils.withYsonColumn."""
    java_column = _to_java_column(col)
    return DataFrame(
        df._sc._jvm.tech.ytsaurus.spyt.PythonUtils.withYsonColumn(df._jdf, col_name, java_column),
        df.sql_ctx,
    )


def uint64_to_string_udf(s_col):
    sc = SparkContext._active_spark_context
    cols = sc._gateway.new_array(sc._jvm.Column, 1)
    cols[0] = _to_java_column(s_col)
    jc = sc._jvm.org.apache.spark.sql.spyt.types.UInt64Long.toStringUdf().apply(cols)
    return Column(jc)


def string_to_uint64_udf(s_col):
    sc = SparkContext._active_spark_context
    cols = sc._gateway.new_array(sc._jvm.Column, 1)
    cols[0] = _to_java_column(s_col)
    jc = sc._jvm.org.apache.spark.sql.spyt.types.UInt64Long.fromStringUdf().apply(cols)
    return Column(jc)


def xx_hash64_zero_seed_udf(*s_cols):
    sc = SparkContext._active_spark_context
    sz = len(s_cols)
    cols = sc._gateway.new_array(sc._jvm.Column, sz)
    for i in range(sz):
        cols[i] = _to_java_column(s_cols[i])
    jc = sc._jvm.tech.ytsaurus.spyt.common.utils.XxHash64ZeroSeed.xxHash64ZeroSeedUdf(cols)
    return Column(jc)


def register_xxHash64ZeroSeed(spark):
    sc = SparkContext._active_spark_context
    sc._jvm.tech.ytsaurus.spyt.common.utils.XxHash64ZeroSeed.registerFunction(spark._jsparkSession)


def cityhash_udf(*s_cols):
    sc = SparkContext._active_spark_context
    sz = len(s_cols)
    cols = sc._gateway.new_array(sc._jvm.Column, sz)
    for i in range(sz):
        cols[i] = _to_java_column(s_cols[i])
    jc = sc._jvm.tech.ytsaurus.spyt.common.utils.CityHash.cityHashUdf(cols)
    return Column(jc)


def register_cityHash(spark):
    sc = SparkContext._active_spark_context
    sc._jvm.tech.ytsaurus.spyt.common.utils.CityHash.registerFunction(spark._jsparkSession)
