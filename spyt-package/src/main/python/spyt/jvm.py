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

from pyspark import SparkContext, SparkConf  # noqa: F401  (re-exported)
from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField

from .utils import check_spark_version

if check_spark_version(less_than="4.0.0"):
    from pyspark.sql.column import _to_java_column
else:
    from pyspark.sql.classic.column import _to_java_column


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
