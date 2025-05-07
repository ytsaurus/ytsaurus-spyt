"""SPYT extensions for pyspark module.

Usage notes: spyt module must be imported before any of pyspark.* modules in order
for extensions to take an effect.
"""
import logging

from .dependency_utils import require_pyspark

require_pyspark()

from .client import connect, spark_session, connect_direct, direct_spark_session, \
    info, stop, jvm_process_pid, yt_client, is_stopped  # noqa: E402
from .extensions import read_yt, read_schema_hint, write_yt, sorted_by, optimize_for, withYsonColumn, transform, \
    write_schema_hint  # noqa: E402
from .utils import check_spark_version  # noqa: E402
from spyt.types import UInt64Type  # noqa: E402
import pyspark.context  # noqa: E402
import pyspark.sql.types  # noqa: E402
import pyspark.sql.readwriter  # noqa: E402
import pyspark.cloudpickle.cloudpickle  # noqa: E402
import pyspark.cloudpickle.cloudpickle_fast  # noqa: E402
from types import CodeType  # noqa: E402

__all__ = [
    'connect',
    'connect_direct',
    'spark_session',
    'direct_spark_session',
    'info',
    'stop',
    'jvm_process_pid',
    'yt_client',
    'is_stopped'
]


def configure_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    handler.setFormatter(formatter)

    logger.addHandler(handler)


def initialize():
    pyspark.sql.types._atomic_types.append(UInt64Type)
    # exact copy of the corresponding line in pyspark/sql/types.py
    pyspark.sql.types._all_atomic_types = dict((t.typeName(), t) for t in pyspark.sql.types._atomic_types)
    pyspark.sql.types._acceptable_types.update({UInt64Type: (int,)})

    pyspark.sql.readwriter.DataFrameReader.yt = read_yt
    pyspark.sql.readwriter.DataFrameReader.schema_hint = read_schema_hint

    pyspark.sql.readwriter.DataFrameWriter.yt = write_yt
    pyspark.sql.readwriter.DataFrameWriter.schema_hint = write_schema_hint
    pyspark.sql.readwriter.DataFrameWriter.sorted_by = sorted_by
    pyspark.sql.readwriter.DataFrameWriter.optimize_for = optimize_for

    pyspark.sql.dataframe.DataFrame.withYsonColumn = withYsonColumn
    pyspark.sql.dataframe.DataFrame.transform = transform

    pyspark.context.SparkContext.PACKAGE_EXTENSIONS += ('.whl',)

    if check_spark_version(less_than="3.4.0"):
        from .extensions import _extract_code_globals, _code_reduce

        pyspark.cloudpickle.cloudpickle._extract_code_globals = _extract_code_globals
        pyspark.cloudpickle.cloudpickle_fast._extract_code_globals = _extract_code_globals
        pyspark.cloudpickle.cloudpickle_fast._code_reduce = _code_reduce
        pyspark.cloudpickle.cloudpickle_fast.CloudPickler.dispatch_table[CodeType] = _code_reduce


configure_logging()
initialize()
