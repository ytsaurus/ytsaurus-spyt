"""SPYT extensions for pyspark module.

Usage notes: spyt module must be imported before any of pyspark.* modules in order
for extensions to take an effect.
"""
from importlib.util import find_spec
import logging
from types import CodeType

from .dependency_utils import require_pyspark, is_classic_pyspark

require_pyspark()

from .client import connect, spark_session, connect_direct, direct_spark_session, \
    info, stop, yt_client  # noqa: E402
from .extensions import read_yt, read_schema_hint, write_yt, sorted_by, optimize_for, withYsonColumn, transform, \
    write_schema_hint  # noqa: E402
from .types import UInt64Type  # noqa: E402
from .utils import check_spark_version  # noqa: E402
import pyspark.sql.types  # noqa: E402
import pyspark.sql.readwriter  # noqa: E402
if is_classic_pyspark():
    from .jvm import register_whl_package_extension, jvm_process_pid, is_stopped  # noqa: E402
    import pyspark.cloudpickle.cloudpickle  # noqa: E402
    import pyspark.cloudpickle.cloudpickle_fast  # noqa: E402

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
    # TODO support pyspark-connect for uint 64
    if is_classic_pyspark():
        pyspark.sql.types._atomic_types.append(UInt64Type)
        if check_spark_version(less_than="4.0.0"):
            # exact copy of the corresponding line in pyspark/sql/types.py
            pyspark.sql.types._all_atomic_types = dict((t.typeName(), t) for t in pyspark.sql.types._atomic_types)
        else:
            pyspark.sql.types._all_mappable_types["uint64"] = UInt64Type
        pyspark.sql.types._acceptable_types.update({UInt64Type: (int,)})

    pyspark.sql.readwriter.DataFrameReader.yt = read_yt
    pyspark.sql.readwriter.DataFrameReader.schema_hint = read_schema_hint

    pyspark.sql.readwriter.DataFrameWriter.yt = write_yt
    pyspark.sql.readwriter.DataFrameWriter.schema_hint = write_schema_hint
    pyspark.sql.readwriter.DataFrameWriter.sorted_by = sorted_by
    pyspark.sql.readwriter.DataFrameWriter.optimize_for = optimize_for

    pyspark.sql.dataframe.DataFrame.withYsonColumn = withYsonColumn
    pyspark.sql.dataframe.DataFrame.transform = transform

    if is_classic_pyspark():
        register_whl_package_extension()

    if is_classic_pyspark() and check_spark_version(less_than="3.4.0"):
        from .extensions import _extract_code_globals, _code_reduce

        pyspark.cloudpickle.cloudpickle._extract_code_globals = _extract_code_globals
        pyspark.cloudpickle.cloudpickle_fast._extract_code_globals = _extract_code_globals
        pyspark.cloudpickle.cloudpickle_fast._code_reduce = _code_reduce
        pyspark.cloudpickle.cloudpickle_fast.CloudPickler.dispatch_table[CodeType] = _code_reduce

    if check_spark_version(greater_than_or_equal="3.5.0") and find_spec("pyarrow") is not None:
        import pyarrow as pa
        import pyspark.sql.pandas.types as pyspark_pandas_types
        from pyspark.sql.pandas.types import from_arrow_type

        def spyt_from_arrow_type(at: "pa.DataType", prefer_timestamp_ntz: bool = False) -> pyspark.sql.types.DataType:
            import pyarrow.types as types
            if types.is_uint64(at):
                return UInt64Type()
            else:
                return from_arrow_type(at, prefer_timestamp_ntz)

        pyspark_pandas_types.from_arrow_type = spyt_from_arrow_type


configure_logging()
initialize()
