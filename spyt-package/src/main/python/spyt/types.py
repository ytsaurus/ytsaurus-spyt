import io
from datetime import datetime, timedelta, timezone
from typing import Union

import yt.yson as yt_yson
from pyspark import SparkContext
from pyspark.sql.column import _to_java_column, Column
from pyspark.sql.types import UserDefinedType, BinaryType, IntegralType, LongType, IntegerType

MIN_DATE32 = -53375809
MAX_DATE32 = 53375807
MIN_DATETIME64 = -4611669897600
MAX_DATETIME64 = 4611669811199
MIN_TIMESTAMP64 = -4611669897600000000
MAX_TIMESTAMP64 = 4611669811199999999
MAX_INTERVAL64 = MAX_TIMESTAMP64 - MIN_TIMESTAMP64
MIN_INTERVAL64 = - MAX_INTERVAL64


class DatetimeType(UserDefinedType):
    def needConversion(self):
        return True

    def serialize(self, obj):
        dt = obj.value
        if dt is not None:
            if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                tz_utc = timezone.utc
                dt = dt.replace(tzinfo=tz_utc)
            return int(dt.timestamp())
        return None

    def deserialize(self, ts):
        if ts is not None:
            return Datetime(datetime.fromtimestamp(ts, timezone.utc).replace(microsecond=0, tzinfo=None))

    @classmethod
    def typeName(cls):
        return "datetime"

    def simpleString(self):
        return 'datetime'

    @classmethod
    def sqlType(cls):
        return LongType()

    @classmethod
    def module(cls):
        return 'spyt.types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.spyt.types.DatetimeType'


class Datetime:
    __UDT__ = DatetimeType()

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "Datetime(%s)" % self.value

    def __str__(self):
        return "(%s)" % self.value

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            other.value == self.value

    @classmethod
    def from_seconds(cls, seconds):
        if seconds is None:
            return None
        dt_value = (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=seconds)) \
            .astimezone(timezone.utc).replace(tzinfo=None)
        return cls(dt_value)


class Date32Type(UserDefinedType):

    def needConversion(self):
        return True

    def serialize(self, obj):
        return int(obj.value) if (obj.value is not None) else None

    def deserialize(self, d):
        if d is not None and MIN_DATE32 <= d <= MAX_DATE32:
            return Date32(d)

    @classmethod
    def typeName(cls):
        return "date32"

    def simpleString(self):
        return 'date32'

    @classmethod
    def sqlType(cls):
        return IntegerType()

    @classmethod
    def module(cls):
        return 'spyt.types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.spyt.types.Date32Type'


class Date32:
    __UDT__ = Date32Type()

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "Date32(%s)" % self.value

    def __str__(self):
        return "(%s)" % self.value

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            other.value == self.value


class Datetime64Type(UserDefinedType):

    def needConversion(self):
        return True

    def serialize(self, obj):
        return int(obj.value) if (obj.value is not None) else None

    def deserialize(self, d):
        if d is not None and MIN_DATETIME64 <= d <= MAX_DATETIME64:
            return Datetime64(d)

    @classmethod
    def typeName(cls):
        return "datetime64"

    def simpleString(self):
        return 'datetime64'

    @classmethod
    def sqlType(cls):
        return LongType()

    @classmethod
    def module(cls):
        return 'spyt.types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.spyt.types.Datetime64Type'


class Datetime64:
    __UDT__ = Datetime64Type()

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "Datetime64(%s)" % self.value

    def __str__(self):
        return "(%s)" % self.value

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            other.value == self.value


class Timestamp64Type(UserDefinedType):

    def needConversion(self):
        return True

    def serialize(self, obj):
        return int(obj.value) if (obj.value is not None) else None

    def deserialize(self, d):
        if d is not None and MIN_TIMESTAMP64 <= d <= MAX_TIMESTAMP64:
            return Timestamp64(d)

    @classmethod
    def typeName(cls):
        return "timestamp64"

    def simpleString(self):
        return 'timestamp64'

    @classmethod
    def sqlType(cls):
        return LongType()

    @classmethod
    def module(cls):
        return 'spyt.types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.spyt.types.Timestamp64Type'


class Timestamp64:
    __UDT__ = Timestamp64Type()

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "Timestamp64(%s)" % self.value

    def __str__(self):
        return "(%s)" % self.value

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            other.value == self.value


class Interval64Type(UserDefinedType):

    def needConversion(self):
        return True

    def serialize(self, obj):
        return int(obj.value) if (obj.value is not None) else None

    def deserialize(self, d):
        if d is not None and MIN_INTERVAL64 <= d <= MAX_INTERVAL64:
            return Interval64(d)

    @classmethod
    def typeName(cls):
        return "interval64"

    def simpleString(self):
        return 'interval64'

    @classmethod
    def sqlType(cls):
        return LongType()

    @classmethod
    def module(cls):
        return 'spyt.types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.spyt.types.Interval64Type'


class Interval64:
    __UDT__ = Interval64Type()

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "Interval64(%s)" % self.value

    def __str__(self):
        return "(%s)" % self.value

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            other.value == self.value


class YsonType(UserDefinedType):
    def needConversion(self):
        return True

    @classmethod
    def typeName(cls):
        return "yson"

    @classmethod
    def module(cls):
        return 'spyt.types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.spyt.types.YsonType'

    def simpleString(self):
        return 'yson'

    @classmethod
    def sqlType(cls):
        return BinaryType()

    def serialize(self, obj):
        return yt_yson.dumps(obj.value, "binary")

    def fromInternal(self, binary_data: bytearray) -> Union[yt_yson.YsonMap, None]:
        if binary_data is None:
            return None
        yson: yt_yson.YsonMap = yt_yson.load(io.BytesIO(binary_data))
        return yson


class Yson:
    __UDT__ = YsonType()

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "Yson(%s)" % self.value

    def __str__(self):
        return "(%s)" % self.value

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            other.value == self.value


UINT64_MAX = 0xffffffffffffffff


class UInt64Type(IntegralType):
    """Unsigned 64-bit integer type
    """

    def simpleString(self):
        return 'uint64'

    @classmethod
    def typeName(cls):
        return "uint64"

    def needConversion(self):
        return True

    def toInternal(self, py_integer):
        if py_integer is None:
            return None
        if py_integer < 0 or py_integer > UINT64_MAX:
            raise ValueError(f"object of UInt64Type out of range, got: {py_integer}")
        return py_integer if py_integer <= 0x7fffffffffffffff else py_integer - UINT64_MAX - 1

    def fromInternal(self, j_long):
        return j_long if j_long is None or j_long >= 0 else (j_long & UINT64_MAX)


def uint64_to_string(number):
    if number is None:
        return None
    else:
        # convert to unsigned value
        return str(number & UINT64_MAX)


def string_to_uint64(number):
    if number is None:
        return None
    else:
        return int(number)


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


def tuple_type(element_types):
    """
    :param element_types: List[DataType]
    :return: StructType
    """
    from pyspark.sql.types import StructType, StructField
    struct_fields = [StructField("_{}".format(i + 1), element_type) for i, element_type in enumerate(element_types)]
    return StructType(struct_fields)


def variant_over_struct_type(elements):
    """
    :param elements: List[Tuple[str, DataType]]
    :return: StructType
    """
    from pyspark.sql.types import StructType, StructField
    struct_fields = [StructField("_v{}".format(element_name), element_type) for element_name, element_type in elements]
    return StructType(struct_fields)


def variant_over_tuple_type(element_types):
    """
    :param element_types: List[DataType]
    :return: StructType
    """
    elements = [("_{}".format(i + 1), element_type) for i, element_type in enumerate(element_types)]
    return variant_over_struct_type(elements)
