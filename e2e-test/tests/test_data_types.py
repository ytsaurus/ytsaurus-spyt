from common.helpers import assert_items_equal
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType
from spyt.types import Date32, Datetime64, Timestamp64, Interval64, Date32Type, Datetime64Type, Timestamp64Type, \
    Interval64Type, Datetime, DatetimeType, MIN_DATE32, MIN_DATETIME64, MIN_TIMESTAMP64, MIN_INTERVAL64, MAX_DATE32, \
    MAX_DATETIME64, MAX_TIMESTAMP64, MAX_INTERVAL64, YsonType, Yson
from yt.wrapper.format import RowsIterator

yt_wide_types_rows = [
    {"date32": MIN_DATE32, "datetime64": MIN_DATETIME64, "timestamp64": MIN_TIMESTAMP64,
     "interval64": MIN_INTERVAL64},
    {"date32": 0, "datetime64": 0, "timestamp64": 0, "interval64": 0},
    {"date32": MAX_DATE32, "datetime64": MAX_DATETIME64, "timestamp64": MAX_TIMESTAMP64,
     "interval64": MAX_INTERVAL64},
    {"date32": None, "datetime64": None, "timestamp64": None, "interval64": None}
]
spark_wide_types_rows = [
    Row(date32=Date32(MIN_DATE32), datetime64=Datetime64(MIN_DATETIME64), timestamp64=Timestamp64(MIN_TIMESTAMP64),
        interval64=Interval64(MIN_INTERVAL64)),
    Row(date32=Date32(0), datetime64=Datetime64(0), timestamp64=Timestamp64(0), interval64=Interval64(0)),
    Row(date32=Date32(MAX_DATE32), datetime64=Datetime64(MAX_DATETIME64), timestamp64=Timestamp64(MAX_TIMESTAMP64),
        interval64=Interval64(MAX_INTERVAL64)),
    Row(date32=None, datetime64=None, timestamp64=None, interval64=None)
]

yt_datetime_type_rows = [
    {"datetime": 8640000},
    {"datetime": 1549719671},
    {"datetime": 0},
    {"datetime": None},
]
spark_datetime_type_rows = [
    Row(datetime=Datetime.from_seconds(8640000)),
    Row(datetime=Datetime.from_seconds(1549719671)),
    Row(datetime=Datetime.from_seconds(0)),
    Row(datetime=None)
]

yt_yson_rows = [
    {"yson": {"string": "string1", "int": 1234567890}},
    {"yson": {"string": "string2", "short": 321, "long": 6347568734657887}},
    {"yson": None}
]
spark_yson_type_schema = StructType([
    StructField("yson", YsonType(), nullable=True)
])


def test_read_uint64_type(yt_client, tmp_dir, local_session):
    table_path = f"{tmp_dir}/uint64_table_in"
    yt_client.create("table", table_path, attributes={"schema": [
        {"name": "id", "type": "uint64", "nullable": True},
        {"name": "value", "type": "string"}
    ]})
    rows = [
        {"id": 1, "value": "value 1"},
        {"id": 2, "value": "value 2"},
        {"id": 3, "value": "value 3"},
        {"id": 9223372036854775816, "value": "value 4"},
        {"id": 9223372036854775813, "value": "value 5"},
        {"id": 18446744073709551615, "value": "value 6"},
        {"id": None, "value": "value 7"}
    ]
    yt_client.write_table(table_path, rows)

    df = local_session.read.yt(table_path)
    result = df.collect()

    assert_items_equal(result, [
        Row(id=1, value="value 1"),
        Row(id=2, value="value 2"),
        Row(id=3, value="value 3"),
        Row(id=9223372036854775816, value="value 4"),
        Row(id=9223372036854775813, value="value 5"),
        Row(id=18446744073709551615, value="value 6"),
        Row(id=None, value="value 7")
    ])


def test_join_tables_with_uint64_type(yt_client, tmp_dir, local_session):
    table_1_path = f"{tmp_dir}/uint64_table_1"
    table_2_path = f"{tmp_dir}/uint64_table_2"
    joined_path = f"{tmp_dir}/uint64_joined"

    yt_client.create("table", table_1_path, attributes={"schema": [
        {"name": "id", "type": "int64"},
        {"name": "data", "type": "uint64"},
        {"name": "value", "type": "string"}
    ]})

    rows_1 = [
        {"id": 1, "data": 1, "value": "value 1"},
        {"id": 3, "data": 2, "value": "value 2"},
        {"id": 5, "data": 3, "value": "value 3"},
        {"id": 7, "data": 9223372036854775816, "value": "value 4"},
        {"id": 8, "data": 9223372036854775813, "value": "value 5"},
        {"id": 9, "data": 18446744073709551615, "value": "value 6"}
    ]

    yt_client.write_table(table_1_path, rows_1)

    yt_client.create("table", table_2_path, attributes={"schema": [
        {"name": "id", "type": "int64"},
        {"name": "extra_data", "type": "uint64"}
    ]})

    rows_2 = [
        {"id": 1, "extra_data": 1},
        {"id": 4, "extra_data": 2},
        {"id": 7, "extra_data": 18446744073709551615},
        {"id": 9, "extra_data": 9223372036854775816},
    ]

    yt_client.write_table(table_2_path, rows_2)

    df_table_1 = local_session.read.yt(table_1_path)
    df_table_2 = local_session.read.yt(table_2_path)
    df_joined = df_table_1.join(
        df_table_2,
        on=[df_table_1.id == df_table_2.id],
        how="inner"
    ).select(
        df_table_1.id,
        df_table_1.data,
        df_table_1.value,
        df_table_2.extra_data
    )
    df_joined.write.mode("overwrite").optimize_for("scan").yt(joined_path)

    result = yt_client.read_table(joined_path)
    rows_result = [
        {"id": 1, "data": 1, "value": "value 1", "extra_data": 1},
        {"id": 7, "data": 9223372036854775816, "value": "value 4", "extra_data": 18446744073709551615},
        {"id": 9, "data": 18446744073709551615, "value": "value 6", "extra_data": 9223372036854775816}
    ]
    assert_items_equal(result, rows_result)


def test_write_uint64_type(yt_client, tmp_dir, local_session):
    table_path = f"{tmp_dir}/uint64_table_out"

    rows = [
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 9223372036854775813),
        (5, 9223372036854775816),
        (6, 18446744073709551615),
        (7, None)
    ]
    df = local_session.createDataFrame(rows, "id int, value uint64")
    df.write.mode("overwrite").optimize_for("scan").yt(table_path)

    result = yt_client.read_table(table_path)
    rows_result = [
        {"id": 1, "value": 1},
        {"id": 2, "value": 2},
        {"id": 3, "value": 3},
        {"id": 4, "value": 9223372036854775813},
        {"id": 5, "value": 9223372036854775816},
        {"id": 6, "value": 18446744073709551615},
        {"id": 7, "value": None}
    ]
    assert_items_equal(result, rows_result)


def test_read_wide_types(yt_client, tmp_dir, local_session):
    table_path = f"{tmp_dir}/wide_types_table_in"
    yt_client.create("table", table_path, attributes={"schema": [
        {"name": "date32", "type": "date32", "nullable": True},
        {"name": "datetime64", "type": "datetime64", "nullable": True},
        {"name": "timestamp64", "type": "timestamp64", "nullable": True},
        {"name": "interval64", "type": "interval64", "nullable": True},
    ]})
    yt_client.write_table(table_path, yt_wide_types_rows)

    df = local_session.read.yt(table_path)
    result = df.collect()
    assert_items_equal(result, spark_wide_types_rows)


def test_write_wide_types(yt_client, tmp_dir, local_session):
    table_path = f"{tmp_dir}/wide_types_table_out"
    schema = StructType([
        StructField("date32", Date32Type(), True),
        StructField("datetime64", Datetime64Type(), True),
        StructField("timestamp64", Timestamp64Type(), True),
        StructField("interval64", Interval64Type(), True)
    ])
    df = local_session.createDataFrame(data=spark_wide_types_rows, schema=schema)
    df.write.mode("overwrite").optimize_for("scan").yt(table_path)

    result = yt_client.read_table(table_path)
    assert_items_equal(result, yt_wide_types_rows)


def test_read_datetime_type(yt_client, tmp_dir, local_session):
    table_path = f"{tmp_dir}/datetime_type_table_in"
    yt_client.create("table", table_path, attributes={"schema": [
        {"name": "datetime", "type": "datetime", "nullable": True},
    ]})
    yt_client.write_table(table_path, yt_datetime_type_rows)

    df = local_session.read.yt(table_path)
    result = df.collect()
    assert_items_equal(result, spark_datetime_type_rows)


def test_write_datetime_type(yt_client, tmp_dir, local_session):
    table_path = f"{tmp_dir}/datetime_type_table_out"
    schema = StructType([
        StructField("datetime", DatetimeType(), True),
    ])
    df = local_session.createDataFrame(data=spark_datetime_type_rows, schema=schema)
    df.write.mode("overwrite").optimize_for("scan").yt(table_path)

    result = yt_client.read_table(table_path)
    assert_items_equal(result, yt_datetime_type_rows)


def test_read_yson_type(yt_client, tmp_dir, local_session):
    table_path = f"{tmp_dir}/yson_table_in"
    schema = [{"name": "yson", "type_v3": {"type_name": "optional", "item": "yson"}}]
    yt_client.create("table", table_path, attributes={"schema": schema})
    yt_client.write_table(table_path, yt_yson_rows)

    def get_string_column(yson):
        return str(yson["string"]) if yson is not None else None

    get_string_column_udf = udf(get_string_column, StringType())

    df = local_session.read.yt(table_path)
    parsed_df = df.withColumn('parsed_string', get_string_column_udf('yson')).select("parsed_string")
    result = parsed_df.collect()

    assert parsed_df.schema == StructType([
        StructField("parsed_string", StringType(), nullable=True)
    ])

    assert_items_equal(result, [
        Row(parsed_string="string1"),
        Row(parsed_string="string2"),
        Row(parsed_string=None)
    ])


def test_write_yson_type(yt_client, tmp_dir, local_session):
    table_path = f"{tmp_dir}/yson_table_out"

    data = [
        Row(yson=Yson(dict(string="string1", int=1234567890))),
        Row(yson=Yson(dict(string="string2", short=321, long=6347568734657887))),
        Row(yson=Yson(None))
    ]
    df = local_session.createDataFrame(data=data, schema=spark_yson_type_schema)
    df.write.mode("overwrite").optimize_for("scan").yt(table_path)

    result: RowsIterator = yt_client.read_table(table_path)
    assert_items_equal(result, yt_yson_rows)
