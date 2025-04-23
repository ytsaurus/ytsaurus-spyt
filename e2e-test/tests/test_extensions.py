from datetime import date, datetime, timezone

import yt.type_info.typing as ti
import yt.wrapper as yt_wrapper
from common.helpers import assert_items_equal
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from spyt.types import Date32, Datetime64, Timestamp64, Interval64, Datetime, MIN_DATE32, MIN_DATETIME64, \
    MIN_TIMESTAMP64, MIN_INTERVAL64, MAX_DATE32, \
    MAX_DATETIME64, MAX_TIMESTAMP64, MAX_INTERVAL64, YsonType
from yt.wrapper import YtClient
from yt.wrapper.format import RowsIterator
from yt.wrapper.schema import TableSchema


def test_read_schema_hint(yt_client, tmp_dir, local_session: SparkSession):
    table_path = f"{tmp_dir}/read_schema_hint_table"

    yt_schema = yt_wrapper.schema.TableSchema() \
        .add_column("money", ti.Optional[ti.Int32]) \
        .add_column("address", ti.Optional[ti.Yson])

    yt_client.create("table", table_path, attributes={"schema": yt_schema})
    yt_client.write_table(table_path, [
        {
            "money": 100000,
            "address": {
                "street": "123 Main St",
                "zipcode": 12345
            }
        },
        {
            "money": None,
            "address": {
                "street": None,
                "zipcode": 11111
            }
        }
    ])

    reader: DataFrameReader = local_session.read.format("yt")

    read_df: DataFrame = reader.yt(table_path)
    schema_with_schema_hint_without_metadata = StructType([
        StructField(field.name, field.dataType, field.nullable)
        for field in read_df.schema
    ])
    assert schema_with_schema_hint_without_metadata == StructType([
        StructField("money", IntegerType(), nullable=True),
        StructField("address", YsonType(), nullable=True)
    ])

    fields = {
        'money': LongType(),
        'address': {
            'street': StringType(),
            'zipcode': IntegerType()
        }
    }
    reader = reader.schema_hint(fields)
    read_df_with_schema_hint: DataFrame = reader.yt(table_path)
    schema_with_schema_hint_without_metadata = StructType([
        StructField(field.name, field.dataType, field.nullable)
        for field in read_df_with_schema_hint.schema
    ])
    assert schema_with_schema_hint_without_metadata == StructType([
        StructField("money", LongType(), nullable=True),
        StructField("address", StructType([
            StructField("street", StringType(), nullable=True),
            StructField("zipcode", IntegerType(), nullable=True),
        ]), nullable=True)
    ])

    assert_items_equal(read_df_with_schema_hint.collect(), [
        Row(money=100000, address=Row(street="123 Main St", zipcode=12345)),
        Row(money=None, address=Row(street=None, zipcode=11111))
    ])


def test_write_schema_hint(yt_client: YtClient, tmp_dir, local_session: SparkSession):
    table_path = f"{tmp_dir}/write_schema_hint_table"

    columns = [
        "int64", "uint64", "float", "double", "boolean", "string",
        "int8", "uint8",
        "int16", "uint16", "int32", "uint32", "utf8", "date", "datetime",
        "timestamp", "interval", "date32", "datetime64", "timestamp64", "interval64",
        "json", "uuid"
    ]

    spark_rows = [
        Row(
            int64=123456789012345, uint64=123456789012345, float=3.14, double=2.71828,
            boolean=True, string="example",
            int8=127, uint8=255, int16=32767, uint16=65535, int32=2147483647, uint32=4294967295,
            utf8="utf8text",
            date=date.fromisoformat("1970-04-11"), datetime=Datetime(datetime.fromisoformat("1970-04-11T00:00:00")),
            timestamp=datetime(1970, 4, 11, 0, 0, 0, tzinfo=timezone.utc), interval=365,
            date32=Date32(MIN_DATE32), datetime64=Datetime64(MIN_DATETIME64),
            timestamp64=Timestamp64(MIN_TIMESTAMP64), interval64=Interval64(MIN_INTERVAL64),
            json="""{"id": 1, "name": "Ann"}""", uuid="4b336d46-6576-4c7a-5436-4f6847644642"
        ),
        Row(
            int64=987654321098765, uint64=987654321098765, float=1.618, double=1.41421,
            boolean=False, string="another",
            int8=-128, uint8=0, int16=-32768, uint16=0, int32=-2147483648, uint32=0,
            utf8="moretext",
            date=date.fromisoformat("2019-02-09"), datetime=Datetime(datetime.fromisoformat("2019-02-09T13:41:11")),
            timestamp=datetime(2019, 2, 9, 13, 41, 11, tzinfo=timezone.utc), interval=183,
            date32=Date32(MAX_DATE32), datetime64=Datetime64(MAX_DATETIME64),
            timestamp64=Timestamp64(MAX_TIMESTAMP64), interval64=Interval64(MAX_INTERVAL64),
            json="""{"id": 2, "name": "Bob"}""", uuid="4b336d01-6576-4c7a-5436-4f6847644642"
        )
    ]

    yt_rows = [
        {
            "int64": 123456789012345,
            "uint64": 123456789012345,
            "float": 3.14,
            "double": 2.71828,
            "boolean": True,
            "string": "example",
            "int8": 127,
            "uint8": 255,
            "int16": 32767,
            "uint16": 65535,
            "int32": 2147483647,
            "uint32": 4294967295,
            "utf8": "utf8text",
            "date": 100,
            "datetime": 8640000,
            "timestamp": 8640000000000,
            "interval": 365,
            "date32": MIN_DATE32,
            "datetime64": MIN_DATETIME64,
            "timestamp64": MIN_TIMESTAMP64,
            "interval64": MIN_INTERVAL64,
            "json": """{"id": 1, "name": "Ann"}""",
            "uuid":'Fm3KvezLT6OhGdFB',
        },
        {
            "int64": 987654321098765,
            "uint64": 987654321098765,
            "float": 1.618,
            "double": 1.41421,
            "boolean": False,
            "string": "another",
            "int8": -128,
            "uint8": 0,
            "int16": -32768,
            "uint16": 0,
            "int32": -2147483648,
            "uint32": 0,
            "utf8": "moretext",
            "date": 17936,
            "datetime": 1549719671,
            "timestamp": 1549719671000000,
            "interval": 183,
            "date32": MAX_DATE32,
            "datetime64": MAX_DATETIME64,
            "timestamp64": MAX_TIMESTAMP64,
            "interval64": MAX_INTERVAL64,
            "json": """{"id": 2, "name": "Bob"}""",
            "uuid":'\x01m3KvezLT6OhGdFB',
        }
    ]

    df = local_session.createDataFrame(spark_rows, columns)
    writer: DataFrameWriter = df.write.format("yt")
    fields: dict = {yt_type: yt_type for yt_type in columns}
    writer = writer.schema_hint(fields)
    writer.yt(table_path)

    yt_schema: TableSchema = yt_client.get_table_schema(table_path)
    assert list(yt_schema.to_yson_type()) == [{'name': 'int64', 'type_v3': 'int64'},
                                              {'name': 'uint64', 'type_v3': 'uint64'},
                                              {'name': 'float', 'type_v3': 'float'},
                                              {'name': 'double', 'type_v3': 'double'},
                                              {'name': 'boolean', 'type_v3': 'bool'},
                                              {'name': 'string', 'type_v3': 'string'},
                                              {'name': 'int8', 'type_v3': 'int8'},
                                              {'name': 'uint8', 'type_v3': 'uint8'},
                                              {'name': 'int16', 'type_v3': 'int16'},
                                              {'name': 'uint16', 'type_v3': 'uint16'},
                                              {'name': 'int32', 'type_v3': 'int32'},
                                              {'name': 'uint32', 'type_v3': 'uint32'},
                                              {'name': 'utf8', 'type_v3': 'utf8'},
                                              {'name': 'date', 'type_v3': 'date'},
                                              {'name': 'datetime', 'type_v3': 'datetime'},
                                              {'name': 'timestamp', 'type_v3': 'timestamp'},
                                              {'name': 'interval', 'type_v3': 'interval'},
                                              {'name': 'date32', 'type_v3': 'date32'},
                                              {'name': 'datetime64', 'type_v3': 'datetime64'},
                                              {'name': 'timestamp64', 'type_v3': 'timestamp64'},
                                              {'name': 'interval64', 'type_v3': 'interval64'},
                                              {'name': 'json', 'type_v3': 'json'},
                                              {'name': 'uuid', 'type_v3': 'uuid'}]

    result: RowsIterator = yt_client.read_table(table_path)
    assert_items_equal(result, yt_rows)
