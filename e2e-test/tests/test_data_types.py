import spyt

from common.helpers import assert_items_equal
from pyspark.sql import Row


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
