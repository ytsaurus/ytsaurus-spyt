import pytest
from pyspark.sql import Row
from py4j.protocol import Py4JJavaError

spark_conf_distributed_enabled = {
    "spark.yt.read.ytDistributedReading.enabled": "true"
}

spark_conf_distributed_disabled = {
    "spark.yt.read.ytDistributedReading.enabled": "false"
}

spark_conf_partitioning_disabled = spark_conf_distributed_disabled | {
    "spark.yt.read.ytPartitioning.enabled": "false"
}

spark_conf_omit_rows_disabled = {
    "spark.yt.read.ytOmitInaccessibleRows.enabled": "false"
}

spark_conf_omit_columns_disabled = {
    "spark.yt.read.ytOmitInaccessibleColumns.enabled": "false"
}


def create_table_with_acl(yt_client, table_path, acl_rules, optimize_for: str = "lookup"):
    yt_client.create(
        "table",
        table_path,
        attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"},
            ],
            "optimize_for": optimize_for,
            "acl": acl_rules,
        },
    )

    yt_client.write_table(
        table_path,
        [
            {"key": i, "value": str(i)}
            for i in range(5)
        ]
    )


def create_rls_table(yt_client, table_path, user_name, optimize_for: str = "lookup"):
    acl_rules = [
        {
            "action": "allow",
            "subjects": [user_name],
            "permissions": ["read"],
            "row_access_predicate": "key in (3, 4)"
        },
    ]
    create_table_with_acl(yt_client, table_path, acl_rules, optimize_for)


def create_cls_table(yt_client, table_path, user_name, optimize_for: str = "lookup"):
    acl_rules = [
        {
            "action": "deny",
            "subjects": [user_name],
            "permissions": ["read"],
            "columns": ["value"]
        }
    ]
    create_table_with_acl(yt_client, table_path, acl_rules, optimize_for)


def setup_table(tmp_dir, yt_client, local_session_with_user, table_creator, optimize_for: str):
    user_name = local_session_with_user.conf.get("spark.hadoop.yt.user")
    table_path = f"{tmp_dir}/test_table"
    table_creator(yt_client, table_path, user_name, optimize_for)
    return table_path


# TODO: resolve rowCount meta issue for RLS tables  SPYT-988 (keepling)
@pytest.mark.parametrize("optimize_for", ["lookup", ]) #"scan"])
@pytest.mark.parametrize(
    "local_session_with_user",
    [
        pytest.param(spark_conf_partitioning_disabled, id="partitioning_disabled"),
        pytest.param(spark_conf_distributed_disabled, id="distributed_disabled"),
        pytest.param(spark_conf_distributed_enabled, id="distributed_enabled"),
    ],
    indirect=True
)
def test_rls_success(tmp_dir, yt_client, local_session_with_user, optimize_for):
    table_path = setup_table(tmp_dir, yt_client, local_session_with_user, create_rls_table, optimize_for)

    available_rows = [
        Row(key=3, value="3"),
        Row(key=4, value="4")
    ]

    result = local_session_with_user.read.yt(table_path).collect()
    assert list(result) == available_rows


@pytest.mark.skip(reason="TODO: fix CLS SPYT-968/SPYT-969")
@pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
@pytest.mark.parametrize(
    "local_session_with_user",
    [
        pytest.param(spark_conf_partitioning_disabled, id="partitioning_disabled"),
        # TODO: add for partition_tables based methods SPYT-968 (keepling)
        pytest.param(spark_conf_distributed_disabled, id="distributed_disabled"),
        pytest.param(spark_conf_distributed_enabled, id="distributed_enabled"),
    ],
    indirect=True
)
def test_cls_success(tmp_dir, yt_client, local_session_with_user, optimize_for):
    table_path = setup_table(tmp_dir, yt_client, local_session_with_user, create_cls_table, optimize_for)

    # TODO: fix schema resolution for CLS SPYT-969 (keepling)
    # now it returns restricted column with empty values
    expected_rows = [Row(key=i) for i in range(5)]

    result = local_session_with_user.read.yt(table_path).collect()
    assert list(result) == expected_rows


# TODO: resolve rowCount meta issue for RLS tables  SPYT-988 (keepling)
@pytest.mark.parametrize("optimize_for", ["lookup", ]) #"scan"])
@pytest.mark.parametrize(
    "local_session_with_user, table_creator",
    [
        pytest.param(spark_conf_omit_rows_disabled, create_rls_table, id="rls_omit_disabled"),
        pytest.param(spark_conf_omit_columns_disabled, create_cls_table, id="cls_omit_disabled"),
    ],
    indirect=["local_session_with_user"]
)
def test_omit_disabled_raises_exception(tmp_dir, yt_client, local_session_with_user, table_creator, optimize_for):
    table_path = setup_table(tmp_dir, yt_client, local_session_with_user, table_creator, optimize_for)
    with pytest.raises(Exception) as exc_info:
        local_session_with_user.read.yt(table_path).collect()

    error_message = str(exc_info.value).lower()
    assert "access denied for user" in error_message, \
        f"Expected 'access denied for user' in error, but got: {error_message}"
