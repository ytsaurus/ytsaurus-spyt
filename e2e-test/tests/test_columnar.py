from unittest.mock import Mock

import pytest

from spyt.utils import register_columnar_function


@pytest.fixture
def spark():
    return Mock()


@pytest.mark.parametrize("name, options, expected_sql", [
    (
        "score",
        None,
        "REGISTER COLUMNAR FUNCTION `score` AS 'example.Provider' OPTIONS '{}'",
    ),
    (
        "score`name",
        {"library": "lib'quoted.so", "path": "a\\b"},
        "REGISTER COLUMNAR FUNCTION `score``name` AS 'example.Provider' "
        "OPTIONS '{\"library\": \"lib''quoted.so\", \"path\": \"a\\\\b\"}'",
    ),
])
def test_register_columnar_function(spark, name, options, expected_sql):
    register_columnar_function(spark, name, "example.Provider", options)
    spark.sql.assert_called_once_with(expected_sql)
    spark.sql.return_value.collect.assert_called_once_with()


@pytest.mark.parametrize("name, provider, options", [
    ("", "Provider", {}),
    ("f", "", {}),
    ("f", "Provider", []),
    ("f", "Provider", {"delta": 1}),
    ("f", "Provider", {1: "delta"}),
])
def test_rejects_invalid_arguments(spark, name, provider, options):
    with pytest.raises(ValueError):
        register_columnar_function(spark, name, provider, options)
    spark.sql.assert_not_called()
