# type: ignore
import typing as t

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.starrocks]


@pytest.fixture
def adapter(make_mocked_engine_adapter: t.Callable) -> StarRocksEngineAdapter:
    return make_mocked_engine_adapter(StarRocksEngineAdapter)


def test_comments(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)

    allowed_table_comment_length = StarRocksEngineAdapter.MAX_TABLE_COMMENT_LENGTH
    truncated_table_comment = "a" * allowed_table_comment_length
    long_table_comment = truncated_table_comment + "b"

    allowed_column_comment_length = StarRocksEngineAdapter.MAX_COLUMN_COMMENT_LENGTH
    truncated_column_comment = "c" * allowed_column_comment_length
    long_column_comment = truncated_column_comment + "d"

    fetchone_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.starrocks.StarRocksEngineAdapter.fetchone"
    )
    fetchone_mock.return_value = ["test_table", "CREATE TABLE test_table (a INT)"]

    adapter.create_table(
        "test_table",
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description=long_table_comment,
        column_descriptions={"a": long_column_comment},
    )

    adapter.ctas(
        "test_table",
        parse_one("SELECT a, b FROM source_table"),
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description=long_table_comment,
        column_descriptions={"a": long_column_comment},
    )

    sql_calls = to_sql_calls(adapter)
    # StarRocks supports COMMENT in CREATE TABLE and in CTAS (with table comment)
    # Column comments after CTAS are added via ALTER TABLE
    assert sql_calls == [
        f"CREATE TABLE IF NOT EXISTS `test_table` (`a` INT COMMENT '{truncated_column_comment}', `b` INT) COMMENT '{truncated_table_comment}'",
        f"CREATE TABLE IF NOT EXISTS `test_table` COMMENT '{truncated_table_comment}' AS SELECT CAST(`a` AS INT) AS `a`, CAST(`b` AS INT) AS `b` FROM (SELECT `a`, `b` FROM `source_table`) AS `_subquery`",
        f"ALTER TABLE `test_table` MODIFY `a` INT COMMENT '{truncated_column_comment}'",
    ]


def test_create_schema(adapter: StarRocksEngineAdapter):
    """StarRocks uses DATABASE instead of SCHEMA"""
    adapter.create_schema("test_db")

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == ["CREATE DATABASE IF NOT EXISTS `test_db`"]


def test_drop_schema(adapter: StarRocksEngineAdapter):
    """StarRocks uses DATABASE instead of SCHEMA"""
    adapter.drop_schema("test_db")

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == ["DROP DATABASE IF EXISTS `test_db`"]


def test_insert_overwrite_with_dynamic_overwrite_enabled(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    """When dynamic_overwrite is enabled, use INSERT OVERWRITE strategy"""
    adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)

    # Mock _is_dynamic_overwrite_enabled to return True
    mocker.patch.object(adapter, "_is_dynamic_overwrite_enabled", return_value=True)

    adapter._insert_overwrite_by_condition(
        table_name="test_table",
        source_queries=[],
        target_columns_to_types={"a": exp.DataType.build("INT")},
    )

    # Verify the adapter checked for dynamic_overwrite
    adapter._is_dynamic_overwrite_enabled.assert_called_once()


def test_insert_overwrite_with_dynamic_overwrite_disabled(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    """When dynamic_overwrite is disabled, use DELETE_INSERT strategy (default)"""
    adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)

    # Mock _is_dynamic_overwrite_enabled to return False
    mocker.patch.object(adapter, "_is_dynamic_overwrite_enabled", return_value=False)

    adapter._insert_overwrite_by_condition(
        table_name="test_table",
        source_queries=[],
        target_columns_to_types={"a": exp.DataType.build("INT")},
    )

    # Verify the adapter checked for dynamic_overwrite
    adapter._is_dynamic_overwrite_enabled.assert_called_once()


def test_is_dynamic_overwrite_enabled_true(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    """Test that dynamic_overwrite detection works when enabled"""
    adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)

    fetchone_mock = mocker.patch.object(adapter, "fetchone", return_value=(1,))

    # Clear the lru_cache to ensure fresh call
    adapter._is_dynamic_overwrite_enabled.cache_clear()

    result = adapter._is_dynamic_overwrite_enabled()

    assert result is True
    fetchone_mock.assert_called_once_with("SELECT @@dynamic_overwrite")


def test_is_dynamic_overwrite_enabled_false(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    """Test that dynamic_overwrite detection works when disabled"""
    adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)

    fetchone_mock = mocker.patch.object(adapter, "fetchone", return_value=(0,))

    # Clear the lru_cache to ensure fresh call
    adapter._is_dynamic_overwrite_enabled.cache_clear()

    result = adapter._is_dynamic_overwrite_enabled()

    assert result is False
    fetchone_mock.assert_called_once_with("SELECT @@dynamic_overwrite")


def test_is_dynamic_overwrite_enabled_exception(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    """Test that dynamic_overwrite detection handles exceptions gracefully"""
    adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)

    fetchone_mock = mocker.patch.object(adapter, "fetchone", side_effect=Exception("Query failed"))

    # Clear the lru_cache to ensure fresh call
    adapter._is_dynamic_overwrite_enabled.cache_clear()

    result = adapter._is_dynamic_overwrite_enabled()

    assert result is False
    fetchone_mock.assert_called_once_with("SELECT @@dynamic_overwrite")


def test_adapter_settings():
    """Test that adapter class settings are correct"""
    assert StarRocksEngineAdapter.DIALECT == "starrocks"
    assert StarRocksEngineAdapter.DEFAULT_BATCH_SIZE == 10000
    assert StarRocksEngineAdapter.SUPPORTS_TRANSACTIONS is False
    assert StarRocksEngineAdapter.SUPPORTS_INDEXES is True
    assert StarRocksEngineAdapter.SUPPORTS_MATERIALIZED_VIEWS is False
    assert StarRocksEngineAdapter.MAX_TABLE_COMMENT_LENGTH == 1024
    assert StarRocksEngineAdapter.MAX_COLUMN_COMMENT_LENGTH == 1024
    assert StarRocksEngineAdapter.MAX_IDENTIFIER_LENGTH == 256
