import typing as t

import pytest
import sqlglot
from pytest_mock import MockerFixture
from sqlglot import exp

from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
    DataObjectType,
    InsertOverwriteStrategy,
)
from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.starrocks]


@pytest.fixture
def adapter(make_mocked_engine_adapter: t.Callable) -> StarRocksEngineAdapter:
    return make_mocked_engine_adapter(StarRocksEngineAdapter)


def test_adapter_settings(adapter: StarRocksEngineAdapter):
    """Test that StarRocks-specific adapter class settings are correct."""
    assert StarRocksEngineAdapter.DIALECT == "starrocks"
    assert StarRocksEngineAdapter.DEFAULT_BATCH_SIZE == 10000
    assert StarRocksEngineAdapter.SUPPORTS_TRANSACTIONS is False
    assert StarRocksEngineAdapter.SUPPORTS_INDEXES is True
    assert StarRocksEngineAdapter.SUPPORTS_REPLACE_TABLE is False
    assert StarRocksEngineAdapter.SUPPORTS_MATERIALIZED_VIEWS is False
    assert StarRocksEngineAdapter.SUPPORTS_TUPLE_IN is False
    assert StarRocksEngineAdapter.MAX_TABLE_COMMENT_LENGTH == 1024
    assert StarRocksEngineAdapter.MAX_COLUMN_COMMENT_LENGTH == 1024
    assert StarRocksEngineAdapter.SUPPORTS_QUERY_EXECUTION_TRACKING is True
    assert StarRocksEngineAdapter.MAX_IDENTIFIER_LENGTH == 256
    assert StarRocksEngineAdapter.SUPPORTS_GRANTS is True
    assert StarRocksEngineAdapter.VIEW_SUPPORTED_PRIVILEGES == frozenset({"SELECT"})
    assert (
        StarRocksEngineAdapter.COMMENT_CREATION_TABLE == CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    )
    assert (
        StarRocksEngineAdapter.COMMENT_CREATION_VIEW
        == CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS
    )
    assert (
        StarRocksEngineAdapter.INSERT_OVERWRITE_STRATEGY == InsertOverwriteStrategy.INSERT_OVERWRITE
    )
    assert adapter.catalog_support == CatalogSupport.SINGLE_CATALOG_ONLY


def test_ping(adapter: StarRocksEngineAdapter):
    """Test ping functionality."""
    adapter.ping()
    adapter._connection_pool.get().ping.assert_called_once_with(reconnect=False)


@pytest.mark.parametrize(
    "method,schema,expected_sql",
    [
        ("create_schema", "test_db", "CREATE DATABASE IF NOT EXISTS `test_db`"),
        ("drop_schema", "test_db", "DROP DATABASE IF EXISTS `test_db`"),
    ],
)
def test_schema_operations(
    adapter: StarRocksEngineAdapter, method: str, schema: str, expected_sql: str
):
    """StarRocks uses DATABASE instead of SCHEMA."""
    getattr(adapter, method)(schema)
    assert to_sql_calls(adapter) == [expected_sql]


@pytest.mark.parametrize(
    "target,source,exists,expected_sql",
    [
        (
            "target_table",
            "source_table",
            True,
            "CREATE TABLE IF NOT EXISTS `target_table` LIKE `source_table`",
        ),
        (
            "test_db.target_table",
            "test_db.source_table",
            True,
            "CREATE TABLE IF NOT EXISTS `test_db`.`target_table` LIKE `test_db`.`source_table`",
        ),
        ("target_table", "source_table", False, "CREATE TABLE `target_table` LIKE `source_table`"),
    ],
)
def test_create_table_like(
    adapter: StarRocksEngineAdapter, target: str, source: str, exists: bool, expected_sql: str
):
    """Test CREATE TABLE LIKE with various configurations."""
    if exists:
        adapter.create_table_like(target, source)
    else:
        adapter._create_table_like(target, source, exists=False)
    assert to_sql_calls(adapter) == [expected_sql]


@pytest.mark.parametrize(
    "kwargs,expected_fragments",
    [
        ({}, None),  # Empty properties returns None
        ({"table_description": "Test description"}, ["COMMENT", "Test description"]),
        ({"partitioned_by": [exp.to_column("ds")]}, ["PARTITION BY"]),
        (
            {
                "table_properties": {"primary_key": exp.Tuple(expressions=[exp.to_column("id")])},
            },
            ["PRIMARY KEY"],
        ),
        # HASH with named params (columns + buckets)
        (
            {
                "table_properties": {
                    "distributed_by": sqlglot.parse_one(
                        "HASH(columns := (id), buckets := 10)", dialect="starrocks"
                    ),
                }
            },
            ["DISTRIBUTED BY HASH", "BUCKETS 10"],
        ),
        # HASH with multiple columns
        (
            {
                "table_properties": {
                    "distributed_by": sqlglot.parse_one(
                        "HASH(columns := (col1, col2), buckets := 16)", dialect="starrocks"
                    ),
                }
            },
            ["DISTRIBUTED BY HASH", "col1", "col2", "BUCKETS 16"],
        ),
        # HASH without buckets
        (
            {
                "table_properties": {
                    "distributed_by": sqlglot.parse_one(
                        "HASH(columns := (id))", dialect="starrocks"
                    ),
                }
            },
            ["DISTRIBUTED BY HASH"],
        ),
        # RANDOM with buckets
        (
            {
                "table_properties": {
                    "distributed_by": sqlglot.parse_one(
                        "RANDOM(buckets := 5)", dialect="starrocks"
                    ),
                }
            },
            ["DISTRIBUTED BY RANDOM", "BUCKETS 5"],
        ),
        # RANDOM without buckets
        (
            {
                "table_properties": {
                    "distributed_by": sqlglot.parse_one("RANDOM()", dialect="starrocks"),
                }
            },
            ["DISTRIBUTED BY RANDOM"],
        ),
        (
            {
                "table_properties": {
                    "order_by": exp.Tuple(
                        expressions=[exp.to_column("col1"), exp.to_column("col2")]
                    )
                }
            },
            ["ORDER BY"],
        ),
        # Full example
        (
            {
                "table_description": "Test table",
                "partitioned_by": [exp.to_column("ds")],
                "table_properties": {
                    "primary_key": exp.Tuple(expressions=[exp.to_column("id")]),
                    "distributed_by": sqlglot.parse_one(
                        "HASH(columns := id, buckets := 10)", dialect="starrocks"
                    ),
                },
            },
            ["PRIMARY KEY", "COMMENT", "PARTITION BY", "DISTRIBUTED BY HASH", "BUCKETS 10"],
        ),
    ],
)
def test_build_table_properties_exp(
    adapter: StarRocksEngineAdapter, kwargs: dict, expected_fragments: t.Optional[t.List[str]]
):
    """Test _build_table_properties_exp with various configurations."""
    result = adapter._build_table_properties_exp(**kwargs)
    if expected_fragments is None:
        assert result is None
    else:
        assert result is not None
        sql = result.sql(dialect="starrocks")
        for fragment in expected_fragments:
            assert fragment in sql


@pytest.mark.parametrize(
    "primary_key_expr",
    [
        exp.Tuple(expressions=[exp.to_column("id")]),
        exp.Tuple(expressions=[exp.to_column("id"), exp.to_column("name")]),
        exp.Array(expressions=[exp.to_column("col1")]),
    ],
)
def test_build_primary_key_property(adapter: StarRocksEngineAdapter, primary_key_expr):
    """Test _build_primary_key_property for primary key expressions."""
    result = adapter._build_primary_key_property(primary_key_expr)
    assert isinstance(result, exp.PrimaryKey)
    assert "PRIMARY KEY" in result.sql(dialect="starrocks")


def test_build_table_description_property(adapter: StarRocksEngineAdapter):
    """Test _build_table_description_property."""
    result = adapter._build_table_description_property("Test comment")
    assert isinstance(result, exp.SchemaCommentProperty)
    sql = result.sql(dialect="starrocks")
    assert "COMMENT" in sql and "Test comment" in sql


def test_build_partitioned_by_exp(adapter: StarRocksEngineAdapter):
    """Test _build_partitioned_by_exp."""
    result = adapter._build_partitioned_by_exp([exp.to_column("ds"), exp.to_column("region")])
    assert isinstance(result, exp.PartitionedByProperty)
    assert "PARTITION BY" in result.sql(dialect="starrocks")


@pytest.mark.parametrize(
    "distributed_by_expr,expected_sql",
    [
        # HASH with columns and buckets
        (
            sqlglot.parse_one("HASH(columns := (id, name), buckets := 10)", dialect="starrocks"),
            "DISTRIBUTED BY HASH (id, name) BUCKETS 10",
        ),
        # HASH with single column
        (
            sqlglot.parse_one("HASH(columns := id, buckets := 16)", dialect="starrocks"),
            "DISTRIBUTED BY HASH (id) BUCKETS 16",
        ),
        # HASH without buckets
        (
            sqlglot.parse_one("HASH(columns := (col1, col2))", dialect="starrocks"),
            "DISTRIBUTED BY HASH (col1, col2)",
        ),
        # RANDOM with buckets
        (
            sqlglot.parse_one("RANDOM(buckets := 5)", dialect="starrocks"),
            "DISTRIBUTED BY RANDOM BUCKETS 5",
        ),
        # RANDOM without buckets
        (
            sqlglot.parse_one("RANDOM()", dialect="starrocks"),
            "DISTRIBUTED BY RANDOM",
        ),
    ],
)
def test_build_distributed_by_property(
    adapter: StarRocksEngineAdapter, distributed_by_expr, expected_sql
):
    """Test _build_distributed_by_property with HASH and RANDOM."""
    result = adapter._build_distributed_by_property(distributed_by_expr)
    assert isinstance(result, exp.DistributedByProperty)
    assert result.sql(dialect="starrocks") == expected_sql


@pytest.mark.parametrize(
    "order_by_expr",
    [
        exp.Tuple(expressions=[exp.to_column("col1"), exp.to_column("col2")]),
        exp.to_column("col1"),
    ],
)
def test_build_order_by_property(adapter: StarRocksEngineAdapter, order_by_expr):
    """Test _build_order_by_property with single and multiple columns."""
    result = adapter._build_order_by_property(order_by_expr)
    assert isinstance(result, exp.Order)
    assert "ORDER BY" in result.sql(dialect="starrocks")


def test_dcl_grants_config_expr_strips_catalog(adapter: StarRocksEngineAdapter):
    """Test _dcl_grants_config_expr strips catalog from table reference.

    StarRocks does not support catalog names in GRANT statements, so the adapter
    should strip the catalog before generating the GRANT SQL.
    """
    # Create a table with catalog
    table = exp.to_table("default_catalog.test_db.test_table")
    grants_config = {"SELECT": ["user1"]}

    expressions = adapter._apply_grants_config_expr(table, grants_config, DataObjectType.TABLE)

    assert len(expressions) == 1

    # Check that the catalog is NOT in the generated SQL
    sql = expressions[0].sql(dialect="starrocks")
    assert "default_catalog" not in sql
    assert "test_db.test_table" in sql or "`test_db`.`test_table`" in sql


def test_get_current_grants_config_parses_starrocks_format(
    adapter: StarRocksEngineAdapter, mocker: MockerFixture
):
    """Test _get_current_grants_config handles StarRocks-specific grantee format.

    StarRocks returns grantees in "'username'@'host'" format and may return
    comma-separated privileges. This tests the StarRocks-specific parsing logic.
    """
    # StarRocks returns privileges as comma-separated string and grantee in 'user'@'host' format
    mocker.patch.object(adapter, "fetchall", return_value=[("INSERT, SELECT", "'user1'@'%'")])

    table = exp.to_table("test_db.test_table")
    grants_config = adapter._get_current_grants_config(table)

    assert grants_config == {"INSERT": ["user1"], "SELECT": ["user1"]}
