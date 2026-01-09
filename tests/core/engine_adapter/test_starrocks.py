import typing as t

import pytest
from sqlglot import exp

from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
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
    assert StarRocksEngineAdapter.SUPPORTS_INDEXES is False
    assert StarRocksEngineAdapter.SUPPORTS_REPLACE_TABLE is False
    assert StarRocksEngineAdapter.SUPPORTS_MATERIALIZED_VIEWS is False
    assert StarRocksEngineAdapter.MAX_TABLE_COMMENT_LENGTH == 1024
    assert StarRocksEngineAdapter.MAX_COLUMN_COMMENT_LENGTH == 1024
    assert StarRocksEngineAdapter.MAX_IDENTIFIER_LENGTH == 256
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
        ({"storage_format": "olap"}, ["ENGINE=olap"]),
        ({"table_description": "Test description"}, ["COMMENT", "Test description"]),
        ({"partitioned_by": [exp.to_column("ds")]}, ["PARTITION BY"]),
        (
            {
                "table_format": "DUPLICATE",
                "table_properties": {"key_columns": exp.Tuple(expressions=[exp.to_column("id")])},
            },
            ["DUPLICATE KEY"],
        ),
        (
            {
                "table_properties": {
                    "distributed_by": exp.Tuple(expressions=[exp.to_column("id")]),
                    "buckets": exp.Literal.number(10),
                }
            },
            ["DISTRIBUTED BY HASH", "BUCKETS 10"],
        ),
        (
            {"table_properties": {"buckets": exp.Literal.number(5)}},
            ["DISTRIBUTED BY RANDOM", "BUCKETS 5"],
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
        (
            {
                "storage_format": "OLAP",
                "table_format": "DUPLICATE",
                "table_description": "Test table",
                "partitioned_by": [exp.to_column("ds")],
                "table_properties": {
                    "key_columns": exp.to_column("id"),
                    "distributed_by": exp.to_column("id"),
                    "buckets": exp.Literal.number(10),
                },
            },
            ["ENGINE=OLAP", "DUPLICATE KEY", "COMMENT", "PARTITION BY", "DISTRIBUTED BY"],
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


def test_build_table_properties_exp_invalid_table_type(adapter: StarRocksEngineAdapter):
    """Test that invalid table type raises error."""
    from sqlmesh.utils.errors import SQLMeshError

    with pytest.raises(SQLMeshError):
        adapter._build_table_properties_exp(table_format="INVALID_TYPE")


def test_build_engine_property(adapter: StarRocksEngineAdapter):
    """Test _build_engine_property."""
    result = adapter._build_engine_property("OLAP")
    assert isinstance(result, exp.EngineProperty)
    assert result.sql(dialect="starrocks") == "ENGINE=OLAP"


@pytest.mark.parametrize(
    "table_type,key_columns,expected_type,expected_fragment",
    [
        (
            "PRIMARY KEY",
            exp.Tuple(expressions=[exp.to_column("id")]),
            exp.PrimaryKey,
            "PRIMARY KEY",
        ),
        (None, exp.to_column("id"), exp.DuplicateKeyProperty, "DUPLICATE KEY"),
        ("DUPLICATE", exp.to_column("id"), exp.DuplicateKeyProperty, "DUPLICATE KEY"),
    ],
)
def test_build_table_type_property(
    adapter: StarRocksEngineAdapter, table_type, key_columns, expected_type, expected_fragment
):
    """Test _build_table_type_property for different table types."""
    result = adapter._build_table_type_property(table_type=table_type, key_columns=key_columns)
    assert isinstance(result, expected_type)
    assert expected_fragment in result.sql(dialect="starrocks")


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
    "distributed_by,buckets,expected_fragment",
    [
        (
            exp.Tuple(expressions=[exp.to_column("id")]),
            exp.Literal.number(10),
            "DISTRIBUTED BY HASH",
        ),
        (None, exp.Literal.number(5), "DISTRIBUTED BY RANDOM"),
    ],
)
def test_build_distribution_property(
    adapter: StarRocksEngineAdapter, distributed_by, buckets, expected_fragment
):
    """Test _build_distribution_property with HASH and RANDOM."""
    result = adapter._build_distribution_property(
        distributed_by_expr=distributed_by, buckets_expr=buckets
    )
    assert isinstance(result, exp.DistributedByProperty)
    assert expected_fragment in result.sql(dialect="starrocks")


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
