import typing as t

import pytest
import sqlglot
from pytest_mock import MockerFixture
from sqlglot import exp

from sqlmesh.core.engine_adapter.shared import DataObjectType
from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
from sqlmesh.utils.errors import SQLMeshError
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.starrocks]


@pytest.fixture
def adapter(make_mocked_engine_adapter: t.Callable) -> StarRocksEngineAdapter:
    return make_mocked_engine_adapter(StarRocksEngineAdapter)


def test_ping(adapter: StarRocksEngineAdapter):
    adapter.ping()
    adapter._connection_pool.get().ping.assert_called_once_with(reconnect=False)


def test_create_schema(adapter: StarRocksEngineAdapter):
    adapter.create_schema("test_db")
    assert to_sql_calls(adapter) == ["CREATE DATABASE IF NOT EXISTS `test_db`"]


def test_drop_schema(adapter: StarRocksEngineAdapter):
    adapter.drop_schema("test_db")
    assert to_sql_calls(adapter) == ["DROP DATABASE IF EXISTS `test_db`"]


def test_create_table_like(adapter: StarRocksEngineAdapter):
    adapter.create_table_like("target_table", "source_table")
    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `target_table` LIKE `source_table`"
    ]


def test_table_properties_empty(adapter: StarRocksEngineAdapter):
    assert adapter._build_table_properties_exp() is None


def test_table_properties_comment(adapter: StarRocksEngineAdapter):
    result = adapter._build_table_properties_exp(table_description="Test description")
    assert result and "COMMENT" in result.sql(dialect="starrocks")


def test_table_properties_partition(adapter: StarRocksEngineAdapter):
    result = adapter._build_table_properties_exp(partitioned_by=[exp.to_column("ds")])
    assert result and "PARTITION BY" in result.sql(dialect="starrocks")


def test_table_properties_primary_key(adapter: StarRocksEngineAdapter):
    result = adapter._build_table_properties_exp(
        table_properties={"primary_key": exp.Tuple(expressions=[exp.to_column("id")])}
    )
    assert result and "PRIMARY KEY" in result.sql(dialect="starrocks")


def test_table_properties_order_by(adapter: StarRocksEngineAdapter):
    result = adapter._build_table_properties_exp(
        table_properties={"order_by": exp.Tuple(expressions=[exp.to_column("col1")])}
    )
    assert result and "ORDER BY" in result.sql(dialect="starrocks")


def test_table_properties_distributed_by_hash(adapter: StarRocksEngineAdapter):
    expr = sqlglot.parse_one("HASH(columns := (id, name), buckets := 10)", dialect="starrocks")
    result = adapter._build_distributed_by_property(expr)
    assert result.sql(dialect="starrocks") == "DISTRIBUTED BY HASH (id, name) BUCKETS 10"


def test_table_properties_distributed_by_random(adapter: StarRocksEngineAdapter):
    expr = sqlglot.parse_one("RANDOM(buckets := 5)", dialect="starrocks")
    result = adapter._build_distributed_by_property(expr)
    assert result.sql(dialect="starrocks") == "DISTRIBUTED BY RANDOM BUCKETS 5"


def test_table_properties_distributed_by_hash_missing_columns(adapter: StarRocksEngineAdapter):
    expr = sqlglot.parse_one("HASH(buckets := 10)", dialect="starrocks")
    with pytest.raises(SQLMeshError, match="HASH.*requires 'columns' parameter"):
        adapter._build_distributed_by_property(expr)


def test_table_properties_distributed_by_invalid(adapter: StarRocksEngineAdapter):
    with pytest.raises(SQLMeshError, match="Expected HASH.*or RANDOM"):
        adapter._build_distributed_by_property(exp.Literal.string("invalid"))


def test_grants_strip_catalog(adapter: StarRocksEngineAdapter):
    table = exp.to_table("default_catalog.test_db.test_table")
    expressions = adapter._apply_grants_config_expr(
        table, {"SELECT": ["user1"]}, DataObjectType.TABLE
    )
    assert "default_catalog" not in expressions[0].sql(dialect="starrocks")


def test_grants_parse_grantee_format(adapter: StarRocksEngineAdapter, mocker: MockerFixture):
    mocker.patch.object(adapter, "fetchall", return_value=[("INSERT, SELECT", "'user1'@'%'")])
    grants = adapter._get_current_grants_config(exp.to_table("test_db.test_table"))
    assert grants == {"INSERT": ["user1"], "SELECT": ["user1"]}
