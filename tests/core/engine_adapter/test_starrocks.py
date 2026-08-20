import typing as t

import pytest
import sqlglot
from pytest_mock import MockerFixture
from sqlglot import exp

from sqlmesh.core.engine_adapter.shared import DataObjectType
from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
from sqlmesh.core.model import FullKind, SqlModel
from sqlmesh.core.schema_diff import (
    TableAlterAddColumnOperation,
    TableAlterDropColumnOperation,
    TableAlterOperation,
)
from sqlmesh.utils.errors import MigrationNotSupportedError, SQLMeshError
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.starrocks]


@pytest.fixture
def adapter(make_mocked_engine_adapter: t.Callable) -> StarRocksEngineAdapter:
    return make_mocked_engine_adapter(StarRocksEngineAdapter)


def schema_migration_model(columns: t.Dict[str, exp.DataType]) -> SqlModel:
    return SqlModel(
        name="schema_migration_model",
        dialect="starrocks",
        kind=FullKind(),
        query=exp.select(
            *(exp.cast(exp.Null(), data_type).as_(name) for name, data_type in columns.items())
        ),
        columns=columns,
    )


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


@pytest.mark.parametrize(
    ("current_type", "target_type"),
    [
        ("STRING", "VARCHAR(65533)"),
        ("VARCHAR", "VARCHAR(1)"),
        ("DECIMAL", "DECIMAL(10, 0)"),
        ("DECIMAL64", "DECIMAL(18, 18)"),
    ],
)
def test_physical_type_aliases_do_not_alter(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
    current_type: str,
    target_type: str,
):
    mocker.patch.object(
        adapter,
        "columns",
        side_effect=[
            {"value": exp.DataType.build(current_type, dialect="starrocks")},
            {"value": exp.DataType.build(target_type, dialect="starrocks")},
        ],
    )

    assert adapter.get_alter_operations("current_table", "target_table") == []


def test_real_type_change_requires_rebuild(adapter: StarRocksEngineAdapter):
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {"value": exp.DataType.build("INT")},
        {"value": exp.DataType.build("BIGINT")},
    )

    assert [type(operation) for operation in operations] == [
        TableAlterDropColumnOperation,
        TableAlterAddColumnOperation,
    ]
    with pytest.raises(MigrationNotSupportedError, match="new physical table version"):
        adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))
    assert not adapter.cursor.execute.called


def test_plain_top_level_add_uses_native_ddl(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
):
    current = {"id": exp.DataType.build("INT")}
    target = {**current, "value": exp.DataType.build("STRING", dialect="starrocks")}
    operations = adapter.schema_differ.compare_columns("test_table", current, target)
    assert len(operations) == 1
    assert isinstance(operations[0], TableAlterAddColumnOperation)

    adapter.cursor.fetchall.return_value = []
    mocker.patch.object(
        adapter,
        "columns",
        return_value={**current, "value": exp.DataType.build("VARCHAR(65533)")},
    )

    adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))

    assert to_sql_calls(adapter) == ["ALTER TABLE `test_table` ADD COLUMN `value` STRING"]


def test_schema_change_in_place_allowlist(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
):
    current_columns = {"id": exp.DataType.build("INT")}
    current = schema_migration_model(current_columns)
    mocker.patch.object(adapter, "columns", return_value=current_columns)

    additive_columns = {**current_columns, "value": exp.DataType.build("STRING")}
    assert adapter.can_apply_schema_change_in_place(
        current,
        schema_migration_model(additive_columns),
        current_table="test_table",
    )
    assert not adapter.can_apply_schema_change_in_place(
        current,
        schema_migration_model({"id": exp.DataType.build("BIGINT")}),
        current_table="test_table",
    )


def test_metadata_only_unknown_schema_reuses_physical_table(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
):
    columns = {
        "record_sk": exp.DataType.build("STRING", dialect="starrocks"),
        "record_timestamp": exp.DataType.build("UNKNOWN"),
        "record_metadata": exp.DataType.build("UNKNOWN"),
    }
    current = schema_migration_model(columns)
    target = current.model_copy(update={"tags": ["changed"]})
    live_columns = mocker.patch.object(adapter, "columns")

    assert target.is_metadata_only_change(current)
    assert adapter.can_apply_schema_change_in_place(
        current,
        target,
        current_table="test_table",
    )
    live_columns.assert_not_called()
