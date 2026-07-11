import typing as t

import pytest
import sqlglot
from pytest_mock import MockerFixture
from sqlglot import exp

from sqlmesh.core.engine_adapter.shared import DataObjectType
from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
from sqlmesh.core.schema_diff import (
    TableAlterAddColumnOperation,
    TableAlterChangeColumnTypeOperation,
    TableAlterDropColumnOperation,
)
from sqlmesh.utils.errors import SQLMeshError
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.starrocks]


@pytest.fixture
def adapter(make_mocked_engine_adapter: t.Callable) -> StarRocksEngineAdapter:
    return make_mocked_engine_adapter(StarRocksEngineAdapter)


def schema_change_job(
    job_id: int,
    state: str,
    message: str = "",
    progress: str = "100%",
) -> t.Tuple[t.Any, ...]:
    return (
        job_id,
        "test_table",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        state,
        message,
        progress,
        3600,
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


def test_alter_key_column_type_uses_modify_column(
    adapter: StarRocksEngineAdapter, mocker: MockerFixture
):
    mocker.patch("sqlmesh.core.engine_adapter.starrocks.time.sleep")
    adapter.cursor.fetchall.side_effect = [
        [
            ("id", "bigint", "YES", "true", None, ""),
            ("value", "int", "YES", "false", None, ""),
        ],
        [
            ("id", "varchar(65533)", "YES", "true", None, ""),
            ("value", "int", "YES", "false", None, ""),
        ],
        [
            ("id", "bigint", None, "YES", "YES", "0", "", "", "identifier"),
            ("value", "int", None, "YES", "NO", None, "", "", ""),
        ],
        [],
        [schema_change_job(1, "RUNNING", progress="50%")],
        [schema_change_job(1, "FINISHED")],
    ]

    operations = adapter.get_alter_operations("test_table", "target_table")

    assert len(operations) == 1
    assert isinstance(operations[0], TableAlterChangeColumnTypeOperation)
    assert not operations[0].is_destructive

    adapter.alter_table(operations)

    assert to_sql_calls(adapter) == [
        "DESCRIBE `test_table`",
        "DESCRIBE `target_table`",
        "SHOW FULL COLUMNS FROM `test_table`",
        "SHOW ALTER TABLE COLUMN WHERE TableName = 'test_table' ORDER BY JobId DESC LIMIT 1",
        "ALTER TABLE `test_table` MODIFY COLUMN `id` VARCHAR(65533) KEY NULL DEFAULT '0' COMMENT 'identifier'",
        "SHOW ALTER TABLE COLUMN WHERE TableName = 'test_table' ORDER BY JobId DESC LIMIT 1",
        "SHOW ALTER TABLE COLUMN WHERE TableName = 'test_table' ORDER BY JobId DESC LIMIT 1",
    ]


def test_alter_table_waits_between_schema_changes(
    adapter: StarRocksEngineAdapter, mocker: MockerFixture
):
    mocker.patch("sqlmesh.core.engine_adapter.starrocks.time.sleep")
    adapter.cursor.fetchall.side_effect = [
        [],
        [schema_change_job(1, "FINISHED")],
        [schema_change_job(1, "FINISHED")],
        [schema_change_job(2, "RUNNING")],
        [schema_change_job(2, "FINISHED")],
    ]
    alterations = adapter.schema_differ.compare_columns(
        "test_db.test_table",
        {"id": exp.DataType.build("INT")},
        {
            "id": exp.DataType.build("INT"),
            "first_col": exp.DataType.build("INT"),
            "second_col": exp.DataType.build("INT"),
        },
    )

    adapter.alter_table(alterations)

    show_job = (
        "SHOW ALTER TABLE COLUMN FROM `test_db` WHERE TableName = 'test_table' "
        "ORDER BY JobId DESC LIMIT 1"
    )
    assert to_sql_calls(adapter) == [
        show_job,
        "ALTER TABLE `test_db`.`test_table` ADD COLUMN `first_col` INT",
        show_job,
        show_job,
        "ALTER TABLE `test_db`.`test_table` ADD COLUMN `second_col` INT",
        show_job,
        show_job,
    ]


def test_alter_table_does_not_wait_for_raw_expression(adapter: StarRocksEngineAdapter):
    alteration = sqlglot.parse_one(
        "ALTER TABLE test_db.test_table RENAME test_table_renamed", dialect="starrocks"
    )

    adapter.alter_table([t.cast(exp.Alter, alteration)])

    assert to_sql_calls(adapter) == [
        "ALTER TABLE `test_db`.`test_table` RENAME `test_table_renamed`"
    ]


def test_wait_for_schema_change_raises_on_cancelled_job(
    adapter: StarRocksEngineAdapter, mocker: MockerFixture
):
    mocker.patch.object(
        adapter,
        "_get_latest_schema_change_job",
        return_value=schema_change_job(2, "CANCELLED", "invalid schema change"),
    )

    with pytest.raises(SQLMeshError, match="job 2.*invalid schema change"):
        adapter._wait_for_schema_change(exp.to_table("test_table"), previous_job_id=1)


def test_wait_for_schema_change_times_out(adapter: StarRocksEngineAdapter, mocker: MockerFixture):
    adapter._extra_config["schema_change_timeout"] = 1
    mocker.patch.object(adapter, "_get_latest_schema_change_job", return_value=None)
    mocker.patch("sqlmesh.core.engine_adapter.starrocks.time.monotonic", side_effect=[0, 1])

    with pytest.raises(SQLMeshError, match="Timed out after 1 seconds"):
        adapter._wait_for_schema_change(exp.to_table("test_table"), previous_job_id=None)


def test_alter_unsupported_type_change_uses_drop_add(adapter: StarRocksEngineAdapter):
    adapter.cursor.fetchall.side_effect = [
        [
            ("id", "int", "NO", "true", None, ""),
            ("items", "array<int>", "YES", "false", None, ""),
        ],
        [
            ("id", "int", "NO", "true", None, ""),
            ("items", "int", "YES", "false", None, ""),
        ],
    ]

    operations = adapter.get_alter_operations("test_table", "target_table")

    assert len(operations) == 2
    assert isinstance(operations[0], TableAlterDropColumnOperation)
    assert isinstance(operations[1], TableAlterAddColumnOperation)


@pytest.mark.parametrize(
    ("current_type", "target_type"),
    [
        ("TINYINT", "SMALLINT"),
        ("SMALLINT", "INT"),
        ("INT", "BIGINT"),
        ("INT", "DOUBLE"),
        ("FLOAT", "DOUBLE"),
        ("DATE", "DATETIME"),
        ("BIGINT", "STRING"),
        ("BIGINT", "VARCHAR(65533)"),
        ("VARCHAR(32)", "STRING"),
    ],
)
def test_compatible_type_changes(
    adapter: StarRocksEngineAdapter, current_type: str, target_type: str
):
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {"value": exp.DataType.build(current_type, dialect="starrocks")},
        {"value": exp.DataType.build(target_type, dialect="starrocks")},
    )

    assert len(operations) == 1
    assert isinstance(operations[0], TableAlterChangeColumnTypeOperation)
    assert not operations[0].is_destructive


@pytest.mark.parametrize(
    ("current_type", "target_type"),
    [
        ("BIGINT", "DOUBLE"),
        ("BIGINT", "INT"),
        ("DATETIME", "DATE"),
        ("VARCHAR(32)", "INT"),
    ],
)
def test_lossy_type_changes_remain_destructive(
    adapter: StarRocksEngineAdapter, current_type: str, target_type: str
):
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {"value": exp.DataType.build(current_type, dialect="starrocks")},
        {"value": exp.DataType.build(target_type, dialect="starrocks")},
    )

    assert len(operations) == 2
    assert isinstance(operations[0], TableAlterDropColumnOperation)
    assert isinstance(operations[1], TableAlterAddColumnOperation)
