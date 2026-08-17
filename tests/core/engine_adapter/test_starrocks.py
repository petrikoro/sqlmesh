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
    TableAlterChangeColumnTypeOperation,
    TableAlterDropColumnOperation,
    TableAlterOperation,
)
from sqlmesh.utils import columns_to_types_to_struct
from sqlmesh.utils.errors import MigrationNotSupportedError, SQLMeshError
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


def schema_migration_model(
    columns: t.Dict[str, exp.DataType],
    physical_properties: t.Optional[t.Dict[str, exp.Expr]] = None,
    partitioned_by: t.Optional[t.List[exp.Expr]] = None,
) -> SqlModel:
    return SqlModel(
        name="schema_migration_model",
        dialect="starrocks",
        kind=FullKind(),
        query=exp.select(
            *(exp.cast(exp.Null(), data_type).as_(name) for name, data_type in columns.items())
        ),
        columns=columns,
        physical_properties=physical_properties or {},
        partitioned_by=partitioned_by or [],
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


def test_table_properties_order_by_precedes_rollup(adapter: StarRocksEngineAdapter):
    result = adapter._build_table_properties_exp(
        table_properties={
            "rollup": exp.Tuple(
                expressions=[
                    exp.PropertyEQ(
                        this=exp.to_identifier("rollup_id"),
                        expression=exp.Tuple(expressions=[exp.column("id"), exp.column("value")]),
                    )
                ]
            ),
            "order_by": exp.Tuple(expressions=[exp.column("id")]),
        }
    )
    assert result and result.sql(dialect="starrocks") == (
        "ORDER BY (id) ROLLUP (rollup_id (id, value))"
    )


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


def test_alter_column_type_uses_modify_column(
    adapter: StarRocksEngineAdapter, mocker: MockerFixture
):
    mocker.patch("sqlmesh.core.engine_adapter.starrocks.time.sleep")
    adapter.cursor.fetchall.side_effect = [
        [],
        [schema_change_job(1, "RUNNING", progress="50%")],
        [schema_change_job(1, "FINISHED")],
    ]
    adapter.cursor.fetchone.return_value = (
        "test_table",
        """CREATE TABLE test_table (
            id BIGINT NULL,
            value INT NULL DEFAULT '0' COMMENT 'value column'
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY RANDOM
        ORDER BY(id)
        ROLLUP (rollup_id (id))""",
    )

    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {"id": exp.DataType.build("BIGINT"), "value": exp.DataType.build("INT")},
        {"id": exp.DataType.build("BIGINT"), "value": exp.DataType.build("BIGINT")},
    )
    assert len(operations) == 1
    assert isinstance(operations[0], TableAlterChangeColumnTypeOperation)
    assert not operations[0].is_destructive

    adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))

    show_job = "SHOW ALTER TABLE COLUMN WHERE TableName = 'test_table' ORDER BY JobId DESC LIMIT 1"
    assert to_sql_calls(adapter) == [
        "SHOW CREATE TABLE `test_table`",
        show_job,
        "ALTER TABLE `test_table` MODIFY COLUMN `value` BIGINT NULL "
        "DEFAULT '0' COMMENT 'value column'",
        show_job,
        show_job,
    ]


@pytest.mark.parametrize(
    ("change", "table_properties", "role"),
    [
        ("type", "PRIMARY KEY(value) DISTRIBUTED BY HASH(value)", "primary key"),
        ("drop", "UNIQUE KEY(id, value) DISTRIBUTED BY HASH(id)", "unique key"),
        (
            "drop",
            "DUPLICATE KEY(id) PARTITION BY value DISTRIBUTED BY HASH(id)",
            "partitioning",
        ),
        ("drop", "DUPLICATE KEY(id) DISTRIBUTED BY HASH(value)", "distribution"),
        (
            "drop",
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) ORDER BY(id, value)",
            "sort key",
        ),
        ("drop", "DUPLICATE KEY(id, value) DISTRIBUTED BY HASH(id)", "duplicate key"),
        (
            "drop",
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) ROLLUP (rollup_value (id, value))",
            "rollup",
        ),
        ("drop", "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)", None),
    ],
    ids=[
        "type-primary-key",
        "drop-unique-key",
        "drop-partition",
        "drop-distribution",
        "drop-sort-key",
        "drop-duplicate-key",
        "drop-rollup",
        "drop-unconstrained",
    ],
)
def test_alter_validates_constrained_columns(
    adapter: StarRocksEngineAdapter,
    change: str,
    table_properties: str,
    role: t.Optional[str],
):
    current = {
        "id": exp.DataType.build("INT"),
        "value": exp.DataType.build("INT"),
    }
    target = (
        {"id": current["id"]}
        if change == "drop"
        else {"id": current["id"], "value": exp.DataType.build("BIGINT")}
    )
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        current,
        target,
    )
    adapter.cursor.fetchone.return_value = (
        "test_table",
        f"CREATE TABLE test_table (id INT NULL, value INT NULL) ENGINE=OLAP {table_properties}",
    )

    if role is None:
        adapter.cursor.fetchall.side_effect = [[], [schema_change_job(1, "FINISHED")]]
        adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))
    else:
        with pytest.raises(MigrationNotSupportedError) as ex:
            adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))

        message = str(ex.value)
        assert role in message
        assert "ALTER TABLE `test_table`" in message
        if change == "type":
            assert "MODIFY COLUMN `value` BIGINT" in message


@pytest.mark.parametrize(
    ("property_name", "current_value", "target_value"),
    [
        (
            "primary_key",
            exp.Tuple(expressions=[exp.column("id")]),
            exp.Tuple(expressions=[exp.column("value")]),
        ),
        ("partitioned_by", exp.column("id"), exp.column("value")),
    ],
)
def test_physical_property_change_requires_new_table(
    adapter: StarRocksEngineAdapter,
    property_name: str,
    current_value: exp.Expr,
    target_value: exp.Expr,
):
    columns = {
        "id": exp.DataType.build("INT"),
        "value": exp.DataType.build("INT"),
        "other": exp.DataType.build("INT"),
    }

    is_partition = property_name == "partitioned_by"
    can_apply = adapter.can_apply_schema_change_in_place(
        schema_migration_model(
            columns,
            physical_properties=None if is_partition else {property_name: current_value},
            partitioned_by=[current_value] if is_partition else None,
        ),
        schema_migration_model(
            columns,
            physical_properties=None if is_partition else {property_name: target_value},
            partitioned_by=[target_value] if is_partition else None,
        ),
        current_table="test_table",
    )

    assert not can_apply


def test_schema_change_in_place_uses_live_columns(
    adapter: StarRocksEngineAdapter, mocker: MockerFixture
):
    model_columns = {"id": exp.DataType.build("INT")}
    live_columns = {**model_columns, "partitioned_at": exp.DataType.build("DATE")}
    mocker.patch.object(adapter, "columns", return_value=live_columns)
    mocker.patch.object(
        adapter,
        "_get_table_definition",
        return_value=sqlglot.parse_one(
            "CREATE TABLE test_table (id INT, partitioned_at DATE) ENGINE=OLAP "
            "DUPLICATE KEY(id) PARTITION BY partitioned_at DISTRIBUTED BY HASH(id)",
            dialect="starrocks",
        ),
    )

    can_apply = adapter.can_apply_schema_change_in_place(
        schema_migration_model(model_columns),
        schema_migration_model(model_columns),
        current_table="test_table",
    )

    assert not can_apply


def test_schema_change_in_place_rejects_aggregate_key_table(
    adapter: StarRocksEngineAdapter, mocker: MockerFixture
):
    current = {
        "site_id": exp.DataType.build("LARGEINT", dialect="starrocks"),
        "event_date": exp.DataType.build("DATE", dialect="starrocks"),
    }
    mocker.patch.object(adapter, "columns", return_value=current)
    adapter.cursor.fetchone.return_value = (
        "aggregate_keys_only",
        """CREATE TABLE `aggregate_keys_only` (
          `site_id` largeint(40) NOT NULL COMMENT "",
          `event_date` date NOT NULL COMMENT ""
        ) ENGINE=OLAP
        AGGREGATE KEY(`site_id`, `event_date`)
        DISTRIBUTED BY HASH(`site_id`)
        PROPERTIES (
          "compression" = "LZ4",
          "fast_schema_evolution" = "true",
          "replicated_storage" = "true",
          "replication_num" = "1"
        )""",
    )

    target = {**current, "value": exp.DataType.build("BIGINT", dialect="starrocks")}
    assert not adapter.can_apply_schema_change_in_place(
        schema_migration_model(current),
        schema_migration_model(target),
        current_table="aggregate_keys_only",
    )

    operations = adapter.schema_differ.compare_columns("aggregate_keys_only", current, target)
    with pytest.raises(MigrationNotSupportedError, match="Aggregate Key table"):
        adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))


def test_schema_change_in_place_rejects_legacy_partition_column_change(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
):
    current = {
        "id": exp.DataType.build("INT"),
        "value": exp.DataType.build("INT"),
    }
    mocker.patch.object(adapter, "columns", return_value=current)
    mocker.patch.object(
        adapter,
        "_get_table_definition",
        return_value=sqlglot.parse_one(
            "CREATE TABLE test_table (id INT, value INT) ENGINE=OLAP "
            "DUPLICATE KEY(id) PARTITION BY RANGE(value) "
            "(PARTITION p1 VALUES LESS THAN (10)) DISTRIBUTED BY HASH(id)",
            dialect="starrocks",
        ),
    )

    assert not adapter.can_apply_schema_change_in_place(
        schema_migration_model(current),
        schema_migration_model({"id": current["id"]}),
        current_table="test_table",
    )


def test_schema_change_in_place_rejects_live_generated_column_definition(
    adapter: StarRocksEngineAdapter, mocker: MockerFixture
):
    current = {
        "id": exp.DataType.build("INT"),
        "source": exp.DataType.build("INT"),
        "generated": exp.DataType.build("BIGINT"),
    }
    mocker.patch.object(adapter, "columns", return_value=current)
    adapter.cursor.fetchone.return_value = (
        "test_table",
        """CREATE TABLE `test_table` (
          `id` int(11) NOT NULL COMMENT "",
          `source` int(11) NULL COMMENT "",
          `generated` bigint(20) NULL AS `source` * 2 COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
          "fast_schema_evolution" = "true",
          "replication_num" = "1"
        )""",
    )

    assert not adapter.can_apply_schema_change_in_place(
        schema_migration_model(current),
        schema_migration_model(
            {
                "id": current["id"],
                "generated": current["generated"],
            }
        ),
        current_table="test_table",
    )


@pytest.mark.parametrize(
    ("current", "target", "definition"),
    [
        (
            {"id": "INT", "sequence": "BIGINT"},
            {"id": "INT", "sequence": "STRING"},
            "CREATE TABLE test_table (id INT, sequence BIGINT AUTO_INCREMENT) ENGINE=OLAP "
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
        ),
        (
            {"id": "INT", "embedding": "ARRAY<FLOAT>"},
            {"id": "INT", "embedding": "ARRAY<DOUBLE>"},
            "CREATE TABLE test_table (id INT, embedding ARRAY<FLOAT>, "
            "INDEX embedding_idx (embedding) USING VECTOR) ENGINE=OLAP "
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
        ),
    ],
    ids=["auto-increment", "vector-index"],
)
def test_schema_change_in_place_rejects_special_column_changes(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
    current: t.Dict[str, str],
    target: t.Dict[str, str],
    definition: str,
):
    current_types = {
        name: exp.DataType.build(data_type, dialect="starrocks")
        for name, data_type in current.items()
    }
    target_types = {
        name: exp.DataType.build(data_type, dialect="starrocks")
        for name, data_type in target.items()
    }
    mocker.patch.object(adapter, "columns", return_value=current_types)
    adapter.cursor.fetchone.return_value = ("test_table", definition)

    assert not adapter.can_apply_schema_change_in_place(
        schema_migration_model(current_types),
        schema_migration_model(target_types),
        current_table="test_table",
    )


def test_schema_change_in_place_rejects_unavailable_target_schema(
    adapter: StarRocksEngineAdapter,
):
    current = {"id": exp.DataType.build("INT")}

    can_apply = adapter.can_apply_schema_change_in_place(
        schema_migration_model(current),
        schema_migration_model({}),
        current_table="test_table",
    )

    assert not can_apply


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
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT) ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
    )

    adapter.alter_table(t.cast(t.List[TableAlterOperation], alterations))

    show_job = (
        "SHOW ALTER TABLE COLUMN FROM `test_db` WHERE TableName = 'test_table' "
        "ORDER BY JobId DESC LIMIT 1"
    )
    assert to_sql_calls(adapter) == [
        "SHOW CREATE TABLE `test_db`.`test_table`",
        show_job,
        "ALTER TABLE `test_db`.`test_table` ADD COLUMN `first_col` INT",
        show_job,
        show_job,
        "ALTER TABLE `test_db`.`test_table` ADD COLUMN `second_col` INT",
        show_job,
        show_job,
    ]


def test_alter_table_accepts_synchronous_schema_change_with_previous_job(
    adapter: StarRocksEngineAdapter, mocker: MockerFixture
):
    current = {"id": exp.DataType.build("INT")}
    target = {**current, "value": exp.DataType.build("INT")}
    operations = adapter.schema_differ.compare_columns("test_table", current, target)
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT) ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
    )
    latest_job = mocker.patch.object(
        adapter,
        "_get_latest_schema_change_job",
        return_value=schema_change_job(1, "FINISHED"),
    )
    columns = mocker.patch.object(adapter, "columns", return_value=target)
    sleep = mocker.patch("sqlmesh.core.engine_adapter.starrocks.time.sleep")

    adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))

    assert to_sql_calls(adapter) == [
        "SHOW CREATE TABLE `test_table`",
        "ALTER TABLE `test_table` ADD COLUMN `value` INT",
    ]
    assert latest_job.call_count == 2
    columns.assert_called_once_with(exp.to_table("test_table"))
    sleep.assert_not_called()


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
        adapter._wait_for_schema_change(
            exp.to_table("test_table"),
            previous_job_id=1,
            expected_table_struct=columns_to_types_to_struct({"id": exp.DataType.build("INT")}),
        )


def test_wait_for_schema_change_times_out(adapter: StarRocksEngineAdapter, mocker: MockerFixture):
    adapter._extra_config["schema_change_timeout"] = 1
    mocker.patch.object(adapter, "_get_latest_schema_change_job", return_value=None)
    mocker.patch.object(adapter, "columns", return_value={"id": exp.DataType.build("INT")})
    mocker.patch("sqlmesh.core.engine_adapter.starrocks.time.monotonic", side_effect=[0, 1])

    with pytest.raises(SQLMeshError, match="Timed out after 1 seconds"):
        adapter._wait_for_schema_change(
            exp.to_table("test_table"),
            previous_job_id=None,
            expected_table_struct=columns_to_types_to_struct({"id": exp.DataType.build("BIGINT")}),
        )


def test_get_alter_operations_uses_drop_add_for_unsupported_type_change(
    adapter: StarRocksEngineAdapter,
):
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
