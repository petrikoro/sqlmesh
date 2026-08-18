import time
import typing as t

import pytest
from pymysql.err import ProgrammingError
from pytest import FixtureRequest
from sqlglot import exp, parse_one
from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
from sqlmesh.core.model import FullKind, SqlModel
from sqlmesh.core.schema_diff import TableAlterOperation
from sqlmesh.utils.errors import MigrationNotSupportedError, PlanError
from tests.core.engine_adapter.integration import (
    TestContext,
    generate_pytest_params,
    ENGINES_BY_NAME,
    IntegrationTestEngine,
)


@pytest.fixture(params=list(generate_pytest_params(ENGINES_BY_NAME["starrocks"])))
def ctx(
    request: FixtureRequest,
    create_test_context: t.Callable[[IntegrationTestEngine, str, str], t.Iterable],
) -> t.Iterable[TestContext]:
    yield from create_test_context(*request.param)


@pytest.fixture
def engine_adapter(ctx: TestContext) -> StarRocksEngineAdapter:
    assert isinstance(ctx.engine_adapter, StarRocksEngineAdapter)
    return ctx.engine_adapter


def _frontend_config_enabled(
    engine_adapter: StarRocksEngineAdapter, config_key: str
) -> t.Iterator[None]:
    config = engine_adapter.fetchone(f"ADMIN SHOW FRONTEND CONFIG LIKE '{config_key}'")
    description = engine_adapter.cursor.description
    assert config and description
    config_by_name = {
        str(column[0]).casefold(): value for column, value in zip(description, config)
    }
    key = config_by_name.get("key")
    value = config_by_name.get("value")
    assert key == config_key
    previous_value = str(value).lower()
    assert previous_value in {"true", "false"}
    changed = previous_value != "true"
    if changed:
        engine_adapter.execute(f"ADMIN SET FRONTEND CONFIG ('{config_key}'='true')")

    try:
        yield
    finally:
        if changed:
            engine_adapter.execute(f"ADMIN SET FRONTEND CONFIG ('{config_key}'='{previous_value}')")


@pytest.fixture
def experimental_gin_enabled(
    engine_adapter: StarRocksEngineAdapter,
) -> t.Iterator[None]:
    yield from _frontend_config_enabled(engine_adapter, "enable_experimental_gin")


@pytest.fixture
def range_distribution_enabled(
    engine_adapter: StarRocksEngineAdapter,
) -> t.Iterator[None]:
    row = engine_adapter.fetchone("SELECT @@enable_range_distribution")
    assert row
    previous_value = str(row[0]).casefold()
    assert previous_value in {"0", "1", "false", "true"}

    engine_adapter.execute("SET enable_range_distribution = true")
    try:
        yield
    finally:
        engine_adapter.execute(f"SET enable_range_distribution = {previous_value}")


def _schema_migration_model(
    model_name: exp.Table,
    value_type: str,
    id_type: str = "INT",
    order_by: t.Optional[str] = None,
) -> SqlModel:
    value_data_type = exp.DataType.build(value_type, dialect="starrocks")
    id_data_type = exp.DataType.build(id_type, dialect="starrocks")

    physical_properties: t.Dict[str, exp.Expr] = {
        "primary_key": exp.Tuple(expressions=[exp.column("id")]),
        "distributed_by": parse_one("HASH(columns := id)", dialect="starrocks"),
    }
    if order_by:
        physical_properties["order_by"] = exp.Tuple(expressions=[exp.column(order_by)])

    return SqlModel(
        name=model_name.sql(dialect="starrocks"),
        dialect="starrocks",
        kind=FullKind(),
        query=exp.select(
            exp.cast(exp.Literal.number(1), id_data_type).as_("id"),
            exp.cast(exp.Literal.string("42"), value_data_type).as_("value"),
        ),
        columns={"id": id_data_type, "value": value_data_type},
        physical_properties=physical_properties,
    )


def _alter_columns(
    engine_adapter: StarRocksEngineAdapter,
    table: exp.Table,
    current: t.Dict[str, str],
    target: t.Dict[str, str],
) -> None:
    def parse_types(columns: t.Dict[str, str]) -> t.Dict[str, exp.DataType]:
        return {
            name: exp.DataType.build(data_type, dialect="starrocks")
            for name, data_type in columns.items()
        }

    operations = engine_adapter.schema_differ.compare_columns(
        table,
        parse_types(current),
        parse_types(target),
    )
    engine_adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))


def _wait_for_synchronous_mv(
    engine_adapter: StarRocksEngineAdapter,
    table: exp.Table,
    timeout: float = 60,
) -> None:
    database = exp.to_identifier(table.db).sql(dialect="starrocks", identify=True)
    deadline = time.monotonic() + timeout
    while True:
        jobs = [
            row
            for row in engine_adapter.fetchall(f"SHOW ALTER MATERIALIZED VIEW FROM {database}")
            if len(row) > 8 and str(row[1]).casefold() == table.name.casefold()
        ]
        if jobs:
            job = max(jobs, key=lambda row: int(row[0]))
            state = str(job[8]).upper()
            if state == "FINISHED":
                return
            if state in {"CANCELLED", "CANCELED", "FAILED"}:
                raise AssertionError(f"Synchronous materialized-view build failed: {job}")
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"Timed out waiting for a synchronous materialized view on {table}"
            )
        time.sleep(0.2)


def test_engine_adapter(ctx: TestContext):
    """Test basic connectivity to StarRocks."""
    assert isinstance(ctx.engine_adapter, StarRocksEngineAdapter)
    assert ctx.engine_adapter.fetchone("SELECT 1") == (1,)


def test_engine_adapter_dialect(ctx: TestContext):
    """Test that the dialect is correctly set to starrocks."""
    assert ctx.engine_adapter.dialect == "starrocks"


def test_context_applies_forward_only_schema_migration(ctx: TestContext):
    model_name = ctx.table("PLAN_SCHEMA_MIGRATION")
    context = ctx.create_context()

    context.upsert_model(_schema_migration_model(model_name, "SMALLINT"))
    initial_plan = context.plan("prod", auto_apply=True, no_prompts=True)
    initial_snapshot = initial_plan.new_snapshots[0]

    context.upsert_model(_schema_migration_model(model_name, "BIGINT"))
    supported_plan = context.plan(
        "prod",
        forward_only=True,
        auto_apply=True,
        no_prompts=True,
    )
    supported_snapshot = supported_plan.new_snapshots[0]

    assert supported_snapshot.table_name() == initial_snapshot.table_name()
    physical_table = exp.to_table(supported_snapshot.table_name())
    assert context.engine_adapter.columns(physical_table)["value"].is_type(exp.DataType.Type.BIGINT)
    assert context.engine_adapter.fetchall(
        exp.select("id", "value").from_(physical_table), quote_identifiers=True
    ) == ((1, 42),)


def test_context_applies_supported_key_schema_migration(ctx: TestContext):
    model_name = ctx.table("PLAN_KEY_SCHEMA_MIGRATION")
    context = ctx.create_context()

    context.upsert_model(_schema_migration_model(model_name, "INT", id_type="VARCHAR(32)"))
    initial_plan = context.plan("prod", auto_apply=True, no_prompts=True)
    initial_snapshot = initial_plan.new_snapshots[0]

    context.upsert_model(_schema_migration_model(model_name, "INT", id_type="VARCHAR(64)"))
    supported_plan = context.plan(
        "prod",
        forward_only=True,
        auto_apply=True,
        no_prompts=True,
    )
    supported_snapshot = supported_plan.new_snapshots[0]

    assert supported_snapshot.table_name() == initial_snapshot.table_name()
    physical_table = exp.to_table(supported_snapshot.table_name())
    assert context.engine_adapter.columns(physical_table)["id"] == exp.DataType.build(
        "VARCHAR(64)", dialect="starrocks"
    )


@pytest.mark.parametrize(
    "table_properties",
    [
        "PRIMARY KEY(id) DISTRIBUTED BY HASH(id) ORDER BY(value)",
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(value)",
    ],
    ids=["primary-key-sort", "distribution"],
)
def test_alter_restricted_varchar_column_to_string(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
    table_properties: str,
):
    table = ctx.table("VARCHAR_TO_STRING_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} (id INT NOT NULL, value VARCHAR(32) NOT NULL) "
        f"ENGINE=OLAP {table_properties}"
    )
    _alter_columns(
        engine_adapter,
        table,
        {"id": "INT", "value": "VARCHAR(32)"},
        {"id": "INT", "value": "STRING"},
    )

    assert engine_adapter.columns(table)["value"] == exp.DataType.build(
        "VARCHAR(65533)", dialect="starrocks"
    )


def test_alter_partition_column_varchar_length_increase(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    table = ctx.table("PARTITION_VARCHAR_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} (id INT NOT NULL, value VARCHAR(32) NOT NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id) "
        "PARTITION BY LIST(value) (PARTITION p_a VALUES IN ('a')) "
        "DISTRIBUTED BY HASH(id)"
    )
    _alter_columns(
        engine_adapter,
        table,
        {"id": "INT", "value": "VARCHAR(32)"},
        {"id": "INT", "value": "VARCHAR(64)"},
    )

    assert engine_adapter.columns(table)["value"] == exp.DataType.build(
        "VARCHAR(64)", dialect="starrocks"
    )


def test_context_rebuilds_rejected_schema_migration(ctx: TestContext):
    model_name = ctx.table("PLAN_REJECTED_SCHEMA_MIGRATION")
    context = ctx.create_context()

    context.upsert_model(_schema_migration_model(model_name, "INT"))
    initial_plan = context.plan("prod", auto_apply=True, no_prompts=True)
    initial_snapshot = initial_plan.new_snapshots[0]

    context.upsert_model(_schema_migration_model(model_name, "INT", id_type="BIGINT"))
    with pytest.raises(PlanError, match="requires a new physical table"):
        context.plan("prod", forward_only=True, no_prompts=True)

    rebuilt_plan = context.plan("prod", auto_apply=True, no_prompts=True)
    rebuilt_snapshot = rebuilt_plan.new_snapshots[0]

    assert rebuilt_snapshot.table_name() != initial_snapshot.table_name()
    physical_table = exp.to_table(rebuilt_snapshot.table_name())
    assert context.engine_adapter.columns(physical_table)["id"].is_type(exp.DataType.Type.BIGINT)


def test_context_rebuilds_primary_key_sort_column_change(ctx: TestContext):
    model_name = ctx.table("PLAN_PRIMARY_KEY_SORT_SCHEMA_MIGRATION")
    context = ctx.create_context()

    context.upsert_model(_schema_migration_model(model_name, "INT", order_by="value"))
    initial_plan = context.plan("prod", auto_apply=True, no_prompts=True)
    initial_snapshot = initial_plan.new_snapshots[0]

    context.upsert_model(_schema_migration_model(model_name, "BIGINT", order_by="value"))
    with pytest.raises(PlanError, match="requires a new physical table"):
        context.plan("prod", forward_only=True, no_prompts=True)

    rebuilt_plan = context.plan("prod", auto_apply=True, no_prompts=True)
    rebuilt_snapshot = rebuilt_plan.new_snapshots[0]

    assert rebuilt_snapshot.table_name() != initial_snapshot.table_name()
    physical_table = exp.to_table(rebuilt_snapshot.table_name())
    assert context.engine_adapter.columns(physical_table)["value"].is_type(exp.DataType.Type.BIGINT)


def test_alter_supported_duplicate_key_column(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    table = ctx.table("DUPLICATE_KEY_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} (id INT NOT NULL, key_value INT NOT NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id, key_value) DISTRIBUTED BY HASH(id)"
    )
    _alter_columns(
        engine_adapter,
        table,
        {"id": "INT", "key_value": "INT"},
        {"id": "INT", "key_value": "BIGINT"},
    )

    assert engine_adapter.columns(table)["key_value"].is_type(exp.DataType.Type.BIGINT)


def test_alter_rejects_implicit_range_distribution_sort_key_change(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
    range_distribution_enabled: None,
):
    table = ctx.table("RANGE_DISTRIBUTION_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} (id INT NOT NULL, value INT NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id) "
        "PROPERTIES ('fast_schema_evolution'='true')"
    )

    assert engine_adapter._get_table_distribution_type(table) == "RANGE"

    with pytest.raises(MigrationNotSupportedError, match="range distribution sort key"):
        _alter_columns(
            engine_adapter,
            table,
            {"id": "INT", "value": "INT"},
            {"id": "BIGINT", "value": "INT"},
        )


def test_alter_allows_implicit_range_distribution_varchar_widening(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
    range_distribution_enabled: None,
):
    table = ctx.table("RANGE_DISTRIBUTION_VARCHAR_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} (id VARCHAR(32) NOT NULL, value INT NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id) "
        "PROPERTIES ('fast_schema_evolution'='true')"
    )

    assert engine_adapter._get_table_distribution_type(table) == "RANGE"
    _alter_columns(
        engine_adapter,
        table,
        {"id": "VARCHAR(32)", "value": "INT"},
        {"id": "VARCHAR(64)", "value": "INT"},
    )
    assert engine_adapter.columns(table)["id"] == exp.DataType.build(
        "VARCHAR(64)", dialect="starrocks"
    )


def test_alter_supported_varbinary_duplicate_key_column(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    table = ctx.table("VARBINARY_DUPLICATE_KEY_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} "
        "(id INT NOT NULL, key_value VARBINARY(32) NOT NULL, value INT NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id, key_value) DISTRIBUTED BY HASH(id)"
    )
    _alter_columns(
        engine_adapter,
        table,
        {
            "id": "INT",
            "key_value": "VARBINARY(32)",
            "value": "INT",
        },
        {
            "id": "INT",
            "key_value": "VARBINARY(64)",
            "value": "INT",
        },
    )

    assert engine_adapter.columns(table)["key_value"] == exp.DataType.build(
        "VARBINARY(64)", dialect="starrocks"
    )


def test_alter_supported_nonleading_duplicate_sort_column(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    table = ctx.table("DUPLICATE_SORT_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} (id INT NOT NULL, value INT NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) ORDER BY(id, value)"
    )
    _alter_columns(
        engine_adapter,
        table,
        {"id": "INT", "value": "INT"},
        {"id": "INT", "value": "DOUBLE"},
    )

    assert engine_adapter.columns(table)["value"].is_type(exp.DType.DOUBLE)


def test_alter_uses_starrocks_413_parameterized_conversion_rules(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    table = ctx.table("PARAMETERIZED_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} "
        "(id INT NOT NULL, integer_value BIGINT, decimal_value DECIMAL32(9, 2), "
        "decimal_string_value DECIMAL32(9, 2), binary_value VARBINARY(32), "
        "bare_decimal_value DECIMAL(9, 0), bare_binary_value VARBINARY(32)) "
        "ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)"
    )
    engine_adapter.execute(
        f"INSERT INTO {table_sql} (id, bare_decimal_value, bare_binary_value) VALUES (1, 9, '')"
    )
    _alter_columns(
        engine_adapter,
        table,
        {
            "id": "INT",
            "integer_value": "BIGINT",
            "decimal_value": "DECIMAL32(9, 2)",
            "decimal_string_value": "DECIMAL32(9, 2)",
            "binary_value": "VARBINARY(32)",
            "bare_decimal_value": "DECIMAL(9, 0)",
            "bare_binary_value": "VARBINARY(32)",
        },
        {
            "id": "INT",
            "integer_value": "LARGEINT",
            "decimal_value": "DECIMAL64(12, 4)",
            "decimal_string_value": "VARCHAR(32)",
            "binary_value": "VARBINARY(64)",
            "bare_decimal_value": "DECIMAL",
            "bare_binary_value": "VARBINARY(1048576)",
        },
    )

    columns = engine_adapter.columns(table)
    assert columns["integer_value"].is_type(exp.DType.INT128)
    assert columns["decimal_value"] == exp.DataType.build("DECIMAL(12, 4)", dialect="starrocks")
    assert columns["decimal_string_value"] == exp.DataType.build("VARCHAR(32)", dialect="starrocks")
    assert columns["binary_value"] == exp.DataType.build("VARBINARY(64)", dialect="starrocks")
    assert columns["bare_decimal_value"] == exp.DataType.build(
        "DECIMAL(10, 0)", dialect="starrocks"
    )
    assert columns["bare_binary_value"] == exp.DataType.build(
        "VARBINARY(1048576)", dialect="starrocks"
    )
    assert engine_adapter.fetchone(
        f"SELECT bare_decimal_value, length(bare_binary_value) FROM {table_sql}"
    ) == (9, 0)


def test_alter_verifies_partial_column_schema(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    table = ctx.table("PARTIAL_COLUMN_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} (id INT NOT NULL, value VARCHAR(32) NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)"
    )

    _alter_columns(
        engine_adapter,
        table,
        {"value": "VARCHAR(32)"},
        {"value": "VARCHAR(64)"},
    )

    assert engine_adapter.columns(table) == {
        "id": exp.DataType.build("INT"),
        "value": exp.DataType.build("VARCHAR(64)"),
    }


def test_json_to_varchar_uses_starrocks_413_minimum_length(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    rejected_table = ctx.table("JSON_VARCHAR_1023_SCHEMA_MIGRATION")
    rejected_table_sql = rejected_table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {rejected_table_sql} (id INT NOT NULL, value JSON NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)"
    )

    with pytest.raises(ProgrammingError, match="JSON needs minimum length of 1024"):
        engine_adapter.execute(
            f"ALTER TABLE {rejected_table_sql} MODIFY COLUMN value VARCHAR(1023) NULL"
        )

    supported_table = ctx.table("JSON_VARCHAR_1024_SCHEMA_MIGRATION")
    supported_table_sql = supported_table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {supported_table_sql} (id INT NOT NULL, value JSON NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)"
    )
    _alter_columns(
        engine_adapter,
        supported_table,
        {"id": "INT", "value": "JSON"},
        {"id": "INT", "value": "VARCHAR(1024)"},
    )

    assert engine_adapter.columns(supported_table)["value"] == exp.DataType.build(
        "VARCHAR(1024)", dialect="starrocks"
    )


def test_alter_rejects_non_key_target_for_duplicate_key(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    table = ctx.table("DUPLICATE_KEY_INVALID_TARGET")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} (id INT NOT NULL, key_value INT NOT NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id, key_value) DISTRIBUTED BY HASH(id)"
    )
    with pytest.raises(MigrationNotSupportedError, match="duplicate key"):
        _alter_columns(
            engine_adapter,
            table,
            {"id": "INT", "key_value": "INT"},
            {"id": "INT", "key_value": "DOUBLE"},
        )


def test_alter_rejects_colocated_distribution_varchar_growth(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    table = ctx.table("COLOCATED_DISTRIBUTION_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} (id INT NOT NULL, value VARCHAR(32) NOT NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(value) "
        f"PROPERTIES ('colocate_with'='{ctx.add_test_suffix('schema_migration_group')}')"
    )
    with pytest.raises(MigrationNotSupportedError, match="colocated distribution"):
        _alter_columns(
            engine_adapter,
            table,
            {"id": "INT", "value": "VARCHAR(32)"},
            {"id": "INT", "value": "VARCHAR(64)"},
        )


def test_alter_gin_indexed_column_type(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
    experimental_gin_enabled: None,
):
    table = ctx.table("GIN_INDEX_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} (id INT NOT NULL, value VARCHAR(32) NULL, "
        "INDEX gin_idx (value) USING GIN) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)"
    )

    with pytest.raises(MigrationNotSupportedError, match="gin index"):
        _alter_columns(
            engine_adapter,
            table,
            {"id": "INT", "value": "VARCHAR(32)"},
            {"id": "INT", "value": "INT"},
        )

    _alter_columns(
        engine_adapter,
        table,
        {"id": "INT", "value": "VARCHAR(32)"},
        {"id": "INT", "value": "VARCHAR(64)"},
    )

    assert engine_adapter.columns(table)["value"] == exp.DataType.build(
        "VARCHAR(64)", dialect="starrocks"
    )


def test_alter_validates_rollup_column_role(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    table = ctx.table("ROLLUP_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} (id INT NOT NULL, value INT NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) "
        "ROLLUP (rollup_value (id, value))"
    )
    with pytest.raises(MigrationNotSupportedError, match="rollup key"):
        _alter_columns(
            engine_adapter,
            table,
            {"id": "INT", "value": "INT"},
            {"id": "INT", "value": "DOUBLE"},
        )

    _alter_columns(
        engine_adapter,
        table,
        {"id": "INT", "value": "INT"},
        {"id": "INT", "value": "BIGINT"},
    )
    assert engine_adapter.columns(table)["value"].is_type(exp.DType.BIGINT)


def test_alter_rejects_rollup_value_column(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    table = ctx.table("ROLLUP_VALUE_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} (id INT NOT NULL, value INT NULL) "
        "ENGINE=OLAP UNIQUE KEY(id) DISTRIBUTED BY HASH(id) ROLLUP (r (id, value))"
    )
    with pytest.raises(MigrationNotSupportedError, match="rollup value"):
        _alter_columns(
            engine_adapter,
            table,
            {"id": "INT", "value": "INT"},
            {"id": "INT", "value": "BIGINT"},
        )


def test_alter_rejects_synchronous_mv_definition_dependencies(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    table = ctx.table("SYNC_MV_DEPENDENCY_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    mv = exp.table_(ctx.add_test_suffix("sync_mv_dependency"), db=table.db)
    mv_sql = mv.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} (id INT NOT NULL, group_col INT NOT NULL, "
        "amount INT NULL, predicate_col INT NULL) ENGINE=OLAP DUPLICATE KEY(id) "
        "DISTRIBUTED BY HASH(id)"
    )
    engine_adapter.execute(
        f"CREATE MATERIALIZED VIEW {mv_sql} AS SELECT group_col, "
        f"SUM(amount) AS total_amount FROM {table_sql} WHERE predicate_col > 0 "
        "GROUP BY group_col"
    )
    _wait_for_synchronous_mv(engine_adapter, table)

    _, rollup_names = engine_adapter._get_rollup_metadata(table)
    assert engine_adapter._get_rollup_dependency_columns(table, rollup_names) == {
        "group_col",
        "amount",
        "predicate_col",
    }
    current = {
        "id": "INT",
        "group_col": "INT",
        "amount": "INT",
        "predicate_col": "INT",
    }
    for dependency in ("group_col", "amount", "predicate_col"):
        target = {**current, dependency: "BIGINT"}
        with pytest.raises(MigrationNotSupportedError, match="rollup dependency"):
            _alter_columns(engine_adapter, table, current, target)

    assert all(
        engine_adapter.columns(table)[dependency].is_type(exp.DType.INT)
        for dependency in ("group_col", "amount", "predicate_col")
    )


def test_generated_column_table_definition_fails_closed(
    engine_adapter: StarRocksEngineAdapter, ctx: TestContext
):
    table = ctx.table("GENERATED_COLUMN_SCHEMA_MIGRATION")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"""CREATE TABLE {table_sql} (
          `id` INT NOT NULL,
          `source` INT NULL,
          `generated` BIGINT NULL AS `source` * 2
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")"""
    )

    with pytest.raises(MigrationNotSupportedError, match="Unable to parse"):
        engine_adapter._get_table_definition(table)


def test_create_database(ctx: TestContext):
    """Test creating a database (StarRocks uses DATABASE instead of SCHEMA)."""
    db_name = ctx.add_test_suffix("test_db")
    try:
        ctx.engine_adapter.create_schema(db_name)
        # Verify database was created
        result = ctx.engine_adapter.fetchone(
            f"SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '{db_name}'"
        )
        assert result is not None
        assert result[0] == db_name
    finally:
        ctx.engine_adapter.drop_schema(db_name, ignore_if_not_exists=True)


def test_create_table(ctx: TestContext):
    """Test creating a table in StarRocks."""
    table = ctx.table("TEST_CREATE_TABLE")
    columns_to_types = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("VARCHAR(100)"),
        "created_at": exp.DataType.build("DATETIME"),
    }

    ctx.engine_adapter.create_table(
        table,
        columns_to_types,
    )

    # Verify table exists
    assert ctx.engine_adapter.table_exists(table)

    # Verify columns
    columns = ctx.engine_adapter.columns(table)
    assert "id" in columns
    assert "name" in columns
    assert "created_at" in columns


def test_ctas(ctx: TestContext):
    """Test CREATE TABLE AS SELECT in StarRocks."""
    source_table = ctx.table("CTAS_SOURCE")
    target_table = ctx.table("CTAS_TARGET")

    # Create source table
    ctx.engine_adapter.create_table(
        source_table,
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("VARCHAR(50)")},
    )

    # Insert test data
    ctx.engine_adapter.execute(
        f"INSERT INTO {source_table.sql(dialect='starrocks')} VALUES (1, 'test')"
    )

    # Create target table using CTAS
    query = exp.select("id", "value").from_(source_table)
    ctx.engine_adapter.ctas(
        target_table,
        query,
        columns_to_types={
            "id": exp.DataType.build("INT"),
            "value": exp.DataType.build("VARCHAR(50)"),
        },
    )

    # Verify target table exists and has data
    assert ctx.engine_adapter.table_exists(target_table)
    result = ctx.engine_adapter.fetchone(
        f"SELECT COUNT(*) FROM {target_table.sql(dialect='starrocks')}"
    )
    assert result is not None and result[0] == 1


def test_insert_overwrite(ctx: TestContext):
    """Test INSERT OVERWRITE behavior in StarRocks."""
    table = ctx.table("INSERT_OVERWRITE_TEST")

    # Create table
    ctx.engine_adapter.create_table(
        table,
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("VARCHAR(50)")},
    )

    # Insert initial data
    ctx.engine_adapter.execute(
        f"INSERT INTO {table.sql(dialect='starrocks')} VALUES (1, 'initial')"
    )

    # Verify initial data
    result = ctx.engine_adapter.fetchone(f"SELECT COUNT(*) FROM {table.sql(dialect='starrocks')}")
    assert result is not None and result[0] == 1


def test_table_comments(ctx: TestContext):
    """Test table and column comments in StarRocks."""
    table = ctx.table("COMMENTS_TEST")
    table_comment = "This is a test table"
    column_comment = "This is the ID column"

    ctx.engine_adapter.create_table(
        table,
        {"id": exp.DataType.build("INT"), "name": exp.DataType.build("VARCHAR(100)")},
        table_description=table_comment,
        column_descriptions={"id": column_comment},
    )

    assert ctx.engine_adapter.table_exists(table)


def test_view_creation(ctx: TestContext):
    """Test VIEW creation in StarRocks."""
    source_table = ctx.table("VIEW_SOURCE")
    view = ctx.table("TEST_VIEW")

    # Create source table
    ctx.engine_adapter.create_table(
        source_table,
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("VARCHAR(50)")},
    )

    # Create view
    query = exp.select("id", "value").from_(source_table)
    ctx.engine_adapter.create_view(view, query)

    # Verify view exists
    assert ctx.engine_adapter.table_exists(view)


def test_drop_table(ctx: TestContext):
    """Test dropping a table in StarRocks."""
    table = ctx.table("DROP_TEST")

    # Create table
    ctx.engine_adapter.create_table(
        table,
        {"id": exp.DataType.build("INT")},
    )
    assert ctx.engine_adapter.table_exists(table)

    # Drop table
    ctx.engine_adapter.drop_table(table)
    assert not ctx.engine_adapter.table_exists(table)


def test_columns_types(ctx: TestContext):
    """Test various column types supported by StarRocks."""
    table = ctx.table("COLUMN_TYPES_TEST")

    columns_to_types = {
        "bool_col": exp.DataType.build("BOOLEAN"),
        "tinyint_col": exp.DataType.build("TINYINT"),
        "smallint_col": exp.DataType.build("SMALLINT"),
        "int_col": exp.DataType.build("INT"),
        "bigint_col": exp.DataType.build("BIGINT"),
        "float_col": exp.DataType.build("FLOAT"),
        "double_col": exp.DataType.build("DOUBLE"),
        "decimal_col": exp.DataType.build("DECIMAL(10, 2)"),
        "varchar_col": exp.DataType.build("VARCHAR(255)"),
        "date_col": exp.DataType.build("DATE"),
        "datetime_col": exp.DataType.build("DATETIME"),
    }

    ctx.engine_adapter.create_table(table, columns_to_types)
    assert ctx.engine_adapter.table_exists(table)

    # Verify columns were created
    columns = ctx.engine_adapter.columns(table)
    for col_name in columns_to_types.keys():
        assert col_name in columns, f"Column {col_name} not found in table"
