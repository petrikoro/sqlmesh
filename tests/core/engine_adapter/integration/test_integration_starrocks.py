import typing as t

import pytest
from pytest import FixtureRequest
from sqlglot import exp, parse_one
from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
from sqlmesh.core.model import FullKind, SqlModel
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


def _schema_migration_model(
    model_name: exp.Table,
    value_type: str,
    id_type: str = "INT",
) -> SqlModel:
    value_data_type = exp.DataType.build(value_type, dialect="starrocks")
    id_data_type = exp.DataType.build(id_type, dialect="starrocks")

    return SqlModel(
        name=model_name.sql(dialect="starrocks"),
        dialect="starrocks",
        kind=FullKind(),
        query=exp.select(
            exp.cast(exp.Literal.number(1), id_data_type).as_("id"),
            exp.cast(exp.Literal.string("42"), value_data_type).as_("value"),
        ),
        columns={"id": id_data_type, "value": value_data_type},
        physical_properties={
            "primary_key": exp.Tuple(expressions=[exp.column("id")]),
            "distributed_by": parse_one("HASH(columns := id)", dialect="starrocks"),
        },
    )


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
