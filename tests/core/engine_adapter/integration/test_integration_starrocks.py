import typing as t

import pytest
from pytest import FixtureRequest
from sqlglot import exp, parse_one
from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
from sqlmesh.core.model import FullKind, SqlModel
from sqlmesh.core.schema_diff import TableAlterOperation
from sqlmesh.core.snapshot import SnapshotChangeCategory
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


def _add_column_model(model_name: exp.Table, include_value: bool) -> SqlModel:
    projections = [exp.cast(exp.Literal.number(1), "INT").as_("id")]
    columns = {"id": exp.DataType.build("INT")}
    if include_value:
        projections.append(exp.cast(exp.Literal.string("value"), "STRING").as_("value"))
        columns["value"] = exp.DataType.build("STRING", dialect="starrocks")

    return SqlModel(
        name=model_name.sql(dialect="starrocks"),
        dialect="starrocks",
        kind=FullKind(),
        query=exp.select(*projections),
        columns=columns,
        physical_properties={
            "primary_key": exp.Tuple(expressions=[exp.column("id")]),
            "distributed_by": parse_one("HASH(columns := id)", dialect="starrocks"),
        },
    )


def _schema_migration_parent_model(model_name: exp.Table, value_type: str) -> SqlModel:
    value_data_type = exp.DataType.build(value_type, dialect="starrocks")
    return SqlModel(
        name=model_name.sql(dialect="starrocks"),
        dialect="starrocks",
        kind="VIEW",
        query=exp.select(
            exp.cast(exp.Literal.string("vacancy"), value_data_type).as_("vacancy_name")
        ),
        columns={"vacancy_name": value_data_type},
    )


def _schema_migration_child_model(model_name: exp.Table, parent_name: exp.Table) -> SqlModel:
    return SqlModel(
        name=model_name.sql(dialect="starrocks"),
        dialect="starrocks",
        kind=FullKind(),
        query=exp.select("vacancy_name").from_(parent_name),
    )


def _metadata_parent_model(model_name: exp.Table, with_audit: bool) -> SqlModel:
    return SqlModel(
        name=model_name.sql(dialect="starrocks"),
        dialect="starrocks",
        kind=FullKind(),
        query=exp.select(
            exp.cast(exp.Literal.string("vacancy"), "STRING").as_("vacancy_name")
        ),
        columns={"vacancy_name": exp.DataType.build("STRING", dialect="starrocks")},
        audits=[("not_null", {"columns": exp.column("vacancy_name")})]
        if with_audit
        else [],
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


def test_engine_adapter(ctx: TestContext):
    """Test basic connectivity to StarRocks."""
    assert isinstance(ctx.engine_adapter, StarRocksEngineAdapter)
    assert ctx.engine_adapter.fetchone("SELECT 1") == (1,)


def test_engine_adapter_dialect(ctx: TestContext):
    """Test that the dialect is correctly set to starrocks."""
    assert ctx.engine_adapter.dialect == "starrocks"


def test_forward_only_plan_applies_native_add_column(ctx: TestContext):
    model_name = ctx.table("PLAN_ADD_COLUMN")
    context = ctx.create_context()

    context.upsert_model(_add_column_model(model_name, include_value=False))
    initial_plan = context.plan("prod", auto_apply=True, no_prompts=True)
    initial_snapshot = initial_plan.new_snapshots[0]

    context.upsert_model(_add_column_model(model_name, include_value=True))
    additive_plan = context.plan(
        "prod",
        forward_only=True,
        auto_apply=True,
        no_prompts=True,
    )
    additive_snapshot = additive_plan.new_snapshots[0]

    assert additive_snapshot.table_name() == initial_snapshot.table_name()
    assert context.engine_adapter.columns(exp.to_table(additive_snapshot.table_name())) == {
        "id": exp.DataType.build("INT"),
        "value": exp.DataType.build("VARCHAR(65533)", dialect="starrocks"),
    }


def test_physical_string_alias_requires_no_schema_migration(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    table = ctx.table("PHYSICAL_STRING_ALIAS")
    target_table = ctx.table("PHYSICAL_STRING_ALIAS_TARGET")
    engine_adapter.execute(
        f"CREATE TABLE {table.sql(dialect='starrocks', identify=True)} "
        "(id INT NOT NULL, value TEXT NULL) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)"
    )
    engine_adapter.execute(
        f"CREATE TABLE {target_table.sql(dialect='starrocks', identify=True)} "
        "(id INT NOT NULL, value STRING NULL) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)"
    )
    live_columns = engine_adapter.columns(table)

    assert live_columns["value"] == exp.DataType.build("VARCHAR(65533)", dialect="starrocks")
    assert engine_adapter.get_alter_operations(table, target_table) == []


def test_unsupported_type_change_preserves_existing_table(
    engine_adapter: StarRocksEngineAdapter,
    ctx: TestContext,
):
    table = ctx.table("UNSUPPORTED_TYPE_CHANGE")
    table_sql = table.sql(dialect="starrocks", identify=True)
    engine_adapter.execute(
        f"CREATE TABLE {table_sql} "
        "(id INT NOT NULL, value VARCHAR(1048576) NULL) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)"
    )
    engine_adapter.execute(f"INSERT INTO {table_sql} VALUES (1, 'preserved')")

    with pytest.raises(MigrationNotSupportedError, match="new physical table version"):
        _alter_columns(
            engine_adapter,
            table,
            {"id": "INT", "value": "VARCHAR(1048576)"},
            {"id": "INT", "value": "STRING"},
        )

    assert engine_adapter.columns(table)["value"] == exp.DataType.build(
        "VARCHAR(1048576)", dialect="starrocks"
    )
    assert engine_adapter.fetchall(f"SELECT id, value FROM {table_sql}") == ((1, "preserved"),)


def test_non_forward_plan_rebuilds_indirect_duplicate_key_type_change(ctx: TestContext):
    parent_name = ctx.table("PLAN_SCHEMA_MIGRATION_PARENT")
    child_name = ctx.table("PLAN_SCHEMA_MIGRATION_CHILD")
    context = ctx.create_context()

    context.upsert_model(_schema_migration_parent_model(parent_name, "VARCHAR(1048576)"))
    context.upsert_model(_schema_migration_child_model(child_name, parent_name))
    initial_plan = context.plan("prod", auto_apply=True, no_prompts=True)
    engine_adapter = context.engine_adapter
    assert isinstance(engine_adapter, StarRocksEngineAdapter)
    initial_snapshot = next(
        snapshot for snapshot in initial_plan.new_snapshots if snapshot.name == child_name.sql()
    )
    initial_table = exp.to_table(initial_snapshot.table_name())
    assert engine_adapter.columns(initial_table)["vacancy_name"] == exp.DataType.build(
        "VARCHAR(1048576)", dialect="starrocks"
    )
    show_table = initial_table.copy()
    show_table.set("catalog", None)
    show_create = engine_adapter.fetchone(
        f"SHOW CREATE TABLE {show_table.sql(dialect='starrocks', identify=True)}"
    )
    assert show_create and "DUPLICATE KEY(`vacancy_name`)" in str(show_create[1])

    context.upsert_model(_schema_migration_parent_model(parent_name, "STRING"))
    with pytest.raises(PlanError, match="requires a new physical table"):
        context.plan("prod", forward_only=True, no_prompts=True)

    plan_builder = context.plan_builder("prod")
    parent_snapshot, _ = plan_builder._context_diff.modified_snapshots[parent_name.sql()]
    plan_builder.set_choice(parent_snapshot, SnapshotChangeCategory.NON_BREAKING)
    rebuilt_plan = plan_builder.build()
    rebuilt_snapshot = next(
        snapshot for snapshot in rebuilt_plan.new_snapshots if snapshot.name == child_name.sql()
    )

    assert rebuilt_snapshot.change_category == SnapshotChangeCategory.NON_BREAKING
    assert rebuilt_snapshot.table_name() != initial_snapshot.table_name()

    context.apply(rebuilt_plan)
    rebuilt_table = exp.to_table(rebuilt_snapshot.table_name())
    assert engine_adapter.columns(rebuilt_table)["vacancy_name"] == exp.DataType.build(
        "VARCHAR(65533)", dialect="starrocks"
    )
    assert engine_adapter.fetchall(
        f"SELECT vacancy_name FROM {rebuilt_table.sql(dialect='starrocks', identify=True)}"
    ) == (("vacancy",),)


def test_metadata_only_plan_skips_schema_migration(ctx: TestContext):
    parent_name = ctx.table("PLAN_METADATA_PARENT")
    child_name = ctx.table("PLAN_METADATA_CHILD")
    context = ctx.create_context()

    context.upsert_model(_metadata_parent_model(parent_name, with_audit=False))
    context.upsert_model(_schema_migration_child_model(child_name, parent_name))
    initial_plan = context.plan("prod", auto_apply=True, no_prompts=True)
    initial_child = next(
        snapshot for snapshot in initial_plan.new_snapshots if snapshot.name == child_name.sql()
    )
    child_table = exp.to_table(initial_child.table_name())
    child_table_sql = child_table.sql(dialect="starrocks", identify=True)

    context.engine_adapter.drop_table(child_table)
    context.engine_adapter.execute(
        f"CREATE TABLE {child_table_sql} "
        "(vacancy_name VARCHAR(1048576) NOT NULL) ENGINE=OLAP "
        "DUPLICATE KEY(vacancy_name) DISTRIBUTED BY HASH(vacancy_name)"
    )
    context.engine_adapter.execute(f"INSERT INTO {child_table_sql} VALUES ('preserved')")

    context.upsert_model(_metadata_parent_model(parent_name, with_audit=True))
    metadata_plan = context.plan("prod", no_prompts=True)
    metadata_child = next(
        snapshot for snapshot in metadata_plan.new_snapshots if snapshot.name == child_name.sql()
    )

    assert metadata_child.change_category == SnapshotChangeCategory.METADATA
    assert metadata_child.table_name() == initial_child.table_name()

    context.apply(metadata_plan)

    assert context.engine_adapter.columns(child_table)["vacancy_name"] == exp.DataType.build(
        "VARCHAR(1048576)", dialect="starrocks"
    )
    assert context.engine_adapter.fetchall(f"SELECT vacancy_name FROM {child_table_sql}") == (
        ("preserved",),
    )


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
