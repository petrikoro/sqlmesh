import typing as t
from dataclasses import replace
from unittest.mock import Mock

import pytest
import sqlglot
from pytest_mock import MockerFixture
from sqlglot import exp

from sqlmesh.core.engine_adapter.shared import DataObjectType
from sqlmesh.core.engine_adapter.starrocks import (
    StarRocksEngineAdapter,
    _strip_index_implementation_properties,
)
from sqlmesh.core.model import FullKind, SqlModel
from sqlmesh.core.schema_diff import (
    AlterColumnTypeSupport,
    SchemaDiffer,
    TableAlterAddColumnOperation,
    TableAlterChangeColumnTypeOperation,
    TableAlterDropColumnOperation,
    TableAlterOperation,
)
from sqlmesh.utils.errors import MigrationNotSupportedError, SQLMeshError
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.starrocks]


@pytest.fixture
def adapter(
    make_mocked_engine_adapter: t.Callable,
    mocker: MockerFixture,
) -> StarRocksEngineAdapter:
    adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
    mocker.patch.object(
        adapter,
        "_get_rollup_metadata",
        return_value=({"rollup key": set(), "rollup value": set()}, set()),
    )
    mocker.patch.object(adapter, "_get_rollup_dependency_columns", return_value=set())
    mocker.patch.object(adapter, "_get_table_distribution_type", return_value="RANDOM")
    return adapter


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


def value_type_change(
    adapter: StarRocksEngineAdapter,
    current_type: str,
    target_type: str,
) -> t.List[TableAlterOperation]:
    return t.cast(
        t.List[TableAlterOperation],
        adapter.schema_differ.compare_columns(
            "test_table",
            {
                "id": exp.DataType.build("INT", dialect="starrocks"),
                "value": exp.DataType.build(current_type, dialect="starrocks"),
            },
            {
                "id": exp.DataType.build("INT", dialect="starrocks"),
                "value": exp.DataType.build(target_type, dialect="starrocks"),
            },
        ),
    )


def value_type_change_operation(
    adapter: StarRocksEngineAdapter,
    current_type: str,
    target_type: str,
) -> TableAlterChangeColumnTypeOperation:
    operation = value_type_change(adapter, current_type, target_type)[0]
    assert isinstance(operation, TableAlterChangeColumnTypeOperation)
    return operation


def mock_live_columns(
    adapter: StarRocksEngineAdapter,
    *schemas: t.Dict[str, exp.DataType],
) -> Mock:
    columns = Mock(side_effect=schemas)
    setattr(adapter, "columns", columns)
    return columns


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


def test_get_table_definition_parses_gin_implementation_options(
    adapter: StarRocksEngineAdapter,
):
    adapter.cursor.fetchone.return_value = (
        "test_table",
        """CREATE TABLE test_table (
            id INT NOT NULL,
            value VARCHAR(32) NULL DEFAULT "USING GIN(foo)"
                COMMENT "keep USING VECTOR(bar)",
            INDEX gin_idx (value) USING GIN("imp_lib" = "clucene", "parser" = "none")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id)""",
    )

    table_definition = adapter._get_table_definition(exp.to_table("test_table"))
    value_column = next(
        column
        for column in table_definition.this.expressions
        if isinstance(column, exp.ColumnDef) and column.name == "value"
    )
    default = value_column.find(exp.DefaultColumnConstraint)
    comment = value_column.find(exp.CommentColumnConstraint)

    assert adapter._parse_column_roles(table_definition)["gin index"] == {"value"}
    assert default and default.this.name == "USING GIN(foo)"
    assert comment and comment.this.name == "keep USING VECTOR(bar)"


def test_get_table_definition_parses_ngram_implementation_options(
    adapter: StarRocksEngineAdapter,
):
    adapter.cursor.fetchone.return_value = (
        "test_table",
        """CREATE TABLE test_table (
            id INT NOT NULL,
            value VARCHAR(64) NULL,
            INDEX ngram_idx (`value`) USING NGRAMBF(
                "bloom_filter_fpp" = "0.05",
                "case_sensitive" = "true",
                "gram_num" = "4"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id)""",
    )

    table_definition = adapter._get_table_definition(exp.to_table("test_table"))
    index = table_definition.find(exp.IndexColumnConstraint)

    assert index
    assert index.expressions[0].name == "value"
    assert any(option.args.get("using") == "NGRAMBF" for option in index.args["options"])


def test_get_table_distribution_type_uses_live_metadata(
    adapter: StarRocksEngineAdapter,
):
    adapter.cursor.fetchone.return_value = ("range",)

    distribution_type = StarRocksEngineAdapter._get_table_distribution_type(
        adapter, exp.to_table("test_db.test_table")
    )

    assert distribution_type == "RANGE"
    assert to_sql_calls(adapter) == [
        "SELECT DISTRIBUTE_TYPE FROM information_schema.tables_config "
        "WHERE TABLE_NAME = 'test_table' AND TABLE_SCHEMA = 'test_db'"
    ]


def test_get_table_distribution_type_rejects_missing_metadata(
    adapter: StarRocksEngineAdapter,
):
    adapter.cursor.fetchone.return_value = None

    with pytest.raises(MigrationNotSupportedError, match="Unable to determine"):
        StarRocksEngineAdapter._get_table_distribution_type(
            adapter, exp.to_table("test_db.test_table")
        )


def test_strip_index_implementation_properties_preserves_sql_comments():
    definition = """CREATE TABLE test_table (
        id INT NOT NULL,
        -- INDEX commented_line (id) USING GIN("parser" = "none")
        # INDEX commented_hash (id) USING VECTOR("index_type" = "hnsw"),
        /* INDEX commented_block (id) USING NGRAMBF("gram_num" = "4") */
        INDEX real_idx (id) USING GIN("parser" = "none")
    ) ENGINE=OLAP"""

    normalized = _strip_index_implementation_properties(definition)

    assert '-- INDEX commented_line (id) USING GIN("parser" = "none")' in normalized
    assert '# INDEX commented_hash (id) USING VECTOR("index_type" = "hnsw")' in normalized
    assert '/* INDEX commented_block (id) USING NGRAMBF("gram_num" = "4") */' in normalized
    assert "INDEX real_idx (id) USING GIN" in normalized
    assert 'INDEX real_idx (id) USING GIN("parser" = "none")' not in normalized


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

    current = {"id": exp.DataType.build("BIGINT"), "value": exp.DataType.build("INT")}
    target = {"id": current["id"], "value": exp.DataType.build("BIGINT")}
    operations = adapter.schema_differ.compare_columns("test_table", current, target)
    assert len(operations) == 1
    assert isinstance(operations[0], TableAlterChangeColumnTypeOperation)
    assert not operations[0].is_destructive
    mock_live_columns(adapter, current, target)

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
    ("table_properties", "role"),
    [
        ("UNIQUE KEY(id, value) DISTRIBUTED BY HASH(id)", "unique key"),
        (
            "DUPLICATE KEY(id) PARTITION BY value DISTRIBUTED BY HASH(id)",
            "partitioning",
        ),
        ("DUPLICATE KEY(id) DISTRIBUTED BY HASH(value)", "distribution"),
        (
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) ORDER BY(id, value)",
            "sort key",
        ),
        ("DUPLICATE KEY(id, value) DISTRIBUTED BY HASH(id)", "duplicate key"),
        (
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) ROLLUP (rollup_value (id, value))",
            "rollup key",
        ),
        ("DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)", None),
    ],
    ids=[
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
    table_properties: str,
    role: t.Optional[str],
):
    current = {
        "id": exp.DataType.build("INT"),
        "value": exp.DataType.build("INT"),
    }
    target = {"id": current["id"]}
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        current,
        target,
    )
    adapter.cursor.fetchone.return_value = (
        "test_table",
        f"CREATE TABLE test_table (id INT NULL, value INT NULL) ENGINE=OLAP {table_properties}",
    )
    if role == "rollup key":
        t.cast(t.Any, adapter._get_rollup_metadata).return_value = (
            {"rollup key": {"id", "value"}, "rollup value": set()},
            {"rollup_value"},
        )

    if role is None:
        adapter.cursor.fetchall.side_effect = [[], [schema_change_job(1, "FINISHED")]]
        mock_live_columns(adapter, target)
        adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))
    else:
        with pytest.raises(MigrationNotSupportedError) as ex:
            adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))

        message = str(ex.value)
        assert role in message
        assert "ALTER TABLE `test_table`" in message


@pytest.mark.parametrize(
    ("table_properties", "is_key_column"),
    [
        ("UNIQUE KEY(id, value) DISTRIBUTED BY HASH(id)", True),
        ("DUPLICATE KEY(id, value) DISTRIBUTED BY HASH(id)", True),
        ("DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) ORDER BY(id, value)", False),
    ],
    ids=[
        "unique-key",
        "duplicate-key",
        "sort-key",
    ],
)
def test_supported_type_change_modifies_constrained_column(
    adapter: StarRocksEngineAdapter,
    table_properties: str,
    is_key_column: bool,
):
    operations = value_type_change(adapter, "INT", "BIGINT")
    adapter.cursor.fetchone.return_value = (
        "test_table",
        f"CREATE TABLE test_table (id INT NULL, value INT NOT NULL) ENGINE=OLAP {table_properties}",
    )
    adapter.cursor.fetchall.side_effect = [[], [schema_change_job(1, "FINISHED")]]
    mock_live_columns(
        adapter,
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("BIGINT")},
    )

    adapter.alter_table(operations)

    expected_key = " KEY" if is_key_column else ""
    assert f"ALTER TABLE `test_table` MODIFY COLUMN `value` BIGINT{expected_key} NOT NULL" in (
        to_sql_calls(adapter)
    )


def test_explicit_decimal_type_change_modifies_key_column(
    adapter: StarRocksEngineAdapter,
):
    operations = value_type_change(adapter, "DECIMAL32(9, 2)", "DECIMAL64(12, 4)")
    assert len(operations) == 1
    assert isinstance(operations[0], TableAlterChangeColumnTypeOperation)
    assert not operations[0].is_destructive
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value DECIMAL32(9, 2) NOT NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id, value) DISTRIBUTED BY HASH(id)",
    )
    adapter.cursor.fetchall.side_effect = [[], [schema_change_job(1, "FINISHED")]]
    mock_live_columns(
        adapter,
        {
            "id": exp.DataType.build("INT"),
            "value": exp.DataType.build("DECIMAL64(12, 4)", dialect="starrocks"),
        },
    )

    adapter.alter_table(operations)

    assert (
        "ALTER TABLE `test_table` MODIFY COLUMN `value` DECIMAL64(12, 4) KEY NOT NULL"
        in to_sql_calls(adapter)
    )


def test_varbinary_length_increase_modifies_duplicate_key_column(
    adapter: StarRocksEngineAdapter,
):
    operations = value_type_change(adapter, "VARBINARY(32)", "VARBINARY(64)")
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value VARBINARY(32) NOT NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id, value) DISTRIBUTED BY HASH(id)",
    )
    adapter.cursor.fetchall.side_effect = [[], [schema_change_job(1, "FINISHED")]]
    mock_live_columns(
        adapter,
        {
            "id": exp.DataType.build("INT"),
            "value": exp.DataType.build("VARBINARY(64)", dialect="starrocks"),
        },
    )

    adapter.alter_table(operations)

    assert (
        "ALTER TABLE `test_table` MODIFY COLUMN `value` VARBINARY(64) KEY NOT NULL"
        in to_sql_calls(adapter)
    )


def test_bare_varbinary_change_requires_explicit_length(
    adapter: StarRocksEngineAdapter,
):
    operations = value_type_change(adapter, "VARBINARY(32)", "VARBINARY")

    with pytest.raises(MigrationNotSupportedError, match=r"explicit VARBINARY\(n\)"):
        adapter.alter_table(operations)

    assert not adapter.cursor.execute.called


def test_bare_varbinary_rejects_unrelated_in_place_change(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
):
    current = {
        "id": exp.DataType.build("INT"),
        "value": exp.DataType.build("INT"),
        "payload": exp.DataType.build("VARBINARY", dialect="starrocks"),
    }
    target = {**current, "value": exp.DataType.build("BIGINT")}
    mocker.patch.object(adapter, "columns", return_value=current)

    assert not adapter.can_apply_schema_change_in_place(
        schema_migration_model(current),
        schema_migration_model(target),
        current_table="test_table",
    )
    assert not adapter.cursor.execute.called


@pytest.mark.parametrize(
    ("table_properties", "role"),
    [
        ("PRIMARY KEY(value) DISTRIBUTED BY HASH(value)", "primary key"),
        ("DUPLICATE KEY(id) DISTRIBUTED BY HASH(value)", "distribution"),
        (
            "DUPLICATE KEY(id) PARTITION BY value DISTRIBUTED BY HASH(id)",
            "partitioning",
        ),
        (
            "PRIMARY KEY(id) DISTRIBUTED BY HASH(id) ORDER BY(value)",
            "sort key",
        ),
    ],
    ids=["primary-key", "distribution", "partition", "primary-key-sort"],
)
def test_supported_type_change_rejects_unsafe_constrained_column(
    adapter: StarRocksEngineAdapter,
    table_properties: str,
    role: str,
):
    operations = value_type_change(adapter, "INT", "BIGINT")
    adapter.cursor.fetchone.return_value = (
        "test_table",
        f"CREATE TABLE test_table (id INT NULL, value INT NOT NULL) ENGINE=OLAP {table_properties}",
    )

    with pytest.raises(MigrationNotSupportedError, match=role):
        adapter.alter_table(operations)


def test_varchar_length_increase_rejects_colocated_distribution(
    adapter: StarRocksEngineAdapter,
):
    operations = value_type_change(adapter, "VARCHAR(32)", "VARCHAR(64)")
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value VARCHAR(32) NOT NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(value) "
        "PROPERTIES ('colocate_with'='group_a')",
    )

    with pytest.raises(MigrationNotSupportedError, match="colocated distribution"):
        adapter.alter_table(operations)


def test_varchar_length_increase_modifies_partition_column(
    adapter: StarRocksEngineAdapter,
):
    operations = value_type_change(adapter, "VARCHAR(32)", "VARCHAR(64)")
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value VARCHAR(32) NOT NULL) "
        "ENGINE=OLAP DUPLICATE KEY(id) PARTITION BY value DISTRIBUTED BY HASH(id)",
    )
    adapter.cursor.fetchall.side_effect = [[], [schema_change_job(1, "FINISHED")]]
    mock_live_columns(
        adapter,
        {
            "id": exp.DataType.build("INT"),
            "value": exp.DataType.build("VARCHAR(64)", dialect="starrocks"),
        },
    )

    adapter.alter_table(operations)

    assert "ALTER TABLE `test_table` MODIFY COLUMN `value` VARCHAR(64) NOT NULL" in to_sql_calls(
        adapter
    )


@pytest.mark.parametrize(
    ("table_properties", "role"),
    [
        ("UNIQUE KEY(id, value) DISTRIBUTED BY HASH(id)", "unique key"),
        ("DUPLICATE KEY(id, value) DISTRIBUTED BY HASH(id)", "duplicate key"),
    ],
    ids=["unique-key", "duplicate-key"],
)
def test_non_key_target_type_rejects_key_column(
    adapter: StarRocksEngineAdapter,
    table_properties: str,
    role: str,
):
    operations = value_type_change(adapter, "INT", "DOUBLE")
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value INT NOT NULL) "
        f"ENGINE=OLAP {table_properties}",
    )

    with pytest.raises(MigrationNotSupportedError, match=role):
        adapter.alter_table(operations)


def test_non_key_target_type_modifies_nonleading_duplicate_sort_column(
    adapter: StarRocksEngineAdapter,
):
    operations = value_type_change(adapter, "INT", "DOUBLE")
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value INT NULL) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) ORDER BY(id, value)",
    )
    adapter.cursor.fetchall.side_effect = [[], [schema_change_job(1, "FINISHED")]]
    mock_live_columns(
        adapter,
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("DOUBLE")},
    )

    adapter.alter_table(operations)

    assert "ALTER TABLE `test_table` MODIFY COLUMN `value` DOUBLE NULL" in to_sql_calls(adapter)


def test_non_key_target_type_rejects_leading_duplicate_sort_column(
    adapter: StarRocksEngineAdapter,
):
    operations = value_type_change(adapter, "INT", "DOUBLE")
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value INT NULL) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) ORDER BY(value)",
    )

    with pytest.raises(MigrationNotSupportedError, match="leading sort key"):
        adapter.alter_table(operations)


@pytest.mark.parametrize("role", ["rollup key", "rollup value"])
def test_non_key_target_type_rejects_rollup_column(
    adapter: StarRocksEngineAdapter,
    role: str,
):
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("INT")},
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("DOUBLE")},
    )
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value INT NULL) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
    )
    t.cast(t.Any, adapter._get_rollup_metadata).return_value = (
        {
            "rollup key": {"value"} if role == "rollup key" else set(),
            "rollup value": {"value"} if role == "rollup value" else set(),
        },
        {"test_rollup"},
    )

    with pytest.raises(MigrationNotSupportedError, match=role):
        adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))


def test_supported_type_change_modifies_rollup_key(adapter: StarRocksEngineAdapter):
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("INT")},
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("BIGINT")},
    )
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value INT NULL) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
    )
    t.cast(t.Any, adapter._get_rollup_metadata).return_value = (
        {"rollup key": {"value"}, "rollup value": set()},
        {"test_rollup"},
    )
    adapter.cursor.fetchall.side_effect = [[], [schema_change_job(1, "FINISHED")]]
    mock_live_columns(
        adapter,
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("BIGINT")},
    )

    adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))

    assert "ALTER TABLE `test_table` MODIFY COLUMN `value` BIGINT NULL" in to_sql_calls(adapter)


def test_supported_type_change_rejects_aggregated_rollup_key(
    adapter: StarRocksEngineAdapter,
):
    operations = value_type_change(adapter, "INT", "BIGINT")
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value INT NULL) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
    )
    t.cast(t.Any, adapter._get_rollup_metadata).return_value = (
        {"rollup key": {"value"}, "rollup value": set()},
        {"test_rollup"},
    )
    t.cast(t.Any, adapter._get_rollup_dependency_columns).return_value = {"value"}

    with pytest.raises(MigrationNotSupportedError, match="rollup dependency"):
        adapter.alter_table(operations)


def test_supported_type_change_rejects_rollup_value(adapter: StarRocksEngineAdapter):
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("INT")},
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("BIGINT")},
    )
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value INT NULL) ENGINE=OLAP "
        "UNIQUE KEY(id) DISTRIBUTED BY HASH(id)",
    )
    t.cast(t.Any, adapter._get_rollup_metadata).return_value = (
        {"rollup key": set(), "rollup value": {"value"}},
        {"test_rollup"},
    )

    with pytest.raises(MigrationNotSupportedError, match="rollup value"):
        adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))


def test_destructive_type_change_rejects_preserved_default(
    adapter: StarRocksEngineAdapter,
):
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("VARCHAR(32)")},
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("INT")},
    )
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value VARCHAR(32) NULL DEFAULT 'abc') "
        "ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
    )

    with pytest.raises(MigrationNotSupportedError, match="default value"):
        adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))


@pytest.mark.parametrize(
    ("target_type", "expected"),
    [("INT", False), ("VARCHAR(64)", True)],
)
def test_gin_index_type_change_depends_on_string_target(
    adapter: StarRocksEngineAdapter,
    target_type: str,
    expected: bool,
):
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("VARCHAR(32)")},
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build(target_type)},
    )
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value VARCHAR(32) NULL, "
        "INDEX gin_idx (value) USING GIN) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
    )

    if expected:
        adapter.cursor.fetchall.side_effect = [[], [schema_change_job(1, "FINISHED")]]
        mock_live_columns(
            adapter,
            {
                "id": exp.DataType.build("INT"),
                "value": exp.DataType.build(target_type, dialect="starrocks"),
            },
        )
        adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))
        assert "ALTER TABLE `test_table` MODIFY COLUMN `value` VARCHAR(64) NULL" in to_sql_calls(
            adapter
        )
    else:
        with pytest.raises(MigrationNotSupportedError, match="gin index"):
            adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))


@pytest.mark.parametrize(
    ("table_properties", "is_key_column", "target_type"),
    [
        ("PRIMARY KEY(value) DISTRIBUTED BY HASH(value)", True, "VARCHAR(64)"),
        ("DUPLICATE KEY(id) DISTRIBUTED BY HASH(value)", False, "VARCHAR(64)"),
        ("PRIMARY KEY(value) DISTRIBUTED BY HASH(value)", True, "STRING"),
        ("DUPLICATE KEY(id) DISTRIBUTED BY HASH(value)", False, "STRING"),
    ],
    ids=["primary-key", "distribution", "primary-key-string", "distribution-string"],
)
def test_varchar_length_increase_modifies_restricted_column(
    adapter: StarRocksEngineAdapter,
    table_properties: str,
    is_key_column: bool,
    target_type: str,
):
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {
            "id": exp.DataType.build("INT"),
            "value": exp.DataType.build("VARCHAR(32)", dialect="starrocks"),
        },
        {
            "id": exp.DataType.build("INT"),
            "value": exp.DataType.build(target_type, dialect="starrocks"),
        },
    )
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NULL, value VARCHAR(32) NOT NULL) "
        f"ENGINE=OLAP {table_properties}",
    )
    adapter.cursor.fetchall.side_effect = [[], [schema_change_job(1, "FINISHED")]]
    mock_live_columns(
        adapter,
        {
            "id": exp.DataType.build("INT"),
            "value": exp.DataType.build(target_type, dialect="starrocks"),
        },
    )

    adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))

    expected_key = " KEY" if is_key_column else ""
    assert (
        f"ALTER TABLE `test_table` MODIFY COLUMN `value` {target_type}{expected_key} NOT NULL"
        in to_sql_calls(adapter)
    )


def test_unsupported_type_change_rejects_key_column(adapter: StarRocksEngineAdapter):
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {"id": exp.DataType.build("VARCHAR(32)", dialect="starrocks")},
        {"id": exp.DataType.build("DATETIME", dialect="starrocks")},
    )
    assert isinstance(operations[0], TableAlterDropColumnOperation)
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id VARCHAR(32) NOT NULL) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
    )

    with pytest.raises(MigrationNotSupportedError, match="duplicate key"):
        adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))


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


@pytest.mark.parametrize(
    ("current_type", "target_type", "expected"),
    [
        ("INT", "BIGINT", False),
        ("VARCHAR(32)", "VARCHAR(64)", True),
        ("VARCHAR(32)", "DATETIME", False),
    ],
    ids=[
        "unsupported-primary-key-modify",
        "varchar-length-increase",
        "unsupported-drop-add",
    ],
)
def test_key_type_change_in_place_depends_on_modify_support(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
    current_type: str,
    target_type: str,
    expected: bool,
):
    current_columns = {
        "id": exp.DataType.build(current_type, dialect="starrocks"),
        "value": exp.DataType.build("INT", dialect="starrocks"),
    }
    target_columns = {
        **current_columns,
        "id": exp.DataType.build(target_type, dialect="starrocks"),
    }
    mocker.patch.object(adapter, "columns", return_value=current_columns)
    adapter.cursor.fetchone.return_value = (
        "test_table",
        f"CREATE TABLE test_table (id {current_type} NOT NULL, value INT NULL) ENGINE=OLAP "
        "PRIMARY KEY(id) DISTRIBUTED BY HASH(id)",
    )
    properties: t.Dict[str, exp.Expr] = {"primary_key": exp.Tuple(expressions=[exp.column("id")])}

    assert (
        adapter.can_apply_schema_change_in_place(
            schema_migration_model(current_columns, physical_properties=properties),
            schema_migration_model(target_columns, physical_properties=properties),
            current_table="test_table",
        )
        is expected
    )


@pytest.mark.parametrize(
    ("current_type", "target_type", "fast_schema_evolution", "expected"),
    [
        ("INT", "BIGINT", True, False),
        ("VARCHAR(32)", "VARCHAR(64)", True, True),
        ("VARCHAR(32)", "VARCHAR(64)", False, False),
    ],
    ids=["integer-change", "varchar-widen", "varchar-widen-without-fast-evolution"],
)
def test_implicit_range_sort_key_change_in_place_matches_server_support(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
    current_type: str,
    target_type: str,
    fast_schema_evolution: bool,
    expected: bool,
):
    current = {
        "id": exp.DataType.build(current_type, dialect="starrocks"),
        "value": exp.DataType.build("INT", dialect="starrocks"),
    }
    target = {
        **current,
        "id": exp.DataType.build(target_type, dialect="starrocks"),
    }
    mocker.patch.object(adapter, "columns", return_value=current)
    t.cast(t.Any, adapter._get_table_distribution_type).return_value = "RANGE"
    adapter.cursor.fetchone.return_value = (
        "test_table",
        f"CREATE TABLE test_table (id {current_type} NOT NULL, value INT NULL) ENGINE=OLAP "
        f"DUPLICATE KEY(id) PROPERTIES ('fast_schema_evolution'="
        f"'{str(fast_schema_evolution).lower()}')",
    )

    assert (
        adapter.can_apply_schema_change_in_place(
            schema_migration_model(current),
            schema_migration_model(target),
            current_table="test_table",
        )
        is expected
    )


def test_implicit_range_sort_key_drop_requires_new_table(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
):
    current = {
        "id": exp.DataType.build("INT", dialect="starrocks"),
        "value": exp.DataType.build("INT", dialect="starrocks"),
    }
    mocker.patch.object(adapter, "columns", return_value=current)
    t.cast(t.Any, adapter._get_table_distribution_type).return_value = "RANGE"
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value INT NULL) ENGINE=OLAP "
        "DUPLICATE KEY(id) PROPERTIES ('fast_schema_evolution'='true')",
    )

    assert not adapter.can_apply_schema_change_in_place(
        schema_migration_model(current),
        schema_migration_model({"value": current["value"]}),
        current_table="test_table",
    )


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
    t.cast(t.Any, adapter._get_rollup_metadata).assert_not_called()


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
        (
            {"id": "INT", "value": "INT"},
            {"id": "INT", "value": "BIGINT"},
            "CREATE TABLE test_table (id INT NOT NULL, value INT NULL) ENGINE=OLAP "
            "PRIMARY KEY(id) DISTRIBUTED BY HASH(id) ORDER BY(value)",
        ),
        (
            {"id": "INT", "value": "VARCHAR(32)"},
            {"id": "INT", "value": "VARCHAR(64)"},
            "CREATE TABLE test_table (id INT NOT NULL, value VARCHAR(32) NOT NULL) "
            "ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(value) "
            "PROPERTIES ('colocate_with'='group_a')",
        ),
        (
            {"id": "INT", "value": "VARCHAR(32)"},
            {"id": "INT", "value": "INT"},
            "CREATE TABLE test_table (id INT NOT NULL, value VARCHAR(32), "
            "INDEX gin_idx (value) USING GIN) ENGINE=OLAP "
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
        ),
        (
            {"id": "INT", "value": "INT"},
            {"id": "INT", "value": "DOUBLE"},
            "CREATE TABLE test_table (id INT NOT NULL, value INT NOT NULL) ENGINE=OLAP "
            "DUPLICATE KEY(id, value) DISTRIBUTED BY HASH(id)",
        ),
        (
            {"id": "INT", "value": "VARCHAR(32)"},
            {"id": "INT", "value": "INT"},
            "CREATE TABLE test_table (id INT NOT NULL, value VARCHAR(32) DEFAULT 'abc') "
            "ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
        ),
    ],
    ids=[
        "auto-increment",
        "vector-index",
        "primary-key-sort",
        "colocated-distribution",
        "gin-index",
        "invalid-key-type",
        "incompatible-default",
    ],
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


@pytest.mark.parametrize(
    "definition",
    [
        "CREATE TABLE test_table (id INT NOT NULL, value VARCHAR(32)) ENGINE=OLAP "
        "PRIMARY KEY(id) DISTRIBUTED BY HASH(id) ORDER BY(value)",
        "CREATE TABLE test_table (id INT NOT NULL, value VARCHAR(32), "
        "INDEX gin_idx (value) USING GIN) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
        "CREATE TABLE test_table (id INT NOT NULL, value VARCHAR(32)) ENGINE=OLAP "
        "DUPLICATE KEY(id) PARTITION BY value DISTRIBUTED BY HASH(id)",
    ],
    ids=["primary-key-sort", "gin-index", "partition"],
)
def test_schema_change_in_place_allows_restricted_varchar_growth(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
    definition: str,
):
    current = {
        "id": exp.DataType.build("INT"),
        "value": exp.DataType.build("VARCHAR(32)"),
    }
    target = {**current, "value": exp.DataType.build("VARCHAR(64)")}
    mocker.patch.object(adapter, "columns", return_value=current)
    adapter.cursor.fetchone.return_value = ("test_table", definition)

    assert adapter.can_apply_schema_change_in_place(
        schema_migration_model(current),
        schema_migration_model(target),
        current_table="test_table",
    )


def test_get_rollup_metadata_from_desc_all(adapter: StarRocksEngineAdapter):
    adapter.cursor.fetchall.return_value = [
        ("rollup_value", "DUP_KEYS", "id", "int", "NO", "true", None, ""),
        ("", "", "value", "int", "YES", "false", None, "NONE"),
        ("", "", "", "", "", "", "", ""),
        ("test_table", "DUP_KEYS", "id", "int", "NO", "true", None, ""),
        ("", "", "value", "int", "YES", "false", None, "NONE"),
    ]

    assert StarRocksEngineAdapter._get_rollup_metadata(
        adapter,
        exp.to_table("test_db.test_table"),
    ) == (
        {"rollup key": {"id"}, "rollup value": {"value"}},
        {"rollup_value"},
    )
    assert "DESC `test_db`.`test_table` ALL" in to_sql_calls(adapter)


@pytest.mark.parametrize(
    ("definition", "expected_dependencies"),
    [
        (
            "CREATE MATERIALIZED VIEW aggregate_key AS "
            "SELECT group_col, SUM(amount) FROM test_table GROUP BY group_col",
            {"group_col", "amount"},
        ),
        (
            "CREATE MATERIALIZED VIEW aliased_aggregate AS "
            "SELECT id, SUM(source_col + 1) AS total FROM test_table GROUP BY id",
            {"id", "source_col"},
        ),
        (
            "CREATE MATERIALIZED VIEW predicate_only AS "
            "SELECT id, value FROM test_table WHERE predicate_col > 0",
            {"predicate_col"},
        ),
        (
            "CREATE MATERIALIZED VIEW simple_rollup AS "
            "SELECT id, value FROM test_table GROUP BY id, value",
            set(),
        ),
        (
            "CREATE MATERIALIZED VIEW unique_rollup AS "
            "SELECT id, REPLACE(value) FROM test_table GROUP BY id",
            {"id", "value"},
        ),
    ],
    ids=[
        "aggregated-rollup-key",
        "aliased-aggregate-input",
        "predicate-only",
        "simple-rollup",
        "unique-rollup-replace",
    ],
)
def test_get_rollup_dependency_columns_from_sync_definition(
    adapter: StarRocksEngineAdapter,
    definition: str,
    expected_dependencies: t.Set[str],
):
    adapter.cursor.fetchall.return_value = [("test_mv", definition)]

    assert (
        StarRocksEngineAdapter._get_rollup_dependency_columns(
            adapter,
            exp.to_table("test_db.test_table"),
            {"test_mv"},
        )
        == expected_dependencies
    )
    assert (
        "SELECT TABLE_NAME, MATERIALIZED_VIEW_DEFINITION FROM "
        "information_schema.materialized_views WHERE TABLE_SCHEMA = 'test_db' "
        "AND TABLE_NAME = 'test_table'"
    ) in to_sql_calls(adapter)


@pytest.mark.parametrize(
    "rows",
    [
        [("test_mv", None)],
        [("test_mv", "CREATE MATERIALIZED VIEW test_mv")],
        [
            (
                "unrelated_mv",
                "CREATE MATERIALIZED VIEW unrelated_mv AS SELECT id FROM other_table",
            )
        ],
        [],
    ],
    ids=["missing-definition", "missing-query", "unmatched-definition", "missing-row"],
)
def test_get_rollup_dependency_columns_fails_closed(
    adapter: StarRocksEngineAdapter,
    rows: t.List[t.Tuple[t.Any, ...]],
):
    adapter.cursor.fetchall.return_value = rows

    with pytest.raises(MigrationNotSupportedError, match="synchronous materialized"):
        StarRocksEngineAdapter._get_rollup_dependency_columns(
            adapter,
            exp.to_table("test_db.test_table"),
            {"test_mv"},
        )


def test_get_rollup_dependency_columns_ignores_unrelated_unreadable_definition(
    adapter: StarRocksEngineAdapter,
):
    adapter.cursor.fetchall.return_value = [
        ("unrelated_mv", None),
        (
            "test_mv",
            "CREATE MATERIALIZED VIEW test_mv AS "
            "SELECT id, value FROM test_table WHERE predicate_col > 0",
        ),
    ]

    assert StarRocksEngineAdapter._get_rollup_dependency_columns(
        adapter,
        exp.to_table("test_db.test_table"),
        {"test_mv"},
    ) == {"predicate_col"}


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


@pytest.mark.parametrize(
    "metadata_helper",
    [
        "_get_table_distribution_type",
        "_get_rollup_metadata",
        "_get_rollup_dependency_columns",
    ],
)
def test_schema_change_in_place_propagates_metadata_inspection_failures(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
    metadata_helper: str,
):
    current = {
        "id": exp.DataType.build("INT"),
        "value": exp.DataType.build("INT"),
    }
    target = {"id": current["id"]}
    mocker.patch.object(adapter, "columns", return_value=current)
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT, value INT) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY RANDOM",
    )
    if metadata_helper == "_get_rollup_dependency_columns":
        t.cast(t.Any, adapter._get_rollup_metadata).return_value = (
            {"rollup key": {"id"}, "rollup value": set()},
            {"test_rollup"},
        )
    getattr(adapter, metadata_helper).side_effect = RuntimeError("metadata inspection failed")

    with pytest.raises(RuntimeError, match="metadata inspection failed"):
        adapter.can_apply_schema_change_in_place(
            schema_migration_model(current),
            schema_migration_model(target),
            current_table="test_table",
        )


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
    mock_live_columns(
        adapter,
        {
            "id": exp.DataType.build("INT"),
            "first_col": exp.DataType.build("INT"),
        },
        {
            "id": exp.DataType.build("INT"),
            "first_col": exp.DataType.build("INT"),
        },
        {
            "id": exp.DataType.build("INT"),
            "first_col": exp.DataType.build("INT"),
            "second_col": exp.DataType.build("INT"),
        },
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
    t.cast(t.Any, adapter._get_rollup_metadata).assert_not_called()


def test_alter_table_prevalidates_rollup_dependencies_before_any_operation(
    adapter: StarRocksEngineAdapter,
):
    current = {"id": exp.DataType.build("INT"), "value": exp.DataType.build("INT")}
    add_operation = adapter.schema_differ.compare_columns(
        "test_table",
        current,
        {**current, "new_col": exp.DataType.build("INT")},
    )[0]
    change_operation = value_type_change(adapter, "INT", "BIGINT")[0]
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value INT NULL) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
    )
    t.cast(t.Any, adapter._get_rollup_metadata).return_value = (
        {"rollup key": {"value"}, "rollup value": set()},
        {"test_rollup"},
    )
    t.cast(t.Any, adapter._get_rollup_dependency_columns).return_value = {"value"}

    with pytest.raises(MigrationNotSupportedError, match="rollup dependency"):
        adapter.alter_table(t.cast(t.List[TableAlterOperation], [add_operation, change_operation]))

    assert not any(sql.startswith("ALTER TABLE") for sql in to_sql_calls(adapter))


def test_alter_table_rejects_mixed_table_batch(adapter: StarRocksEngineAdapter):
    current = {"id": exp.DataType.build("INT")}
    target = {**current, "value": exp.DataType.build("INT")}
    alterations = [
        adapter.schema_differ.compare_columns(table, current, target)[0]
        for table in ("first_table", "second_table")
    ]

    with pytest.raises(SQLMeshError, match="must target exactly one table"):
        adapter.alter_table(t.cast(t.List[TableAlterOperation], alterations))

    assert not adapter.cursor.execute.called


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
    adapter.cursor.fetchall.return_value = [schema_change_job(1, "FINISHED")]
    columns = mocker.patch.object(adapter, "columns", return_value=target)
    sleep = mocker.patch("sqlmesh.core.engine_adapter.starrocks.time.sleep")

    adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))

    show_job = "SHOW ALTER TABLE COLUMN WHERE TableName = 'test_table' ORDER BY JobId DESC LIMIT 1"
    assert to_sql_calls(adapter) == [
        "SHOW CREATE TABLE `test_table`",
        show_job,
        "ALTER TABLE `test_table` ADD COLUMN `value` INT",
        show_job,
    ]
    assert adapter.cursor.fetchall.call_count == 2
    columns.assert_called_once_with(exp.to_table("test_table"))
    sleep.assert_not_called()


def test_alter_table_verifies_partial_column_type_change(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
):
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {"value": exp.DataType.build("VARCHAR(32)")},
        {"value": exp.DataType.build("VARCHAR(64)")},
    )
    adapter.cursor.fetchone.return_value = (
        "test_table",
        "CREATE TABLE test_table (id INT NOT NULL, value VARCHAR(32) NULL) ENGINE=OLAP "
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)",
    )
    adapter.cursor.fetchall.side_effect = [[], [schema_change_job(1, "FINISHED")]]
    columns = mocker.patch.object(
        adapter,
        "columns",
        return_value={
            "id": exp.DataType.build("INT"),
            "value": exp.DataType.build("VARCHAR(64)"),
        },
    )

    adapter.alter_table(t.cast(t.List[TableAlterOperation], operations))

    assert "ALTER TABLE `test_table` MODIFY COLUMN `value` VARCHAR(64) NULL" in to_sql_calls(
        adapter
    )
    columns.assert_called_once_with(exp.to_table("test_table"))


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
        return_value=mocker.Mock(
            job_id=2,
            state="CANCELLED",
            message="invalid schema change",
            progress="0%",
        ),
    )
    mocker.patch.object(adapter, "columns", return_value={"value": exp.DataType.build("INT")})

    with pytest.raises(SQLMeshError, match="job 2.*invalid schema change"):
        adapter._wait_for_schema_change(
            exp.to_table("test_table"),
            previous_job_id=1,
            operation=value_type_change_operation(adapter, "INT", "BIGINT"),
        )


@pytest.mark.parametrize("state", ["RUNNING", "CANCELLED"])
def test_wait_for_schema_change_ignores_unrelated_newer_job_when_operation_is_applied(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
    state: str,
):
    mocker.patch.object(
        adapter,
        "_get_latest_schema_change_job",
        return_value=mocker.Mock(
            job_id=3,
            state=state,
            message="unrelated schema change",
            progress="50%",
        ),
    )
    columns = mocker.patch.object(
        adapter,
        "columns",
        return_value={"value": exp.DataType.build("BIGINT")},
    )
    sleep = mocker.patch("sqlmesh.core.engine_adapter.starrocks.time.sleep")

    adapter._wait_for_schema_change(
        exp.to_table("test_table"),
        previous_job_id=1,
        operation=value_type_change_operation(adapter, "INT", "BIGINT"),
    )

    columns.assert_called_once_with(exp.to_table("test_table"))
    sleep.assert_not_called()


def test_wait_for_schema_change_times_out(adapter: StarRocksEngineAdapter, mocker: MockerFixture):
    adapter._extra_config.update(schema_change_timeout=1, schema_change_poll_interval=1)
    mocker.patch.object(adapter, "_get_latest_schema_change_job", return_value=None)
    mocker.patch.object(adapter, "columns", return_value={"value": exp.DataType.build("INT")})
    mocker.patch("sqlmesh.core.engine_adapter.starrocks.time.monotonic", side_effect=[0, 1])

    with pytest.raises(SQLMeshError, match="Timed out after 1 seconds"):
        adapter._wait_for_schema_change(
            exp.to_table("test_table"),
            previous_job_id=None,
            operation=value_type_change_operation(adapter, "INT", "BIGINT"),
        )


def test_wait_for_schema_change_rejects_unexpected_finished_job(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
):
    mocker.patch.object(
        adapter,
        "_get_latest_schema_change_job",
        return_value=mocker.Mock(
            job_id=2,
            state="FINISHED",
            message="",
            progress="100%",
        ),
    )
    mocker.patch.object(adapter, "columns", return_value={"value": exp.DataType.build("INT")})

    with pytest.raises(SQLMeshError, match="job 2 finished.*expected operation"):
        adapter._wait_for_schema_change(
            exp.to_table("test_table"),
            previous_job_id=1,
            operation=value_type_change_operation(adapter, "INT", "BIGINT"),
        )


def test_wait_for_schema_change_caps_sleep_at_remaining_timeout(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
):
    adapter._extra_config.update(schema_change_timeout=5, schema_change_poll_interval=3600)
    mocker.patch.object(adapter, "_get_latest_schema_change_job", return_value=None)
    mocker.patch.object(adapter, "columns", return_value={"value": exp.DataType.build("INT")})
    mocker.patch("sqlmesh.core.engine_adapter.starrocks.time.monotonic", side_effect=[0, 1, 5])
    sleep = mocker.patch("sqlmesh.core.engine_adapter.starrocks.time.sleep")

    with pytest.raises(SQLMeshError, match="Timed out after 5 seconds"):
        adapter._wait_for_schema_change(
            exp.to_table("test_table"),
            previous_job_id=None,
            operation=value_type_change_operation(adapter, "INT", "BIGINT"),
        )

    sleep.assert_called_once_with(4)


def test_is_operation_applied_does_not_copy_live_varbinary_length(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
):
    mocker.patch.object(
        adapter,
        "columns",
        return_value={"value": exp.DataType.build("VARBINARY(32)", dialect="starrocks")},
    )

    assert not adapter._is_operation_applied(
        exp.to_table("test_table"),
        value_type_change_operation(adapter, "VARBINARY(32)", "VARBINARY"),
    )


def test_is_operation_applied_detects_dropped_column(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
):
    operation = adapter.schema_differ.compare_columns(
        "test_table",
        {"id": exp.DataType.build("INT"), "value": exp.DataType.build("INT")},
        {"id": exp.DataType.build("INT")},
    )[0]
    assert isinstance(operation, TableAlterDropColumnOperation)
    mocker.patch.object(adapter, "columns", return_value={"id": exp.DataType.build("INT")})

    assert adapter._is_operation_applied(exp.to_table("test_table"), operation)


def test_wait_for_schema_change_with_no_new_job_initially(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
):
    mocker.patch.object(
        adapter,
        "_get_latest_schema_change_job",
        side_effect=[
            None,
            mocker.Mock(job_id=2, state="FINISHED", message="", progress="100%"),
        ],
    )
    mocker.patch.object(
        adapter,
        "columns",
        side_effect=[
            {"value": exp.DataType.build("VARBINARY(32)", dialect="starrocks")},
            {"value": exp.DataType.build("VARBINARY(1048576)", dialect="starrocks")},
        ],
    )
    sleep = mocker.patch("sqlmesh.core.engine_adapter.starrocks.time.sleep")

    adapter._wait_for_schema_change(
        exp.to_table("test_table"),
        previous_job_id=1,
        operation=value_type_change_operation(adapter, "VARBINARY(32)", "VARBINARY(1048576)"),
    )

    sleep.assert_called_once_with(1)


@pytest.mark.parametrize(
    ("expected_type", "introspected_type"),
    [
        ("STRING", "VARCHAR(65533)"),
        ("VARCHAR", "VARCHAR(1)"),
        ("CHAR", "CHAR(1)"),
        ("DECIMAL", "DECIMAL(10, 0)"),
        ("DECIMAL(7)", "DECIMAL(7, 0)"),
        ("DECIMAL32", "DECIMAL(9, 9)"),
        ("DECIMAL32(5)", "DECIMAL(5, 5)"),
        ("DECIMAL64", "DECIMAL(18, 18)"),
        ("DECIMAL64(5)", "DECIMAL(5, 5)"),
        ("DECIMAL128", "DECIMAL(38, 38)"),
        ("DECIMAL128(5)", "DECIMAL(5, 5)"),
        ("DECIMAL256", "DECIMAL(76, 76)"),
        ("DECIMAL256(5)", "DECIMAL(5, 5)"),
        ("DECIMAL64(12, 4)", "DECIMAL(12, 4)"),
    ],
)
def test_wait_for_synchronous_schema_change_normalizes_type_aliases(
    adapter: StarRocksEngineAdapter,
    mocker: MockerFixture,
    expected_type: str,
    introspected_type: str,
):
    mocker.patch.object(adapter, "_get_latest_schema_change_job", return_value=None)
    mocker.patch.object(
        adapter,
        "columns",
        return_value={"value": exp.DataType.build(introspected_type, dialect="starrocks")},
    )
    sleep = mocker.patch("sqlmesh.core.engine_adapter.starrocks.time.sleep")

    adapter._wait_for_schema_change(
        exp.to_table("test_table"),
        previous_job_id=None,
        operation=replace(
            value_type_change_operation(adapter, "INT", "BIGINT"),
            column_type=exp.DataType.build(expected_type, dialect="starrocks"),
        ),
    )

    sleep.assert_not_called()


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
    ("current_type", "target_type", "support"),
    [
        *(
            (current_type, target_type, AlterColumnTypeSupport.MODIFY)
            for current_type, target_type in [
                ("TINYINT", "SMALLINT"),
                ("TINYINT", "LARGEINT"),
                ("SMALLINT", "INT"),
                ("SMALLINT", "LARGEINT"),
                ("INT", "BIGINT"),
                ("INT", "LARGEINT"),
                ("INT", "DOUBLE"),
                ("BIGINT", "LARGEINT"),
                ("FLOAT", "DOUBLE"),
                ("DATE", "DATETIME"),
                ("CHAR(32)", "VARCHAR(64)"),
                ("DECIMAL(10, 2)", "DECIMAL(12, 4)"),
                ("DECIMAL32(9, 2)", "DECIMAL64(12, 4)"),
                ("DECIMAL64(18, 4)", "DECIMAL128(20, 6)"),
                ("DECIMAL128(38, 9)", "DECIMAL256(50, 10)"),
                ("DECIMAL(9, 0)", "DECIMAL"),
                ("VARBINARY(32)", "VARBINARY(64)"),
                ("BIGINT", "STRING"),
                ("BIGINT", "VARCHAR(20)"),
                ("BIGINT", "VARCHAR(65533)"),
                ("DECIMAL(38, 9)", "VARCHAR(42)"),
                ("DECIMAL32(9, 2)", "VARCHAR(32)"),
                ("VARCHAR", "VARCHAR(32)"),
                ("VARCHAR(32)", "STRING"),
            ]
        ),
        *(
            (current_type, target_type, AlterColumnTypeSupport.MODIFY_DESTRUCTIVE)
            for current_type, target_type in [
                ("BIGINT", "DOUBLE"),
                ("DATETIME", "DATE"),
                ("VARCHAR(32)", "INT"),
                ("VARCHAR(32)", "DECIMAL(10, 2)"),
                ("VARCHAR(32)", "JSON"),
                ("JSON", "VARCHAR(1024)"),
                ("JSON", "STRING"),
                ("BIGINT", "VARCHAR(19)"),
                ("BIGINT", "VARCHAR"),
                ("VARCHAR(32)", "DECIMAL64(12, 4)"),
                ("VARBINARY(64)", "VARBINARY(32)"),
                ("VARBINARY(32)", "VARBINARY"),
            ]
        ),
        *(
            (current_type, target_type, AlterColumnTypeSupport.DROP_AND_ADD)
            for current_type, target_type in [
                ("BIGINT", "INT"),
                ("CHAR(32)", "VARCHAR(16)"),
                ("JSON", "VARCHAR(1023)"),
                ("DECIMAL(38, 9)", "VARCHAR(41)"),
                ("DECIMAL(10, 2)", "CHAR(32)"),
                ("DECIMAL(10, 2)", "DECIMAL(11, 4)"),
                ("ARRAY<FLOAT>", "ARRAY<DOUBLE>"),
                ("MAP<VARCHAR, INT>", "MAP<VARCHAR, BIGINT>"),
                ("STRUCT<a INT>", "STRUCT<a BIGINT>"),
                ("VARCHAR(1048576)", "VARCHAR(65533)"),
                ("VARCHAR(1048576)", "STRING"),
                ("VARCHAR(32)", "VARCHAR"),
                ("DECIMAL(38, 9)", "DOUBLE"),
            ]
        ),
    ],
)
def test_type_change_support(
    adapter: StarRocksEngineAdapter,
    current_type: str,
    target_type: str,
    support: AlterColumnTypeSupport,
):
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {"value": exp.DataType.build(current_type, dialect="starrocks")},
        {"value": exp.DataType.build(target_type, dialect="starrocks")},
    )

    if support is AlterColumnTypeSupport.DROP_AND_ADD:
        assert [type(operation) for operation in operations] == [
            TableAlterDropColumnOperation,
            TableAlterAddColumnOperation,
        ]
    else:
        assert len(operations) == 1
        assert isinstance(operations[0], TableAlterChangeColumnTypeOperation)
        assert operations[0].is_destructive is (
            support is AlterColumnTypeSupport.MODIFY_DESTRUCTIVE
        )


@pytest.mark.parametrize(
    ("current_type", "target_type"),
    [
        ("STRING", "VARCHAR(65533)"),
        ("VARCHAR(65533)", "STRING"),
        ("VARCHAR", "VARCHAR(1)"),
        ("VARCHAR(1)", "VARCHAR"),
        ("DECIMAL", "DECIMAL(10, 0)"),
        ("DECIMAL(7)", "DECIMAL(7, 0)"),
        ("DECIMAL32", "DECIMAL(9, 9)"),
        ("DECIMAL32(5)", "DECIMAL(5, 5)"),
        ("DECIMAL64", "DECIMAL(18, 18)"),
        ("DECIMAL64(5)", "DECIMAL(5, 5)"),
        ("DECIMAL128", "DECIMAL(38, 38)"),
        ("DECIMAL128(5)", "DECIMAL(5, 5)"),
        ("DECIMAL256", "DECIMAL(76, 76)"),
        ("DECIMAL256(5)", "DECIMAL(5, 5)"),
    ],
)
def test_semantically_identical_type_aliases_do_not_alter(
    adapter: StarRocksEngineAdapter,
    current_type: str,
    target_type: str,
):
    assert value_type_change(adapter, current_type, target_type) == []


def test_bare_varbinary_is_destructive_without_explicit_length(
    adapter: StarRocksEngineAdapter,
):
    operations = adapter.schema_differ.compare_columns(
        "test_table",
        {"value": exp.DataType.build("VARBINARY(32)", dialect="starrocks")},
        {"value": exp.DataType.build("VARBINARY", dialect="starrocks")},
    )

    assert len(operations) == 1
    assert isinstance(operations[0], TableAlterChangeColumnTypeOperation)
    assert operations[0].is_destructive
    assert operations[0].column_type == exp.DataType.build("VARBINARY", dialect="starrocks")


@pytest.mark.parametrize(
    ("current_type_name", "target_type_name"),
    [
        ("VARCHAR(1048576)", "VARCHAR(65533)"),
        ("DECIMAL(38, 9)", "DOUBLE"),
    ],
)
def test_starrocks_type_support_takes_precedence_over_coercion(
    current_type_name: str,
    target_type_name: str,
):
    current_type = exp.DataType.build(current_type_name, dialect="starrocks")
    target_type = exp.DataType.build(target_type_name, dialect="starrocks")
    schema_differ = SchemaDiffer(
        **StarRocksEngineAdapter.SCHEMA_DIFFER_KWARGS,
        coerceable_types={current_type: {target_type}},
    )

    operations = schema_differ.compare_columns(
        "test_table",
        {"value": current_type},
        {"value": target_type},
    )

    assert len(operations) == 2
    assert isinstance(operations[0], TableAlterDropColumnOperation)
    assert isinstance(operations[1], TableAlterAddColumnOperation)
