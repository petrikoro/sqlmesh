import typing as t

import pytest
from pytest_mock import MockFixture
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one
from sqlglot.helper import ensure_list

from sqlmesh.core.engine_adapter import PostgresEngineAdapter
from sqlmesh.utils.errors import SQLMeshError
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.postgres]


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        (
            {
                "schema_name": "test_schema",
            },
            'DROP SCHEMA IF EXISTS "test_schema"',
        ),
        (
            {
                "schema_name": "test_schema",
                "ignore_if_not_exists": False,
            },
            'DROP SCHEMA "test_schema"',
        ),
        (
            {
                "schema_name": "test_schema",
                "cascade": True,
            },
            'DROP SCHEMA IF EXISTS "test_schema" CASCADE',
        ),
        (
            {
                "schema_name": "test_schema",
                "cascade": True,
                "ignore_if_not_exists": False,
            },
            'DROP SCHEMA "test_schema" CASCADE',
        ),
    ],
)
def test_drop_schema(kwargs, expected, make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    adapter.drop_schema(**kwargs)

    assert to_sql_calls(adapter) == ensure_list(expected)


def test_drop_schema_with_catalog(make_mocked_engine_adapter: t.Callable, mocker: MockFixture):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    adapter.get_current_catalog = mocker.MagicMock(return_value="other_catalog")

    with pytest.raises(
        SQLMeshError, match="requires that all catalog operations be against a single catalog"
    ):
        adapter.drop_schema("test_catalog.test_schema")


def test_comments(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    adapter.create_table(
        "test_table",
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description="\\",
        column_descriptions={"a": "\\"},
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        'CREATE TABLE IF NOT EXISTS "test_table" ("a" INT, "b" INT)',
        """COMMENT ON TABLE "test_table" IS '\\'""",
        """COMMENT ON COLUMN "test_table"."a" IS '\\'""",
    ]


def test_create_table_like(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    adapter.create_table_like("target_table", "source_table")
    adapter.cursor.execute.assert_called_once_with(
        'CREATE TABLE IF NOT EXISTS "target_table" (LIKE "source_table" INCLUDING ALL)'
    )


def test_merge_version_gte_15(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)
    adapter.server_version = (15, 0)

    adapter.merge(
        target_table="target",
        source_table=t.cast(exp.Select, parse_one('SELECT "ID", ts, val FROM source')),
        target_columns_to_types={
            "ID": exp.DataType.build("int"),
            "ts": exp.DataType.build("timestamp"),
            "val": exp.DataType.build("int"),
        },
        unique_key=[exp.to_identifier("ID", quoted=True)],
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        """MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT "ID", "ts", "val" FROM "source") AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."ID" = "__MERGE_SOURCE__"."ID" WHEN MATCHED THEN UPDATE SET "ID" = "__MERGE_SOURCE__"."ID", "ts" = "__MERGE_SOURCE__"."ts", "val" = "__MERGE_SOURCE__"."val" WHEN NOT MATCHED THEN INSERT ("ID", "ts", "val") VALUES ("__MERGE_SOURCE__"."ID", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")"""
    ]


def test_merge_version_lt_15(
    make_mocked_engine_adapter: t.Callable, make_temp_table_name: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)
    adapter.server_version = (14, 0)

    temp_table_mock = mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter._get_temp_table")
    table_name = "test"
    temp_table_id = "abcdefgh"
    temp_table_mock.return_value = make_temp_table_name(table_name, temp_table_id)

    adapter.merge(
        target_table="target",
        source_table=t.cast(exp.Select, parse_one('SELECT "ID", ts, val FROM source')),
        target_columns_to_types={
            "ID": exp.DataType.build("int"),
            "ts": exp.DataType.build("timestamp"),
            "val": exp.DataType.build("int"),
        },
        unique_key=[exp.to_identifier("ID", quoted=True)],
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        'CREATE TABLE "__temp_test_abcdefgh" AS SELECT CAST("ID" AS INT) AS "ID", CAST("ts" AS TIMESTAMP) AS "ts", CAST("val" AS INT) AS "val" FROM (SELECT "ID", "ts", "val" FROM "source") AS "_subquery"',
        'DELETE FROM "target" WHERE "ID" IN (SELECT "ID" FROM "__temp_test_abcdefgh")',
        'INSERT INTO "target" ("ID", "ts", "val") SELECT DISTINCT ON ("ID") "ID", "ts", "val" FROM "__temp_test_abcdefgh"',
        'DROP TABLE IF EXISTS "__temp_test_abcdefgh"',
    ]


def test_alter_table_drop_column_cascade(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    current_table_name = "test_table"
    target_table_name = "target_table"

    def table_columns(table_name: str) -> t.Dict[str, exp.DataType]:
        if table_name == current_table_name:
            return {"id": exp.DataType.build("int"), "test_column": exp.DataType.build("int")}
        return {"id": exp.DataType.build("int")}

    adapter.columns = table_columns

    adapter.alter_table(adapter.get_alter_operations(current_table_name, target_table_name))
    assert to_sql_calls(adapter) == [
        'ALTER TABLE "test_table" DROP COLUMN "test_column" CASCADE',
    ]


def test_server_version(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    fetchone_mock = mocker.patch.object(adapter, "fetchone")
    fetchone_mock.return_value = ("14.0",)
    assert adapter.server_version == (14, 0)

    del adapter.server_version
    fetchone_mock.return_value = ("15.8",)
    assert adapter.server_version == (15, 8)

    del adapter.server_version
    fetchone_mock.return_value = ("15.13 (Debian 15.13-1.pgdg120+1)",)
    assert adapter.server_version == (15, 13)


def test_sync_grants_config(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)
    relation = exp.to_table("test_schema.test_table", dialect="postgres")
    new_grants_config = {"SELECT": ["user1", "user2"], "INSERT": ["user3"]}

    current_grants = [("SELECT", "old_user"), ("UPDATE", "admin_user")]
    fetchall_mock = mocker.patch.object(adapter, "fetchall", return_value=current_grants)

    adapter.sync_grants_config(relation, new_grants_config)

    fetchall_mock.assert_called_once()
    executed_query = fetchall_mock.call_args[0][0]
    executed_sql = executed_query.sql(dialect="postgres")

    assert executed_sql == (
        "SELECT privilege_type, grantee FROM information_schema.role_table_grants "
        "WHERE table_schema = 'test_schema' AND table_name = 'test_table' "
        "AND grantor = current_role AND grantee <> current_role"
    )

    sql_calls = to_sql_calls(adapter)
    assert len(sql_calls) == 4

    assert 'GRANT SELECT ON "test_schema"."test_table" TO "user1", "user2"' in sql_calls
    assert 'GRANT INSERT ON "test_schema"."test_table" TO "user3"' in sql_calls
    assert 'REVOKE SELECT ON "test_schema"."test_table" FROM "old_user"' in sql_calls
    assert 'REVOKE UPDATE ON "test_schema"."test_table" FROM "admin_user"' in sql_calls


def test_sync_grants_config_with_overlaps(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)
    relation = exp.to_table("test_schema.test_table", dialect="postgres")
    new_grants_config = {"SELECT": ["user1", "user2", "user3"], "INSERT": ["user2", "user4"]}

    current_grants = [
        ("SELECT", "user1"),
        ("SELECT", "user5"),
        ("INSERT", "user2"),
        ("UPDATE", "user3"),
    ]
    fetchall_mock = mocker.patch.object(adapter, "fetchall", return_value=current_grants)

    adapter.sync_grants_config(relation, new_grants_config)

    fetchall_mock.assert_called_once()
    executed_query = fetchall_mock.call_args[0][0]
    executed_sql = executed_query.sql(dialect="postgres")

    assert executed_sql == (
        "SELECT privilege_type, grantee FROM information_schema.role_table_grants "
        "WHERE table_schema = 'test_schema' AND table_name = 'test_table' "
        "AND grantor = current_role AND grantee <> current_role"
    )

    sql_calls = to_sql_calls(adapter)
    assert len(sql_calls) == 4

    assert 'GRANT SELECT ON "test_schema"."test_table" TO "user2", "user3"' in sql_calls
    assert 'GRANT INSERT ON "test_schema"."test_table" TO "user4"' in sql_calls
    assert 'REVOKE SELECT ON "test_schema"."test_table" FROM "user5"' in sql_calls
    assert 'REVOKE UPDATE ON "test_schema"."test_table" FROM "user3"' in sql_calls


def test_diff_grants_configs(make_mocked_engine_adapter: t.Callable):
    new_grants = {"select": ["USER1", "USER2"], "insert": ["user3"]}
    old_grants = {"SELECT": ["user1", "user4"], "UPDATE": ["user5"]}

    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)
    additions, removals = adapter._diff_grants_configs(new_grants, old_grants)

    assert additions["select"] == ["USER2"]
    assert additions["insert"] == ["user3"]

    assert removals["SELECT"] == ["user4"]
    assert removals["UPDATE"] == ["user5"]


def test_sync_grants_config_with_default_schema(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)
    relation = exp.to_table("test_table", dialect="postgres")  # No schema
    new_grants_config = {"SELECT": ["user1"], "INSERT": ["user2"]}

    currrent_grants = [("UPDATE", "old_user")]
    fetchall_mock = mocker.patch.object(adapter, "fetchall", return_value=currrent_grants)
    get_schema_mock = mocker.patch.object(adapter, "_get_current_schema", return_value="public")

    adapter.sync_grants_config(relation, new_grants_config)

    get_schema_mock.assert_called_once()

    fetchall_mock.assert_called_once()
    executed_query = fetchall_mock.call_args[0][0]
    executed_sql = executed_query.sql(dialect="postgres")

    assert executed_sql == (
        "SELECT privilege_type, grantee FROM information_schema.role_table_grants "
        "WHERE table_schema = 'public' AND table_name = 'test_table' "
        "AND grantor = current_role AND grantee <> current_role"
    )


def test_get_dependent_views(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    """Test that _get_dependent_views generates correct SQL query."""
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)
    mocker.patch.object(adapter, "_get_current_schema", return_value="public")
    adapter.cursor.fetchall.return_value = [
        ("public", "view1", "SELECT * FROM test_table", False),
        ("public", "mat_view1", "SELECT * FROM test_table", True),
    ]

    result = adapter._get_dependent_views("public.test_table")

    assert len(result) == 2
    assert result[0] == ("public", "view1", "SELECT * FROM test_table", False)
    assert result[1] == ("public", "mat_view1", "SELECT * FROM test_table", True)

    # Verify the SQL query structure
    sql_calls = to_sql_calls(adapter)
    assert len(sql_calls) == 1
    assert "pg_depend" in sql_calls[0].lower()
    assert "pg_rewrite" in sql_calls[0].lower()
    assert "pg_class" in sql_calls[0].lower()
    assert "pg_get_viewdef" in sql_calls[0].lower()


def test_recreate_dependent_views_regular(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    """Test that regular views are recreated with CREATE OR REPLACE (preserves grants)."""
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    dependent_views = [
        ("public", "view1", "SELECT * FROM test_table", False),
    ]

    adapter._recreate_dependent_views(dependent_views)

    sql_calls = to_sql_calls(adapter)
    assert len(sql_calls) == 1
    assert "CREATE OR REPLACE VIEW" in sql_calls[0]
    assert '"public"."view1"' in sql_calls[0]


def test_recreate_dependent_views_materialized(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    """Test that materialized views are recreated with DROP + CREATE and grants are restored."""
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    # Mock _get_table_grants to return aggregated grants
    adapter.cursor.fetchall.return_value = [("analyst", "SELECT, UPDATE", None)]

    dependent_views = [
        ("public", "mat_view1", "SELECT * FROM test_table", True),
    ]

    adapter._recreate_dependent_views(dependent_views)

    sql_calls = to_sql_calls(adapter)
    # Should have: fetchall (grants query), DROP, CREATE, GRANT
    assert any("DROP MATERIALIZED VIEW" in sql for sql in sql_calls)
    assert any("CREATE MATERIALIZED VIEW" in sql for sql in sql_calls)
    assert any("GRANT SELECT, UPDATE ON" in sql for sql in sql_calls)


def test_get_table_indexes(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    """Test that _get_table_indexes retrieves index definitions excluding constraint-backed indexes."""
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)
    mocker.patch.object(adapter, "_get_current_schema", return_value="public")
    adapter.cursor.fetchall.return_value = [
        ("CREATE INDEX idx_col1 ON public.test_table (col1)",),
        ("CREATE INDEX idx_col2 ON public.test_table (col2)",),
    ]

    result = adapter._get_table_indexes("public.test_table")

    assert len(result) == 2
    assert "CREATE INDEX idx_col1" in result[0]
    assert "CREATE INDEX idx_col2" in result[1]

    sql_calls = to_sql_calls(adapter)
    assert len(sql_calls) == 1
    assert "pg_indexes" in sql_calls[0].lower()
    # Excludes primary key ('p') and unique ('u') constraint-backed indexes via pg_constraint
    assert "pg_constraint" in sql_calls[0].lower()
    assert "'p'" in sql_calls[0]  # Primary key constraint type
    assert "'u'" in sql_calls[0]  # Unique constraint type


def test_recreate_indexes(make_mocked_engine_adapter: t.Callable):
    """Test that _recreate_indexes executes index definitions."""
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    index_definitions = [
        "CREATE INDEX idx_col1 ON public.test_table (col1)",
        "CREATE INDEX idx_col2 ON public.test_table (col2)",
    ]
    table = exp.to_table("public.test_table")

    adapter._recreate_indexes(index_definitions, table)

    sql_calls = to_sql_calls(adapter)
    assert len(sql_calls) == 2
    assert sql_calls[0] == "CREATE INDEX idx_col1 ON public.test_table (col1)"
    assert sql_calls[1] == "CREATE INDEX idx_col2 ON public.test_table (col2)"


def test_get_table_grants(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    """Test that _get_table_grants retrieves aggregated grants from information_schema."""
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)
    mocker.patch.object(adapter, "_get_current_schema", return_value="public")
    # Aggregated format: (grantee, privileges, columns)
    adapter.cursor.fetchall.return_value = [
        ("analyst", "SELECT, UPDATE", None),  # Table-level grants aggregated
        ("writer", "INSERT", None),  # Table-level grant
        ("analyst", "SELECT", "email, name"),  # Column-level grant with aggregated columns
    ]

    result = adapter._get_table_grants("public.test_table")

    assert len(result) == 3
    assert ("analyst", "SELECT, UPDATE", None) in result
    assert ("writer", "INSERT", None) in result
    assert ("analyst", "SELECT", "email, name") in result

    # Verify SQL uses information_schema with aggregation
    sql_calls = to_sql_calls(adapter)
    assert len(sql_calls) == 1
    sql = sql_calls[0].lower()
    assert "information_schema" in sql
    assert "role_table_grants" in sql  # Table-level grants
    assert "column_privileges" in sql  # Column-level grants
    assert "string_agg" in sql  # Aggregation function
    assert "group by" in sql  # Grouping
    assert "union" in sql  # Combined query


def test_get_table_grants_current_user_not_quoted(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    """Test that current_user is not quoted in _get_table_grants SQL query.

    current_user is a SQL keyword that returns the current user, not a column name.
    If quoted as "current_user", PostgreSQL interprets it as a column reference
    and raises: psycopg2.errors.UndefinedColumn: column "current_user" does not exist
    """
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)
    mocker.patch.object(adapter, "_get_current_schema", return_value="public")
    adapter.cursor.fetchall.return_value = []

    adapter._get_table_grants("public.test_table")

    sql_calls = to_sql_calls(adapter)
    assert len(sql_calls) == 1
    sql = sql_calls[0]

    # current_user should appear unquoted (as SQL keyword), not as "current_user" (column ref)
    assert "current_user" in sql.lower()
    # Should NOT have quoted "current_user" which would be interpreted as a column
    assert '"current_user"' not in sql


def test_apply_table_grants(make_mocked_engine_adapter: t.Callable):
    """Test that _apply_table_grants applies aggregated grants correctly."""
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    # Aggregated format: (grantee, privileges, columns)
    grants = [
        ("analyst", "SELECT, UPDATE", None),  # Table-level with multiple privileges
        ("writer", "INSERT", None),  # Table-level single privilege
        ("analyst", "SELECT", "email, name"),  # Column-level with multiple columns
    ]
    table = exp.to_table("public.test_table")

    adapter._apply_table_grants(table, grants)

    sql_calls = to_sql_calls(adapter)
    assert len(sql_calls) == 3
    # Table-level grant with multiple privileges
    assert any(
        "GRANT SELECT, UPDATE ON" in sql and "test_table" in sql and "analyst" in sql
        for sql in sql_calls
    )
    # Table-level grant with single privilege
    assert any(
        "GRANT INSERT ON" in sql and "test_table" in sql and "writer" in sql for sql in sql_calls
    )
    # Column-level grant with multiple columns
    assert any(
        "GRANT SELECT (email, name) ON" in sql and "test_table" in sql and "analyst" in sql
        for sql in sql_calls
    )


def test_replace_query_table_not_exists(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    """Test replace_query when table doesn't exist - should just create it."""
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    # Table doesn't exist
    mocker.patch.object(adapter, "get_data_object", return_value=None)

    adapter.replace_query(
        "test_schema.test_table",
        parse_one("SELECT 1 as id, 'test' as name"),
        {"id": exp.DataType.build("INT"), "name": exp.DataType.build("TEXT")},
    )

    sql_calls = to_sql_calls(adapter)
    # Should create table directly without swap logic
    assert any("CREATE TABLE" in sql for sql in sql_calls)
    # Should not have rename operations
    assert not any("ALTER TABLE" in sql and "RENAME" in sql for sql in sql_calls)


def test_replace_query_with_swap(
    make_mocked_engine_adapter: t.Callable,
    make_temp_table_name: t.Callable,
    mocker: MockerFixture,
):
    """Test replace_query with atomic swap when table exists."""
    from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    # Table exists
    mocker.patch.object(
        adapter,
        "get_data_object",
        return_value=DataObject(
            catalog="db", schema="test_schema", name="test_table", type=DataObjectType.TABLE
        ),
    )

    # Mock temp table names
    temp_table_mock = mocker.patch.object(adapter, "_get_temp_table")
    temp_table_mock.side_effect = [
        make_temp_table_name("test_table", "temp1"),
        make_temp_table_name("test_table", "old1"),
    ]

    # Mock methods that query the database
    mocker.patch.object(adapter, "_get_dependent_views", return_value=[])
    mocker.patch.object(adapter, "_get_table_indexes", return_value=[])
    mocker.patch.object(adapter, "_get_table_grants", return_value=[])
    mocker.patch.object(
        adapter,
        "columns",
        return_value={"id": exp.DataType.build("INT"), "name": exp.DataType.build("TEXT")},
    )

    adapter.replace_query(
        "test_schema.test_table",
        parse_one("SELECT 1 as id, 'test' as name"),
    )

    sql_calls = to_sql_calls(adapter)

    # Should have: BEGIN, CREATE temp table, INSERT, RENAME x2, DROP old, COMMIT
    assert any("CREATE TABLE" in sql for sql in sql_calls)
    assert any("INSERT INTO" in sql for sql in sql_calls)
    # Two renames: target -> old, temp -> target
    rename_calls = [sql for sql in sql_calls if "ALTER TABLE" in sql and "RENAME" in sql]
    assert len(rename_calls) == 2
    assert any("DROP TABLE" in sql for sql in sql_calls)


def test_replace_query_self_referencing(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    """Test replace_query falls back to base implementation for self-referencing queries."""
    from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    # Table exists
    mocker.patch.object(
        adapter,
        "get_data_object",
        return_value=DataObject(
            catalog="db", schema="test_schema", name="test_table", type=DataObjectType.TABLE
        ),
    )

    mocker.patch.object(adapter, "columns", return_value={"id": exp.DataType.build("INT")})

    # Mock _insert_overwrite_by_condition to track if it's called
    insert_overwrite_mock = mocker.patch.object(adapter, "_insert_overwrite_by_condition")

    # Self-referencing query
    adapter.replace_query(
        "test_schema.test_table",
        parse_one("SELECT id + 1 as id FROM test_schema.test_table"),
    )

    # Should fall back to _insert_overwrite_by_condition
    insert_overwrite_mock.assert_called_once()


def test_replace_query_with_dependent_views(
    make_mocked_engine_adapter: t.Callable,
    make_temp_table_name: t.Callable,
    mocker: MockerFixture,
):
    """Test replace_query recreates dependent views after swap."""
    from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    mocker.patch.object(
        adapter,
        "get_data_object",
        return_value=DataObject(
            catalog="db", schema="test_schema", name="test_table", type=DataObjectType.TABLE
        ),
    )

    temp_table_mock = mocker.patch.object(adapter, "_get_temp_table")
    temp_table_mock.side_effect = [
        make_temp_table_name("test_table", "temp1"),
        make_temp_table_name("test_table", "old1"),
    ]

    # Has a dependent view
    mocker.patch.object(
        adapter,
        "_get_dependent_views",
        return_value=[("test_schema", "dependent_view", "SELECT * FROM test_table", False)],
    )
    mocker.patch.object(adapter, "_get_table_indexes", return_value=[])
    mocker.patch.object(adapter, "_get_table_grants", return_value=[])
    mocker.patch.object(adapter, "columns", return_value={"id": exp.DataType.build("INT")})

    adapter.replace_query(
        "test_schema.test_table",
        parse_one("SELECT 1 as id"),
    )

    sql_calls = to_sql_calls(adapter)

    # Should recreate the dependent view
    assert any("CREATE OR REPLACE VIEW" in sql and "dependent_view" in sql for sql in sql_calls)


def test_replace_query_with_indexes_and_grants(
    make_mocked_engine_adapter: t.Callable,
    make_temp_table_name: t.Callable,
    mocker: MockerFixture,
):
    """Test replace_query restores indexes and grants after swap."""
    from sqlmesh.core.engine_adapter.shared import DataObject, DataObjectType

    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    mocker.patch.object(
        adapter,
        "get_data_object",
        return_value=DataObject(
            catalog="db", schema="test_schema", name="test_table", type=DataObjectType.TABLE
        ),
    )

    temp_table_mock = mocker.patch.object(adapter, "_get_temp_table")
    temp_table_mock.side_effect = [
        make_temp_table_name("test_table", "temp1"),
        make_temp_table_name("test_table", "old1"),
    ]

    mocker.patch.object(adapter, "_get_dependent_views", return_value=[])
    mocker.patch.object(
        adapter,
        "_get_table_indexes",
        return_value=['CREATE INDEX idx_id ON "test_schema"."test_table" (id)'],
    )
    # Aggregated grants format
    mocker.patch.object(
        adapter,
        "_get_table_grants",
        return_value=[("analyst", "SELECT, UPDATE", None)],
    )
    mocker.patch.object(adapter, "columns", return_value={"id": exp.DataType.build("INT")})

    adapter.replace_query(
        "test_schema.test_table",
        parse_one("SELECT 1 as id"),
    )

    sql_calls = to_sql_calls(adapter)

    # Should recreate index
    assert any("CREATE INDEX idx_id" in sql for sql in sql_calls)
    # Should restore aggregated grants
    assert any("GRANT SELECT, UPDATE ON" in sql for sql in sql_calls)
