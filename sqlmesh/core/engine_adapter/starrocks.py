from __future__ import annotations

import typing as t
from functools import lru_cache

from sqlglot import exp

from sqlmesh.core.engine_adapter.mysql import MySQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    CommentCreationTable,
    CommentCreationView,
    InsertOverwriteStrategy,
    SourceQuery,
    set_catalog,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName


@set_catalog()
class StarRocksEngineAdapter(MySQLEngineAdapter):
    """StarRocks engine adapter."""

    DIALECT = "starrocks"
    DEFAULT_BATCH_SIZE = 10000
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_INDEXES = True
    # StarRocks supports COMMENT in CREATE TABLE but not in CTAS
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    # StarRocks supports COMMENT in CREATE VIEW but not via ALTER/COMMENT commands
    COMMENT_CREATION_VIEW = CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS
    MAX_TABLE_COMMENT_LENGTH = 1024
    MAX_COLUMN_COMMENT_LENGTH = 1024
    # Though StarRocks supports asynchronous materialized views since v2.4, we don't support them yet.
    SUPPORTS_MATERIALIZED_VIEWS = False
    # While StarRocks tables can have names up to 1024 characters,
    # database (schema) names are limited to 256 characters.
    # see https://docs.starrocks.io/docs/sql-reference/System_limit/
    MAX_IDENTIFIER_LENGTH = 256
    # StarRocks natively supports INSERT OVERWRITE syntax for partitioned tables,
    # but we default to DELETE_INSERT to support clusters with disabled dynamic_overwrite
    # see https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/INSERT/#dynamic-overwrite
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT

    @lru_cache(maxsize=1)
    def _is_dynamic_overwrite_enabled(self) -> bool:
        """Check if dynamic_overwrite is enabled in StarRocks."""
        try:
            return bool((result := self.fetchone("SELECT @@dynamic_overwrite")) and result[0] == 1)
        except Exception:
            return False

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        if self._is_dynamic_overwrite_enabled():
            insert_overwrite_strategy_override = InsertOverwriteStrategy.INSERT_OVERWRITE

        super()._insert_overwrite_by_condition(
            table_name=table_name,
            source_queries=source_queries,
            target_columns_to_types=target_columns_to_types,
            where=where,
            insert_overwrite_strategy_override=insert_overwrite_strategy_override,
            **kwargs,
        )

    def delete_from(self, table_name: TableName, where: t.Union[str, exp.Expression]) -> None:
        # StarRocks DELETE WHERE clause doesn't support boolean literals like TRUE/FALSE,
        # also we cannot use DELETE with WHERE in some cases.
        # So we use TRUNCATE TABLE instead when deleting all rows.
        # Another option is to use dynamic_overwrite which is supported in StarRocks since v2.4.
        if where == exp.true():
            return self.execute(
                exp.TruncateTable(
                    expressions=[
                        exp.to_table(table_name, dialect=self.dialect),
                    ]
                )
            )
        return super().delete_from(table_name, where)

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
        properties: t.Optional[t.List[exp.Expression]] = None,
    ) -> None:
        properties = properties or []
        return super()._create_schema(
            schema_name=schema_name,
            ignore_if_exists=ignore_if_exists,
            warn_on_error=warn_on_error,
            properties=properties,
            kind="DATABASE",  # In StarRocks, a schema is a database
        )

    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
        **drop_args: t.Dict[str, exp.Expression],
    ) -> None:
        return self._drop_object(
            name=schema_name,
            exists=ignore_if_not_exists,
            kind="DATABASE",  # In StarRocks, a schema is a database
            cascade=False,
            **drop_args,
        )
