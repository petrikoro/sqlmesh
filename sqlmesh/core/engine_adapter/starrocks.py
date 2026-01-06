from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.mysql import MySQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    CommentCreationTable,
    CommentCreationView,
    InsertOverwriteStrategy,
    set_catalog,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName


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
    # Though StarRocks supports asynchronous materialized views, we don't support them yet.
    SUPPORTS_MATERIALIZED_VIEWS = False
    # While StarRocks tables can have names up to 1024 characters,
    # database (schema) names are limited to 256 characters.
    # see https://docs.starrocks.io/docs/sql-reference/System_limit/
    MAX_IDENTIFIER_LENGTH = 256
    # We always use INSERT_OVERWRITE here because StarRocks has some pretty strict limitations on DELETE statements—
    # for example, its WHERE clauses aren't very flexible and don't allow functions like CAST in conditions.
    # To get the expected behavior, you should turn on dynamic_overwrite (available since StarRocks 2.4).
    # For more details, see: https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/INSERT/#dynamic-overwrite
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE

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
