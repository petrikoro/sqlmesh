from __future__ import annotations

import logging
import re
import time
import typing as t
from dataclasses import dataclass

from sqlglot import Token, TokenType, exp, parse_one, tokenize
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.mixins import (
    LogicalMergeMixin,
    PandasNativeFetchDFSupportMixin,
    GetCurrentCatalogFromFunctionMixin,
    RowDiffMixin,
    GrantsFromInfoSchemaMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    InsertOverwriteStrategy,
    set_catalog,
)
from sqlmesh.core.schema_diff import (
    AlterColumnTypeSupport,
    TableAlterAddColumnOperation,
    TableAlterChangeColumnTypeOperation,
    TableAlterColumnOperation,
    TableAlterDropColumnOperation,
    TableAlterOperation,
)
from sqlmesh.utils import columns_to_types_all_known
from sqlmesh.utils.errors import MigrationNotSupportedError, SQLMeshError

logger = logging.getLogger(__name__)

# StarRocks 4.1.3 conversion rules and their parameter-level validation:
# https://github.com/StarRocks/starrocks/blob/4.1.3/fe/fe-core/src/main/java/com/starrocks/catalog/SchemaChangeTypeCompatibility.java#L42-L163
# https://github.com/StarRocks/starrocks/blob/4.1.3/fe/fe-core/src/main/java/com/starrocks/catalog/Column.java#L538-L578
_STRING_ALIAS_LENGTH = 65533
_MIN_JSON_VARCHAR_LENGTH = 1024
_DECIMAL_TYPES = {
    exp.DType.DECIMAL,
    exp.DType.DECIMAL32,
    exp.DType.DECIMAL64,
    exp.DType.DECIMAL128,
    exp.DType.DECIMAL256,
}
# TypeFactory delegates family defaults to PrimitiveType:
# https://github.com/StarRocks/starrocks/blob/4.1.3/fe/fe-core/src/main/java/com/starrocks/type/TypeFactory.java#L250-L305
_DECIMAL_DEFAULTS = {
    exp.DType.DECIMAL: (10, 0),
    exp.DType.DECIMAL32: (9, 9),
    exp.DType.DECIMAL64: (18, 18),
    exp.DType.DECIMAL128: (38, 38),
    exp.DType.DECIMAL256: (76, 76),
}
_STRING_ALTER_TYPES = {
    (exp.DType.CHAR, exp.DType.CHAR),
    (exp.DType.CHAR, exp.DType.VARCHAR),
    (exp.DType.VARCHAR, exp.DType.VARCHAR),
}
# Split StarRocks-supported conversions by whether every source value is preserved.
_SAFE_ALTER_TYPES = {
    exp.DType.TINYINT: {
        exp.DType.SMALLINT,
        exp.DType.INT,
        exp.DType.BIGINT,
        exp.DType.INT128,
        exp.DType.DOUBLE,
    },
    exp.DType.SMALLINT: {
        exp.DType.INT,
        exp.DType.BIGINT,
        exp.DType.INT128,
        exp.DType.DOUBLE,
    },
    exp.DType.INT: {
        exp.DType.BIGINT,
        exp.DType.INT128,
        exp.DType.DOUBLE,
    },
    exp.DType.BIGINT: {exp.DType.INT128},
    exp.DType.FLOAT: {exp.DType.DOUBLE},
    exp.DType.DATE: {exp.DType.DATETIME},
}
# Minimum VARCHAR capacities needed to preserve each type's text representation.
_VARCHAR_DISPLAY_LENGTHS = {
    exp.DType.TINYINT: 4,
    exp.DType.SMALLINT: 6,
    exp.DType.INT: 11,
    exp.DType.BIGINT: 20,
    exp.DType.INT128: 40,
    exp.DType.FLOAT: 12,
    exp.DType.DOUBLE: 22,
}
_LOSSY_ALTER_TYPES = {
    exp.DType.INT: {exp.DType.DATE},
    exp.DType.BIGINT: {exp.DType.DOUBLE},
    exp.DType.VARCHAR: {
        exp.DType.TINYINT,
        exp.DType.SMALLINT,
        exp.DType.INT,
        exp.DType.BIGINT,
        exp.DType.INT128,
        exp.DType.FLOAT,
        exp.DType.DOUBLE,
        exp.DType.DATE,
        exp.DType.JSON,
    },
    exp.DType.DATETIME: {exp.DType.DATE},
}
_ROLLUP_AGGREGATE_FUNCTIONS = {
    "approx_count_distinct",
    "bitmap_union",
    "bitmap_union_count",
    "hll_raw_agg",
    "hll_union",
    "hll_union_agg",
    "ndv",
    "percentile_approx",
    "percentile_union",
    "replace_if_not_null",
    "starrocks_replace",
}
_KEY_ROLES = ("primary key", "unique key", "duplicate key")


@dataclass(frozen=True)
class _SchemaChangeJob:
    job_id: int
    state: str
    message: str
    progress: t.Any


@dataclass(frozen=True)
class _SchemaChangeContext:
    """Live table metadata needed to validate and render a schema change."""

    columns_by_role: t.Dict[str, t.Set[str]]
    column_definitions: t.Dict[str, exp.ColumnDef]
    fast_schema_evolution: bool

    def roles_for(self, column_name: str) -> t.Set[str]:
        column_name = column_name.lower()
        return {role for role, columns in self.columns_by_role.items() if column_name in columns}

    def is_key(self, column_name: str) -> bool:
        column_name = column_name.lower()
        return any(column_name in self.columns_by_role[role] for role in _KEY_ROLES)

    @property
    def is_primary_key_table(self) -> bool:
        return bool(self.columns_by_role["primary key"])


def _type_parameters(data_type: exp.DataType) -> t.Tuple[int, ...]:
    try:
        return tuple(int(parameter.name) for parameter in data_type.expressions)
    except ValueError:
        return ()


def _decimal_parameters(data_type: exp.DataType) -> t.Tuple[int, int]:
    parameters = _type_parameters(data_type)
    default_precision, default_scale = _DECIMAL_DEFAULTS[data_type.this]

    precision = parameters[0] if parameters else default_precision
    if len(parameters) == 2:
        scale = parameters[1]
    elif data_type.this == exp.DType.DECIMAL:
        scale = 0
    else:
        scale = min(precision, default_scale)
    return precision, scale


def _string_capacity(data_type: exp.DataType) -> t.Optional[int]:
    if data_type.is_type(exp.DType.TEXT):
        # StarRocks STRING is an alias for VARCHAR(65533), not maximum-sized VARCHAR.
        return _STRING_ALIAS_LENGTH
    if data_type.is_type(exp.DType.CHAR, exp.DType.VARCHAR):
        # StarRocks defaults an omitted CHAR or VARCHAR length to one.
        if not data_type.expressions:
            return 1
        parameters = _type_parameters(data_type)
        return parameters[0] if len(parameters) == 1 else None
    return None


def _is_decimal(data_type: exp.DataType) -> bool:
    return data_type.is_type(*_DECIMAL_TYPES)


def _base_type(data_type: exp.DataType) -> exp.DType:
    return exp.DType.VARCHAR if data_type.is_type(exp.DType.TEXT) else data_type.this


def _normalize_data_type(data_type: exp.DataType) -> exp.DataType:
    def canonicalize(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.DataType):
            return node
        if node.is_type(exp.DType.TEXT):
            return exp.DataType.build(f"VARCHAR({_STRING_ALIAS_LENGTH})")
        if node.is_type(exp.DType.CHAR, exp.DType.VARCHAR) and not node.expressions:
            type_name = "CHAR" if node.is_type(exp.DType.CHAR) else "VARCHAR"
            return exp.DataType.build(f"{type_name}(1)", dialect="starrocks")
        if _is_decimal(node):
            precision, scale = _decimal_parameters(node)
            return exp.DataType.build(f"DECIMAL({precision}, {scale})", dialect="starrocks")
        return node

    return t.cast(exp.DataType, data_type.transform(canonicalize))


def _has_bare_varbinary(expression: exp.Expression) -> bool:
    return any(
        data_type.is_type(exp.DType.VARBINARY) and not data_type.expressions
        for data_type in expression.find_all(exp.DataType)
    )


def _has_rollup_aggregate(query: exp.Select) -> bool:
    if query.find(exp.AggFunc):
        return True
    return any(
        function.name.casefold() in _ROLLUP_AGGREGATE_FUNCTIONS
        or function.name.casefold().endswith("_union")
        for function in query.find_all(exp.Anonymous)
    )


def _normalize_rollup_definition(definition: str) -> str:
    # StarRocks synthesizes REPLACE(column) for Unique Key rollups. SQLGlot interprets REPLACE
    # as the three-argument string function, so give only this generated one-argument form an
    # unambiguous name before parsing it.
    return re.sub(
        r"\bREPLACE\s*\(\s*(`(?:``|[^`])+`|[A-Za-z_][\w$]*)\s*\)",
        r"STARROCKS_REPLACE(\1)",
        definition,
        flags=re.IGNORECASE,
    )


def _is_safe_decimal_change(current_type: exp.DataType, new_type: exp.DataType) -> bool:
    current_precision, current_scale = _decimal_parameters(current_type)
    target_precision, target_scale = _decimal_parameters(new_type)
    return current_scale <= target_scale and (
        current_precision - current_scale <= target_precision - target_scale
    )


def _classify_decimal_type_change(
    current_type: exp.DataType,
    new_type: exp.DataType,
) -> AlterColumnTypeSupport:
    if not _is_decimal(current_type):
        return (
            AlterColumnTypeSupport.MODIFY_DESTRUCTIVE
            if _base_type(current_type) == exp.DType.VARCHAR
            else AlterColumnTypeSupport.DROP_AND_ADD
        )

    if _is_decimal(new_type):
        return (
            AlterColumnTypeSupport.MODIFY
            if _is_safe_decimal_change(current_type, new_type)
            else AlterColumnTypeSupport.DROP_AND_ADD
        )

    if _base_type(new_type) != exp.DType.VARCHAR:
        return AlterColumnTypeSupport.DROP_AND_ADD

    target_capacity = _string_capacity(new_type)
    if target_capacity is None:
        return AlterColumnTypeSupport.DROP_AND_ADD

    precision, scale = _decimal_parameters(current_type)
    # StarRocks reserves three characters for a sign, zero, and overflow, plus a decimal
    # point when the scale is nonzero.
    required_capacity = precision + 3 + (1 if scale else 0)
    return (
        AlterColumnTypeSupport.MODIFY
        if target_capacity >= required_capacity
        else AlterColumnTypeSupport.DROP_AND_ADD
    )


def _classify_varbinary_type_change(
    current_type: exp.DataType,
    new_type: exp.DataType,
) -> AlterColumnTypeSupport:
    current_parameters = _type_parameters(current_type)
    target_parameters = _type_parameters(new_type)

    # StarRocks expands bare VARBINARY to the FE's mutable max_varchar_length setting. Use
    # MODIFY COLUMN to preserve data, then require an explicit length before execution.
    # https://github.com/StarRocks/starrocks/blob/4.1.3/fe/fe-core/src/main/java/com/starrocks/sql/analyzer/ColumnDefAnalyzer.java#L114-L121
    if not target_parameters or not current_parameters:
        return AlterColumnTypeSupport.MODIFY_DESTRUCTIVE
    if len(current_parameters) != 1 or len(target_parameters) != 1:
        return AlterColumnTypeSupport.DROP_AND_ADD
    return (
        AlterColumnTypeSupport.MODIFY
        if target_parameters[0] >= current_parameters[0]
        else AlterColumnTypeSupport.MODIFY_DESTRUCTIVE
    )


def _classify_string_type_change(
    current_type: exp.DataType,
    new_type: exp.DataType,
) -> AlterColumnTypeSupport:
    current_capacity = _string_capacity(current_type)
    target_capacity = _string_capacity(new_type)
    return (
        AlterColumnTypeSupport.MODIFY
        if current_capacity is not None
        and target_capacity is not None
        and target_capacity >= current_capacity
        else AlterColumnTypeSupport.DROP_AND_ADD
    )


def _classify_to_varchar(
    current_type: exp.DType,
    new_type: exp.DataType,
) -> AlterColumnTypeSupport:
    target_capacity = _string_capacity(new_type)
    if target_capacity is None:
        return AlterColumnTypeSupport.DROP_AND_ADD

    if current_type == exp.DType.JSON:
        return (
            AlterColumnTypeSupport.MODIFY_DESTRUCTIVE
            if target_capacity >= _MIN_JSON_VARCHAR_LENGTH
            else AlterColumnTypeSupport.DROP_AND_ADD
        )

    required_capacity = _VARCHAR_DISPLAY_LENGTHS.get(current_type)
    if required_capacity is None:
        return AlterColumnTypeSupport.DROP_AND_ADD
    return (
        AlterColumnTypeSupport.MODIFY
        if target_capacity >= required_capacity
        else AlterColumnTypeSupport.MODIFY_DESTRUCTIVE
    )


def _classify_type_change(
    current_type: exp.DataType, new_type: exp.DataType
) -> AlterColumnTypeSupport:
    """Classify StarRocks 4.1.3 schema changes, including type parameters."""

    if _normalize_data_type(current_type) == _normalize_data_type(new_type):
        return AlterColumnTypeSupport.NO_ALTER

    current_base_type = _base_type(current_type)
    target_base_type = _base_type(new_type)

    if _is_decimal(current_type) or _is_decimal(new_type):
        return _classify_decimal_type_change(current_type, new_type)

    if (current_base_type, target_base_type) in _STRING_ALTER_TYPES:
        return _classify_string_type_change(current_type, new_type)

    if current_base_type == target_base_type == exp.DType.VARBINARY:
        return _classify_varbinary_type_change(current_type, new_type)

    if target_base_type == exp.DType.VARCHAR:
        return _classify_to_varchar(current_base_type, new_type)

    if target_base_type in _SAFE_ALTER_TYPES.get(current_base_type, set()):
        return AlterColumnTypeSupport.MODIFY
    if target_base_type in _LOSSY_ALTER_TYPES.get(current_base_type, set()):
        return AlterColumnTypeSupport.MODIFY_DESTRUCTIVE
    return AlterColumnTypeSupport.DROP_AND_ADD


def _parenthesized_clause_end(tokens: t.Sequence[Token], start_index: int) -> t.Optional[int]:
    depth = 0
    for token in tokens[start_index:]:
        if token.token_type is TokenType.L_PAREN:
            depth += 1
        elif token.token_type is TokenType.R_PAREN:
            depth -= 1
            if depth == 0:
                return token.end + 1
    return None


def _strip_index_implementation_properties(sql: str) -> str:
    """Strip unparseable index properties without touching quoted text or comments."""

    tokens = tokenize(sql, dialect="starrocks")
    depth = 0
    in_index = False
    removals: t.List[t.Tuple[int, int]] = []

    for index, token in enumerate(tokens):
        if token.token_type is TokenType.L_PAREN:
            depth += 1
        elif token.token_type is TokenType.R_PAREN:
            depth -= 1
        elif depth == 1 and token.token_type is TokenType.COMMA:
            in_index = False
        elif depth == 1 and token.token_type is TokenType.INDEX:
            in_index = True
        elif (
            depth == 1
            and in_index
            and token.token_type is TokenType.USING
            and index + 2 < len(tokens)
            and tokens[index + 2].token_type is TokenType.L_PAREN
        ):
            properties_start = index + 2
            properties_end = _parenthesized_clause_end(tokens, properties_start)
            if properties_end is not None:
                removals.append((tokens[properties_start].start, properties_end))

    for start, end in reversed(removals):
        sql = f"{sql[:start]}{sql[end:]}"
    return sql


if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DCL, GrantsConfig
    from sqlmesh.core.model import Model
    from sqlmesh.core.node import IntervalUnit


@set_catalog()
class StarRocksEngineAdapter(
    LogicalMergeMixin,
    PandasNativeFetchDFSupportMixin,
    GetCurrentCatalogFromFunctionMixin,
    RowDiffMixin,
    GrantsFromInfoSchemaMixin,
):
    """StarRocks engine adapter."""

    DIALECT = "starrocks"
    DEFAULT_BATCH_SIZE = 10000
    # StarRocks supports only limited number of use cases for transactions.
    # See: https://docs.starrocks.io/docs/loading/SQL_transaction/
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_INDEXES = True
    SUPPORTS_GRANTS = True
    SUPPORTS_REPLACE_TABLE = False
    CURRENT_CATALOG_EXPRESSION = exp.func("catalog")
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
    MAX_TABLE_COMMENT_LENGTH = 1024
    MAX_COLUMN_COMMENT_LENGTH = 1024
    SUPPORTS_QUERY_EXECUTION_TRACKING = True
    SUPPORTS_TUPLE_IN = False
    # Though StarRocks supports materialized views, we don't support them yet.
    SUPPORTS_MATERIALIZED_VIEWS = False
    # While StarRocks tables can have names up to 1024 characters,
    # database (schema) names are limited to 256 characters.
    # See https://docs.starrocks.io/docs/sql-reference/System_limit/
    MAX_IDENTIFIER_LENGTH = 256
    CASE_SENSITIVE_GRANTEES = True
    _KEY_TYPES = {
        exp.DType.BOOLEAN,
        exp.DType.TINYINT,
        exp.DType.SMALLINT,
        exp.DType.INT,
        exp.DType.BIGINT,
        exp.DType.INT128,
        exp.DType.CHAR,
        exp.DType.VARCHAR,
        exp.DType.VARBINARY,
        exp.DType.TEXT,
        exp.DType.DATE,
        exp.DType.DATETIME,
    } | _DECIMAL_TYPES
    SCHEMA_DIFFER_KWARGS = {
        # StarRocks' compatibility rules depend on type parameters for string and decimal
        # conversions, so use the complete type definitions instead of enum-only pairs.
        "alter_column_type_support": _classify_type_change,
    }

    VIEW_SUPPORTED_PRIVILEGES: t.FrozenSet[str] = frozenset({"SELECT"})
    _TABLE_TYPE_MAP = {
        "BASE TABLE": "table",
        "VIEW": "view",
    }

    def __init__(self, *args: t.Any, **kwargs: t.Any):
        super().__init__(*args, **kwargs)
        # StarRocks has a default internal catalog named "default_catalog"
        self._default_catalog = self._default_catalog or "default_catalog"

    @property
    def catalog_support(self) -> CatalogSupport:
        # StarRocks has a default internal catalog (default_catalog) and supports external catalogs,
        # but external catalogs require connectors to external systems (Hive, JDBC, etc.).
        # Since internal catalogs cannot be created/dropped dynamically, we treat it as single catalog only.
        return CatalogSupport.SINGLE_CATALOG_ONLY

    def ping(self) -> None:
        self._connection_pool.get().ping(reconnect=False)

    def alter_table(
        self,
        alter_expressions: t.Union[t.List[exp.Alter], t.List[TableAlterOperation]],
    ) -> None:
        if not alter_expressions or isinstance(alter_expressions[0], exp.Alter):
            super().alter_table(t.cast(t.List[exp.Alter], alter_expressions))
            return

        alter_operations = t.cast(t.List[TableAlterColumnOperation], alter_expressions)
        table = alter_operations[0].target_table
        if any(operation.target_table != table for operation in alter_operations[1:]):
            raise SQLMeshError("StarRocks alter batches must target exactly one table.")
        prepared_changes = self._prepare_alter_operations(alter_operations)

        for alter_operation, alter_statement in prepared_changes:
            latest_job = self._get_latest_schema_change_job(table)
            previous_job_id = latest_job.job_id if latest_job else None

            super().alter_table([alter_statement])
            self._wait_for_schema_change(
                table,
                previous_job_id,
                alter_operation,
            )

    def _prepare_alter_operations(
        self,
        alter_operations: t.List[TableAlterColumnOperation],
    ) -> t.List[t.Tuple[TableAlterColumnOperation, exp.Alter]]:
        """Validate and render all operations before executing any schema changes."""

        if any(
            _has_bare_varbinary(operation.expected_table_struct) for operation in alter_operations
        ):
            raise MigrationNotSupportedError(
                "StarRocks cannot safely apply and verify an in-place schema change with a "
                "bare VARBINARY. Declare an explicit VARBINARY(n) length."
            )

        table = alter_operations[0].target_table
        table_definition = self._get_table_definition(table)
        if not any(self._requires_column_role_check(operation) for operation in alter_operations):
            return [(operation, operation.expression) for operation in alter_operations]

        context = self._get_schema_change_context(table, table_definition)
        return [
            (operation, self._prepare_alter_operation(operation, context))
            for operation in alter_operations
        ]

    def _prepare_alter_operation(
        self,
        alter_operation: TableAlterColumnOperation,
        context: _SchemaChangeContext,
    ) -> exp.Alter:
        if not self._requires_column_role_check(alter_operation):
            return alter_operation.expression

        if reason := self._unsupported_column_change_reason(alter_operation, context):
            raise MigrationNotSupportedError(
                f"{reason} "
                "A new physical table version is required; use a non-forward-only plan "
                "and disable the model's 'forward_only' setting if it is configured."
            )

        if (
            not isinstance(alter_operation, TableAlterChangeColumnTypeOperation)
            or not alter_operation.column_parts[0].is_primitive
        ):
            return alter_operation.expression

        column_name = alter_operation.column.name.lower()
        column_definition = context.column_definitions.get(column_name)
        if column_definition is None:
            raise MigrationNotSupportedError(
                f"Unable to reconstruct column '{column_name}' from the live StarRocks "
                "table definition."
            )

        return self._build_modify_column_expression(
            alter_operation,
            column_definition,
            is_key_column=context.is_key(column_name),
        )

    def _get_latest_schema_change_job(self, table: exp.Table) -> t.Optional[_SchemaChangeJob]:
        database = (
            f" FROM {exp.to_identifier(table.db).sql(dialect=self.dialect, identify=True)}"
            if table.db
            else ""
        )
        rows = self.fetchall(
            f"SHOW ALTER TABLE COLUMN{database} "
            f"WHERE TableName = {exp.Literal.string(table.name).sql(dialect=self.dialect)} "
            "ORDER BY JobId DESC LIMIT 1"
        )
        if not rows:
            return None

        row = rows[0]
        return _SchemaChangeJob(
            job_id=int(row[0]),
            state=str(row[9]).upper(),
            message=str(row[10]),
            progress=row[11],
        )

    def _wait_for_schema_change(
        self,
        table: exp.Table,
        previous_job_id: t.Optional[int],
        operation: TableAlterColumnOperation,
    ) -> None:
        timeout = float(self._extra_config.get("schema_change_timeout", 3600))
        poll_interval = float(self._extra_config.get("schema_change_poll_interval", 1))
        deadline = time.monotonic() + timeout
        latest_job = None

        while True:
            job = self._get_latest_schema_change_job(table)

            # The latest job may belong to another concurrent schema change on this table.
            if self._is_operation_applied(table, operation):
                return

            if job and (previous_job_id is None or job.job_id > previous_job_id):
                latest_job = job
                if job.state == "FINISHED":
                    raise SQLMeshError(
                        f"StarRocks schema change job {job.job_id} finished, but {table} does "
                        "not reflect the expected operation."
                    )
                if job.state in {"CANCELLED", "CANCELED", "FAILED"}:
                    raise SQLMeshError(
                        f"StarRocks schema change job {job.job_id} for {table} failed: {job.message}"
                    )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                details = (
                    f" Last state: {latest_job.state}, progress: {latest_job.progress}."
                    if latest_job
                    else ""
                )
                raise SQLMeshError(
                    f"Timed out after {timeout:g} seconds waiting for a StarRocks schema "
                    f"change on {table}.{details}"
                )
            time.sleep(min(poll_interval, remaining))

    def _is_operation_applied(
        self,
        table: exp.Table,
        operation: TableAlterColumnOperation,
    ) -> bool:
        columns = self.columns(table)
        column_name = operation.column.name
        if isinstance(operation, TableAlterDropColumnOperation):
            return column_name not in columns
        if not isinstance(
            operation,
            (TableAlterAddColumnOperation, TableAlterChangeColumnTypeOperation),
        ):
            return False

        actual_type = columns.get(column_name)
        if actual_type is None:
            return False
        return _normalize_data_type(actual_type) == _normalize_data_type(operation.column_type)

    def _get_table_definition(self, table: exp.Table) -> exp.Create:
        table = table.copy()
        table.set("catalog", None)
        table_sql = table.sql(dialect=self.dialect, identify=True)
        row = self.fetchone(f"SHOW CREATE TABLE {table_sql}")
        if not row or len(row) < 2 or not isinstance(row[1], str):
            raise MigrationNotSupportedError(
                f"Unable to read the live StarRocks table definition for {table_sql}."
            )
        definition_sql = row[1]

        # Aggregate Key schema changes require key and aggregation metadata that generic schema
        # operations don't preserve, so these tables must be rebuilt instead.
        if re.search(
            r"^\s*AGGREGATE\s+KEY\s*\(",
            definition_sql,
            re.IGNORECASE | re.MULTILINE,
        ):
            raise MigrationNotSupportedError(
                f"In-place schema changes are not supported for the Aggregate Key table "
                f"{table_sql}."
            )

        # SHOW CREATE TABLE renders index implementation properties immediately after USING
        # (for example, USING GIN("parser" = "none")), which SQLGlot doesn't parse. The
        # properties don't affect column-role validation, so retain the index type and remove
        # only its parenthesized implementation options.
        try:
            table_definition = parse_one(
                _strip_index_implementation_properties(definition_sql),
                dialect=self.dialect,
            )
        except SqlglotError as ex:
            raise MigrationNotSupportedError(
                f"Unable to parse the live StarRocks table definition for {table_sql}."
            ) from ex

        if not isinstance(table_definition, exp.Create) or not isinstance(
            table_definition.this, exp.Schema
        ):
            raise MigrationNotSupportedError(
                f"Unable to parse the live StarRocks table definition for {table_sql} "
                "into a complete CREATE TABLE statement."
            )

        return table_definition

    def can_apply_schema_change_in_place(
        self,
        current: Model,
        target: Model,
        current_table: TableName,
        **render_kwargs: t.Any,
    ) -> bool:
        # SQLMesh does not update partitioning or physical properties during schema migration.
        if current.partitioned_by != target.partitioned_by:
            return False

        current_properties = current.render_physical_properties(**render_kwargs)
        target_properties = target.render_physical_properties(**render_kwargs)
        if current_properties != target_properties:
            return False

        target_columns = target.columns_to_types
        if not target_columns or not columns_to_types_all_known(target_columns):
            return False

        current_columns = self.columns(current_table)
        if not current_columns or not columns_to_types_all_known(current_columns):
            return False

        try:
            operations = self.schema_differ.compare_columns(
                current_table,
                current_columns,
                target_columns,
                ignore_destructive=target.on_destructive_change.is_ignore,
                ignore_additive=target.on_additive_change.is_ignore,
            )
            if not operations:
                return True
            self._prepare_alter_operations(operations)
        except MigrationNotSupportedError:
            return False
        return True

    @staticmethod
    def _requires_column_role_check(
        operation: TableAlterColumnOperation,
    ) -> bool:
        return (
            isinstance(
                operation,
                (TableAlterDropColumnOperation, TableAlterChangeColumnTypeOperation),
            )
            and len(operation.column_parts) == 1
        )

    @classmethod
    def _parse_column_roles(cls, expression: exp.Expr) -> t.Dict[str, t.Set[str]]:
        primary_key = expression.find(exp.PrimaryKey)
        unique_key = expression.find(exp.UniqueKeyProperty)
        duplicate_key = expression.find(exp.DuplicateKeyProperty)
        distributed_by = expression.find(exp.DistributedByProperty)
        order = expression.find(exp.Order)
        partition = expression.find(
            exp.PartitionedByProperty,
            exp.PartitionByRangeProperty,
            exp.PartitionByListProperty,
        )

        column_definitions = list(expression.find_all(exp.ColumnDef))
        distribution_columns = cls._column_names(
            distributed_by.expressions if distributed_by else ()
        )
        has_colocated_distribution = any(
            property_.name.lower() == "colocate_with" and bool(property_.text("value"))
            for property_ in expression.find_all(exp.Property)
        )
        sort_key_expressions = list(order.find_all(exp.Column)) if order else []

        return {
            "primary key": cls._column_names(primary_key.expressions if primary_key else ()),
            "unique key": cls._column_names(unique_key.expressions if unique_key else ()),
            "duplicate key": cls._column_names(duplicate_key.expressions if duplicate_key else ()),
            "partitioning": cls._column_names(cls._partition_expressions(partition)),
            "distribution": distribution_columns,
            "colocated distribution": (
                distribution_columns.copy() if has_colocated_distribution else set()
            ),
            "sort key": cls._column_names(sort_key_expressions),
            "leading sort key": cls._column_names(sort_key_expressions[:1]),
            "rollup key": set(),
            "rollup value": set(),
            "rollup dependency": set(),
            "generated column expression": cls._column_names(
                constraint.this for constraint in expression.find_all(exp.ComputedColumnConstraint)
            ),
            "auto increment": {
                column.name.lower()
                for column in column_definitions
                if column.find(exp.AutoIncrementColumnConstraint)
            },
            "default value": {
                column.name.lower()
                for column in column_definitions
                if column.find(exp.DefaultColumnConstraint)
            },
            **cls._parse_index_column_roles(expression),
        }

    @staticmethod
    def _partition_expressions(
        partition: t.Optional[exp.Expr],
    ) -> t.Iterable[exp.Expr]:
        if isinstance(partition, exp.PartitionedByProperty):
            return partition.this.expressions
        return partition.args.get("partition_expressions", ()) if partition else ()

    @classmethod
    def _parse_index_column_roles(cls, expression: exp.Expr) -> t.Dict[str, t.Set[str]]:
        columns_by_role: t.Dict[str, t.Set[str]] = {
            "vector index": set(),
            "gin index": set(),
        }
        for index in expression.find_all(exp.IndexColumnConstraint):
            index_types = {option.text("using").upper() for option in index.args.get("options", ())}
            columns = cls._column_names(index.expressions)
            if "VECTOR" in index_types:
                columns_by_role["vector index"].update(columns)
            if index_types.intersection({"GIN", "INVERTED"}):
                columns_by_role["gin index"].update(columns)
        return columns_by_role

    def _get_schema_change_context(
        self,
        table: exp.Table,
        table_definition: exp.Create,
    ) -> _SchemaChangeContext:
        return _SchemaChangeContext(
            columns_by_role=self._get_live_column_roles(table, table_definition),
            column_definitions={
                column.name.lower(): column
                for column in table_definition.this.expressions
                if isinstance(column, exp.ColumnDef)
            },
            fast_schema_evolution=self._is_table_property_enabled(
                table_definition, "fast_schema_evolution"
            ),
        )

    def _get_live_column_roles(
        self,
        table: exp.Table,
        table_definition: exp.Create,
    ) -> t.Dict[str, t.Set[str]]:
        columns_by_role = self._parse_column_roles(table_definition)
        if self._get_table_distribution_type(table) == "RANGE":
            # Range boundaries use the explicit sort key, or the table key when none is set.
            range_sort_columns = (
                columns_by_role["sort key"]
                or columns_by_role["primary key"]
                or columns_by_role["unique key"]
                or columns_by_role["duplicate key"]
            )
            columns_by_role["range distribution sort key"] = set(range_sort_columns)
        rollup_columns_by_role, rollup_names = self._get_rollup_metadata(table)
        for role, columns in rollup_columns_by_role.items():
            columns_by_role[role].update(columns)
        if rollup_names:
            columns_by_role["rollup dependency"].update(
                self._get_rollup_dependency_columns(table, rollup_names)
            )
        return columns_by_role

    def _get_table_distribution_type(self, table: exp.Table) -> str:
        schema_filter: exp.Expr = exp.Literal.string(table.db) if table.db else exp.func("database")
        query = (
            exp.select("DISTRIBUTE_TYPE")
            .from_(exp.table_("tables_config", db="information_schema"))
            .where(
                exp.column("TABLE_NAME").eq(exp.Literal.string(table.name)),
                exp.column("TABLE_SCHEMA").eq(schema_filter),
            )
        )
        row = self.fetchone(query)
        if not row or not row[0]:
            raise MigrationNotSupportedError(
                f"Unable to determine the distribution type of the live StarRocks table {table}."
            )
        return str(row[0]).upper()

    @staticmethod
    def _is_table_property_enabled(expression: exp.Expr, property_name: str) -> bool:
        return any(
            property_.name.casefold() == property_name.casefold()
            and property_.text("value").casefold() == "true"
            for property_ in expression.find_all(exp.Property)
        )

    def _get_rollup_metadata(
        self, table: exp.Table
    ) -> t.Tuple[t.Dict[str, t.Set[str]], t.Set[str]]:
        table = table.copy()
        table.set("catalog", None)
        table_sql = table.sql(dialect=self.dialect, identify=True)

        rows = self.fetchall(f"DESC {table_sql} ALL")

        base_index_name = table.name.casefold()
        current_index_name = None
        columns_by_role: t.Dict[str, t.Set[str]] = {
            "rollup key": set(),
            "rollup value": set(),
        }
        rollup_names: t.Set[str] = set()
        for row in rows:
            if len(row) < 6:
                raise MigrationNotSupportedError(
                    f"Unable to interpret rollup metadata for the live StarRocks table {table_sql}."
                )
            index_name, _, column_name, _, _, key_flag, *_ = row
            if index_name:
                index_name = str(index_name)
                current_index_name = index_name.casefold()
                if current_index_name != base_index_name:
                    rollup_names.add(index_name)
            if not current_index_name or current_index_name == base_index_name or not column_name:
                continue

            role = {"true": "rollup key", "false": "rollup value"}.get(str(key_flag).lower())
            if role is None:
                raise MigrationNotSupportedError(
                    f"Unable to interpret the rollup key flag for column '{column_name}' in "
                    f"the live StarRocks table {table_sql}."
                )
            columns_by_role[role].add(str(column_name).lower())
        return columns_by_role, rollup_names

    def _get_rollup_dependency_columns(
        self, table: exp.Table, rollup_names: t.Set[str]
    ) -> t.Set[str]:
        """Return base columns that StarRocks will not modify while a rollup uses them."""

        schema_filter: exp.Expr = exp.Literal.string(table.db) if table.db else exp.func("database")
        # StarRocks 4.1.3 applies TABLE_NAME equality to the base table when fetching synchronous
        # MVs, even though the returned TABLE_NAME is the rollup name. Adding REFRESH_TYPE here
        # prevents this FE evaluation and filters out every returned rollup.
        # https://github.com/StarRocks/starrocks/blob/4.1.3/fe/fe-core/src/main/java/com/starrocks/catalog/system/information/MaterializedViewsSystemTable.java#L124-L180
        # https://github.com/StarRocks/starrocks/blob/4.1.3/fe/fe-core/src/main/java/com/starrocks/catalog/system/information/MaterializedViewsSystemTable.java#L302-L330
        rows = self.fetchall(
            exp.select("TABLE_NAME", "MATERIALIZED_VIEW_DEFINITION")
            .from_(exp.table_("materialized_views", db="information_schema"))
            .where(
                exp.column("TABLE_SCHEMA").eq(schema_filter),
                exp.column("TABLE_NAME").eq(exp.Literal.string(table.name)),
            )
        )

        table_name = table.name.casefold()
        expected_rollup_names = {name.casefold() for name in rollup_names}
        inspected_rollup_names: t.Set[str] = set()
        dependencies: t.Set[str] = set()
        for row in rows:
            if not row:
                raise MigrationNotSupportedError(
                    f"Unable to inspect a synchronous materialized-view definition while "
                    f"validating the live StarRocks table {table}."
                )

            rollup_name = str(row[0]).casefold()
            if rollup_name not in expected_rollup_names:
                continue
            query = self._parse_rollup_query(table, row)

            referenced_tables = {source.name.casefold() for source in query.find_all(exp.Table)}
            if table_name not in referenced_tables:
                continue

            inspected_rollup_names.add(rollup_name)
            dependencies.update(self._rollup_dependency_columns(query))

        missing_rollup_names = expected_rollup_names - inspected_rollup_names
        if missing_rollup_names:
            raise MigrationNotSupportedError(
                "Unable to find inspectable synchronous materialized-view definitions for "
                f"the following rollups on the live StarRocks table {table}: "
                f"{', '.join(sorted(missing_rollup_names))}."
            )
        return dependencies

    def _parse_rollup_query(
        self,
        table: exp.Table,
        row: t.Tuple[t.Any, ...],
    ) -> exp.Select:
        rollup_name = row[0]
        if len(row) < 2 or not isinstance(row[1], str) or not row[1].strip():
            raise MigrationNotSupportedError(
                f"Unable to inspect synchronous materialized view '{rollup_name}' while "
                f"validating the live StarRocks table {table}."
            )

        try:
            definition = parse_one(_normalize_rollup_definition(row[1]), dialect=self.dialect)
        except SqlglotError as ex:
            raise MigrationNotSupportedError(
                f"Unable to parse synchronous materialized view '{rollup_name}' while "
                f"validating the live StarRocks table {table}."
            ) from ex

        query = definition.find(exp.Select)
        if query is None:
            raise MigrationNotSupportedError(
                f"Unable to inspect synchronous materialized view '{rollup_name}' while "
                f"validating the live StarRocks table {table}."
            )
        return query

    @classmethod
    def _rollup_dependency_columns(cls, query: exp.Select) -> t.Set[str]:
        dependencies: t.Set[str] = set()
        if _has_rollup_aggregate(query):
            # StarRocks blocks every visible column in an aggregated rollup, as well as
            # source columns referenced by aliased or complex projection expressions.
            dependencies.update(cls._column_names(query.expressions))
            if group := query.args.get("group"):
                dependencies.update(cls._column_names(group.expressions))

        if where := query.args.get("where"):
            dependencies.update(cls._column_names([where]))
        return dependencies

    def _allowed_type_change_roles(
        self,
        operation: TableAlterChangeColumnTypeOperation,
        context: _SchemaChangeContext,
    ) -> t.Set[str]:
        """Return column roles that StarRocks permits for this exact type change."""

        allowed_roles = {"sort key"}
        if operation.column_type.this in self._KEY_TYPES:
            allowed_roles.update({"unique key", "duplicate key", "rollup key"})

        if self._is_varchar_length_increase(operation):
            allowed_roles.update({"partitioning", "primary key", "distribution"})
            # StarRocks 4.1.3 permits this exception only when fast schema evolution is enabled;
            # the generated MODIFY does not reposition, reorder, or otherwise change the column.
            # https://github.com/StarRocks/starrocks/blob/4.1.3/fe/fe-core/src/main/java/com/starrocks/alter/SchemaChangeHandler.java#L1006-L1030
            if context.fast_schema_evolution:
                allowed_roles.add("range distribution sort key")
        elif context.is_primary_key_table:
            allowed_roles.discard("sort key")

        # A floating-point leading sort key produces no valid StarRocks short key. A later
        # Duplicate-table sort column is valid because a preceding column remains the short key.
        if not operation.column_type.is_type(exp.DType.FLOAT, exp.DType.DOUBLE):
            allowed_roles.add("leading sort key")
        if operation.column_type.is_type(exp.DType.CHAR, exp.DType.VARCHAR, exp.DType.TEXT):
            allowed_roles.add("gin index")
        if not operation.is_destructive:
            allowed_roles.add("default value")
        return allowed_roles

    def _unsupported_column_change_reason(
        self,
        operation: TableAlterColumnOperation,
        context: _SchemaChangeContext,
    ) -> t.Optional[str]:
        column_name = operation.column.name
        blocked_roles = context.roles_for(column_name)

        if isinstance(operation, TableAlterChangeColumnTypeOperation):
            blocked_roles -= self._allowed_type_change_roles(operation, context)
        else:
            # A default does not prevent its column from being dropped.
            blocked_roles.discard("default value")

        if not blocked_roles:
            return None

        # Render the schema-diff operation so the error matches the attempted StarRocks SQL.
        operation_sql = operation.expression.sql(dialect=self.dialect, identify=True)

        return (
            f"StarRocks cannot apply {operation_sql} because column '{column_name}' is used by "
            "the following table constraints or properties: "
            f"{', '.join(sorted(blocked_roles))}."
        )

    @staticmethod
    def _is_varchar_length_increase(
        operation: TableAlterChangeColumnTypeOperation,
    ) -> bool:
        if not operation.current_type.is_type(exp.DType.VARCHAR):
            return False

        current_capacity = _string_capacity(operation.current_type)
        target_capacity = _string_capacity(operation.column_type)
        return (
            current_capacity is not None
            and target_capacity is not None
            and target_capacity > current_capacity
        )

    @staticmethod
    def _column_names(expressions: t.Iterable[exp.Expr]) -> t.Set[str]:
        names: t.Set[str] = set()
        for expression in expressions:
            if isinstance(expression, exp.Identifier):
                names.add(expression.name.lower())
            else:
                names.update(column.name.lower() for column in expression.find_all(exp.Column))
        return names

    def _build_modify_column_expression(
        self,
        operation: TableAlterChangeColumnTypeOperation,
        column_definition: exp.ColumnDef,
        is_key_column: bool = False,
    ) -> exp.Alter:
        column_definition = column_definition.copy()
        column_definition.set("this", operation.column.copy())
        column_definition.set("kind", operation.column_type.copy())
        if is_key_column:
            column_definition.set(
                "constraints",
                [
                    exp.ColumnConstraint(kind=exp.Var(this="KEY")),
                    *column_definition.constraints,
                ],
            )

        return exp.Alter(
            this=operation.target_table,
            kind="TABLE",
            actions=[exp.ModifyColumn(this=column_definition)],
        )

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
        properties: t.Optional[t.List[exp.Expr]] = None,
    ) -> None:
        # StarRocks uses databases instead of schemas
        return self._create_schema(
            schema_name=schema_name,
            ignore_if_exists=ignore_if_exists,
            warn_on_error=warn_on_error,
            properties=properties or [],
            kind="DATABASE",
        )

    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
        **drop_args: t.Dict[str, exp.Expr],
    ) -> None:
        # StarRocks doesn't support CASCADE clause
        return self._drop_object(
            name=schema_name,
            exists=ignore_if_not_exists,
            kind="DATABASE",
            cascade=False,
            **drop_args,
        )

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        catalog = self.get_current_catalog()
        query = (
            exp.select("table_name", "table_schema", "table_type")
            .from_(exp.table_("tables", db="information_schema"))
            .where(exp.column("table_schema").eq(to_schema(schema_name).db))
        )
        if object_names:
            query = query.where(exp.column("table_name").isin(*object_names))
        rows = self.fetchdf(query).itertuples(index=False, name=None)
        return [
            DataObject(
                catalog=catalog,
                schema=table_schema,
                name=table_name,
                type=DataObjectType.from_str(
                    self._TABLE_TYPE_MAP.get(
                        str(table_type),
                        str(table_type),
                    )
                ),
            )
            for table_name, table_schema, table_type in rows
        ]

    def _create_table_like(
        self,
        target_table_name: TableName,
        source_table_name: TableName,
        exists: bool,
        **kwargs: t.Any,
    ) -> None:
        self.execute(
            exp.Create(
                this=exp.to_table(target_table_name),
                kind="TABLE",
                exists=exists,
                properties=exp.Properties(
                    expressions=[
                        exp.LikeProperty(
                            this=exp.to_table(source_table_name),
                        ),
                    ],
                ),
            )
        )

    def _build_table_properties_exp(
        self,
        catalog_name: t.Optional[str] = None,
        table_format: t.Optional[str] = None,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expr]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[exp.Expr]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expr]] = None,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        properties: t.List[exp.Expr] = []
        props = {k.lower(): v for k, v in (table_properties or {}).items()}

        if table_description:
            properties.append(self._build_table_description_property(table_description))

        if partitioned_by:
            properties.append(self._build_partitioned_by_exp(partitioned_by))

        if primary_key_expr := props.pop("primary_key", None):
            properties.append(self._build_primary_key_property(primary_key_expr))

        if distributed_by_expr := props.pop("distributed_by", None):
            properties.append(self._build_distributed_by_property(distributed_by_expr))

        if order_by_expr := props.pop("order_by", None):
            properties.append(self._build_order_by_property(order_by_expr))

        if rollup_expr := props.pop("rollup", None):
            properties.append(self._build_rollup_property(rollup_expr))

        properties.extend(self._table_or_view_properties_to_expressions(props))

        return exp.Properties(expressions=properties) if properties else None

    def _build_engine_property(self, engine: str) -> exp.EngineProperty:
        return exp.EngineProperty(this=engine)

    def _build_primary_key_property(
        self,
        primary_key_expr: exp.Expr,
    ) -> exp.PrimaryKey:
        return exp.PrimaryKey(expressions=primary_key_expr.expressions)

    def _build_table_description_property(
        self, table_description: str
    ) -> exp.SchemaCommentProperty:
        return exp.SchemaCommentProperty(
            this=exp.Literal.string(self._truncate_table_comment(table_description))
        )

    def _build_partitioned_by_exp(
        self,
        partitioned_by: t.List[exp.Expr],
        **kwargs: t.Any,
    ) -> exp.PartitionedByProperty:
        return exp.PartitionedByProperty(this=exp.Schema(expressions=partitioned_by))

    def _build_distributed_by_property(
        self,
        distributed_by_expr: exp.Expr,
    ) -> exp.DistributedByProperty:
        if isinstance(distributed_by_expr, exp.Rand):
            buckets_prop = distributed_by_expr.args.get("this")

            return exp.DistributedByProperty(
                kind=exp.var("RANDOM"),
                buckets=buckets_prop.expression if buckets_prop else None,
            )

        if (
            isinstance(distributed_by_expr, exp.Anonymous)
            and distributed_by_expr.name.upper() == "HASH"
        ):
            props = {p.this.name.lower(): p.expression for p in distributed_by_expr.expressions}

            if not (columns := props.get("columns")):
                raise SQLMeshError(
                    f"Invalid 'distributed_by' value: {distributed_by_expr}. "
                    "'HASH' distribution requires 'columns' parameter."
                )

            return exp.DistributedByProperty(
                kind=exp.var("HASH"),
                expressions=columns.expressions if isinstance(columns, exp.Tuple) else [columns],
                buckets=props.get("buckets"),
            )

        raise SQLMeshError(
            f"Invalid 'distributed_by' value: {distributed_by_expr}. "
            "Expected HASH(columns := (col1, col2, ...)) or RANDOM()."
        )

    def _build_rollup_property(self, rollup_expr: exp.Expr) -> exp.RollupProperty:
        return exp.RollupProperty(
            expressions=[
                exp.Schema(this=expr.this, expressions=expr.expression.expressions)
                for expr in rollup_expr.expressions
            ]
        )

    def _build_order_by_property(self, order_by_expr: exp.Expr) -> exp.Order:
        exprs = (
            order_by_expr.expressions
            if isinstance(order_by_expr, (exp.Tuple, exp.Array))
            else [order_by_expr]
        )
        return exp.Order(expressions=[exp.Tuple(expressions=exprs)])

    @staticmethod
    def _grant_object_kind(table_type: DataObjectType) -> str:
        if table_type == DataObjectType.VIEW:
            return "VIEW"
        return "TABLE"

    def _get_current_schema(self) -> str:
        result = self.fetchone(exp.select(exp.func("database")))
        if result and result[0]:
            return str(result[0])
        raise SQLMeshError("Unable to determine current schema/database")

    def _get_current_grants_config(self, table: exp.Table) -> "GrantsConfig":
        schema_identifier = table.args.get("db") or normalize_identifiers(
            exp.to_identifier(self._get_current_schema(), quoted=True), dialect=self.dialect
        )
        schema_name = (
            schema_identifier.this if hasattr(schema_identifier, "this") else str(schema_identifier)
        )
        table_name = table.args.get("this").this  # type: ignore

        query = (
            exp.select("PRIVILEGE_TYPE", "GRANTEE")
            .from_(exp.table_("grants_to_users", db="sys"))
            .where(
                exp.and_(
                    exp.column("OBJECT_DATABASE").eq(exp.Literal.string(schema_name)),
                    exp.column("OBJECT_NAME").eq(exp.Literal.string(table_name)),
                    exp.column("OBJECT_TYPE").isin(
                        exp.Literal.string("TABLE"), exp.Literal.string("VIEW")
                    ),
                )
            )
        )

        try:
            results = self.fetchall(query)
        except Exception as e:
            logger.warning(f"Failed to query grants from sys.grants_to_users: {e}")
            return {}

        grants_dict: t.Dict[str, t.List[str]] = {}
        for privilege_raw, grantee_raw in results:
            if privilege_raw is None or grantee_raw is None:
                continue

            privileges_str = str(privilege_raw)
            grantee = str(grantee_raw)
            if not privileges_str or not grantee:
                continue

            # StarRocks returns grantee in format "'username'@'host'",
            # we extract just the username
            if "@" in grantee:
                grantee = grantee.split("@")[0].strip("'")

            # StarRocks may return multiple privileges as comma-separated string (e.g., "INSERT, SELECT")
            privileges = [p.strip() for p in privileges_str.split(",")]
            for privilege in privileges:
                if not privilege:
                    continue
                grantees = grants_dict.setdefault(privilege, [])
                if grantee not in grantees:
                    grantees.append(grantee)

        return grants_dict

    def _dcl_grants_config_expr(
        self,
        dcl_cmd: t.Type["DCL"],
        table: exp.Table,
        grants_config: "GrantsConfig",
        table_type: DataObjectType = DataObjectType.TABLE,
    ) -> t.List[exp.Expr]:
        # StarRocks doesn't support catalog in GRANT/REVOKE statements - strip it
        table_without_catalog = table.copy()
        table_without_catalog.set("catalog", None)

        # Filter out unsupported privileges for views
        # StarRocks only supports SELECT on views, not INSERT, UPDATE, DELETE, etc.
        if table_type == DataObjectType.VIEW:
            filtered_grants_config: GrantsConfig = {
                privilege: grantees
                for privilege, grantees in grants_config.items()
                if privilege.upper() in self.VIEW_SUPPORTED_PRIVILEGES
            }
            grants_config = filtered_grants_config

        return super()._dcl_grants_config_expr(
            dcl_cmd, table_without_catalog, grants_config, table_type
        )
