from __future__ import annotations

import logging
import time
import typing as t

from sqlglot import exp
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
    TableAlterAddColumnOperation,
    TableAlterColumnOperation,
    TableAlterOperation,
)
from sqlmesh.utils import columns_to_types_all_known
from sqlmesh.utils.errors import MigrationNotSupportedError, SQLMeshError

logger = logging.getLogger(__name__)

# StarRocks reports STRING as its physical alias VARCHAR(65533).
_STRING_ALIAS_LENGTH = 65533
_DECIMAL_DEFAULTS = {
    exp.DType.DECIMAL: (10, 0),
    exp.DType.DECIMAL32: (9, 9),
    exp.DType.DECIMAL64: (18, 18),
    exp.DType.DECIMAL128: (38, 38),
    exp.DType.DECIMAL256: (76, 76),
}


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


def _normalize_data_type(data_type: exp.DataType) -> exp.DataType:
    def canonicalize(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.DataType):
            return node
        if node.is_type(exp.DType.TEXT):
            return exp.DataType.build(f"VARCHAR({_STRING_ALIAS_LENGTH})")
        if node.is_type(exp.DType.CHAR, exp.DType.VARCHAR) and not node.expressions:
            type_name = "CHAR" if node.is_type(exp.DType.CHAR) else "VARCHAR"
            return exp.DataType.build(f"{type_name}(1)", dialect="starrocks")
        if node.this in _DECIMAL_DEFAULTS:
            precision, scale = _decimal_parameters(node)
            return exp.DataType.build(f"DECIMAL({precision}, {scale})", dialect="starrocks")
        return node

    return t.cast(exp.DataType, data_type.transform(canonicalize))


def _normalize_columns(
    columns: t.Dict[str, exp.DataType],
) -> t.Dict[str, exp.DataType]:
    return {name: _normalize_data_type(data_type) for name, data_type in columns.items()}


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

    def get_alter_operations(
        self,
        current_table_name: TableName,
        target_table_name: TableName,
        *,
        ignore_destructive: bool = False,
        ignore_additive: bool = False,
    ) -> t.List[TableAlterOperation]:
        """Compare physical StarRocks types so aliases don't produce DDL."""
        return t.cast(
            t.List[TableAlterOperation],
            self.schema_differ.compare_columns(
                current_table_name,
                _normalize_columns(self.columns(current_table_name)),
                _normalize_columns(self.columns(target_table_name)),
                ignore_destructive=ignore_destructive,
                ignore_additive=ignore_additive,
            ),
        )

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
            super().alter_table([alter_statement])
            self._wait_for_schema_change(table, alter_operation)

    def _prepare_alter_operations(
        self,
        alter_operations: t.List[TableAlterColumnOperation],
    ) -> t.List[t.Tuple[TableAlterColumnOperation, exp.Alter]]:
        """Allow only native, top-level, non-destructive ADD COLUMN operations."""

        unsupported = [
            operation
            for operation in alter_operations
            if not isinstance(operation, TableAlterAddColumnOperation)
            or operation.is_part_of_destructive_change
            or len(operation.column_parts) != 1
        ]
        if unsupported:
            columns = ", ".join(
                operation.column.sql(dialect=self.dialect, identify=True)
                for operation in unsupported
            )
            raise MigrationNotSupportedError(
                "StarRocks has no allowlisted native schema change for "
                f"column(s) {columns}. A new physical table version is required; use a "
                "non-forward-only plan and disable the model's 'forward_only' setting if "
                "it is configured."
            )

        return [(operation, operation.expression) for operation in alter_operations]

    def _wait_for_schema_change(
        self,
        table: exp.Table,
        operation: TableAlterColumnOperation,
    ) -> None:
        timeout = float(self._extra_config.get("schema_change_timeout", 3600))
        poll_interval = float(self._extra_config.get("schema_change_poll_interval", 1))
        deadline = time.monotonic() + timeout

        while not self._is_operation_applied(table, operation):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SQLMeshError(
                    f"Timed out after {timeout:g} seconds waiting for a StarRocks schema "
                    f"change on {table}."
                )
            time.sleep(min(poll_interval, remaining))

    def _is_operation_applied(
        self,
        table: exp.Table,
        operation: TableAlterColumnOperation,
    ) -> bool:
        if not isinstance(operation, TableAlterAddColumnOperation):
            return False

        columns = self.columns(table)
        column_name = operation.column.name
        actual_type = columns.get(column_name)
        if actual_type is None:
            return False
        return _normalize_data_type(actual_type) == _normalize_data_type(operation.column_type)

    def can_apply_schema_change_in_place(
        self,
        current: Model,
        target: Model,
        current_table: TableName,
        **render_kwargs: t.Any,
    ) -> bool:
        # A metadata-only change with an unchanged inferred schema cannot alter the physical
        # table. Don't let unresolved types turn tag, schedule, or owner updates into rebuilds.
        if (
            target.is_metadata_only_change(current)
            and target.columns_to_types == current.columns_to_types
        ):
            return True

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

        operations = self.schema_differ.compare_columns(
            current_table,
            _normalize_columns(current_columns),
            _normalize_columns(target_columns),
            ignore_destructive=target.on_destructive_change.is_ignore,
            ignore_additive=target.on_additive_change.is_ignore,
        )
        return all(
            isinstance(operation, TableAlterAddColumnOperation)
            and not operation.is_part_of_destructive_change
            and len(operation.column_parts) == 1
            for operation in operations
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
