from __future__ import annotations

import logging
import re
import time
import typing as t

from sqlglot import exp, parse_one
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
    TableAlterChangeColumnTypeOperation,
    TableAlterColumnOperation,
    TableAlterDropColumnOperation,
    TableAlterOperation,
)
from sqlmesh.utils import columns_to_types_all_known, columns_to_types_to_struct
from sqlmesh.utils.errors import MigrationNotSupportedError, SQLMeshError

logger = logging.getLogger(__name__)

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
    SCHEMA_DIFFER_KWARGS = {
        "compatible_types": {
            exp.DataType.build("TINYINT", dialect="starrocks"): {
                exp.DataType.build("SMALLINT", dialect="starrocks"),
                exp.DataType.build("INT", dialect="starrocks"),
                exp.DataType.build("BIGINT", dialect="starrocks"),
                exp.DataType.build("DOUBLE", dialect="starrocks"),
                exp.DataType.build("STRING", dialect="starrocks"),
                exp.DataType.build("VARCHAR(65533)", dialect="starrocks"),
            },
            exp.DataType.build("SMALLINT", dialect="starrocks"): {
                exp.DataType.build("INT", dialect="starrocks"),
                exp.DataType.build("BIGINT", dialect="starrocks"),
                exp.DataType.build("DOUBLE", dialect="starrocks"),
                exp.DataType.build("STRING", dialect="starrocks"),
                exp.DataType.build("VARCHAR(65533)", dialect="starrocks"),
            },
            exp.DataType.build("INT", dialect="starrocks"): {
                exp.DataType.build("BIGINT", dialect="starrocks"),
                exp.DataType.build("DOUBLE", dialect="starrocks"),
                exp.DataType.build("STRING", dialect="starrocks"),
                exp.DataType.build("VARCHAR(65533)", dialect="starrocks"),
            },
            exp.DataType.build("BIGINT", dialect="starrocks"): {
                exp.DataType.build("STRING", dialect="starrocks"),
                exp.DataType.build("VARCHAR(65533)", dialect="starrocks"),
            },
            exp.DataType.build("LARGEINT", dialect="starrocks"): {
                exp.DataType.build("STRING", dialect="starrocks"),
                exp.DataType.build("VARCHAR(65533)", dialect="starrocks"),
            },
            exp.DataType.build("FLOAT", dialect="starrocks"): {
                exp.DataType.build("DOUBLE", dialect="starrocks"),
                exp.DataType.build("STRING", dialect="starrocks"),
                exp.DataType.build("VARCHAR(65533)", dialect="starrocks"),
            },
            exp.DataType.build("DOUBLE", dialect="starrocks"): {
                exp.DataType.build("STRING", dialect="starrocks"),
                exp.DataType.build("VARCHAR(65533)", dialect="starrocks"),
            },
            exp.DataType.build("DATE", dialect="starrocks"): {
                exp.DataType.build("DATETIME", dialect="starrocks"),
            },
        },
        "types_with_unlimited_length": {
            # STRING is an alias for VARCHAR(65533).
            exp.DataType.build("STRING", dialect="starrocks").this: {
                exp.DataType.build("VARCHAR", dialect="starrocks").this,
            },
        },
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
        table_definition = self._get_table_definition(alter_operations[0].target_table)
        column_restrictions = self._column_change_restrictions(table_definition)
        column_definitions = {
            column.name.lower(): column
            for column in table_definition.this.expressions
            if isinstance(column, exp.ColumnDef)
        }
        prepared_changes: t.List[t.Tuple[TableAlterColumnOperation, exp.Alter]] = []

        for alter_operation in alter_operations:
            # Operations without top-level column roles use the schema differ's native expression.
            if not self._requires_column_role_check(alter_operation):
                prepared_changes.append((alter_operation, alter_operation.expression))
                continue

            column_name = alter_operation.column.name.lower()

            # StarRocks rejects changes to columns used by these table properties.
            if reason := self._unsupported_column_change_reason(
                alter_operation,
                column_restrictions,
            ):
                raise MigrationNotSupportedError(
                    f"{reason} "
                    "A new physical table version is required; use a non-forward-only plan "
                    "and disable the model's 'forward_only' setting if it is configured."
                )

            # Drops and non-primitive type changes use the schema differ's native expression.
            if (
                not isinstance(alter_operation, TableAlterChangeColumnTypeOperation)
                or not alter_operation.column_parts[0].is_primitive
            ):
                prepared_changes.append((alter_operation, alter_operation.expression))
                continue

            column_definition = column_definitions.get(column_name)
            if column_definition is None:
                raise MigrationNotSupportedError(
                    f"Unable to reconstruct column '{column_name}' from the live StarRocks "
                    "table definition."
                )

            prepared_changes.append(
                (
                    alter_operation,
                    self._build_modify_column_expression(
                        alter_operation,
                        column_definition,
                    ),
                )
            )

        for alter_operation, alter_statement in prepared_changes:
            table = alter_operation.target_table
            latest_job = self._get_latest_schema_change_job(table)
            previous_job_id = int(latest_job[0]) if latest_job else None

            super().alter_table([alter_statement])
            self._wait_for_schema_change(
                table,
                previous_job_id,
                alter_operation.expected_table_struct,
            )

    def _get_latest_schema_change_job(self, table: exp.Table) -> t.Optional[t.Tuple[t.Any, ...]]:
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
        return rows[0] if rows else None

    def _wait_for_schema_change(
        self,
        table: exp.Table,
        previous_job_id: t.Optional[int],
        expected_table_struct: exp.DataType,
    ) -> None:
        timeout = float(self._extra_config.get("schema_change_timeout", 3600))
        poll_interval = float(self._extra_config.get("schema_change_poll_interval", 1))
        deadline = time.monotonic() + timeout
        state = progress = None

        while True:
            job = self._get_latest_schema_change_job(table)
            if job and (previous_job_id is None or int(job[0]) > previous_job_id):
                job_id = int(job[0])
                state = str(job[9]).upper()
                progress = job[11]
                if state == "FINISHED":
                    return
                if state in {"CANCELLED", "CANCELED", "FAILED"}:
                    raise SQLMeshError(
                        f"StarRocks schema change job {job_id} for {table} failed: {job[10]}"
                    )
            # Fast schema evolution can update the live schema without creating an ALTER job.
            elif columns_to_types_to_struct(self.columns(table)) == expected_table_struct:
                return

            if time.monotonic() >= deadline:
                details = f" Last state: {state}, progress: {progress}." if state else ""
                raise SQLMeshError(
                    f"Timed out after {timeout:g} seconds waiting for a StarRocks schema "
                    f"change on {table}.{details}"
                )
            time.sleep(poll_interval)

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

        try:
            table_definition = parse_one(definition_sql, dialect=self.dialect)
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

        operations = self.schema_differ.compare_columns(
            "_schema_change_check",
            current_columns,
            target_columns,
            ignore_destructive=target.on_destructive_change.is_ignore,
            ignore_additive=target.on_additive_change.is_ignore,
        )

        if not operations:
            return True

        # Every executable migration requires a complete live definition.
        try:
            table_definition = self._get_table_definition(exp.to_table(current_table))
        except MigrationNotSupportedError:
            return False

        # Only top-level drops and type changes need validation against StarRocks column roles.
        column_operations = [
            operation for operation in operations if self._requires_column_role_check(operation)
        ]

        # Additive changes need no further column-role validation.
        if not column_operations:
            return True

        column_restrictions = self._column_change_restrictions(table_definition)

        # Reuse the execution-time decision so planning cannot accept a rejected operation.
        return not any(
            self._unsupported_column_change_reason(operation, column_restrictions)
            for operation in column_operations
        )

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
    def _column_change_restrictions(cls, expression: exp.Expr) -> t.Dict[str, t.Set[str]]:
        primary_key = expression.find(exp.PrimaryKey)
        unique_key = expression.find(exp.UniqueKeyProperty)
        duplicate_key = expression.find(exp.DuplicateKeyProperty)
        distributed_by = expression.find(exp.DistributedByProperty)
        order = expression.find(exp.Order)
        rollup = expression.find(exp.RollupProperty)

        partition = expression.find(
            exp.PartitionedByProperty,
            exp.PartitionByRangeProperty,
            exp.PartitionByListProperty,
        )
        if isinstance(partition, exp.PartitionedByProperty):
            partition_expressions = partition.this.expressions
        else:
            partition_expressions = (
                partition.args.get("partition_expressions", []) if partition else []
            )
        partition_columns = cls._column_names(partition_expressions)
        auto_increment_columns = {
            column.name.lower()
            for column in expression.find_all(exp.ColumnDef)
            if column.find(exp.AutoIncrementColumnConstraint)
        }
        vector_index_columns = cls._column_names(
            column
            for index in expression.find_all(exp.IndexColumnConstraint)
            if any(option.args.get("using") == "VECTOR" for option in index.args.get("options", ()))
            for column in index.expressions
        )

        return {
            "primary key": cls._column_names(primary_key.expressions if primary_key else ()),
            "unique key": cls._column_names(unique_key.expressions if unique_key else ()),
            "duplicate key": cls._column_names(duplicate_key.expressions if duplicate_key else ()),
            "partitioning": partition_columns,
            "distribution": cls._column_names(distributed_by.expressions if distributed_by else ()),
            "sort key": cls._column_names(order.expressions if order else ()),
            "rollup": cls._column_names(
                column
                for index in (rollup.expressions if rollup else ())
                for column in index.expressions
            ),
            "generated column expression": cls._column_names(
                constraint.this for constraint in expression.find_all(exp.ComputedColumnConstraint)
            ),
            "auto increment": auto_increment_columns,
            "vector index": vector_index_columns,
        }

    def _unsupported_column_change_reason(
        self,
        operation: TableAlterColumnOperation,
        column_restrictions: t.Dict[str, t.Set[str]],
    ) -> t.Optional[str]:
        column_name = operation.column.name

        # A column can have multiple roles, so report every restriction in a stable order.
        restrictions = sorted(
            restriction
            for restriction, columns in column_restrictions.items()
            if column_name.lower() in columns
        )

        if not restrictions:
            return None

        # Render the schema-diff operation so the error matches the attempted StarRocks SQL.
        operation_sql = operation.expression.sql(dialect=self.dialect, identify=True)

        return (
            f"StarRocks cannot apply {operation_sql} because column '{column_name}' is used by "
            "the following table constraints or properties: "
            f"{', '.join(restrictions)}."
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
    ) -> exp.Alter:
        column_definition = column_definition.copy()
        column_definition.set("this", operation.column.copy())
        column_definition.set("kind", operation.column_type.copy())

        return exp.Alter(
            this=operation.target_table,
            kind="TABLE",
            actions=[
                exp.ModifyColumn(
                    this=column_definition,
                )
            ],
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
