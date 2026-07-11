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
from sqlmesh.core.schema_diff import TableAlterChangeColumnTypeOperation, TableAlterOperation
from sqlmesh.utils.errors import SQLMeshError

logger = logging.getLogger(__name__)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DCL, GrantsConfig
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

        full_columns: t.Optional[t.Dict[str, t.Tuple[t.Any, ...]]] = None
        alter_statements: t.List[exp.Alter] = []

        for alter_operation in t.cast(t.List[TableAlterOperation], alter_expressions):
            if not (
                isinstance(alter_operation, TableAlterChangeColumnTypeOperation)
                and len(alter_operation.column_parts) == 1
                and alter_operation.column_parts[0].is_primitive
            ):
                alter_statements.append(alter_operation.expression)
                continue

            if full_columns is None:
                full_columns = self._get_full_columns(alter_operation.target_table)

            alter_statements.append(
                self._build_modify_column_expression(
                    alter_operation,
                    full_columns[alter_operation.column_parts[0].name],
                )
            )

        for alter_statement in alter_statements:
            table = exp.to_table(alter_statement.this)
            latest_job = self._get_latest_schema_change_job(table)
            previous_job_id = int(latest_job[0]) if latest_job else None

            super().alter_table([alter_statement])
            self._wait_for_schema_change(table, previous_job_id)

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

    def _wait_for_schema_change(self, table: exp.Table, previous_job_id: t.Optional[int]) -> None:
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

            if time.monotonic() >= deadline:
                details = f" Last state: {state}, progress: {progress}." if state else ""
                raise SQLMeshError(
                    f"Timed out after {timeout:g} seconds waiting for a StarRocks schema "
                    f"change on {table}.{details}"
                )
            time.sleep(poll_interval)

    def _get_full_columns(self, table: exp.Table) -> t.Dict[str, t.Tuple[t.Any, ...]]:
        table = table.copy()
        table.set("catalog", None)
        return {
            str(row[0]): row
            for row in self.fetchall(
                f"SHOW FULL COLUMNS FROM {table.sql(dialect=self.dialect, identify=True)}"
            )
        }

    def _build_modify_column_expression(
        self,
        operation: TableAlterChangeColumnTypeOperation,
        column_metadata: t.Tuple[t.Any, ...],
    ) -> exp.Alter:
        _, _, _, nullable, key, default, extra, _, comment = column_metadata
        definition = [
            operation.column.sql(dialect=self.dialect, identify=True),
            operation.column_type.sql(dialect=self.dialect),
            "KEY" if str(key).upper() == "YES" else str(extra or ""),
            "NULL" if str(nullable).upper() == "YES" else "NOT NULL",
        ]
        for keyword, value in (("DEFAULT", default), ("COMMENT", comment)):
            if value is not None:
                definition.extend(
                    [keyword, exp.Literal.string(str(value)).sql(dialect=self.dialect)]
                )

        return exp.Alter(
            this=operation.target_table,
            kind="TABLE",
            actions=[exp.Command(this=f"MODIFY COLUMN {' '.join(filter(None, definition))}")],
        )

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
        properties: t.Optional[t.List[exp.Expression]] = None,
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
        **drop_args: t.Dict[str, exp.Expression],
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
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[exp.Expression]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        properties: t.List[exp.Expression] = []
        props = {k.lower(): v for k, v in (table_properties or {}).items()}

        if table_description:
            properties.append(self._build_table_description_property(table_description))

        if partitioned_by:
            properties.append(self._build_partitioned_by_exp(partitioned_by))

        if primary_key_expr := props.pop("primary_key", None):
            properties.append(self._build_primary_key_property(primary_key_expr))

        if distributed_by_expr := props.pop("distributed_by", None):
            properties.append(self._build_distributed_by_property(distributed_by_expr))

        if rollup_expr := props.pop("rollup", None):
            properties.append(self._build_rollup_property(rollup_expr))

        if order_by_expr := props.pop("order_by", None):
            properties.append(self._build_order_by_property(order_by_expr))

        properties.extend(self._table_or_view_properties_to_expressions(props))

        return exp.Properties(expressions=properties) if properties else None

    def _build_engine_property(self, engine: str) -> exp.EngineProperty:
        return exp.EngineProperty(this=engine)

    def _build_primary_key_property(
        self,
        primary_key_expr: exp.Expression,
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
        partitioned_by: t.List[exp.Expression],
        **kwargs: t.Any,
    ) -> exp.PartitionedByProperty:
        return exp.PartitionedByProperty(this=exp.Schema(expressions=partitioned_by))

    def _build_distributed_by_property(
        self,
        distributed_by_expr: exp.Expression,
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

    def _build_rollup_property(self, rollup_expr: exp.Expression) -> exp.RollupProperty:
        return exp.RollupProperty(
            expressions=[
                exp.Schema(this=expr.this, expressions=expr.expression.expressions)
                for expr in rollup_expr.expressions
            ]
        )

    def _build_order_by_property(self, order_by_expr: exp.Expression) -> exp.Order:
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
    ) -> t.List[exp.Expression]:
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
