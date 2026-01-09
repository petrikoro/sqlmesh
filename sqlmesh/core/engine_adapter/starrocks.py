from __future__ import annotations

import logging
import typing as t

from sqlglot import exp

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.mixins import (
    LogicalMergeMixin,
    NonTransactionalTruncateMixin,
    PandasNativeFetchDFSupportMixin,
    GetCurrentCatalogFromFunctionMixin,
    RowDiffMixin,
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
from sqlmesh.utils.errors import SQLMeshError

logger = logging.getLogger(__name__)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.node import IntervalUnit


@set_catalog()
class StarRocksEngineAdapter(
    LogicalMergeMixin,
    PandasNativeFetchDFSupportMixin,
    NonTransactionalTruncateMixin,
    GetCurrentCatalogFromFunctionMixin,
    RowDiffMixin,
):
    """StarRocks engine adapter."""

    DIALECT = "starrocks"
    DEFAULT_BATCH_SIZE = 10000
    SUPPORTS_TRANSACTIONS = False
    # StarRocks does support indexes, but we don't support them yet.
    SUPPORTS_INDEXES = False
    SUPPORTS_REPLACE_TABLE = False
    CURRENT_CATALOG_EXPRESSION = exp.func("catalog")
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
    MAX_TABLE_COMMENT_LENGTH = 1024
    MAX_COLUMN_COMMENT_LENGTH = 1024
    SUPPORTS_QUERY_EXECUTION_TRACKING = True
    # Though StarRocks supports asynchronous materialized views, we don't support them yet.
    SUPPORTS_MATERIALIZED_VIEWS = False
    # While StarRocks tables can have names up to 1024 characters,
    # database (schema) names are limited to 256 characters.
    # See https://docs.starrocks.io/docs/sql-reference/System_limit/
    MAX_IDENTIFIER_LENGTH = 256

    STARROCKS_SUPPORTED_TABLE_TYPES = frozenset(
        (
            "PRIMARY",
            "DUPLICATE",
        )
    )
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

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
        properties: t.Optional[t.List[exp.Expression]] = None,
    ) -> None:
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
        # StarRocks doesn't support CASCADE clause and drops schemas unconditionally.
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
        """Returns all the data objects that exist in the given schema."""
        catalog = self.get_current_catalog()
        query = (
            exp.select("table_name", "table_schema", "table_type")
            .from_(exp.table_("tables", db="information_schema"))
            .where(exp.column("table_schema").eq(to_schema(schema_name).db))
        )
        if object_names:
            query = query.where(exp.column("table_name").isin(*object_names))
        return [
            DataObject(
                catalog=catalog,
                schema=row.table_schema,
                name=row.table_name,
                type=DataObjectType.from_str(
                    self._TABLE_TYPE_MAP.get(
                        str(row.table_type),
                        str(row.table_type),
                    )
                ),
            )
            for row in self.fetchdf(query).itertuples()
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
        """Creates a SQLGlot table properties expression for StarRocks DDL."""

        properties: t.List[exp.Expression] = []
        props = {k.lower(): v for k, v in (table_properties or {}).items()}

        # StarRocks engine type
        # See: https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/#engine
        if storage_format:
            properties.append(self._build_engine_property(storage_format))

        # StarRocks table type
        # See: https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/#key
        key_columns_expr = props.pop("key_columns", None)
        if table_format or key_columns_expr:
            if table_format and table_format.upper() not in self.STARROCKS_SUPPORTED_TABLE_TYPES:
                raise SQLMeshError(
                    "Invalid or unsupported table type: %s, supported types: %s",
                    table_format,
                    ", ".join(self.STARROCKS_SUPPORTED_TABLE_TYPES),
                )
            properties.append(self._build_table_type_property(table_format, key_columns_expr))

        if table_description:
            properties.append(self._build_table_description_property(table_description))

        if partitioned_by:
            properties.append(self._build_partitioned_by_exp(partitioned_by))

        buckets_expr = props.pop("buckets", None)
        distributed_by_expr = props.pop("distributed_by", None)
        if distributed_by_expr or buckets_expr:
            properties.append(self._build_distribution_property(distributed_by_expr, buckets_expr))

        if rollup_expr := props.pop("rollup", None):
            properties.append(self._build_rollup_property(rollup_expr))

        if order_by_expr := props.pop("order_by", None):
            properties.append(self._build_order_by_property(order_by_expr))

        # Add remaining properties as PROPERTIES
        properties.extend(self._table_or_view_properties_to_expressions(props))

        return exp.Properties(expressions=properties) if properties else None

    def _build_engine_property(self, engine: str) -> exp.EngineProperty:
        """Build engine property."""
        return exp.EngineProperty(this=engine)

    def _build_table_type_property(
        self,
        table_type: t.Optional[str] = None,
        key_columns: t.Optional[exp.Expression] = None,
    ) -> t.Union[exp.PrimaryKey, exp.DuplicateKeyProperty]:
        """Build table type property.

        Args:
            table_type: The table type ('PRIMARY KEY' or 'DUPLICATE KEY').
            key_columns: The key columns expression from physical_properties.

        Returns:
            PrimaryKey or DuplicateKeyProperty expression.
        """
        cols: t.List[exp.Expression] = []
        if key_columns:
            if isinstance(key_columns, (exp.Tuple, exp.Array)):
                cols = [exp.to_column(c.name) for c in key_columns.expressions]
            else:
                cols = [exp.to_column(key_columns.name)]

        if table_type and table_type.upper() == "PRIMARY KEY":
            return exp.PrimaryKey(expressions=cols)
        return exp.DuplicateKeyProperty(expressions=cols)

    def _build_table_description_property(
        self, table_description: str
    ) -> exp.SchemaCommentProperty:
        """Build table comment property."""
        return exp.SchemaCommentProperty(
            this=exp.Literal.string(self._truncate_table_comment(table_description))
        )

    def _build_partitioned_by_exp(
        self,
        partitioned_by: t.List[exp.Expression],
        **kwargs: t.Any,
    ) -> exp.PartitionedByProperty:
        """Build partitioned by property."""
        return exp.PartitionedByProperty(this=exp.Schema(expressions=partitioned_by))

    def _build_distribution_property(
        self,
        distributed_by_expr: t.Optional[exp.Expression],
        buckets_expr: t.Optional[exp.Expression],
    ) -> exp.DistributedByProperty:
        """Build distribution property."""
        if not distributed_by_expr:
            return exp.DistributedByProperty(kind=exp.var("RANDOM"), buckets=buckets_expr)

        exprs = (
            distributed_by_expr.expressions
            if isinstance(distributed_by_expr, exp.Tuple)
            else [distributed_by_expr]
        )
        return exp.DistributedByProperty(
            kind=exp.var("HASH"),
            expressions=[exp.to_column(col.name) for col in exprs],
            buckets=buckets_expr,
        )

    def _build_rollup_property(self, rollup_expr: exp.Expression) -> exp.RollupProperty:
        """Build rollup property."""
        return exp.RollupProperty(
            expressions=[
                exp.Schema(this=expr.this, expressions=expr.expression.expressions)
                for expr in rollup_expr.expressions
            ]
        )

    def _build_order_by_property(self, order_by_expr: exp.Expression) -> exp.Order:
        """Build order by property."""
        exprs = (
            order_by_expr.expressions
            if isinstance(order_by_expr, (exp.Tuple, exp.Array))
            else [order_by_expr]
        )
        columns = [exp.to_column(col.name) for col in exprs]
        return exp.Order(expressions=[exp.Tuple(expressions=columns)])
