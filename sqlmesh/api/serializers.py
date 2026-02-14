from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.api.models import (
    Column,
    LineageColumn,
    Model,
    ModelDetails,
    ModelType,
    ProcessedSampleData,
    Reference,
    RowDiff,
    SchemaDiff,
    TableDiff,
)
from sqlmesh.core.context import Context
from sqlmesh.core.lineage import column_description
from sqlmesh.core.model import Model as SQLMeshModel
from sqlmesh.utils.date import now, to_datetime


class TableDiffLike(t.Protocol):
    def schema_diff(self) -> t.Any: ...

    def row_diff(self, temp_schema: t.Optional[str] = None) -> t.Any: ...

    @property
    def key_columns(self) -> t.Tuple[t.List[exp.Column], t.List[exp.Column], t.List[str]]: ...


def serialize_all_models(
    context: Context, render_queries: t.Optional[t.Set[str]] = None
) -> t.List[Model]:
    render_queries = render_queries or set()
    return sorted(
        [
            serialize_model(context, model, model.name in render_queries)
            for model in context.models.values()
        ],
        key=lambda model: model.name,
    )


def serialize_model(context: Context, model: SQLMeshModel, render_query: bool = False) -> Model:
    model_type = _get_model_type(model)
    default_catalog = model.default_catalog
    dialect = model.dialect or "SQLGlot"
    time_column = (
        f"{model.time_column.column} | {model.time_column.format}" if model.time_column else None
    )
    tags = ", ".join(model.tags) if model.tags else None
    partitioned_by = (
        ", ".join(expr.sql(pretty=True, dialect=model.dialect) for expr in model.partitioned_by)
        if model.partitioned_by
        else None
    )
    clustered_by = (
        ", ".join([c.sql(dialect=model.dialect) for c in model.clustered_by])
        if model.clustered_by
        else None
    )
    lookback = model.lookback if model.lookback > 0 else None
    columns_to_types = model.columns_to_types or {}

    columns = []
    for name, data_type in columns_to_types.items():
        description = model.column_descriptions.get(name)
        if not description and render_query:
            # The column name is already normalized in `columns_to_types`, so we need to quote it.
            description = column_description(context, model.name, name, quote_column=True)

        columns.append(Column(name=name, type=str(data_type), description=description))

    details = ModelDetails(
        owner=model.owner,
        kind=model.kind.name,
        batch_size=model.batch_size,
        cron=model.cron,
        stamp=model.stamp,
        start=model.start,
        retention=model.retention,
        table_format=model.table_format,
        storage_format=model.storage_format,
        time_column=time_column,
        tags=tags,
        references=[
            Reference(name=ref.name, expression=ref.expression.sql(), unique=ref.unique)
            for ref in model.all_references
        ],
        partitioned_by=partitioned_by,
        clustered_by=clustered_by,
        lookback=lookback,
        cron_prev=to_datetime(model.cron_prev(value=now())),
        cron_next=to_datetime(model.cron_next(value=now())),
        interval_unit=model.interval_unit,
        annotated=model.annotated,
    )

    sql = None
    if render_query:
        query = model.render_query() or (
            model.query if hasattr(model, "query") else exp.select('"FAILED TO RENDER QUERY"')
        )
        sql = query.sql(pretty=True, dialect=model.dialect)  # ty:ignore[unresolved-attribute]

    path = model._path
    return Model(
        name=model.name,
        fqn=model.fqn,
        path=str(path.absolute().relative_to(context.path).as_posix()) if path else None,
        full_path=str(path.absolute().as_posix()) if path else None,
        dialect=dialect,
        columns=columns,
        details=details,
        description=model.description,
        sql=sql,
        type=model_type,
        default_catalog=default_catalog,
        hash=model.data_hash,
    )


def serialize_lineage_column(
    models: t.Mapping[str, t.Iterable[str]],
    expression: t.Optional[str] = None,
    source: t.Optional[str] = None,
) -> LineageColumn:
    normalized_models = {
        model_name: set(column_names) for model_name, column_names in models.items()
    }
    return LineageColumn(source=source, expression=expression, models=normalized_models)


def serialize_external_lineage_column(model_name: str, column_name: str) -> LineageColumn:
    return serialize_lineage_column(
        models={},
        expression=f"FROM {model_name}",
        source=f"SELECT {column_name} FROM {model_name}",
    )


def serialize_table_diff(
    diff: TableDiffLike,
    source_name: str,
    target_name: str,
    temp_schema: t.Optional[str] = None,
) -> TableDiff:
    _schema_diff = diff.schema_diff()
    _row_diff = diff.row_diff(temp_schema=temp_schema)
    schema_diff = SchemaDiff(
        source=_schema_diff.source,
        target=_schema_diff.target,
        source_schema=_schema_diff.source_schema,
        target_schema=_schema_diff.target_schema,
        added=_schema_diff.added,
        removed=_schema_diff.removed,
        modified=_schema_diff.modified,
    )

    processed_sample_data = _process_sample_data(_row_diff, source_name, target_name)

    row_diff = RowDiff(
        source=_row_diff.source,
        target=_row_diff.target,
        stats=_row_diff.stats,
        sample=_to_dict(_row_diff.sample),
        joined_sample=_to_dict(_row_diff.joined_sample),
        s_sample=_to_dict(_row_diff.s_sample),
        t_sample=_to_dict(_row_diff.t_sample),
        column_stats=_to_dict(_row_diff.column_stats),
        source_count=_row_diff.source_count,
        target_count=_row_diff.target_count,
        count_pct_change=_row_diff.count_pct_change,
        decimals=getattr(_row_diff, "decimals", 3),
        processed_sample_data=processed_sample_data,
    )

    s_index, t_index, _ = diff.key_columns
    return TableDiff(
        schema_diff=schema_diff,
        row_diff=row_diff,
        on=[(s.name, t.name) for s, t in zip(s_index, t_index)],  # ty:ignore[invalid-argument-type]
    )


def _get_model_type(model: SQLMeshModel) -> ModelType:
    if model.is_sql:
        return ModelType.SQL
    if model.is_python:
        return ModelType.PYTHON
    return ModelType.SOURCE


def _cells_match(x: t.Any, y: t.Any) -> bool:
    # Lazily import pandas and numpy as we do in core.
    import numpy as np
    import pandas as pd

    def _normalize(val: t.Any) -> t.Any:
        if pd.isnull(val):
            val = None
        return list(val) if isinstance(val, (pd.Series, np.ndarray)) else val

    return _normalize(x) == _normalize(y)


def _process_sample_data(
    row_diff: t.Any, source_name: str, target_name: str
) -> ProcessedSampleData:
    if row_diff.joined_sample.shape[0] == 0:
        return ProcessedSampleData(
            column_differences=[],
            source_only=_to_records(row_diff.s_sample) if row_diff.s_sample.shape[0] > 0 else [],
            target_only=_to_records(row_diff.t_sample) if row_diff.t_sample.shape[0] > 0 else [],
        )

    keys: t.List[str] = []
    columns: t.Dict[str, t.List[str]] = {}

    source_prefix, source_display = (
        (f"{source_name}__", source_name.upper())
        if source_name.lower() != row_diff.source.lower()
        else ("s__", "SOURCE")
    )
    target_prefix, target_display = (
        (f"{target_name}__", target_name.upper())
        if target_name.lower() != row_diff.target.lower()
        else ("t__", "TARGET")
    )

    for column in row_diff.joined_sample.columns:
        if column.lower().startswith(source_prefix.lower()):
            column_name = column[len(source_prefix) :]
            target_column = None
            for joined_column in row_diff.joined_sample.columns:
                if joined_column.lower() == (target_prefix + column_name).lower():
                    target_column = joined_column
                    break

            if target_column:
                columns[column_name] = [column, target_column]
        elif not column.lower().startswith(target_prefix.lower()):
            keys.append(column)

    column_differences = []
    for column_name, (source_column, target_column) in columns.items():
        column_table = row_diff.joined_sample[keys + [source_column, target_column]]
        column_table = column_table[
            column_table.apply(
                lambda row: not _cells_match(row[source_column], row[target_column]),
                axis=1,
            )
        ]
        column_table = column_table.rename(
            columns={
                source_column: source_display,
                target_column: target_display,
            }
        )

        if len(column_table) > 0:
            for row in _to_records(column_table):
                row["__column_name__"] = column_name
                row["__source_name__"] = source_display
                row["__target_name__"] = target_display
                column_differences.append(row)

    return ProcessedSampleData(
        column_differences=column_differences,
        source_only=_to_records(row_diff.s_sample) if row_diff.s_sample.shape[0] > 0 else [],
        target_only=_to_records(row_diff.t_sample) if row_diff.t_sample.shape[0] > 0 else [],
    )


def _replace_nulls(df: t.Any) -> t.Any:
    import numpy as np
    import pandas as pd

    return df.replace({np.nan: None, pd.NA: None})


def _to_dict(df: t.Any) -> t.Dict[str, t.Any]:
    return _replace_nulls(df).to_dict()


def _to_records(df: t.Any) -> t.List[t.Dict[str, t.Any]]:
    return _replace_nulls(df).to_dict("records")
