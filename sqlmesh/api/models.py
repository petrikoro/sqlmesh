from __future__ import annotations

import enum
import typing as t

from sqlglot import exp

from sqlmesh.core.node import IntervalUnit
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.pydantic import PydanticModel, ValidationInfo, field_validator


class ModelType(str, enum.Enum):
    PYTHON = "python"
    SQL = "sql"
    SEED = "seed"
    EXTERNAL = "external"
    SOURCE = "source"


class Reference(PydanticModel):
    name: str
    expression: str
    unique: bool


class ModelDetails(PydanticModel):
    owner: t.Optional[str] = None
    kind: t.Optional[str] = None
    batch_size: t.Optional[int] = None
    cron: t.Optional[str] = None
    stamp: t.Optional[TimeLike] = None
    start: t.Optional[TimeLike] = None
    retention: t.Optional[int] = None
    table_format: t.Optional[str] = None
    storage_format: t.Optional[str] = None
    time_column: t.Optional[str] = None
    tags: t.Optional[str] = None
    references: t.List[Reference] = []
    partitioned_by: t.Optional[str] = None
    clustered_by: t.Optional[str] = None
    lookback: t.Optional[int] = None
    cron_prev: t.Optional[TimeLike] = None
    cron_next: t.Optional[TimeLike] = None
    interval_unit: t.Optional[IntervalUnit] = None
    annotated: t.Optional[bool] = None


class Column(PydanticModel):
    name: str
    type: str
    description: t.Optional[str] = None


class Model(PydanticModel):
    name: str
    fqn: str
    path: t.Optional[str] = None
    full_path: t.Optional[str] = None
    """
    As opposed to path, which is relative to the project root, full_path is the absolute path to the model file.
    """
    dialect: str
    type: ModelType
    columns: t.List[Column]
    description: t.Optional[str] = None
    details: t.Optional[ModelDetails] = None
    sql: t.Optional[str] = None
    definition: t.Optional[str] = None
    default_catalog: t.Optional[str] = None
    hash: str


class LineageColumn(PydanticModel):
    source: t.Optional[str] = None
    expression: t.Optional[str] = None
    models: t.Dict[str, t.Set[str]]


class SchemaDiff(PydanticModel):
    source: str
    target: str
    source_schema: t.Dict[str, str]
    target_schema: t.Dict[str, str]
    added: t.Dict[str, str]
    removed: t.Dict[str, str]
    modified: t.Dict[str, str]

    @field_validator(
        "source_schema", "target_schema", "added", "removed", "modified", mode="before"
    )
    @classmethod
    def validate_schema(
        cls,
        v: t.Union[
            t.Dict[str, exp.DataType],
            t.List[t.Tuple[str, exp.DataType]],
            t.Dict[str, t.Tuple[exp.DataType, exp.DataType]],
            t.Dict[str, str],
        ],
        info: ValidationInfo,
    ) -> t.Dict[str, str]:
        if isinstance(v, dict):
            # The modified field can have tuples of (source_type, target_type).
            if info.field_name == "modified" and any(isinstance(val, tuple) for val in v.values()):
                return {
                    k: f"{str(val[0])} -> {str(val[1])}"
                    for k, val in v.items()
                    if isinstance(val, tuple)
                    and isinstance(val[0], exp.DataType)
                    and isinstance(val[1], exp.DataType)
                }
            return {k: str(val) for k, val in v.items()}
        if isinstance(v, list):
            return {k: str(val) for k, val in v}
        return v


class ProcessedSampleData(PydanticModel):
    column_differences: t.List[t.Dict[str, t.Any]]
    source_only: t.List[t.Dict[str, t.Any]]
    target_only: t.List[t.Dict[str, t.Any]]


class RowDiff(PydanticModel):
    source: str
    target: str
    stats: t.Dict[str, float]
    sample: t.Dict[str, t.Any]
    joined_sample: t.Dict[str, t.Any]
    s_sample: t.Dict[str, t.Any]
    t_sample: t.Dict[str, t.Any]
    column_stats: t.Dict[str, t.Any]
    source_count: int
    target_count: int
    count_pct_change: float
    decimals: int
    processed_sample_data: t.Optional[ProcessedSampleData] = None


class TableDiff(PydanticModel):
    schema_diff: SchemaDiff
    row_diff: RowDiff
    on: t.List[t.List[str]]
