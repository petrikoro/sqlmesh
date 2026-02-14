"""Transport-agnostic SQLMesh API request / response models."""

from __future__ import annotations

import typing as t

from pydantic import field_validator

from sqlmesh.api.models import LineageColumn, Model, TableDiff
from sqlmesh.utils.pydantic import PydanticModel

API_FEATURE = "sqlmesh/api"


class ApiRequest(PydanticModel):
    """
    Request to call the SQLMesh API.
    This is a generic request that can be used to call any API endpoint.
    """

    requestId: str
    url: str
    method: t.Optional[str] = "GET"
    params: t.Optional[t.Dict[str, t.Any]] = None
    body: t.Optional[t.Dict[str, t.Any]] = None


class BaseAPIResponse(PydanticModel):
    # Shared error field used by LSP custom method responses.
    response_error: t.Optional[str] = None


class ApiResponseGetModels(BaseAPIResponse):
    """
    Response from the SQLMesh API for the get_models endpoint.
    """

    data: t.List[Model]

    @field_validator("data", mode="before")
    def sanitize_datetime_fields(cls, data: t.List[Model]) -> t.List[Model]:
        """
        Convert datetime objects to None to avoid serialization issues.
        """
        if isinstance(data, list):
            for model in data:
                if hasattr(model, "details") and model.details:
                    # Convert datetime fields to None to avoid serialization issues
                    for field in ["stamp", "start", "cron_prev", "cron_next"]:
                        if (
                            hasattr(model.details, field)
                            and getattr(model.details, field) is not None
                        ):
                            setattr(model.details, field, None)
        return data


class ApiResponseGetLineage(BaseAPIResponse):
    """
    Response from the SQLMesh API for the get_lineage endpoint.
    """

    data: t.Dict[str, t.List[str]]


class ApiResponseGetColumnLineage(BaseAPIResponse):
    """
    Response from the SQLMesh API for the get_column_lineage endpoint.
    """

    data: t.Dict[str, t.Dict[str, LineageColumn]]


class ApiResponseGetTableDiff(BaseAPIResponse):
    """
    Response from the SQLMesh API for the get_table_diff endpoint.
    """

    data: t.Optional[TableDiff]


ApiResponse = t.Union[
    ApiResponseGetModels,
    ApiResponseGetColumnLineage,
    ApiResponseGetLineage,
    ApiResponseGetTableDiff,
]

_DOCUMENTED_MODELS: t.Tuple[t.Type[PydanticModel], ...] = (
    ApiRequest,
    ApiResponseGetModels,
    ApiResponseGetLineage,
    ApiResponseGetColumnLineage,
    ApiResponseGetTableDiff,
)


def get_api_schemas() -> t.Dict[str, t.Dict[str, t.Any]]:
    """Return JSON schemas for SQLMesh API models for doc generation."""
    return {model.__name__: model.model_json_schema() for model in _DOCUMENTED_MODELS}
