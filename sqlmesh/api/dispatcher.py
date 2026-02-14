from __future__ import annotations

import typing as t
import urllib.parse

from sqlmesh.api.handlers import column_lineage, get_models, get_table_diff, model_lineage
from sqlmesh.api.protocol import (
    ApiRequest,
    ApiResponse,
    ApiResponseGetColumnLineage,
    ApiResponseGetLineage,
    ApiResponseGetModels,
    ApiResponseGetTableDiff,
)
from sqlmesh.core.context import Context

_TRUE_VALUES = frozenset({"1", "true", "t", "yes", "y", "on"})
_FALSE_VALUES = frozenset({"0", "false", "f", "no", "n", "off", ""})


def _as_bool(value: t.Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized_value = value.strip().lower()
        if normalized_value in _TRUE_VALUES:
            return True
        if normalized_value in _FALSE_VALUES:
            return False
        return default
    return bool(value)


def _as_int(value: t.Any, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def dispatch_api_request(context: Context, request: t.Any) -> ApiResponse:
    """Dispatch a SQLMesh API request to a typed response."""
    request = ApiRequest.model_validate(request)
    parsed_url = urllib.parse.urlparse(request.url)
    path_parts = tuple(parsed_url.path.strip("/").split("/"))
    params = request.params or {}

    if request.method != "GET":
        raise NotImplementedError(f"API request not implemented: {request.url}")

    if path_parts == ("api", "models"):
        return ApiResponseGetModels(data=get_models(context))

    if path_parts[:2] == ("api", "lineage"):
        if len(path_parts) == 3:
            model_name = urllib.parse.unquote(path_parts[2])
            lineage = model_lineage(model_name, context)
            non_set_lineage = {k: v for k, v in lineage.items() if v is not None}
            return ApiResponseGetLineage(data=non_set_lineage)

        if len(path_parts) == 4:
            model_name = urllib.parse.unquote(path_parts[2])
            column = urllib.parse.unquote(path_parts[3])
            models_only = _as_bool(params.get("models_only"), default=False)
            return ApiResponseGetColumnLineage(
                data=column_lineage(model_name, column, models_only, context)
            )

    if path_parts[:2] == ("api", "table_diff"):
        if not params:
            return ApiResponseGetTableDiff(data=None)

        table_diff_result = get_table_diff(
            context=context,
            source=params.get("source", ""),
            target=params.get("target", ""),
            on=params.get("on"),
            model_or_snapshot=params.get("model_or_snapshot"),
            where=params.get("where"),
            temp_schema=params.get("temp_schema"),
            limit=_as_int(params.get("limit", 20), default=20),
        )
        return ApiResponseGetTableDiff(data=table_diff_result)

    raise NotImplementedError(f"API request not implemented: {request.url}")
