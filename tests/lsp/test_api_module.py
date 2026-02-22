from __future__ import annotations

from sqlmesh.api.handlers import column_lineage, get_models, model_lineage
from sqlmesh.api.protocol import (
    ApiResponseGetColumnLineage,
    ApiResponseGetLineage,
    ApiResponseGetModels,
    ApiResponseGetTableDiff,
)
from sqlmesh.core.context import Context


def test_api_handlers_get_models() -> None:
    context = Context(paths=["examples/sushi"])
    models = get_models(context)
    assert models


def test_api_response_schema_does_not_include_legacy_error_field() -> None:
    schema = ApiResponseGetModels.model_json_schema()
    properties = schema.get("properties", {})

    assert "response_error" in properties
    assert "error" not in properties


def test_api_handlers_model_lineage() -> None:
    context = Context(paths=["examples/sushi"])
    lineage = model_lineage("sushi.customers", context)
    assert lineage
    # The graph keys use fully-qualified names (e.g. "memory"."sushi"."customers")
    assert any("customers" in key for key in lineage)


def test_api_handlers_model_lineage_unknown_model_raises() -> None:
    import pytest

    context = Context(paths=["examples/sushi"])
    with pytest.raises(ValueError):
        model_lineage("nonexistent.model", context)


def test_api_handlers_column_lineage() -> None:
    context = Context(paths=["examples/sushi"])
    result = column_lineage("sushi.customers", "customer_id", models_only=False, context=context)
    assert result
    # The graph keys use fully-qualified names
    assert any("customers" in key for key in result)


def test_api_handlers_column_lineage_models_only() -> None:
    context = Context(paths=["examples/sushi"])
    result = column_lineage("sushi.customers", "customer_id", models_only=True, context=context)
    assert result
    assert any("customers" in key for key in result)


def test_api_handlers_column_lineage_unknown_model_raises() -> None:
    import pytest

    context = Context(paths=["examples/sushi"])
    with pytest.raises(ValueError):
        column_lineage("nonexistent.model", "col", models_only=False, context=context)


def test_api_response_get_models_serializes() -> None:
    context = Context(paths=["examples/sushi"])
    models = get_models(context)
    response = ApiResponseGetModels(data=models)
    dumped = response.model_dump(mode="json")
    assert "data" in dumped
    assert isinstance(dumped["data"], list)
    assert len(dumped["data"]) > 0
    assert dumped["response_error"] is None


def test_api_response_get_lineage_serializes() -> None:
    context = Context(paths=["examples/sushi"])
    lineage = model_lineage("sushi.customers", context)
    non_set_lineage = {k: v for k, v in lineage.items() if v is not None}
    response = ApiResponseGetLineage(data=non_set_lineage)
    dumped = response.model_dump(mode="json")
    assert "data" in dumped
    assert isinstance(dumped["data"], dict)


def test_api_response_get_column_lineage_serializes() -> None:
    context = Context(paths=["examples/sushi"])
    result = column_lineage("sushi.customers", "customer_id", models_only=False, context=context)
    response = ApiResponseGetColumnLineage(data=result)
    dumped = response.model_dump(mode="json")
    assert "data" in dumped
    assert isinstance(dumped["data"], dict)


def test_api_response_get_table_diff_none_data() -> None:
    """Verify that a table diff response with None data serializes correctly."""
    response = ApiResponseGetTableDiff(data=None)
    dumped = response.model_dump(mode="json")
    assert dumped["data"] is None
    assert dumped["response_error"] is None


def test_api_response_with_error() -> None:
    """Verify that response_error propagates through all response types."""
    cases: list[tuple[type, dict[str, object]]] = [
        (ApiResponseGetModels, {"data": []}),
        (ApiResponseGetLineage, {"data": {}}),
        (ApiResponseGetColumnLineage, {"data": {}}),
        (ApiResponseGetTableDiff, {"data": None}),
    ]
    for response_cls, kwargs in cases:
        resp = response_cls(response_error="something went wrong", **kwargs)
        dumped = resp.model_dump(mode="json")
        assert dumped["response_error"] == "something went wrong"
