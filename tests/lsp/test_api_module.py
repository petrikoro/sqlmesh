from sqlmesh.core.context import Context


def test_api_module_dispatch_get_models() -> None:
    from sqlmesh.api import API_FEATURE, ApiRequest, dispatch_api_request, get_api_schemas

    assert API_FEATURE == "sqlmesh/api"
    assert "ApiRequest" in get_api_schemas()

    context = Context(paths=["examples/sushi"])
    response = dispatch_api_request(
        context,
        ApiRequest(
            requestId="request-1",
            url="/api/models",
            method="GET",
            params={},
            body={},
        ),
    )

    assert response.data


def test_api_module_dispatch_column_lineage_parses_models_only(monkeypatch) -> None:
    from sqlmesh.api import ApiRequest
    from sqlmesh.api import dispatcher as api_dispatcher

    captured: dict = {"models_only": None}

    def _fake_column_lineage(
        model_name: str, column: str, models_only: bool, context: Context
    ) -> dict:
        captured["models_only"] = models_only
        return {}

    monkeypatch.setattr(api_dispatcher, "column_lineage", _fake_column_lineage)

    context = Context(paths=["examples/sushi"])
    api_dispatcher.dispatch_api_request(
        context,
        ApiRequest(
            requestId="request-2",
            url="/api/lineage/sushi.items/item_id",
            method="GET",
            params={"models_only": "false"},
            body={},
        ),
    )

    assert captured["models_only"] is False


def test_api_module_schema_does_not_include_legacy_error_field() -> None:
    from sqlmesh.api import get_api_schemas

    models_schema = get_api_schemas()["ApiResponseGetModels"]
    properties = models_schema.get("properties", {})

    assert "response_error" in properties
    assert "error" not in properties


def test_api_module_dispatch_accepts_raw_request_dict() -> None:
    from sqlmesh.api import dispatch_api_request

    context = Context(paths=["examples/sushi"])
    response = dispatch_api_request(
        context,
        {
            "requestId": "request-raw",
            "url": "/api/models",
            "method": "GET",
            "params": {},
            "body": {},
        },
    )

    assert response.data
