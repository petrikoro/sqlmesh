from __future__ import annotations

import typing as t

import pytest

from sqlmesh.api.dispatcher import dispatch_api_request

_FAKE_CONTEXT: t.Any = object()


def _request(
    url: str,
    *,
    method: str = "GET",
    params: t.Optional[dict] = None,
) -> dict:
    return {
        "requestId": "request-1",
        "url": url,
        "method": method,
        "params": params,
        "body": {},
    }


def test_dispatch_api_request_rejects_non_get_method() -> None:
    with pytest.raises(NotImplementedError, match="API request not implemented: /api/models"):
        dispatch_api_request(_FAKE_CONTEXT, _request("/api/models", method="POST"))


def test_dispatch_api_request_rejects_unknown_route() -> None:
    with pytest.raises(NotImplementedError, match="API request not implemented: /api/unknown"):
        dispatch_api_request(_FAKE_CONTEXT, _request("/api/unknown"))


def test_dispatch_api_request_table_diff_empty_params_returns_none(monkeypatch) -> None:
    from sqlmesh.api import dispatcher as api_dispatcher

    def _fail_get_table_diff(**kwargs):
        raise AssertionError("get_table_diff should not be called for empty params")

    monkeypatch.setattr(api_dispatcher, "get_table_diff", _fail_get_table_diff)

    response = dispatch_api_request(_FAKE_CONTEXT, _request("/api/table_diff", params={}))
    assert response.data is None


def test_dispatch_api_request_table_diff_invalid_limit_uses_default(monkeypatch) -> None:
    from sqlmesh.api import dispatcher as api_dispatcher

    captured_kwargs = {}

    def _fake_get_table_diff(**kwargs):
        captured_kwargs.update(kwargs)
        return None

    monkeypatch.setattr(api_dispatcher, "get_table_diff", _fake_get_table_diff)

    response = dispatch_api_request(
        _FAKE_CONTEXT,
        _request(
            "/api/table_diff",
            params={
                "source": "dev.table",
                "target": "prod.table",
                "limit": "not-a-number",
            },
        ),
    )

    assert response.data is None
    assert captured_kwargs["source"] == "dev.table"
    assert captured_kwargs["target"] == "prod.table"
    assert captured_kwargs["limit"] == 20


def test_dispatch_api_request_column_lineage_decodes_url_segments(monkeypatch) -> None:
    from sqlmesh.api import dispatcher as api_dispatcher

    captured: t.Dict[str, t.Any] = {}

    def _fake_column_lineage(
        model_name: str, column_name: str, models_only: bool, context: object
    ) -> dict:
        captured["model_name"] = model_name
        captured["column_name"] = column_name
        captured["models_only"] = models_only
        return {}

    monkeypatch.setattr(api_dispatcher, "column_lineage", _fake_column_lineage)

    response = dispatch_api_request(
        _FAKE_CONTEXT,
        _request(
            "/api/lineage/raw.orders/order%20id",
            params={"models_only": "yes"},
        ),
    )

    assert response.data == {}
    assert captured == {
        "model_name": "raw.orders",
        "column_name": "order id",
        "models_only": True,
    }


def test_dispatch_api_request_column_lineage_invalid_models_only_defaults_false(
    monkeypatch,
) -> None:
    from sqlmesh.api import dispatcher as api_dispatcher

    captured = {}

    def _fake_column_lineage(
        model_name: str, column_name: str, models_only: bool, context: object
    ) -> dict:
        captured["models_only"] = models_only
        return {}

    monkeypatch.setattr(api_dispatcher, "column_lineage", _fake_column_lineage)

    response = dispatch_api_request(
        _FAKE_CONTEXT,
        _request(
            "/api/lineage/raw.orders/id",
            params={"models_only": "not-a-bool"},
        ),
    )

    assert response.data == {}
    assert captured["models_only"] is False
