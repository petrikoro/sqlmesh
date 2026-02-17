from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from sqlmesh.core.config.docs import DocsConfig, ExternalLinkConfig
from sqlmesh.core.context import Context
from sqlmesh.core.docs.generator import (
    CATALOG_FILENAME,
    INDEX_FILENAME,
    MANIFEST_FILENAME,
    STATIC_INDEX_FILENAME,
    DocsGenerator,
)


@pytest.fixture(scope="module")
def sushi_context() -> Context:
    return Context(paths=["examples/sushi"])


@pytest.fixture()
def output_dir(tmp_path: Path) -> Path:
    return tmp_path / "docs"


def _read_docs_json_artifacts(output_dir: Path) -> tuple[dict, dict]:
    manifest_data = json.loads((output_dir / MANIFEST_FILENAME).read_text(encoding="utf-8"))
    catalog_data = json.loads((output_dir / CATALOG_FILENAME).read_text(encoding="utf-8"))
    return manifest_data, catalog_data


def _build_docs_data(context: Context, select_models: list[str] | None = None) -> tuple[dict, dict]:
    return DocsGenerator(context).build_docs_data(select_models=select_models)


def test_generate_creates_index_html(sushi_context: Context, output_dir: Path) -> None:
    generator = DocsGenerator(sushi_context)
    result = generator.generate(output_path=str(output_dir))

    assert result == output_dir / INDEX_FILENAME
    assert result.exists()
    html = result.read_text(encoding="utf-8")
    assert "<!DOCTYPE html>" in html
    assert 'loadJson("manifest.json")' in html
    assert 'loadJson("catalog.json")' in html
    assert 'id="sqlmesh-manifest-data"' not in html
    assert 'id="sqlmesh-catalog-data"' not in html

    assert (output_dir / MANIFEST_FILENAME).exists()
    assert (output_dir / CATALOG_FILENAME).exists()


def test_generate_static_mode_creates_embedded_single_file(
    sushi_context: Context, output_dir: Path
) -> None:
    generator = DocsGenerator(sushi_context)
    result = generator.generate(output_path=str(output_dir), static=True)
    html = result.read_text(encoding="utf-8")

    assert result == output_dir / STATIC_INDEX_FILENAME
    assert "fonts.googleapis.com" not in html
    assert "fonts.gstatic.com" not in html
    assert 'id="sqlmesh-manifest-data"' in html
    assert 'id="sqlmesh-catalog-data"' in html
    assert 'loadJson("manifest.json")' not in html
    assert 'loadJson("catalog.json")' not in html

    # Static mode should still keep dynamic artifacts alongside static HTML.
    assert (output_dir / INDEX_FILENAME).exists()
    assert (output_dir / MANIFEST_FILENAME).exists()
    assert (output_dir / CATALOG_FILENAME).exists()


def test_generate_default_output_dir(
    sushi_context: Context, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    generator = DocsGenerator(sushi_context)
    result = generator.generate()

    assert result.exists()
    assert result.name == INDEX_FILENAME
    assert result.parent.name == "dbt_artifacts"
    assert result.parent == sushi_context.cache_dir / "dbt_artifacts"
    assert (result.parent / MANIFEST_FILENAME).exists()
    assert (result.parent / CATALOG_FILENAME).exists()


def test_default_mode_writes_valid_manifest_and_catalog_json(
    sushi_context: Context, output_dir: Path
) -> None:
    generator = DocsGenerator(sushi_context)
    generator.generate(output_path=str(output_dir))

    manifest_data, catalog_data = _read_docs_json_artifacts(output_dir)

    assert "models" in manifest_data
    assert "dag" in manifest_data
    assert "project" in manifest_data
    assert "column_lineage" in manifest_data
    assert "models" in catalog_data
    assert isinstance(catalog_data["models"], dict)


def test_docs_data_has_models(sushi_context: Context) -> None:
    manifest_data, catalog_data = _build_docs_data(sushi_context)

    assert len(manifest_data["models"]) > 0
    model = manifest_data["models"][0]
    assert "name" in model
    assert "fqn" in model
    assert "type" in model
    # Columns live in catalog_data, keyed by FQN
    assert model["fqn"] in catalog_data["models"]
    assert "columns" in catalog_data["models"][model["fqn"]]


def test_docs_data_includes_model_meta_and_column_metadata(sushi_context: Context) -> None:
    manifest_data, catalog_data = _build_docs_data(sushi_context)
    model = next(
        (m for m in manifest_data["models"] if m["name"] == "sushi.yaml_documented_orders"), None
    )

    assert model
    assert model["details"]["meta"]["owner_team"] == "finance"
    assert model["details"]["meta"]["contains_pii"] is True

    catalog_columns = {
        column["name"]: column for column in catalog_data["models"][model["fqn"]]["columns"]
    }
    assert catalog_columns["order_id"]["tags"] == ["primary_key", "pii"]
    assert catalog_columns["order_id"]["meta"] == {"classification": "sensitive"}
    assert catalog_columns["event_date"]["tags"] == ["event_time"]
    assert catalog_columns["event_date"]["meta"] == {"grain": "day"}


def test_docs_data_has_dag(sushi_context: Context) -> None:
    manifest_data, _ = _build_docs_data(sushi_context)

    assert len(manifest_data["dag"]) > 0
    for deps in manifest_data["dag"].values():
        assert isinstance(deps, list)


def test_docs_data_has_column_lineage(sushi_context: Context) -> None:
    manifest_data, _ = _build_docs_data(sushi_context)

    col_lineage = manifest_data["column_lineage"]
    assert len(col_lineage) > 0
    for columns in col_lineage.values():
        for sources in columns.values():
            for source in sources:
                assert "model" in source
                assert "column" in source


def test_docs_data_excludes_self_referential_column_lineage(sushi_context: Context) -> None:
    manifest_data, _ = _build_docs_data(sushi_context)

    for fqn, columns in manifest_data["column_lineage"].items():
        for sources in columns.values():
            for source in sources:
                assert source["model"] != fqn


def test_docs_data_project_summary(sushi_context: Context) -> None:
    manifest_data, _ = _build_docs_data(sushi_context)

    project = manifest_data["project"]
    assert project["model_count"] > 0
    assert "type_counts" in project
    assert "kind_counts" in project
    assert "owners" in project
    assert "tags" in project
    assert "column_count" in project
    assert project["column_count"] >= 0
    assert "models_with_description" in project
    assert project["models_with_description"] >= 0
    assert "columns_with_description" in project
    assert project["columns_with_description"] >= 0
    assert "projects" in project
    assert isinstance(project["projects"], list)
    assert "project_counts" in project
    assert isinstance(project["project_counts"], dict)


def test_html_contains_column_lineage(sushi_context: Context, output_dir: Path) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "lineage-canvas" in html


def test_select_models_filters(sushi_context: Context) -> None:
    manifest_full, _ = _build_docs_data(sushi_context)
    total = len(manifest_full["models"])

    target_fqn = manifest_full["models"][0]["fqn"]
    manifest_filtered, _ = _build_docs_data(sushi_context, select_models=[target_fqn])

    assert len(manifest_filtered["models"]) == 1
    assert len(manifest_filtered["models"]) < total


def test_generate_select_models_filters_output(sushi_context: Context, output_dir: Path) -> None:
    manifest_full, _ = _build_docs_data(sushi_context)
    target_fqn = manifest_full["models"][0]["fqn"]

    DocsGenerator(sushi_context).generate(output_path=str(output_dir), select_models=[target_fqn])
    manifest_data, catalog_data = _read_docs_json_artifacts(output_dir)

    assert len(manifest_data["models"]) == 1
    assert manifest_data["models"][0]["fqn"] == target_fqn
    assert set(catalog_data["models"]) == {target_fqn}


def test_context_generate_docs(sushi_context: Context, output_dir: Path) -> None:
    result = sushi_context.generate_docs(output_path=str(output_dir))

    assert result.exists()
    assert result.name == INDEX_FILENAME
    html = result.read_text(encoding="utf-8")
    assert "manifest.json" in html
    assert (output_dir / MANIFEST_FILENAME).exists()
    assert (output_dir / CATALOG_FILENAME).exists()


def test_context_generate_docs_static(sushi_context: Context, output_dir: Path) -> None:
    result = sushi_context.generate_docs(output_path=str(output_dir), static=True)

    assert result.exists()
    assert result.name == STATIC_INDEX_FILENAME
    html = result.read_text(encoding="utf-8")
    assert "window.__SQLMESH_MANIFEST__" in html
    assert "fonts.googleapis.com" not in html
    assert (output_dir / INDEX_FILENAME).exists()
    assert (output_dir / MANIFEST_FILENAME).exists()
    assert (output_dir / CATALOG_FILENAME).exists()


def test_external_links_config_parsing() -> None:
    config = DocsConfig(
        external_links=[
            ExternalLinkConfig(label="Airflow", url="https://airflow.example.com/dags/{name}"),
            ExternalLinkConfig(label="Looker", url="https://looker.example.com/{schema}/{name}"),
        ]
    )
    assert len(config.external_links) == 2
    assert config.external_links[0].label == "Airflow"
    assert "{name}" in config.external_links[0].url


def test_external_links_default_empty() -> None:
    config = DocsConfig()
    assert config.external_links == []


def test_external_links_reject_unsupported_placeholders() -> None:
    with pytest.raises(ValidationError, match="Unsupported placeholders"):
        ExternalLinkConfig(
            label="OwnerLink",
            url="https://example.com/{name}/{owner}/{dialect}",
        )


def test_external_links_in_generated_html(sushi_context: Context, output_dir: Path) -> None:
    sushi_context.config.docs = DocsConfig(
        external_links=[
            ExternalLinkConfig(
                label="Airflow",
                url="https://airflow.example.com/dags/{name}",
            ),
        ]
    )

    sushi_context.generate_docs(output_path=str(output_dir))
    manifest_data, _ = _read_docs_json_artifacts(output_dir)

    assert "external_links" in manifest_data
    assert manifest_data["external_links"][0]["label"] == "Airflow"
    assert "airflow.example.com" in manifest_data["external_links"][0]["url"]


def test_external_links_empty_when_not_configured(sushi_context: Context, output_dir: Path) -> None:
    sushi_context.config.docs = DocsConfig()
    sushi_context.generate_docs(output_path=str(output_dir))
    manifest_data, _ = _read_docs_json_artifacts(output_dir)

    assert manifest_data["external_links"] == []
