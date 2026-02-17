from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from sqlmesh.core.context import Context
from sqlmesh.core.manifest import (
    MANIFEST_FILENAME,
    ManifestGenerator,
    _build_fqn_to_uid,
    _parse_fqn_parts,
)


@pytest.fixture(scope="module")
def sushi_context() -> Context:
    return Context(paths=["examples/sushi"])


def test_generate_creates_manifest_json(sushi_context: Context, tmp_path: Path) -> None:
    generator = ManifestGenerator(sushi_context)
    result = generator.generate(output_path=str(tmp_path))

    assert result == tmp_path / MANIFEST_FILENAME
    assert result.exists()

    data = json.loads(result.read_text(encoding="utf-8"))
    assert "metadata" in data
    assert "nodes" in data


def test_generate_default_output_dir(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    result = generator.generate()

    assert result.exists()
    assert result.name == MANIFEST_FILENAME
    assert result.parent == sushi_context.cache_dir / "dbt_artifacts"


def test_generate_supports_file_output_path(sushi_context: Context, tmp_path: Path) -> None:
    generator = ManifestGenerator(sushi_context)
    target_file = tmp_path / "custom_manifest.json"
    result = generator.generate(output_path=str(target_file))

    assert result == target_file
    assert result.exists()
    data = json.loads(result.read_text(encoding="utf-8"))
    assert "metadata" in data
    assert "nodes" in data


def test_manifest_has_dbt_compatible_metadata(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    manifest = generator.build_manifest()

    meta = manifest["metadata"]
    assert "dbt_schema_version" in meta
    assert "dbt_version" in meta
    assert isinstance(meta["dbt_version"], str)
    assert meta["dbt_version"]
    assert "generated_at" in meta
    assert "adapter_type" in meta
    assert "project_name" in meta


def test_manifest_has_sqlmesh_extensions(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    manifest = generator.build_manifest()

    metadata = manifest["metadata"]
    env = metadata.get("env", {})
    assert "sqlmesh_version" in env
    assert "sqlmesh_project_path" in env
    assert "sqlmesh_model_count" in env
    assert "sqlmesh_dialect" in env
    assert "sqlmesh" not in metadata

    for node in manifest["nodes"].values():
        sm = node["meta"]["_sqlmesh"]
        assert "fqn" in sm
        assert "dialect" in sm
        assert "type" in sm
        assert "hash" in sm


def test_manifest_has_dbt_top_level_keys(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    manifest = generator.build_manifest()

    for key in (
        "metadata",
        "nodes",
        "sources",
        "macros",
        "docs",
        "exposures",
        "metrics",
        "groups",
        "selectors",
        "disabled",
        "parent_map",
        "child_map",
        "group_map",
        "saved_queries",
        "semantic_models",
        "unit_tests",
    ):
        assert key in manifest, f"Missing top-level key: {key}"
    assert "column_lineage" not in manifest


def test_nodes_have_dbt_compatible_shape(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    manifest = generator.build_manifest()
    nodes = manifest["nodes"]

    assert nodes
    for uid, node in nodes.items():
        assert "unique_id" in node
        assert node["unique_id"] == uid
        assert "resource_type" in node
        assert "name" in node
        assert "database" in node or node.get("database") is None
        assert "schema" in node
        assert "fqn" in node
        assert isinstance(node["fqn"], list)
        assert "description" in node
        assert "columns" in node
        assert "depends_on" in node
        assert "macros" in node["depends_on"]
        if node["resource_type"] != "seed":
            assert "nodes" in node["depends_on"]
        assert "config" in node
        assert "materialized" in node["config"]
        assert "tags" in node
        assert "meta" in node
        assert "docs" in node


def test_columns_have_dbt_shape(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    manifest = generator.build_manifest()
    has_columns = False

    for node in manifest["nodes"].values():
        if node["columns"]:
            has_columns = True
            for col_name, col_data in node["columns"].items():
                assert col_data["name"] == col_name
                assert "type" in col_data
                assert "data_type" in col_data
                assert "description" in col_data
                assert "meta" in col_data
                assert "tags" in col_data
    assert has_columns


def test_manifest_includes_yaml_model_meta_and_column_metadata(sushi_context: Context) -> None:
    manifest = ManifestGenerator(sushi_context).build_manifest()
    node = next(
        (node for node in manifest["nodes"].values() if node["name"] == "yaml_documented_orders"),
        None,
    )

    assert node
    assert node["meta"]["owner_team"] == "finance"
    assert node["meta"]["contains_pii"] is True
    assert "_sqlmesh" in node["meta"]
    assert node["config"]["meta"] == {"owner_team": "finance", "contains_pii": True}
    assert node["columns"]["order_id"]["tags"] == ["primary_key", "pii"]
    assert node["columns"]["order_id"]["meta"] == {"classification": "sensitive"}
    assert node["columns"]["event_date"]["tags"] == ["event_time"]
    assert node["columns"]["event_date"]["meta"] == {"grain": "day"}


def test_parent_child_maps(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    manifest = generator.build_manifest()
    parent_map = manifest["parent_map"]
    child_map = manifest["child_map"]

    assert parent_map
    assert child_map

    all_uids = set(manifest["nodes"].keys()) | set(manifest["sources"].keys())
    for uid in all_uids:
        assert uid in parent_map
        assert uid in child_map


def test_parent_child_consistency(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    manifest = generator.build_manifest()
    parent_map = manifest["parent_map"]
    child_map = manifest["child_map"]

    for uid, parents in parent_map.items():
        for parent_uid in parents:
            assert uid in child_map.get(parent_uid, [])


def test_unique_id_format(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    manifest = generator.build_manifest()

    for uid in manifest["nodes"]:
        parts = uid.split(".")
        assert len(parts) >= 3
        assert parts[0] in ("model", "seed", "source")


def test_materialization_uses_sqlmesh_kind(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    manifest = generator.build_manifest()

    for uid, node in manifest["nodes"].items():
        materialized = node["config"]["materialized"]
        assert isinstance(materialized, str) and materialized, (
            f"Node {uid} has empty materialization"
        )
        assert materialized == materialized.lower(), (
            f"Node {uid} materialization not lowercased: {materialized}"
        )


def test_column_lineage_present(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    manifest = generator.build_manifest()
    assert any(
        node.get("meta", {}).get("_sqlmesh", {}).get("column_lineage")
        for node in manifest["nodes"].values()
    )


def test_column_lineage_structure(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    manifest = generator.build_manifest()

    for node in manifest["nodes"].values():
        col_lineage = node.get("meta", {}).get("_sqlmesh", {}).get("column_lineage")
        if not col_lineage:
            continue
        assert isinstance(col_lineage, dict)
        for sources in col_lineage.values():
            assert isinstance(sources, list)
            for source in sources:
                assert "model" in source
                assert "column" in source


def test_select_models_filters_nodes(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    full = generator.build_manifest()
    full_count = len(full["nodes"]) + len(full["sources"])

    first_node = list(full["nodes"].values())[0]
    target_fqn = first_node["meta"]["_sqlmesh"]["fqn"]

    filtered = generator.build_manifest(select_models=[target_fqn])
    filtered_count = len(filtered["nodes"]) + len(filtered["sources"])

    assert filtered_count == 1
    assert filtered_count < full_count


def test_select_models_does_not_support_wildcards(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    full = generator.build_manifest()
    first_node = list(full["nodes"].values())[0]
    target_fqn = first_node["meta"]["_sqlmesh"]["fqn"]

    filtered = generator.build_manifest(select_models=[f"{target_fqn}*"])
    filtered_count = len(filtered["nodes"]) + len(filtered["sources"])

    assert filtered_count == 0


def test_select_models_has_no_dangling_depends_on_nodes(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    full = generator.build_manifest()
    candidate = next(
        (node for node in full["nodes"].values() if node["depends_on"].get("nodes")),
        None,
    )
    if not candidate:
        pytest.skip("No model with upstream dependencies found.")

    filtered = generator.build_manifest(select_models=[candidate["meta"]["_sqlmesh"]["fqn"]])
    valid_uids = set(filtered["nodes"]) | set(filtered["sources"])

    for node in filtered["nodes"].values():
        for dep_uid in node["depends_on"].get("nodes", []):
            assert dep_uid in valid_uids


def test_select_models_has_no_dangling_column_lineage_model_ids(sushi_context: Context) -> None:
    generator = ManifestGenerator(sushi_context)
    full = generator.build_manifest()
    candidate = next(
        (
            node
            for node in full["nodes"].values()
            if node.get("meta", {}).get("_sqlmesh", {}).get("column_lineage")
        ),
        None,
    )
    if not candidate:
        pytest.skip("No model with column lineage found.")

    filtered = generator.build_manifest(select_models=[candidate["meta"]["_sqlmesh"]["fqn"]])
    valid_uids = set(filtered["nodes"]) | set(filtered["sources"])

    for node in filtered["nodes"].values():
        col_lineage = node.get("meta", {}).get("_sqlmesh", {}).get("column_lineage", {})
        for sources in col_lineage.values():
            for source in sources:
                assert source["model"] in valid_uids


def test_manifest_json_is_valid(sushi_context: Context, tmp_path: Path) -> None:
    generator = ManifestGenerator(sushi_context)
    result = generator.generate(output_path=str(tmp_path))

    data = json.loads(result.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    assert int(data["metadata"]["env"]["sqlmesh_model_count"]) > 0


def test_context_generate_manifest(sushi_context: Context, tmp_path: Path) -> None:
    result = sushi_context.generate_manifest(output_path=str(tmp_path))

    assert result.exists()
    assert result.name == MANIFEST_FILENAME

    data = json.loads(result.read_text(encoding="utf-8"))
    assert "nodes" in data
    assert "metadata" in data
    assert len(data["nodes"]) > 0


def test_build_fqn_to_uid_raises_on_unique_id_collision() -> None:
    models = [
        SimpleNamespace(
            fqn="catalog_a.schema.orders",
            type=SimpleNamespace(value="sql"),
        ),
        SimpleNamespace(
            fqn="catalog_b.schema.orders",
            type=SimpleNamespace(value="sql"),
        ),
    ]

    with pytest.raises(ValueError, match="unique_id collision"):
        _build_fqn_to_uid(models)


def test_parse_fqn_parts_handles_empty_input() -> None:
    assert _parse_fqn_parts("") == (None, "default", "unknown")
    assert _parse_fqn_parts("..") == (None, "default", "unknown")


def test_parse_fqn_parts_variants() -> None:
    assert _parse_fqn_parts("name") == (None, "default", "name")
    assert _parse_fqn_parts("schema.name") == (None, "schema", "name")
    assert _parse_fqn_parts("catalog.schema.name") == ("catalog", "schema", "name")
