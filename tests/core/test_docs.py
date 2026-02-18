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


def test_docs_data_uses_full_sql_model_definition(sushi_context: Context) -> None:
    manifest_data, _ = _build_docs_data(sushi_context)
    model = next(
        (m for m in manifest_data["models"] if m["name"] == "sushi.yaml_documented_orders"), None
    )

    assert model
    assert model["definition"]
    assert "MODEL (" in model["definition"]
    assert "name sushi.yaml_documented_orders" in model["definition"]
    assert "SELECT" in model["definition"]


def test_html_template_renders_column_tags_without_properties(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "Model Properties" not in html
    assert "sec-properties" not in html
    assert " Properties</a>" not in html
    assert "<th>Tags</th>" in html
    assert "Array.isArray(c.tags)" in html
    assert "c-tags-cell" in html
    assert "c-tags" in html
    assert "cn-tags" not in html
    assert "cd-tags" not in html
    assert "c.tags.forEach(function(tag)" in html
    assert "columnTagMatch" in html
    assert "columnTagMatches" in html


def test_html_template_renders_markdown_descriptions(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "function renderMarkdown(text)" in html
    assert "function renderInlineMarkdown(text)" in html
    assert "renderMarkdown(model.description)" in html
    assert "renderMarkdown(description)" in html
    assert 'replace(/\\n/g, "<br>")' in html


def test_html_template_supports_column_filter_and_collapsible_descriptions(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "columns-filter-input" in html
    assert "Filter fields by name, type, description, tags..." in html
    assert "window.filterColumnsInSection = function(inputEl)" in html
    assert "data-col-search" in html
    assert "window.toggleColumnDescription = function(button)" in html
    assert "cd-content collapsed" in html
    assert "Show more" in html


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


def test_html_lineage_formats_schema_table_and_full_fqn_tooltip(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "function parseLineageFqnParts(fqn)" in html
    assert "function formatLineageLabel(fqn)" in html
    assert "var name = parsed.table || formatLineageLabel(fqn);" in html
    assert "var fullFqn = parsed.fullFqn;" in html
    assert '<span class="lblock-name" title="' in html
    assert 'document.getElementById("loTitle").textContent' not in html
    assert 'id="loTitle"' not in html


def test_html_lineage_blocks_render_datahub_style_sections(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "h += '<div class=\"lblock-meta\">';" in html
    assert "h += '<div class=\"lblock-meta-top\">';" in html
    assert "h += '<span class=\"lblock-entity-pill\">Table</span>';" in html
    assert 'h += \'<span class="lblock-kind-pill"' in html
    assert (
        'h += \'<div class="lblock-cols-header" onclick="event.stopPropagation(); togBlock(' in html
    )
    assert "h += '<span class=\"lblock-cols-title\">Columns</span>';" in html
    assert "h += '<span class=\"lblock-cols-arrow\">' + ICONS.chevron + '</span>';" in html
    assert "h += '<div class=\"lblock-cols-panel\">';" in html
    assert "h += '<div class=\"lblock-cols-panel open\">';" not in html
    assert (
        "h += '<div class=\"lineage-home-text\">' + ICONS.home + '<span>Home</span></div>';" in html
    )
    assert "current-model-home" not in html
    assert "current-model-tag" not in html
    assert "lineage-col-label current-model-label" not in html
    assert "h += '<div class=\"lineage-col lineage-col-current\">';" in html
    assert ".lineage-home-text {" in html
    assert ".lineage-home-text.hidden { display: none; }" in html
    assert ".lineage-home-text svg { width: 15px; height: 15px; }" in html
    assert ".lineage-col-current {" in html
    assert "gap: 6px;" in html
    assert ".lblock-shell {" in html
    assert "h += '<div class=\"lblock-shell\">';" in html
    assert ".lblock-home-wrap {" not in html
    assert ".lblock-home-pill {" not in html
    assert ".lblock-home-tail {" not in html
    assert ".lblock-home-tail::after {" not in html
    assert "flex: 0 0 340px;" in html
    assert "width: 340px;" in html
    assert "flex-wrap: nowrap;" in html


def test_html_lineage_renders_table_level_edges(sushi_context: Context, output_dir: Path) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "function buildModelLineageMap(lineageMap)" in html
    assert "var modelLineage = buildModelLineageMap(colLineage);" in html
    assert 'class="ltable-anchor" data-ltable="' in html
    assert 'canvas.querySelectorAll("[data-ltable]")' in html
    assert '"table-path"' in html
    assert ".lineage-canvas svg.lineage-svg path.table-path" in html
    assert ".lineage-canvas svg.lineage-svg path.column-path" in html
    assert "var selectedNodeId = getSelectedLineageNode(canvasId);" in html
    assert "if (selectedPathSet) {" in html
    assert '"column-path hl"' in html


def test_html_lineage_has_column_search_and_scroll_container(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "window.filterLineageColumnsInBlock = function(inputEl)" in html
    assert 'class="lblock-col-search"' in html
    assert 'placeholder="Find column"' in html
    assert 'class="lblock-col-list"' in html
    assert ".lblock-col-list {" in html
    assert "max-height: 420px;" in html
    assert "overflow-y: auto;" in html


def test_html_columns_uses_scroll_without_fade_overlay(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert ".col-tbl-wrap {" in html
    assert "max-height: 420px;" in html
    assert "overflow-y: auto;" in html
    assert 'class="columns-scroll-fade"' not in html
    assert "window.updateColumnsSectionFade = function(section)" not in html
    assert 'onscroll="updateColumnsSectionFade(this.closest(' not in html
    assert (
        "h += '<span class=\"columns-filter-count\">' + m.columns.length + ' / ' + m.columns.length + '</span>';"
        in html
    )
    assert "window.expandColumnsInSection = function(buttonEl)" not in html
    assert 'class="columns-more-wrap"' not in html
    assert 'class="columns-more-btn"' not in html


def test_html_lineage_has_expand_and_collapse_all_columns_controls(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "window.lcSetAllColumns = function(canvasId, svgId, isOpen)" in html
    assert 'title="Expand all columns"' in html
    assert 'title="Collapse all columns"' in html
    assert "setBlockColumnsOpen(block, isOpen);" in html
    assert "setBlockColumnsExpanded(block, isOpen, true);" in html
    assert "clearHighlight(canvas);" in html
    assert html.index('title="Collapse all columns"') < html.index('title="Expand all columns"')


def test_html_lineage_has_per_node_branch_toggle_controls(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert 'class="lblock-hop-toggle upstream"' in html
    assert 'class="lblock-hop-toggle downstream"' in html
    assert 'data-hop-dir="upstream"' in html
    assert 'data-hop-dir="downstream"' in html
    assert "window.toggleLineageNeighbors = function(buttonEl, direction)" in html
    assert "function getLineageToggleState(canvasId)" in html
    assert "function getImmediateLineageNeighbors(modelFqn, direction)" in html
    assert "function getLineageBranchNeighbors(modelFqn, direction)" in html
    assert "function applyLineageHiddenState(canvasId)" in html
    assert "var reverseModelLineage = buildReverseModelLineageMap(modelLineage);" in html
    assert 'data-model-fqn="' in html
    assert 'data-ltable-in="' in html
    assert 'data-ltable-out="' in html
    assert (
        'var hasUpToggle = getLineageBranchNeighbors(normalizedFqn, "upstream", normalizedRootFqn).length > 0;'
        in html
    )
    assert (
        'var hasDownToggle = getLineageBranchNeighbors(normalizedFqn, "downstream", normalizedRootFqn).length > 0;'
        in html
    )
    assert (
        'if (hasUpToggle) h += \'<button type="button" class="lblock-hop-toggle upstream"' in html
    )
    assert (
        'if (hasDownToggle) h += \'<button type="button" class="lblock-hop-toggle downstream"'
        in html
    )
    assert 'canvas.querySelectorAll("[data-ltable-in]").forEach(function(el) {' in html
    assert 'canvas.querySelectorAll("[data-ltable-out]").forEach(function(el) {' in html
    assert "var fromEl = srcAnchors.outgoing || srcAnchors.center || srcAnchors.incoming;" in html
    assert (
        "var toEl = targetAnchors.incoming || targetAnchors.center || targetAnchors.outgoing;"
        in html
    )
    assert (
        "var actionableNeighbors = getLineageBranchNeighbors(modelFqn, direction, rootModelFqn).filter(function(neighborFqn) {"
        in html
    )
    assert "var shouldHide = modelFqn && isLineageModelHidden(canvasId, modelFqn);" in html
    assert "Hide upstream branch" in html
    assert "Hide downstream branch" in html
    assert (
        'var fromGap = fromEl.classList && fromEl.classList.contains("lblock-hop-toggle") ? 4 : 0;'
        in html
    )
    assert (
        'var toGap = toEl.classList && toEl.classList.contains("lblock-hop-toggle") ? 4 : 0;'
        in html
    )
    assert 'var homeText = canvas.querySelector(".lineage-col-current .lineage-home-text");' in html
    assert 'homeText.classList.toggle("hidden", hideHome);' in html


def test_html_lineage_branch_neighbors_dont_skip_home_model(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "function getLineageBranchNeighbors(modelFqn, direction)" in html
    assert "if (rootFqn && nextFqn === rootFqn) continue;" not in html


def test_html_lineage_uses_roomier_spacing_and_thinner_lines(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert ".lineage-flow {" in html
    assert "gap: 96px;" in html
    assert ".lineage-col {" in html
    assert "gap: 20px;" in html
    assert ".lineage-canvas svg.lineage-svg path {" in html
    assert "stroke-width: 1;" in html
    assert ".lineage-canvas svg.lineage-svg path.table-path" in html
    assert ".lineage-canvas svg.lineage-svg path.column-path" in html
    assert ".lineage-canvas svg.lineage-svg path.hl" in html
    assert "stroke-width: 1.2;" in html


def test_html_lineage_column_selection_is_click_only(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "function setSelectedLineageNode(canvasId, nodeId)" in html
    assert "function getSelectedLineageNode(canvasId)" in html
    assert 'row.addEventListener("click", function(ev)' in html
    assert "if (ev.target.closest('.lblock-col-list')) return;" in html
    assert 'canvas.dataset.skipClearSelectionClick = "1";' in html
    assert 'if (canvas.dataset.skipClearSelectionClick === "1") return;' in html
    assert 'ev.target.closest(".lblock")' in html
    assert 'ev.target.closest(".lineage-controls")' in html
    assert 'ev.target.closest(".lineage-expand-btn")' in html
    assert "if (bid) togBlock(bid);" not in html
    assert 'row.addEventListener("mouseenter", function()' not in html
    assert 'row.addEventListener("mouseleave", function()' not in html


def test_html_lineage_uses_only_scroll_visible_column_rows_for_paths(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "function isVisible(el) {" in html
    assert 'var list = el.closest(".lblock-col-list");' in html
    assert "if (!list) return false;" in html
    assert "var rowRect = el.getBoundingClientRect();" in html
    assert "var listRect = list.getBoundingClientRect();" in html
    assert (
        "var intersectsViewport = rowRect.bottom > listRect.top && rowRect.top < listRect.bottom;"
        in html
    )
    assert "return intersectsViewport;" in html


def test_html_lineage_falls_back_to_table_anchor_for_offscreen_columns(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "function resolveLineageEndpoint(nodeId, role) {" in html
    assert "if (nodeMap[nodeId]) return { el: nodeMap[nodeId], edgeId: nodeId };" in html
    assert (
        'var fallbackEl = role === "source" ? (anchors.outgoing || anchors.center || anchors.incoming) : (anchors.incoming || anchors.center || anchors.outgoing);'
        in html
    )
    assert "if (!fallbackEl) return null;" in html
    assert 'var fromPoint = resolveLineageEndpoint(srcId, "source");' in html
    assert 'var toPoint = resolveLineageEndpoint(nodeId, "target");' in html
    assert "if (!fromPoint || !toPoint) return;" in html
    assert (
        'paths += makePath(fromPoint.el, toPoint.el, cr, 0, 0, fromPoint.edgeId, toPoint.edgeId, "column-path hl");'
        in html
    )


def test_html_tree_defaults_to_collapsed_tables_under_schema(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "var projOpen = isTreeNodeOpen(projKey, true);" in html
    assert "var catOpen = isTreeNodeOpen(catKey, true);" in html
    assert "var schemaOpen = isTreeNodeOpen(schemaKey, false);" in html
    assert (
        "h += '<span class=\"tree-arrow' + (schemaOpen ? ' open' : '') + '\">' + ICONS.chevron + '</span>';"
        in html
    )
    assert "h += '<div class=\"tree-children' + (schemaOpen ? ' open' : '') + '\">';" in html


def test_html_tree_preserves_expand_state_between_renders(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert "var _treeOpenState = loadTreeOpenState();" in html
    assert "function treeNodeKey(parts)" in html
    assert "function isTreeNodeOpen(key, defaultOpen)" in html
    assert "function setTreeNodeOpen(key, isOpen)" in html
    assert 'data-tree-key="' in html
    assert 'var key = header.dataset.treeKey || "";' in html
    assert 'setTreeNodeOpen(key, children.classList.contains("open"));' in html


def test_html_tree_persists_open_state_in_local_storage(
    sushi_context: Context, output_dir: Path
) -> None:
    result = DocsGenerator(sushi_context).generate(output_path=str(output_dir))
    html = result.read_text(encoding="utf-8")

    assert 'var TREE_OPEN_STATE_STORAGE_KEY = "sqlmesh-docs-tree-open-state";' in html
    assert "function loadTreeOpenState()" in html
    assert "function persistTreeOpenState()" in html
    assert "var raw = localStorage.getItem(TREE_OPEN_STATE_STORAGE_KEY);" in html
    assert (
        "localStorage.setItem(TREE_OPEN_STATE_STORAGE_KEY, JSON.stringify(_treeOpenState));" in html
    )
    assert "var _treeOpenState = loadTreeOpenState();" in html
    assert "persistTreeOpenState();" in html


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
