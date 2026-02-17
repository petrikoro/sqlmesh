from __future__ import annotations

import json
import logging
import typing as t
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from sqlmesh.api.models import Model
from sqlmesh.core.lineage import _column_dependencies_from_model

if t.TYPE_CHECKING:
    from sqlmesh.core.context import Context
    from sqlmesh.core.config.docs import ExternalLinkConfig
    from sqlmesh.core.model import Model as InternalModel

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"
DOCS_DIRNAME = "dbt_artifacts"
CATALOG_FILENAME = "catalog.json"
MANIFEST_FILENAME = "manifest.json"
INDEX_FILENAME = "index.html"
STATIC_INDEX_FILENAME = "static_index.html"


class DocsGenerator:
    """Generate documentation artifacts for a SQLMesh project."""

    def __init__(self, context: Context) -> None:
        self.context = context

    @property
    def default_output_dir(self) -> Path:
        """Return the default docs output directory."""
        return self.context.cache_dir / DOCS_DIRNAME

    def generate(
        self,
        output_path: t.Optional[t.Union[str, Path]] = None,
        select_models: t.Optional[t.Collection[str]] = None,
        static: bool = False,
    ) -> Path:
        """Generate docs files and return the entrypoint HTML path.

        Args:
            output_path: Optional output directory for docs artifacts.
            select_models: Optional model names or FQNs to include.
            static: If ``True``, generate ``static_index.html`` with inline JSON.
        """
        from sqlmesh.api.handlers import get_models

        return generate_docs(
            models=get_models(self.context),
            dag_graph=self.context.dag.graph,
            internal_models=self.context._models,
            external_links=self.context.config.docs.external_links,
            cache_dir=self.context.cache_dir,
            default_catalog=self.context.default_catalog,
            output_path=output_path,
            select_models=select_models,
            static=static,
        )

    def build_docs_data(
        self,
        select_models: t.Optional[t.Collection[str]] = None,
    ) -> t.Tuple[t.Dict[str, t.Any], t.Dict[str, t.Any]]:
        """Build docs payloads without writing files.

        Args:
            select_models: Optional model names or FQNs to include.
        """
        from sqlmesh.api.handlers import get_models

        return build_docs_data(
            models=get_models(self.context),
            dag_graph=self.context.dag.graph,
            internal_models=self.context._models,
            external_links=self.context.config.docs.external_links,
            default_catalog=self.context.default_catalog,
            select_models=select_models,
        )


def generate_docs(
    models: t.List[Model],
    dag_graph: t.Dict[str, t.Set[str]],
    internal_models: t.Mapping[str, InternalModel],
    external_links: t.Sequence[ExternalLinkConfig],
    cache_dir: Path,
    default_catalog: t.Optional[str],
    output_path: t.Optional[t.Union[str, Path]] = None,
    select_models: t.Optional[t.Collection[str]] = None,
    static: bool = False,
) -> Path:
    """Generate the documentation site.

    Args:
        models: Serialised API model list (from ``get_models``).
        dag_graph: Model dependency graph (``context.dag.graph``).
        internal_models: Internal model registry keyed by FQN.
        external_links: Configured external link definitions.
        cache_dir: Cache directory for default output.
        default_catalog: Default catalog name, if any.
        output_path: Directory where docs artifacts will be written.
        select_models: Optional model name patterns to include.
        static: If ``True``, also emits ``static_index.html`` with
                embedded JSON payloads.
    """
    output_dir = Path(output_path) if output_path else cache_dir / DOCS_DIRNAME
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_data, catalog_data = build_docs_data(
        models=models,
        dag_graph=dag_graph,
        internal_models=internal_models,
        external_links=external_links,
        default_catalog=default_catalog,
        select_models=select_models,
    )

    manifest_path = output_dir / MANIFEST_FILENAME
    manifest_path.write_text(_json_dumps(manifest_data), encoding="utf-8")

    catalog_path = output_dir / CATALOG_FILENAME
    catalog_path.write_text(_json_dumps(catalog_data), encoding="utf-8")

    index_html = _render(manifest_data, catalog_data, static=False, inline_json=False)
    index_path = output_dir / INDEX_FILENAME
    index_path.write_text(index_html, encoding="utf-8")

    if static:
        static_html = _render(manifest_data, catalog_data, static=True, inline_json=True)
        static_index_path = output_dir / STATIC_INDEX_FILENAME
        static_index_path.write_text(static_html, encoding="utf-8")
        return static_index_path

    return index_path


def build_docs_data(
    models: t.List[Model],
    dag_graph: t.Dict[str, t.Set[str]],
    internal_models: t.Mapping[str, InternalModel],
    external_links: t.Sequence[ExternalLinkConfig],
    default_catalog: t.Optional[str],
    select_models: t.Optional[t.Collection[str]] = None,
) -> t.Tuple[t.Dict[str, t.Any], t.Dict[str, t.Any]]:
    """Build manifest and catalog JSON payloads for the HTML template.

    Args:
        models: Serialised API model list (from ``get_models``).
        dag_graph: Model dependency graph (``context.dag.graph``).
        internal_models: Internal model registry keyed by FQN.
        external_links: Configured external link definitions.
        default_catalog: Default catalog name, if any.
        select_models: Optional model names or FQNs to include.
    """
    if select_models:
        select_set = set(select_models)
        models = [m for m in models if m.name in select_set or m.fqn in select_set]

    model_fqns = {m.fqn for m in models}

    manifest_models: t.List[t.Dict[str, t.Any]] = []
    catalog_models: t.Dict[str, t.Dict[str, t.Any]] = {}
    dag: t.Dict[str, t.List[str]] = {}

    type_counts: t.Dict[str, int] = {}
    kind_counts: t.Dict[str, int] = {}
    project_counts: t.Dict[str, int] = {}
    owners: t.Set[str] = set()
    tags: t.Set[str] = set()
    total_columns = 0
    models_with_desc = 0
    cols_with_desc = 0

    for m in models:
        details = _build_details(m)
        raw_code = _get_raw_code(internal_models, m)
        model_project = _get_project(internal_models, m)

        manifest_models.append(
            {
                "name": m.name,
                "fqn": m.fqn,
                "path": str(m.path) if m.path else "",
                "dialect": m.dialect,
                "type": m.type.value,
                "description": m.description,
                "hash": m.hash,
                "default_catalog": m.default_catalog,
                "details": details,
                "definition": raw_code or None,
                "project": model_project,
            }
        )

        if m.fqn:
            catalog_models[m.fqn] = {
                "columns": [
                    {
                        "name": c.name,
                        "type": c.type,
                        "description": c.description,
                        "tags": c.tags or [],
                        "meta": c.meta or {},
                    }
                    for c in m.columns
                ]
            }

        dag[m.fqn] = sorted(fqn for fqn in dag_graph.get(m.fqn, set()) if fqn in model_fqns)

        type_counts[m.type.value] = type_counts.get(m.type.value, 0) + 1
        if model_project:
            project_counts[model_project] = project_counts.get(model_project, 0) + 1
        if details:
            kind = details.get("kind")
            if kind:
                kind_counts[kind] = kind_counts.get(kind, 0) + 1
            owner = details.get("owner")
            if owner:
                owners.add(owner)
            tags_str = details.get("tags")
            if tags_str:
                for tag in tags_str.split(","):
                    tag = tag.strip()
                    if tag:
                        tags.add(tag)

        total_columns += len(m.columns)
        if m.description:
            models_with_desc += 1
        cols_with_desc += sum(1 for c in m.columns if c.description)

    manifest_models.sort(key=lambda x: x.get("name", ""))

    column_lineage = _build_column_lineage(models, internal_models, default_catalog)

    manifest_data: t.Dict[str, t.Any] = {
        "project": {
            "name": "SQLMesh Catalog",
            "model_count": len(manifest_models),
            "column_count": total_columns,
            "models_with_description": models_with_desc,
            "columns_with_description": cols_with_desc,
            "type_counts": type_counts,
            "kind_counts": kind_counts,
            "project_counts": project_counts,
            "projects": sorted(project_counts.keys()),
            "owners": sorted(owners),
            "tags": sorted(tags),
        },
        "models": manifest_models,
        "dag": dag,
        "column_lineage": column_lineage,
        "external_links": [{"label": link.label, "url": link.url} for link in external_links],
    }
    catalog_data: t.Dict[str, t.Any] = {"models": catalog_models}

    return manifest_data, catalog_data


def _build_column_lineage(
    models: t.List[Model],
    internal_models: t.Mapping[str, InternalModel],
    default_catalog: t.Optional[str],
) -> t.Dict[str, t.Dict[str, t.List[t.Dict[str, str]]]]:
    """Build column-level lineage keyed by FQN.

    Self-referential lineage is excluded as it clutters the docs UI.
    """
    result: t.Dict[str, t.Dict[str, t.List[t.Dict[str, str]]]] = {}

    for m in models:
        internal = internal_models.get(m.fqn)
        if not internal:
            continue

        col_map: t.Dict[str, t.List[t.Dict[str, str]]] = {}
        for col in m.columns:
            try:
                deps = _column_dependencies_from_model(internal, col.name, default_catalog)
                sources: t.List[t.Dict[str, str]] = [
                    {"model": up_fqn, "column": uc}
                    for up_fqn, up_cols in deps.items()
                    for uc in sorted(up_cols)
                    if up_fqn != m.fqn
                ]
                if sources:
                    col_map[col.name] = sources
            except Exception:
                logger.debug("Could not compute lineage for %s.%s", m.fqn, col.name, exc_info=True)

        if col_map:
            result[m.fqn] = col_map

    return result


def _get_raw_code(internal_models: t.Mapping[str, t.Any], model: Model) -> str:
    """Extract source code for a model."""
    try:
        sqlmesh_model = internal_models.get(model.fqn)
        if sqlmesh_model is None:
            return ""

        source_type = getattr(sqlmesh_model, "source_type", "")

        if source_type == "sql":
            query = getattr(sqlmesh_model, "query", None)
            if query is not None:
                return query.sql(pretty=True, dialect=model.dialect)

        elif source_type == "python":
            path = getattr(sqlmesh_model, "_path", None)
            if path is not None and path.exists():
                return path.read_text(encoding="utf-8")

    except Exception:
        logger.debug("Could not extract raw code for %s", model.fqn, exc_info=True)
    return ""


def _get_project(internal_models: t.Mapping[str, t.Any], model: Model) -> str:
    """Return the project name for a model, or empty string."""
    try:
        sqlmesh_model = internal_models.get(model.fqn)
        if sqlmesh_model is not None:
            return getattr(sqlmesh_model, "project", "") or ""
    except Exception:
        logger.debug("Could not extract project for %s", model.fqn, exc_info=True)
    return ""


def _render(
    manifest_data: t.Dict[str, t.Any],
    catalog_data: t.Dict[str, t.Any],
    static: bool = False,
    inline_json: bool = False,
) -> str:
    """Render HTML template for dynamic or static docs mode."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=False,
    )
    template = env.get_template("index.html")
    manifest_json = _json_dumps(manifest_data)
    catalog_json = _json_dumps(catalog_data)
    # Escape </script> to prevent injection when embedded in script tags.
    manifest_json = manifest_json.replace("</", r"<\/")
    catalog_json = catalog_json.replace("</", r"<\/")
    return template.render(
        manifest_json=manifest_json,
        catalog_json=catalog_json,
        catalog_name="SQLMesh Catalog",
        static=static,
        inline_json=inline_json,
    )


def _json_dumps(payload: t.Dict[str, t.Any]) -> str:
    """Serialize docs payload to compact JSON."""
    return json.dumps(payload, indent=None, default=str)


# Detail attributes included when truthy
_DETAIL_ATTRS_TRUTHY = (
    "kind",
    "owner",
    "cron",
    "time_column",
    "table_format",
    "storage_format",
    "partitioned_by",
    "clustered_by",
)

# Detail attributes included even when falsy (None-check only)
_DETAIL_ATTRS_NULLABLE = ("batch_size", "retention", "lookback", "annotated")


def _build_details(model: Model) -> t.Optional[t.Dict[str, t.Any]]:
    """Build the details dict from a :class:`Model` object."""
    details: t.Dict[str, t.Any] = {}

    if model.details:
        d = model.details
        for attr in _DETAIL_ATTRS_TRUTHY:
            val = getattr(d, attr, None)
            if val:
                details[attr] = val
        if d.start is not None:
            details["start"] = str(d.start)
        if d.interval_unit is not None:
            details["interval_unit"] = str(d.interval_unit.value)
        for attr in _DETAIL_ATTRS_NULLABLE:
            val = getattr(d, attr, None)
            if val is not None:
                details[attr] = val
        if d.tags:
            details["tags"] = d.tags
        if d.meta:
            details["meta"] = d.meta

    # Parse database/schema from FQN
    parts = [p.strip('"') for p in model.fqn.split(".") if p.strip('"')]
    if len(parts) >= 3:
        details["database"] = parts[0]
        details["schema"] = parts[1]
    elif len(parts) >= 2:
        details["schema"] = parts[0]

    # Materialized kind (lowercased for dbt compatibility)
    if model.details and model.details.kind:
        details["materialized"] = model.details.kind.lower()

    return details or None
