from __future__ import annotations

import json
import logging
import typing as t
from datetime import datetime, timezone
from hashlib import md5
from pathlib import Path
from uuid import uuid4

from sqlmesh.api.models import Model
from sqlmesh.core.lineage import _column_dependencies_from_model

if t.TYPE_CHECKING:
    from sqlmesh.core.context import Context
    from sqlmesh.core.model import Model as InternalModel

logger = logging.getLogger(__name__)

MANIFEST_FILENAME = "manifest.json"
CATALOG_FILENAME = "catalog.json"
ARTIFACTS_DIRNAME = "dbt_artifacts"

_MODEL_TYPE_TO_RESOURCE_TYPE: t.Dict[str, str] = {
    "sql": "model",
    "python": "model",
    "seed": "seed",
    "external": "source",
    "source": "source",
}


class ManifestGenerator:
    """Generates dbt-compatible manifest and catalog artifacts for a SQLMesh project."""

    def __init__(self, context: Context) -> None:
        self.context = context

    def generate(
        self,
        output_path: t.Optional[t.Union[str, Path]] = None,
        select_models: t.Optional[t.Collection[str]] = None,
    ) -> Path:
        """Generate ``manifest.json`` / ``catalog.json`` and return manifest path.

        Args:
            output_path: Optional directory or file path to write.
            select_models: Optional model names or FQNs to include.
        """
        from sqlmesh.api.handlers import get_models

        return generate_manifest(
            models=get_models(self.context),
            dag_graph=self.context.dag.graph,
            internal_models=self.context._models,
            project_path=self.context.path,
            cache_dir=self.context.cache_dir,
            adapter_type=adapter_type_name(self.context.engine_adapter),
            default_catalog=self.context.default_catalog,
            default_dialect=self.context.default_dialect,
            output_path=output_path,
            select_models=select_models,
        )

    def build_manifest(
        self,
        select_models: t.Optional[t.Collection[str]] = None,
    ) -> t.Dict[str, t.Any]:
        """Build a manifest payload without writing files.

        Args:
            select_models: Optional model names or FQNs to include.
        """
        from sqlmesh.api.handlers import get_models

        return build_manifest(
            models=get_models(self.context),
            dag_graph=self.context.dag.graph,
            internal_models=self.context._models,
            project_path=self.context.path,
            adapter_type=adapter_type_name(self.context.engine_adapter),
            default_catalog=self.context.default_catalog,
            default_dialect=self.context.default_dialect,
            select_models=select_models,
        )


def generate_manifest(
    models: t.List[Model],
    dag_graph: t.Dict[str, t.Set[str]],
    internal_models: t.Mapping[str, InternalModel],
    project_path: Path,
    cache_dir: Path,
    adapter_type: str,
    default_catalog: t.Optional[str],
    default_dialect: t.Optional[str],
    output_path: t.Optional[t.Union[str, Path]] = None,
    select_models: t.Optional[t.Collection[str]] = None,
) -> Path:
    """Generate dbt-compatible ``manifest.json`` and ``catalog.json``.

    Args:
        models: Serialised API model list (from ``get_models``).
        dag_graph: Model dependency graph (``context.dag.graph``).
        internal_models: Internal model registry keyed by FQN.
        project_path: Project root path.
        cache_dir: Cache directory for default output.
        adapter_type: Engine adapter type name (e.g. ``"duckdb"``).
        default_catalog: Default catalog name, if any.
        default_dialect: Default SQL dialect, if any.
        output_path: Directory or file path for the manifest.
        select_models: Optional model names/FQNs to include.
    """
    if output_path:
        output = Path(output_path)
        if output.suffix:
            output_dir = output.parent
            manifest_path = output
        else:
            output_dir = output
            manifest_path = output / MANIFEST_FILENAME
    else:
        output_dir = cache_dir / ARTIFACTS_DIRNAME
        manifest_path = output_dir / MANIFEST_FILENAME
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = build_manifest(
        models=models,
        dag_graph=dag_graph,
        internal_models=internal_models,
        project_path=project_path,
        adapter_type=adapter_type,
        default_catalog=default_catalog,
        default_dialect=default_dialect,
        select_models=select_models,
    )

    manifest_path.write_text(
        json.dumps(manifest, indent=2, default=str),
        encoding="utf-8",
    )

    catalog = build_catalog(manifest)
    catalog_path = output_dir / CATALOG_FILENAME
    catalog_path.write_text(
        json.dumps(catalog, indent=2, default=str),
        encoding="utf-8",
    )
    return manifest_path


def build_catalog(manifest: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
    """Build a dbt-compatible catalog payload from the manifest payload."""
    metadata = manifest.get("metadata", {})
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})

    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/catalog/v1.json",
            "dbt_version": metadata.get("dbt_version", "1.10.0"),
            "generated_at": metadata.get("generated_at"),
            "invocation_id": metadata.get("invocation_id"),
            "invocation_started_at": metadata.get("invocation_started_at"),
            "env": metadata.get("env", {}),
        },
        "nodes": {
            unique_id: _build_catalog_entry(unique_id, node) for unique_id, node in nodes.items()
        },
        "sources": {
            unique_id: _build_catalog_entry(unique_id, source)
            for unique_id, source in sources.items()
        },
        "errors": None,
    }


def _build_catalog_entry(unique_id: str, relation: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
    columns = relation.get("columns") or {}
    return {
        "metadata": {
            "type": _catalog_relation_type(relation),
            "schema": relation.get("schema"),
            "name": relation.get("name"),
            "database": relation.get("database"),
            "comment": relation.get("description") or None,
            "owner": None,
        },
        "columns": {
            column_name: {
                "name": column_name,
                "type": column.get("type"),
                "index": index,
                "comment": column.get("description") or None,
            }
            for index, (column_name, column) in enumerate(columns.items(), start=1)
        },
        "stats": {},
        "unique_id": unique_id,
    }


def _catalog_relation_type(relation: t.Dict[str, t.Any]) -> str:
    materialized = relation.get("config", {}).get("materialized")
    return "view" if isinstance(materialized, str) and materialized.lower() == "view" else "table"


def build_manifest(
    models: t.List[Model],
    dag_graph: t.Dict[str, t.Set[str]],
    internal_models: t.Mapping[str, InternalModel],
    project_path: Path,
    adapter_type: str,
    default_catalog: t.Optional[str],
    default_dialect: t.Optional[str],
    select_models: t.Optional[t.Collection[str]] = None,
) -> t.Dict[str, t.Any]:
    """Build the full manifest dict (dbt-compatible + sqlmesh extensions).

    This can be used programmatically without writing to disk.

    Args:
        models: Serialised API model list (from ``get_models``).
        dag_graph: Model dependency graph (``context.dag.graph``).
        internal_models: Internal model registry keyed by FQN.
        project_path: Project root path.
        adapter_type: Engine adapter type name (e.g. ``"duckdb"``).
        default_catalog: Default catalog name, if any.
        default_dialect: Default SQL dialect, if any.
        select_models: Optional model names/FQNs to include.
    """
    if select_models:
        select_set = set(select_models)
        models = [m for m in models if m.name in select_set or m.fqn in select_set]

    # Build shared lookups once and reuse across all builders
    fqn_to_uid = _build_fqn_to_uid(models)

    column_lineage = _build_column_lineage(models, fqn_to_uid, internal_models, default_catalog)
    nodes, sources = _build_nodes_and_sources(
        models, fqn_to_uid, dag_graph, column_lineage, internal_models, project_path
    )
    parent_map, child_map = _build_parent_child_maps(models, fqn_to_uid, dag_graph)

    return {
        "metadata": _build_metadata(project_path, adapter_type, default_dialect, len(models)),
        "nodes": nodes,
        "sources": sources,
        "macros": {},
        "docs": {},
        "exposures": {},
        "metrics": {},
        "groups": {},
        "selectors": {},
        "disabled": {},
        "parent_map": parent_map,
        "child_map": child_map,
        "group_map": {},
        "saved_queries": {},
        "semantic_models": {},
        "unit_tests": {},
    }


def _build_metadata(
    project_path: Path,
    adapter_type: str,
    default_dialect: t.Optional[str],
    model_count: int,
) -> t.Dict[str, t.Any]:
    """Build the ``metadata`` block (dbt-compatible + sqlmesh env extras)."""
    try:
        from sqlmesh._version import __version__ as sqlmesh_version
    except ImportError:
        sqlmesh_version = "0.0.0-dev"

    project_name = project_path.name
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    invocation_id = str(uuid4())
    user_id = str(uuid4())

    return {
        # -- dbt-compatible fields --
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
        "dbt_version": "1.10.0",
        "generated_at": generated_at,
        "adapter_type": adapter_type,
        "project_name": project_name,
        "project_id": _project_id(project_name),
        "invocation_id": invocation_id,
        "invocation_started_at": generated_at,
        "user_id": user_id,
        "send_anonymous_usage_stats": False,
        "quoting": {},
        "env": {
            "sqlmesh_version": sqlmesh_version,
            "sqlmesh_project_path": str(project_path),
            "sqlmesh_model_count": str(model_count),
            "sqlmesh_dialect": default_dialect or "",
        },
    }


def _build_nodes_and_sources(
    models: t.List[Model],
    fqn_to_uid: t.Dict[str, str],
    graph: t.Dict[str, t.Set[str]],
    column_lineage: t.Dict[str, t.Dict[str, t.List[t.Dict[str, str]]]],
    internal_models: t.Mapping[str, InternalModel],
    project_path: Path,
) -> t.Tuple[t.Dict[str, t.Any], t.Dict[str, t.Any]]:
    """Convert SQLMesh models into dbt ``nodes`` and ``sources`` dicts."""
    nodes: t.Dict[str, t.Any] = {}
    sources: t.Dict[str, t.Any] = {}
    package_name = project_path.name

    for m in models:
        resource_type = _MODEL_TYPE_TO_RESOURCE_TYPE.get(m.type.value, "model")
        unique_id = fqn_to_uid[m.fqn]
        catalog, schema, name = _parse_fqn_parts(m.fqn)
        fqn_list = [p.strip('"') for p in m.fqn.split(".") if p]
        tags = _parse_tags(m)
        path = str(m.path) if m.path else ""

        # Upstream dependencies
        depends_on_nodes = [
            fqn_to_uid[fqn] for fqn in sorted(graph.get(m.fqn, set())) if fqn in fqn_to_uid
        ]

        # dbt-compatible columns dict
        columns = {
            col.name: {
                "name": col.name,
                "type": col.type,
                "data_type": col.type,
                "description": col.description or "",
                "meta": col.meta or {},
                "tags": col.tags or [],
            }
            for col in m.columns
        }

        # sqlmesh meta (shared between node and source entries)
        sqlmesh_meta = _build_node_sqlmesh_meta(m)
        model_meta = m.details.meta if m.details and m.details.meta else {}
        model_lineage = column_lineage.get(unique_id)
        if model_lineage:
            sqlmesh_meta["column_lineage"] = model_lineage
        model_project = _get_project(internal_models, m)
        if model_project:
            sqlmesh_meta["project"] = model_project

        # Common fields shared by both nodes and sources
        common = {
            "unique_id": unique_id,
            "resource_type": resource_type,
            "name": name,
            "database": catalog,
            "schema": schema,
            "package_name": package_name,
            "fqn": fqn_list,
            "original_file_path": path,
            "path": path,
            "description": m.description or "",
            "columns": columns,
            "tags": tags,
            "meta": {**model_meta, "_sqlmesh": sqlmesh_meta},
        }

        if resource_type == "source":
            sources[unique_id] = {
                **common,
                "source_name": schema,
                "source_description": m.description or "",
                "loader": "sqlmesh",
                "identifier": name,
                "loaded_at_field": None,
                "freshness": None,
            }
        else:
            kind = m.details.kind if m.details else None
            materialized = kind.lower() if kind else "unknown"
            depends_on: t.Dict[str, t.Any] = {"macros": []}
            if resource_type != "seed":
                depends_on["nodes"] = depends_on_nodes
            nodes[unique_id] = {
                **common,
                "alias": name,
                "checksum": {"name": "sqlmesh", "checksum": str(m.hash)},
                "depends_on": depends_on,
                "config": {
                    "materialized": materialized,
                    "schema": schema,
                    "database": catalog,
                    "tags": tags,
                    "meta": dict(model_meta),
                    "enabled": True,
                },
                "docs": {"show": True, "node_color": None},
                "raw_code": _get_raw_code(internal_models, m),
            }
            if resource_type != "seed":
                nodes[unique_id]["compiled"] = False
                nodes[unique_id]["compiled_code"] = None

    return nodes, sources


def _build_parent_child_maps(
    models: t.List[Model],
    fqn_to_uid: t.Dict[str, str],
    graph: t.Dict[str, t.Set[str]],
) -> t.Tuple[t.Dict[str, t.List[str]], t.Dict[str, t.List[str]]]:
    """Build ``parent_map`` and ``child_map`` for the manifest."""
    parent_map: t.Dict[str, t.List[str]] = {uid: [] for uid in fqn_to_uid.values()}
    child_map: t.Dict[str, t.List[str]] = {uid: [] for uid in fqn_to_uid.values()}

    for m in models:
        uid = fqn_to_uid[m.fqn]
        for uf in sorted(graph.get(m.fqn, set())):
            parent_uid = fqn_to_uid.get(uf)
            if parent_uid:
                parent_map[uid].append(parent_uid)
                child_map[parent_uid].append(uid)

    return parent_map, child_map


def _build_column_lineage(
    models: t.List[Model],
    fqn_to_uid: t.Dict[str, str],
    internal_models: t.Mapping[str, InternalModel],
    default_catalog: t.Optional[str],
) -> t.Dict[str, t.Dict[str, t.List[t.Dict[str, str]]]]:
    """Build column-level lineage for every model.

    Returns:

        {
            "unique_id": {
                "column_name": [
                    {"model": "upstream_unique_id", "column": "src_col"},
                    ...
                ]
            }
        }
    """
    result: t.Dict[str, t.Dict[str, t.List[t.Dict[str, str]]]] = {}

    for m in models:
        internal = internal_models.get(m.fqn)
        if not internal:
            continue

        uid = fqn_to_uid[m.fqn]
        col_map: t.Dict[str, t.List[t.Dict[str, str]]] = {}

        for col in m.columns:
            try:
                deps = _column_dependencies_from_model(internal, col.name, default_catalog)
                sources: t.List[t.Dict[str, str]] = [
                    {"model": fqn_to_uid[up_fqn], "column": uc}
                    for up_fqn, up_cols in deps.items()
                    if up_fqn in fqn_to_uid
                    for uc in sorted(up_cols)
                ]
                if sources:
                    col_map[col.name] = sources
            except Exception:
                logger.debug("Could not compute lineage for %s.%s", m.fqn, col.name, exc_info=True)

        if col_map:
            result[uid] = col_map

    return result


def _get_project(internal_models: t.Mapping[str, t.Any], model: Model) -> str:
    """Return the project name for a model, or empty string."""
    try:
        sqlmesh_model = internal_models.get(model.fqn)
        if sqlmesh_model is not None:
            return getattr(sqlmesh_model, "project", "") or ""
    except Exception:
        logger.debug("Could not extract project for %s", model.fqn, exc_info=True)
    return ""


def _get_raw_code(internal_models: t.Mapping[str, t.Any], model: Model) -> str:
    """Extract the source code for a model.

    For SQL models, returns the pretty-printed query text.
    For Python models, reads the source file from disk.
    Returns an empty string for seed, external, or unresolvable models.
    """
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


def adapter_type_name(engine_adapter: t.Any) -> str:
    """Derive the adapter type name from an engine adapter instance."""
    return type(engine_adapter).__name__.replace("EngineAdapter", "").lower() or "unknown"


def _build_fqn_to_uid(models: t.List[Model]) -> t.Dict[str, str]:
    """Build a shared fqn -> unique_id lookup for all models.

    Args:
        models: Serialised API model list.

    Returns:
        Mapping of model FQN to dbt ``unique_id``.

    Raises:
        ValueError: If two FQNs resolve to the same unique id.
    """
    mapping: t.Dict[str, str] = {}
    seen_uids: t.Dict[str, str] = {}
    for m in models:
        uid = _unique_id(m.fqn, _MODEL_TYPE_TO_RESOURCE_TYPE.get(m.type.value, "model"))
        existing_fqn = seen_uids.get(uid)
        if existing_fqn and existing_fqn != m.fqn:
            raise ValueError(f"unique_id collision for '{uid}': '{existing_fqn}' and '{m.fqn}'")
        seen_uids[uid] = m.fqn
        mapping[m.fqn] = uid
    return mapping


def _parse_fqn_parts(fqn: str) -> t.Tuple[t.Optional[str], str, str]:
    """Parse ``catalog.schema.name`` from a dotted FQN string.

    Returns ``(catalog_or_None, schema, name)`` with quotes stripped.
    Invalid / empty values fall back to ``(None, "default", "unknown")``.
    """
    parts = [p.strip('"') for p in fqn.split(".") if p.strip('"')]
    if not parts:
        return None, "default", "unknown"
    if len(parts) >= 3:
        return parts[0], parts[1], ".".join(parts[2:])
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, "default", parts[0]


def _unique_id(fqn: str, resource_type: str) -> str:
    """Build a dbt-style ``unique_id`` from a dotted FQN.

    Format: ``{resource_type}.{schema}.{name}`` with quotes stripped.

    Note: catalog is intentionally omitted to match dbt's unique_id format,
    which uses ``{resource_type}.{project}.{name}`` without a catalog prefix.
    """
    _catalog, schema, name = _parse_fqn_parts(fqn)
    return f"{resource_type}.{schema}.{name}"


def _project_id(project_name: str) -> str:
    """Build a stable dbt-style project id."""
    return md5(project_name.encode("utf-8")).hexdigest()


def _parse_tags(model: Model) -> t.List[str]:
    """Extract tags as a list from the model details."""
    if model.details and model.details.tags:
        return [tag.strip() for tag in model.details.tags.split(",") if tag.strip()]
    return []


# Detail attributes: (name, truthy_only).
# truthy_only=True means the attribute is only included when its value is truthy.
# truthy_only=False means the attribute is included when its value is not None.
_SQLMESH_META_DETAIL_ATTRS: t.Tuple[t.Tuple[str, bool], ...] = (
    ("kind", True),
    ("cron", True),
    ("owner", True),
    ("time_column", True),
    ("table_format", True),
    ("storage_format", True),
    ("partitioned_by", True),
    ("clustered_by", True),
    ("batch_size", False),
    ("retention", False),
    ("lookback", False),
    ("annotated", False),
)


def _build_node_sqlmesh_meta(model: Model) -> t.Dict[str, t.Any]:
    """Build the SQLMesh extension dict stored in ``meta._sqlmesh``."""
    meta: t.Dict[str, t.Any] = {
        "fqn": model.fqn,
        "dialect": model.dialect,
        "type": model.type.value,
        "hash": model.hash,
    }
    if model.details:
        details = model.details
        for attr, truthy_only in _SQLMESH_META_DETAIL_ATTRS:
            val = getattr(details, attr, None)
            if (truthy_only and val) or (not truthy_only and val is not None):
                meta[attr] = val
        if details.start:
            meta["start"] = str(details.start)
        if details.interval_unit:
            meta["interval_unit"] = str(details.interval_unit.value)
    if model.default_catalog:
        meta["default_catalog"] = model.default_catalog
    return meta
