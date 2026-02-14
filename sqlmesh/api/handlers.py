from __future__ import annotations

import typing as t
from collections import defaultdict, deque

from sqlglot import exp

from sqlmesh.api.models import LineageColumn, Model, TableDiff
from sqlmesh.api.serializers import (
    serialize_all_models,
    serialize_external_lineage_column,
    serialize_lineage_column,
    serialize_table_diff,
)
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.lineage import column_dependencies, lineage

if t.TYPE_CHECKING:
    from sqlglot.lineage import Node


def get_models(context: Context) -> t.List[Model]:
    context.refresh()
    return serialize_all_models(context)


def quote_column(column: str, dialect: str) -> str:
    return exp.to_identifier(column, quoted=True).sql(dialect=dialect)


def get_source_name(
    node: Node, default_catalog: t.Optional[str], dialect: str, model_name: str
) -> str:
    table = node.expression.find(exp.Table)
    if table:
        return normalize_model_name(table, default_catalog=default_catalog, dialect=dialect)
    if node.reference_node_name:
        # CTE name or derived table alias.
        return f"{model_name}: {node.reference_node_name}"
    return ""


def get_column_name(node: Node) -> str:
    if isinstance(node.expression, exp.Alias):
        return node.expression.alias_or_name
    return exp.to_column(node.name).name


def create_lineage_adjacency_list(
    model_name: str, column_name: str, context: Context
) -> t.Dict[str, t.Dict[str, LineageColumn]]:
    """Create an adjacency list representation of a column's lineage graph including CTEs."""
    graph: t.Dict[str, t.Dict[str, LineageColumn]] = defaultdict(dict)
    visited = {(model_name, column_name)}
    nodes = deque([(model_name, column_name)])
    while nodes:
        model_name, column = nodes.popleft()
        model = context.get_model(model_name)
        if not model:
            # External model.
            graph[model_name][column] = serialize_external_lineage_column(model_name, column)
            continue

        root = lineage(quote_column(column, model.dialect), model)

        for node in root.walk():
            if root.name == "UNION" and node is root:
                continue
            node_name = (
                get_source_name(
                    node,
                    default_catalog=context.default_catalog,
                    dialect=model.dialect,
                    model_name=model_name,
                )
                or model_name
            )
            node_column = get_column_name(node)
            if node_column in graph[node_name]:
                dependencies = defaultdict(set, graph[node_name][node_column].models)
            else:
                dependencies = defaultdict(set)
            for downstream in node.downstream:
                table = get_source_name(
                    downstream,
                    default_catalog=context.default_catalog,
                    dialect=model.dialect,
                    model_name=model_name,
                )
                if table:
                    downstream_column_name = get_column_name(downstream)
                    dependencies[table].add(downstream_column_name)
                    if (
                        isinstance(downstream.expression, exp.Table)
                        and (table, downstream_column_name) not in visited
                    ):
                        nodes.append((table, downstream_column_name))
                        visited.add((table, downstream_column_name))

            graph[node_name][node_column] = serialize_lineage_column(
                models=dependencies,
                expression=node.expression.sql(pretty=True, dialect=model.dialect),
                source=node.source.sql(pretty=True, dialect=model.dialect),
            )
    return graph


def create_models_only_lineage_adjacency_list(
    model_name: str, column_name: str, context: Context
) -> t.Dict[str, t.Dict[str, LineageColumn]]:
    """Create an adjacency list representation of a column's lineage graph only with models."""
    graph: t.Dict[str, t.Dict[str, LineageColumn]] = defaultdict(dict)
    nodes = deque([(model_name, column_name)])
    # `visited` prevents loops on cyclical references.
    visited = {(model_name, column_name)}
    while nodes:
        model_name, column = nodes.popleft()
        model = context.get_model(model_name)
        dependencies = defaultdict(set)
        if model:
            for table, column_names in column_dependencies(
                context, model_name, quote_column(column, model.dialect)
            ).items():
                for source_column_name in column_names:
                    if (table, source_column_name) not in visited:
                        dependencies[table].add(source_column_name)
                        nodes.append((table, source_column_name))
                        visited.add((table, source_column_name))

        graph[model_name][column] = serialize_lineage_column(models=dependencies)
    return graph


def column_lineage(
    model_name: str,
    column_name: str,
    models_only: bool = False,
    context: t.Optional[Context] = None,
) -> t.Dict[str, t.Dict[str, LineageColumn]]:
    if context is None:
        raise ValueError("A SQLMesh context is required.")

    model = context.get_model(model_name)
    if not model:
        raise ValueError("Unable to get column lineage")

    model_name = model.fqn
    if models_only:
        return create_models_only_lineage_adjacency_list(model_name, column_name, context)
    return create_lineage_adjacency_list(model_name, column_name, context)


def model_lineage(
    model_name: str,
    context: t.Optional[Context] = None,
) -> t.Dict[str, t.Set[str]]:
    if context is None:
        raise ValueError("A SQLMesh context is required.")

    model = context.get_model(model_name)
    if not model:
        raise ValueError("Unable to get model lineage")
    return context.dag.lineage(model.fqn).graph


def get_table_diff(
    context: Context,
    source: str,
    target: str,
    on: t.Optional[str] = None,
    model_or_snapshot: t.Optional[str] = None,
    where: t.Optional[str] = None,
    temp_schema: t.Optional[str] = None,
    limit: int = 20,
) -> t.Optional[TableDiff]:
    table_diffs = context.table_diff(
        source=source,
        target=target,
        on=exp.condition(on) if on else None,
        select_models={model_or_snapshot} if model_or_snapshot else None,
        where=where,
        limit=limit,
        show=False,
    )

    if not table_diffs:
        return None

    diff = table_diffs[0] if isinstance(table_diffs, list) else table_diffs
    return serialize_table_diff(
        diff=diff,
        source_name=source,
        target_name=target,
        temp_schema=temp_schema,
    )
