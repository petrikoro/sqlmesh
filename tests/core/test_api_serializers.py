import typing as t

from sqlglot import exp

from sqlmesh.core.table_diff import RowDiff as CoreRowDiff
from sqlmesh.core.table_diff import SchemaDiff as CoreSchemaDiff


def test_serialize_table_diff_converts_core_models_to_api_models() -> None:
    import pandas as pd

    from sqlmesh.api.serializers import serialize_table_diff

    core_schema_diff = CoreSchemaDiff(
        source="dev_table",
        target="prod_table",
        source_schema={"id": exp.DataType.build("INT")},
        target_schema={
            "id": exp.DataType.build("INT"),
            "name": exp.DataType.build("TEXT"),
        },
    )
    core_row_diff = CoreRowDiff(
        source="dev_table",
        target="prod_table",
        stats={
            "s_count": 1.0,
            "t_count": 1.0,
            "join_count": 0.0,
            "full_match_count": 0.0,
            "s_only_count": 1.0,
            "t_only_count": 1.0,
        },
        sample=pd.DataFrame({"id": [1], "name": [pd.NA]}),
        joined_sample=pd.DataFrame(),
        s_sample=pd.DataFrame({"id": [1], "name": [pd.NA]}),
        t_sample=pd.DataFrame({"id": [2], "name": [pd.NA]}),
        column_stats=pd.DataFrame({"column": ["id"], "pct": [0.0]}),
    )

    class _FakeTableDiff:
        def schema_diff(self) -> CoreSchemaDiff:
            return core_schema_diff

        def row_diff(self, temp_schema: t.Optional[str] = None) -> CoreRowDiff:
            return core_row_diff

        @property
        def key_columns(self):
            return [exp.to_column("id")], [exp.to_column("id")], []

    api_table_diff = serialize_table_diff(
        diff=_FakeTableDiff(),
        source_name="dev",
        target_name="prod",
    )

    assert api_table_diff.schema_diff.added == {"name": "TEXT"}
    assert api_table_diff.on == [["id", "id"]]
    assert api_table_diff.row_diff.sample["name"][0] is None
    assert api_table_diff.row_diff.processed_sample_data is not None
    assert api_table_diff.row_diff.processed_sample_data.source_only[0]["name"] is None


def test_serialize_lineage_column_builders() -> None:
    from sqlmesh.api.serializers import serialize_external_lineage_column, serialize_lineage_column

    external = serialize_external_lineage_column("raw.orders", "id")
    assert external.expression == "FROM raw.orders"
    assert external.source == "SELECT id FROM raw.orders"
    assert external.models == {}

    lineage = serialize_lineage_column(
        models={"raw.orders": ["id", "customer_id"]},
        expression="orders.id",
        source="SELECT id FROM raw.orders",
    )
    assert lineage.expression == "orders.id"
    assert lineage.source == "SELECT id FROM raw.orders"
    assert lineage.models == {"raw.orders": {"id", "customer_id"}}
