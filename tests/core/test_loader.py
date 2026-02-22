import os
import pytest
from pathlib import Path
from sqlmesh.cli.project_init import init_example_project
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.core.context import Context
from sqlmesh.core.loader import SqlMeshLoader
from sqlmesh.utils.errors import ConfigError


def _duckdb_context(tmp_path: Path) -> Context:
    return Context(
        paths=tmp_path, config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    )


@pytest.fixture
def sample_models(request):
    models = {
        "sql": {
            "contents": """
MODEL (
    name test_schema.test_model,
    kind FULL,
);

SELECT 1;
""",
            "path": "models/sql_model.sql",
        },
        "python": {
            "contents": """import typing as t
import pandas as pd  # noqa: TID253
from sqlmesh import ExecutionContext, model

@model(
    "test_schema.test_model",
    kind="FULL",
    columns={
        "id": "int",
    }
)
def execute(
    context: ExecutionContext,
    **kwargs: t.Any,
) -> pd.DataFrame:
    return pd.DataFrame([
        {"id": 1}
    ])
""",
            "path": "models/python_model.py",
        },
        "external": {
            "contents": """
- name: test_schema.test_model
  columns:
    id: INT
""",
            "path": "external_models/external_model.yaml",
        },
    }
    requested_models = request.param.split("_")
    return [v for k, v in models.items() if k in requested_models]


@pytest.mark.parametrize(
    "sample_models",
    ["sql_python", "python_external", "sql_external", "sql_python_external"],
    indirect=True,
)
def test_duplicate_model_names_different_kind(tmp_path: Path, sample_models):
    """Test different (SQL, Python and external) models with duplicate model names raises ValueError."""
    model_1, *models = sample_models
    if len(models) == 2:
        model_2, model_3 = models
    else:
        model_2, model_3 = models[0], None

    init_example_project(tmp_path, engine_type="duckdb")
    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))

    path_1: Path = tmp_path / model_1["path"]
    path_2: Path = tmp_path / model_2["path"]

    path_1.parent.mkdir(parents=True, exist_ok=True)
    path_1.write_text(model_1["contents"])
    path_2.parent.mkdir(parents=True, exist_ok=True)
    path_2.write_text(model_2["contents"])

    if model_3:
        path_3: Path = tmp_path / model_3["path"]
        path_3.parent.mkdir(parents=True, exist_ok=True)
        path_3.write_text(model_3["contents"])

    with pytest.raises(
        ConfigError, match=r'Duplicate model name\(s\) found: "memory"."test_schema"."test_model".'
    ):
        Context(paths=tmp_path, config=config)


@pytest.mark.parametrize("sample_models", ["sql", "external"], indirect=True)
def test_duplicate_model_names_same_kind(tmp_path: Path, sample_models):
    """Test same (SQL and external) models with duplicate model names raises ConfigError."""

    def duplicate_model_path(fpath):
        return Path(fpath).parent / ("duplicate" + Path(fpath).suffix)

    model = sample_models[0]
    init_example_project(tmp_path, engine_type="duckdb")
    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))

    path_1: Path = tmp_path / model["path"]
    path_1.parent.mkdir(parents=True, exist_ok=True)
    path_1.write_text(model["contents"])

    duplicate_fpath = tmp_path / duplicate_model_path(model["path"])
    duplicate_fpath.write_text(model["contents"])

    with pytest.raises(
        ConfigError,
        match=r".*Duplicate .* model name: 'test_schema.test_model'",
    ):
        Context(paths=tmp_path, config=config)


@pytest.mark.registry_isolation
def test_duplicate_python_model_names_raise_error(tmp_path: Path) -> None:
    """Test python models with duplicate model names raises ConfigError if the functions are not identical."""
    init_example_project(tmp_path, engine_type="duckdb")
    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    model_name = "test_schema.test_model"

    path_a = tmp_path / "models/test_schema/test_model_a.py"
    path_b = tmp_path / "models/test_schema/test_model_b.py"

    model_payload_a = f"""from sqlmesh import model
@model(
    name="{model_name}",
    columns={{'"COL"': "int"}},
)
def my_model(context, **kwargs):
    pass"""

    model_payload_b = f"""import typing as t
import pandas as pd  # noqa: TID253
from sqlmesh import ExecutionContext, model

@model(
    name="{model_name}",
    kind="FULL",
    columns={{
        "id": "int",
    }}
)
def execute(
    context: ExecutionContext,
    **kwargs: t.Any,
) -> pd.DataFrame:
    return pd.DataFrame([
        {{"id": 1}}
    ])
"""

    path_a.parent.mkdir(parents=True, exist_ok=True)
    path_a.write_text(model_payload_a)
    path_b.write_text(model_payload_b)

    with pytest.raises(
        ConfigError,
        match=r"Failed to load model from file '.*'.\n\n  Duplicate name: 'test_schema.test_model'.",
    ):
        Context(paths=tmp_path, config=config)


@pytest.mark.slow
def test_duplicate_python_model_names_no_error(tmp_path: Path) -> None:
    """Test python models with duplicate model names raises no error if the functions are identical."""
    init_example_project(tmp_path, engine_type="duckdb")
    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    model_name = "test_schema.test_model"

    path_a = tmp_path / "models/test_schema1/test_model_a.py"
    path_b = tmp_path / "models/test_schema2/test_model_b.py"

    model_payload_a = f"""from sqlmesh import model
@model(
    name="{model_name}",
    columns={{'"COL"': "int"}},
    description="model_payload_a",
)
def my_model(context, **kwargs):
    pass"""

    model_payload_b = f"""from sqlmesh import model
@model(
    name="{model_name}",
    columns={{'"COL"': "int"}},
    description="model_payload_b",
)
def my_model(context, **kwargs):
    pass"""

    path_a.parent.mkdir(parents=True, exist_ok=True)
    path_b.parent.mkdir(parents=True, exist_ok=True)
    path_a.write_text(model_payload_a)
    context = Context(paths=tmp_path, config=config)
    context.load()
    model = context.get_model(f"{model_name}")
    assert model.description == "model_payload_a"
    path_b.write_text(model_payload_b)
    context.load()  # raise no error to duplicate key if the functions are identical (by registry class_method)


def test_model_docs_from_schema_yaml_populates_description_and_columns(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "orders.sql").write_text(
        """
MODEL (
    name test_schema.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    (models_dir / "schema.yml").write_text(
        """
version: 2
models:
  - name: test_schema.orders
    description: Orders from schema.yml
    columns:
      - name: id
        description: Order id from schema.yml
""",
        encoding="utf-8",
    )

    context = _duckdb_context(tmp_path)
    model = context.get_model("test_schema.orders")

    assert model
    assert model.description == "Orders from schema.yml"
    assert model.column_descriptions["id"] == "Order id from schema.yml"


def test_model_docs_from_arbitrary_yaml_filename_match_fully_qualified_name(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "finance_orders.sql").write_text(
        """
MODEL (
    name finance.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    (models_dir / "sales_orders.sql").write_text(
        """
MODEL (
    name sales.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    (models_dir / "custom_docs_file.yml").write_text(
        """
version: 2
models:
  - name: finance.orders
    description: Finance orders docs
""",
        encoding="utf-8",
    )

    context = _duckdb_context(tmp_path)
    finance_model = context.get_model("finance.orders")
    sales_model = context.get_model("sales.orders")

    assert finance_model
    assert sales_model
    assert finance_model.description == "Finance orders docs"
    assert sales_model.description is None


def test_model_docs_yaml_overrides_sql_description_and_merges_columns(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "orders.sql").write_text(
        """
-- SQL description
MODEL (
    name test_schema.orders,
    kind FULL,
    column_descriptions (
        id = 'ID from SQL',
        amount = 'Amount from SQL'
    ),
);

SELECT 1 AS id, 10 AS amount;
""",
        encoding="utf-8",
    )
    (models_dir / "schema.yaml").write_text(
        """
version: 2
models:
  - name: test_schema.orders
    description: Description from YAML
    columns:
      - name: id
        description: ID from YAML
""",
        encoding="utf-8",
    )

    context = _duckdb_context(tmp_path)
    model = context.get_model("test_schema.orders")

    assert model
    assert model.description == "Description from YAML"
    assert model.column_descriptions["id"] == "ID from YAML"
    assert model.column_descriptions["amount"] == "Amount from SQL"


def test_model_docs_yaml_applies_only_top_level_tags(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "orders.sql").write_text(
        """
MODEL (
    name test_schema.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    (models_dir / "model_meta.yaml").write_text(
        """
version: 2
models:
  - name: test_schema.orders
    tags: finance
    config:
      tags:
        - curated
        - pii
""",
        encoding="utf-8",
    )

    context = _duckdb_context(tmp_path)
    model = context.get_model("test_schema.orders")

    assert model
    assert model.tags == ["finance"]


def test_model_docs_from_schema_yaml_populates_meta_and_column_metadata(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "orders.sql").write_text(
        """
MODEL (
    name test_schema.orders,
    kind FULL,
);

SELECT 1 AS id, 10 AS amount;
""",
        encoding="utf-8",
    )
    (models_dir / "schema.yml").write_text(
        """
version: 2
models:
  - name: test_schema.orders
    meta:
      owner_team: finance
      pii: false
    columns:
      - name: id
        tags:
          - primary_key
          - pii
        meta:
          classification: sensitive
      - name: amount
        tags: metric
        meta:
          unit: usd
""",
        encoding="utf-8",
    )

    context = _duckdb_context(tmp_path)
    model = context.get_model("test_schema.orders")

    assert model
    assert model.meta == {"owner_team": "finance", "pii": False}
    assert model.column_tags == {"id": ["primary_key", "pii"], "amount": ["metric"]}
    assert model.column_meta == {
        "id": {"classification": "sensitive"},
        "amount": {"unit": "usd"},
    }


def test_model_docs_yaml_supports_dict_columns_with_tags_and_meta(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "orders.sql").write_text(
        """
MODEL (
    name test_schema.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    (models_dir / "schema.yml").write_text(
        """
version: 2
models:
  - name: test_schema.orders
    columns:
      id:
        description: Identifier
        tags:
          - id_col
        meta:
          quality: gold
""",
        encoding="utf-8",
    )

    context = _duckdb_context(tmp_path)
    model = context.get_model("test_schema.orders")

    assert model
    assert model.column_descriptions["id"] == "Identifier"
    assert model.column_tags == {"id": ["id_col"]}
    assert model.column_meta == {"id": {"quality": "gold"}}


def test_model_docs_meta_keys_are_stringified_for_stable_metadata_hash(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "orders.sql").write_text(
        """
MODEL (
    name test_schema.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    (models_dir / "schema.yml").write_text(
        """
version: 2
models:
  - name: test_schema.orders
    meta:
      1: one
      nested:
        2: two
    columns:
      - name: id
        meta:
          3: three
          nested:
            4: four
""",
        encoding="utf-8",
    )

    context = _duckdb_context(tmp_path)
    model = context.get_model("test_schema.orders")

    assert model
    assert model.meta == {"1": "one", "nested": {"2": "two"}}
    assert model.column_meta == {"id": {"3": "three", "nested": {"4": "four"}}}
    assert isinstance(model.metadata_hash, str)


@pytest.mark.registry_isolation
def test_model_docs_yaml_overrides_python_description_and_columns(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "py_orders.py").write_text(
        """import typing as t
import pandas as pd  # noqa: TID253
from sqlmesh import ExecutionContext, model

@model(
    "test_schema.py_orders",
    kind="FULL",
    columns={"id": "int"},
    description="Description from Python",
    column_descriptions={"id": "ID from Python"},
)
def execute(
    context: ExecutionContext,
    **kwargs: t.Any,
) -> pd.DataFrame:
    return pd.DataFrame([{"id": 1}])
""",
        encoding="utf-8",
    )
    (models_dir / "schema.yml").write_text(
        """
version: 2
models:
  - name: test_schema.py_orders
    description: Description from YAML
    columns:
      - name: id
        description: ID from YAML
""",
        encoding="utf-8",
    )

    context = _duckdb_context(tmp_path)
    model = context.get_model("test_schema.py_orders")

    assert model
    assert model.description == "Description from YAML"
    assert model.column_descriptions["id"] == "ID from YAML"


@pytest.mark.registry_isolation
def test_model_docs_yaml_overrides_python_meta_and_column_metadata(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "py_orders.py").write_text(
        """import typing as t
import pandas as pd  # noqa: TID253
from sqlmesh import ExecutionContext, model

@model(
    "test_schema.py_orders",
    kind="FULL",
    columns={"id": "int"},
    meta={"owner_team": "legacy", "pii": True},
    column_tags={"id": ["legacy_tag"]},
    column_meta={"id": {"classification": "restricted"}},
)
def execute(
    context: ExecutionContext,
    **kwargs: t.Any,
) -> pd.DataFrame:
    return pd.DataFrame([{"id": 1}])
""",
        encoding="utf-8",
    )
    (models_dir / "schema.yml").write_text(
        """
version: 2
models:
  - name: test_schema.py_orders
    meta:
      owner_team: finance
      pii: false
    columns:
      - name: id
        tags:
          - curated
        meta:
          classification: internal
""",
        encoding="utf-8",
    )

    context = _duckdb_context(tmp_path)
    model = context.get_model("test_schema.py_orders")

    assert model
    assert model.meta == {"owner_team": "finance", "pii": False}
    assert model.column_tags == {"id": ["curated"]}
    assert model.column_meta == {"id": {"classification": "internal"}}


def test_model_docs_name_must_be_fully_qualified(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "orders.sql").write_text(
        """
MODEL (
    name test_schema.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    (models_dir / "schema.yml").write_text(
        """
version: 2
models:
  - name: orders
    description: short name is not allowed
""",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="fully qualified"):
        _duckdb_context(tmp_path)


def test_model_docs_ignore_unsupported_yaml_sections(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "orders.sql").write_text(
        """
MODEL (
    name test_schema.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    (models_dir / "schema.yml").write_text(
        """
version: 2
sources:
  - name: raw
    tables:
      - name: orders
        description: ignored source description
""",
        encoding="utf-8",
    )

    context = _duckdb_context(tmp_path)
    model = context.get_model("test_schema.orders")

    assert model
    assert model.description is None


def test_model_docs_multiple_entries_for_same_model_raise_error(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "orders.sql").write_text(
        """
MODEL (
    name test_schema.orders,
    kind FULL,
    column_descriptions (
        amount = 'Amount from SQL'
    ),
);

SELECT 1 AS id, 10 AS amount, 2 AS tax;
""",
        encoding="utf-8",
    )
    (models_dir / "schema.yaml").write_text(
        """
version: 2
models:
  - name: test_schema.orders
    description: First description
    columns:
      - name: id
        description: ID from first patch
      - name: amount
        description: Amount from first patch
    tags:
      - first
  - name: test_schema.orders
    description: Second description
    columns:
      - name: id
        description: ID from second patch
      - name: tax
        description: Tax from second patch
    tags:
      - second
""",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="Duplicate model docs entry name"):
        _duckdb_context(tmp_path)


def test_model_docs_multiple_yaml_files_for_same_model_raise_error(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "orders.sql").write_text(
        """
MODEL (
    name test_schema.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    (models_dir / "a_docs.yaml").write_text(
        """
version: 2
models:
  - name: test_schema.orders
    description: Description from a_docs
""",
        encoding="utf-8",
    )
    (models_dir / "b_docs.yaml").write_text(
        """
version: 2
models:
  - name: test_schema.orders
    description: Description from b_docs
""",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="Duplicate model docs entry name"):
        _duckdb_context(tmp_path)


def test_model_docs_yaml_patch_cache_hit_and_miss_on_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "orders.sql").write_text(
        """
MODEL (
    name test_schema.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    docs_path = models_dir / "schema.yaml"
    docs_path.write_text(
        """
version: 2
models:
  - name: test_schema.orders
    description: Initial description
""",
        encoding="utf-8",
    )

    parse_calls = 0
    original_loader = SqlMeshLoader._load_model_docs_patches_for_file

    def _counted_loader(self: SqlMeshLoader, path: Path) -> list:
        nonlocal parse_calls
        parse_calls += 1
        return original_loader(self, path)

    monkeypatch.setattr(SqlMeshLoader, "_load_model_docs_patches_for_file", _counted_loader)

    context = _duckdb_context(tmp_path)
    assert parse_calls == 1

    context.load()
    assert parse_calls == 1

    docs_path.write_text(
        """
version: 2
models:
  - name: test_schema.orders
    description: Updated description
""",
        encoding="utf-8",
    )
    current_mtime = docs_path.stat().st_mtime
    os.utime(docs_path, (current_mtime + 1, current_mtime + 1))

    context.load()
    assert parse_calls == 2
    model = context.get_model("test_schema.orders")
    assert model
    assert model.description == "Updated description"


def test_model_docs_malformed_yaml_is_not_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "orders.sql").write_text(
        """
MODEL (
    name test_schema.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    (models_dir / "malformed_docs.yaml").write_text(
        """
version: 2
models:
  - name: orders
    description: malformed yaml
    columns: [
""",
        encoding="utf-8",
    )

    parse_calls = 0
    original_loader = SqlMeshLoader._load_model_docs_patches_for_file

    def _counted_loader(self: SqlMeshLoader, path: Path) -> list:
        nonlocal parse_calls
        parse_calls += 1
        return original_loader(self, path)

    monkeypatch.setattr(SqlMeshLoader, "_load_model_docs_patches_for_file", _counted_loader)

    context = _duckdb_context(tmp_path)
    assert parse_calls == 1

    context.load()
    assert parse_calls == 2


def test_model_docs_patch_cache_key_uses_file_and_config_only(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    docs_path = models_dir / "schema.yaml"
    (models_dir / "orders.sql").write_text(
        """
MODEL (
    name test_schema.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    docs_path.write_text(
        """
version: 2
models:
  - name: test_schema.orders
    description: Description from YAML
""",
        encoding="utf-8",
    )

    context = _duckdb_context(tmp_path)
    loader = context._loaders[0]
    assert isinstance(loader, SqlMeshLoader)
    loader._track_file(docs_path)

    cache = SqlMeshLoader._Cache(loader, loader.config_path)
    expected_cache_key = "__".join(
        [
            str(loader._path_mtimes[docs_path]),
            loader.config.fingerprint,
        ]
    )
    assert cache._model_docs_patch_cache_entry_id(docs_path) == expected_cache_key


def test_model_docs_malformed_yaml_is_ignored(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "orders.sql").write_text(
        """
MODEL (
    name test_schema.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    (models_dir / "malformed_docs.yaml").write_text(
        """
version: 2
models:
  - name: orders
    description: malformed yaml
    columns: [
""",
        encoding="utf-8",
    )

    context = _duckdb_context(tmp_path)
    model = context.get_model("test_schema.orders")

    assert model
    assert model.description is None
