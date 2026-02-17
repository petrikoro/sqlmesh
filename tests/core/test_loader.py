import pytest
from pathlib import Path
from sqlmesh.cli.project_init import init_example_project
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.core.context import Context
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
  - name: orders
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
  - name: orders
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


def test_model_docs_yaml_applies_tags_from_tags_and_config_tags(tmp_path: Path) -> None:
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
  - name: orders
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
    assert model.tags == ["finance", "curated", "pii"]


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
  - name: py_orders
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


def test_model_docs_short_name_match_raises_when_ambiguous(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "first_orders.sql").write_text(
        """
MODEL (
    name first.orders,
    kind FULL,
);

SELECT 1 AS id;
""",
        encoding="utf-8",
    )
    (models_dir / "second_orders.sql").write_text(
        """
MODEL (
    name second.orders,
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
    description: ambiguous short name
""",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="ambiguous"):
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
