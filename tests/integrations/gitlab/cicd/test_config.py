import pathlib

import pytest

from sqlmesh.core.config import Config, load_config_from_paths
from sqlmesh.integrations.gitlab.cicd.config import GitLabCICDBotConfig
from tests.utils.test_filesystem import create_temp_file

pytestmark = pytest.mark.gitlab


def test_load_yaml_config_default(tmp_path):
    create_temp_file(
        tmp_path,
        pathlib.Path("config.yaml"),
        """
cicd_bot:
    type: gitlab
model_defaults:
    dialect: duckdb
""",
    )

    config = load_config_from_paths(Config, project_paths=[tmp_path / "config.yaml"])

    assert config.cicd_bot.type_ == "gitlab"
    assert config.cicd_bot.auto_categorize_changes == config.plan.auto_categorize_changes
    assert config.cicd_bot.default_pr_start is None
    assert config.cicd_bot.skip_pr_backfill
    assert not config.cicd_bot.pr_include_unmodified
    assert config.cicd_bot.pr_environment_name is None
    assert not config.cicd_bot.pr_min_intervals
    assert config.cicd_bot.forward_only_branch_suffix == "-forward-only"


def test_properties_inherit_from_project_config(tmp_path):
    create_temp_file(
        tmp_path,
        pathlib.Path("config.yaml"),
        """
cicd_bot:
    type: gitlab
plan:
    auto_categorize_changes:
      sql: full
      python: full
      seed: full
      external: full
    include_unmodified: true
model_defaults:
    dialect: duckdb
""",
    )

    config = load_config_from_paths(Config, project_paths=[tmp_path / "config.yaml"])

    assert config.cicd_bot.auto_categorize_changes == config.plan.auto_categorize_changes
    assert config.cicd_bot.pr_include_unmodified == config.plan.include_unmodified


def test_yaml_overlay_preserves_python_gitlab_provider(tmp_path):
    project_path = tmp_path / "project"
    personal_path = tmp_path / "personal"

    create_temp_file(
        project_path,
        pathlib.Path("config.py"),
        """
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.integrations.gitlab.cicd.config import GitLabCICDBotConfig

config = Config(
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    cicd_bot=GitLabCICDBotConfig(server_url="https://gitlab.internal.example"),
)
""",
    )
    create_temp_file(
        personal_path,
        pathlib.Path("config.yaml"),
        """
cicd_bot:
    pr_environment_name: shared_env
""",
    )

    config = load_config_from_paths(
        Config,
        project_paths=[project_path / "config.py"],
        personal_paths=[personal_path / "config.yaml"],
    )

    assert isinstance(config.cicd_bot, GitLabCICDBotConfig)
    assert config.cicd_bot.server_url == "https://gitlab.internal.example"
    assert config.cicd_bot.pr_environment_name == "shared_env"


def test_explicit_yaml_overlay_can_switch_cicd_provider(tmp_path):
    project_path = tmp_path / "project"
    personal_path = tmp_path / "personal"

    create_temp_file(
        project_path,
        pathlib.Path("config.py"),
        """
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig

config = Config(
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    cicd_bot=GithubCICDBotConfig(pr_environment_name="github_env"),
)
""",
    )
    create_temp_file(
        personal_path,
        pathlib.Path("config.yaml"),
        """
cicd_bot:
    type: gitlab
    server_url: https://gitlab.internal.example
""",
    )

    config = load_config_from_paths(
        Config,
        project_paths=[project_path / "config.py"],
        personal_paths=[personal_path / "config.yaml"],
    )

    assert isinstance(config.cicd_bot, GitLabCICDBotConfig)
    assert config.cicd_bot.server_url == "https://gitlab.internal.example"
