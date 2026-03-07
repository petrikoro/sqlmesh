from __future__ import annotations

import typing as t

from pydantic import Field

from sqlmesh.cicd.shared_config import BaseCICDBotConfig
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig
from sqlmesh.integrations.gitlab.cicd.config import GitLabCICDBotConfig

AnyCICDBotConfig = t.Annotated[
    t.Union[GithubCICDBotConfig, GitLabCICDBotConfig],
    Field(discriminator="type_"),
]


class CICDBotConfig(GithubCICDBotConfig):
    legacy_providerless_config_: bool = Field(default=True, alias="_legacy_providerless_config")


__all__ = [
    "AnyCICDBotConfig",
    "BaseCICDBotConfig",
    "CICDBotConfig",
    "GithubCICDBotConfig",
    "GitLabCICDBotConfig",
]
