from __future__ import annotations

import typing as t

from pydantic import Field

from sqlmesh.cicd.shared_config import BaseCICDBotConfig


class GitLabCICDBotConfig(BaseCICDBotConfig):
    type_: t.Literal["gitlab"] = Field(alias="type", default="gitlab")

    api_v4_url: t.Optional[str] = None
    server_url: t.Optional[str] = None

    FIELDS_FOR_ANALYTICS: t.ClassVar[t.Set[str]] = BaseCICDBotConfig.FIELDS_FOR_ANALYTICS | {
        "api_v4_url",
        "server_url",
    }
