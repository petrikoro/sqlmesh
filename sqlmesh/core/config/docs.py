from __future__ import annotations

import re
import typing as t

from sqlmesh.core.config.base import BaseConfig
from sqlmesh.utils.pydantic import field_validator

_ALLOWED_URL_PLACEHOLDERS = {"fqn", "name", "schema", "catalog", "project"}
_PLACEHOLDER_PATTERN = re.compile(r"\{(\w+)\}")


class ExternalLinkConfig(BaseConfig):
    """Configuration for a single external link shown on model pages.

    Args:
        label: Display label for the link (e.g. "Airflow", "Looker").
        url: URL template. Supports placeholders: ``{fqn}``, ``{name}``,
             ``{schema}``, ``{catalog}``, ``{project}``.
    """

    label: str
    url: str

    @field_validator("url")
    def _validate_url_placeholders(cls, value: str) -> str:
        placeholders = set(_PLACEHOLDER_PATTERN.findall(value))
        unsupported = sorted(placeholders - _ALLOWED_URL_PLACEHOLDERS)
        if unsupported:
            raise ValueError(
                "Unsupported placeholders in external link URL template: "
                f"{', '.join(unsupported)}. Allowed placeholders: "
                f"{', '.join(sorted(_ALLOWED_URL_PLACEHOLDERS))}."
            )
        return value


class DocsConfig(BaseConfig):
    """Configuration for SQLMesh documentation generation.

    Args:
        external_links: A list of external link templates rendered on each
            model's properties section.  Each entry needs a ``label`` and
            a ``url`` (which may contain ``{placeholder}`` variables).
    """

    external_links: t.List[ExternalLinkConfig] = []
