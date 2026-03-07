from __future__ import annotations

import typing as t

from pydantic import Field

from sqlmesh.core.config import CategorizerConfig
from sqlmesh.core.config.base import BaseConfig
from sqlmesh.core.console import get_console
from sqlmesh.utils.date import TimeLike


class BaseCICDBotConfig(BaseConfig):
    legacy_providerless_config_: bool = Field(default=False, alias="_legacy_providerless_config")
    default_pr_start: t.Optional[TimeLike] = None
    default_pr_preview_start: TimeLike = "yesterday"
    auto_categorize_changes_: t.Optional[CategorizerConfig] = Field(
        default=None, alias="auto_categorize_changes"
    )
    skip_pr_backfill_: t.Optional[bool] = Field(default=None, alias="skip_pr_backfill")
    pr_include_unmodified_: t.Optional[bool] = Field(default=None, alias="pr_include_unmodified")
    pr_environment_name: t.Optional[str] = None
    pr_min_intervals: t.Optional[int] = None
    pr_preview_min_intervals: int = Field(default=1, ge=0)
    prod_branch_names_: t.Optional[str] = Field(default=None, alias="prod_branch_name")
    forward_only_branch_suffix_: t.Optional[str] = Field(
        default=None, alias="forward_only_branch_suffix"
    )
    run_on_deploy_to_prod: bool = False

    @property
    def prod_branch_names(self) -> t.List[str]:
        if self.prod_branch_names_:
            return [self.prod_branch_names_]
        return ["main", "master"]

    @property
    def auto_categorize_changes(self) -> CategorizerConfig:
        return self.auto_categorize_changes_ or CategorizerConfig.all_off()

    @property
    def pr_include_unmodified(self) -> bool:
        return self.pr_include_unmodified_ or False

    @property
    def skip_pr_backfill(self) -> bool:
        if self.skip_pr_backfill_ is None:
            get_console().log_warning(
                "`skip_pr_backfill` is unset, defaulting it to `true` (no data will be backfilled).\n"
                "Future versions of SQLMesh will default to `skip_pr_backfill: false` to align with the CLI default behaviour.\n"
                "If you would like to preserve the current behaviour and remove this warning, please explicitly set `skip_pr_backfill: true` in the bot config.\n\n"
                "For more information on configuring the bot, see: https://sqlmesh.readthedocs.io/en/stable/integrations/overview/"
            )
            return True
        return self.skip_pr_backfill_

    @property
    def forward_only_branch_suffix(self) -> str:
        return self.forward_only_branch_suffix_ or "-forward-only"

    FIELDS_FOR_ANALYTICS: t.ClassVar[t.Set[str]] = {
        "auto_categorize_changes",
        "default_pr_start",
        "default_pr_preview_start",
        "skip_pr_backfill",
        "pr_include_unmodified",
        "run_on_deploy_to_prod",
        "pr_min_intervals",
        "pr_preview_min_intervals",
        "forward_only_branch_suffix",
    }
