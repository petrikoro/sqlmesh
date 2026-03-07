from __future__ import annotations

import logging
import typing as t
from dataclasses import dataclass
from functools import cached_property

from sqlmesh.cicd.shared_config import BaseCICDBotConfig
from sqlmesh.core.console import MarkdownConsole, SNAPSHOT_CHANGE_CATEGORY_STR
from sqlmesh.core.plan import Plan, SnapshotIntervals
from sqlmesh.core.plan.definition import UserProvidedFlags
from sqlmesh.core.snapshot.definition import (
    Snapshot,
    SnapshotChangeCategory,
    SnapshotId,
    SnapshotTableInfo,
)
from sqlmesh.utils import Verbosity
from sqlmesh.utils.errors import PlanError

logger = logging.getLogger(__name__)


def generate_plan_flags_section(user_provided_flags: t.Dict[str, UserProvidedFlags]) -> str:
    section = "<details>\n\n<summary>Plan flags</summary>\n\n"
    for flag_name, flag_value in user_provided_flags.items():
        section += f"- `{flag_name}` = `{flag_value}`\n"
    section += "\n</details>"
    return section


def get_linter_stage_title(status: str) -> str:
    return {
        "queued": "Waiting to Run linter",
        "in_progress": "Running linter",
    }.get(status, "Linter results")


def get_test_stage_title(
    *,
    status: str,
    completed_status: t.Optional[str] = None,
    was_successful: t.Optional[bool] = None,
) -> str:
    if status == "queued":
        return "Waiting to Run Tests"
    if status == "in_progress":
        return "Running Tests"
    if was_successful is not None:
        return "Tests Passed" if was_successful else "Tests Failed"
    return {
        "success": "Tests Passed",
        "failure": "Tests Failed",
        "skipped": "Skipped Tests",
    }.get(completed_status or status, "Tests Failed")


def get_virtual_data_environment_title(*, environment_name: str, request_term: str) -> str:
    return f"{request_term} Virtual Data Environment: {environment_name}"


def get_virtual_data_environment_status_summary(
    *, status: str, environment_name: str, request_term: str
) -> t.Optional[str]:
    request_environment_name = f"{request_term} Environment"
    return {
        "queued": f":pause_button: Waiting to create or update {request_environment_name} `{environment_name}`",
        "in_progress": f":rocket: Creating or Updating {request_environment_name} `{environment_name}`",
    }.get(status)


def get_prod_plan_preview_title(*, status: str, request_term: str) -> str:
    if status == "queued":
        return "Waiting to Generate Prod Plan"
    if status == "in_progress":
        return "Generating Prod Plan"
    return {
        "success": "Prod Plan Preview",
        "cancelled": "Cancelled generating prod plan preview",
        "skipped": f"Skipped generating prod plan preview since {request_term} was not synchronized",
        "failure": "Failed to generate prod plan preview",
    }.get(status, f"Got an unexpected conclusion: {status}")


def get_plan_summary(
    *, console: MarkdownConsole, plan: Plan, default_catalog: t.Optional[str]
) -> str:
    orig_verbosity = console.verbosity
    console.verbosity = Verbosity.VERY_VERBOSE

    try:
        console.consume_captured_output()
        if plan.restatements:
            console._print("\n**Restating models**\n")
        else:
            console.show_environment_difference_summary(
                context_diff=plan.context_diff,
                no_diff=False,
            )
        if plan.context_diff.has_changes:
            console.show_model_difference_summary(
                context_diff=plan.context_diff,
                environment_naming_info=plan.environment_naming_info,
                default_catalog=default_catalog,
                no_diff=False,
            )
        difference_summary = console.consume_captured_output()
        console._show_missing_dates(plan, default_catalog)
        missing_dates = console.consume_captured_output()

        plan_flags_section = (
            f"\n\n{generate_plan_flags_section(plan.user_provided_flags)}"
            if plan.user_provided_flags
            else ""
        )

        if not difference_summary and not missing_dates:
            return f"No changes to apply.{plan_flags_section}"

        warnings_block = console.consume_captured_warnings()
        errors_block = console.consume_captured_errors()

        return f"{warnings_block}{errors_block}{difference_summary}\n{missing_dates}{plan_flags_section}"
    except PlanError as e:
        logger.exception("Plan failed to generate")
        return f"Plan failed to generate. Check for pending or unresolved changes. Error: {e}"
    finally:
        console.verbosity = orig_verbosity


def generate_prod_plan_preview_summary(
    *,
    plan_summary: str,
    environment_name: str,
    request_term: str,
    target_environment_name: str = "prod",
) -> str:
    rendered_plan_summary = plan_summary.strip()
    if not rendered_plan_summary:
        return ""

    return (
        f"This is a preview that shows the differences between this {request_term} environment "
        f"`{environment_name}` and `{target_environment_name}`.\n\n"
        "These are the changes that would be deployed.\n\n"
        f"{rendered_plan_summary}"
    )


def generate_request_environment_summary_intro(
    *,
    bot_config: BaseCICDBotConfig,
    environment_name: str,
    request_term: str,
    preview_label: str,
    preview_location: str = "section",
    target_environment_name: str = "prod",
) -> str:
    note = ""
    subset_reasons = []

    if bot_config.skip_pr_backfill:
        subset_reasons.append("`skip_pr_backfill` is enabled")

    if default_pr_start := bot_config.default_pr_start:
        subset_reasons.append(f"`default_pr_start` is set to `{default_pr_start}`")

    if subset_reasons:
        note = (
            "> [!IMPORTANT]\n"
            f"> This {request_term} environment may only contain a subset of data because:\n"
            + "\n".join(f"> - {reason}" for reason in subset_reasons)
            + "\n"
            "> \n"
            f"> This means that deploying to `{target_environment_name}` may not be a simple virtual update if there is still some data to load.\n"
            f"> See `Dates not loaded in {request_term}` below or the `{preview_label}` {preview_location} for more information.\n\n"
        )

    request_environment_name = f"{request_term} environment"
    return (
        f"Here is a summary of data that has been loaded into the {request_environment_name} `{environment_name}` "
        f"and could be deployed to `{target_environment_name}`.\n\n{note}"
    )


def generate_request_environment_summary_list(plan: Plan, request_term: str) -> str:
    added_snapshot_ids = set(plan.context_diff.added)
    modified_snapshot_ids = set(
        snapshot.snapshot_id for snapshot, _ in plan.context_diff.modified_snapshots.values()
    )
    removed_snapshot_ids = set(plan.context_diff.removed_snapshots.keys())

    table_records = sorted(
        [
            SnapshotSummaryRecord(snapshot_id=snapshot_id, plan=plan, request_term=request_term)
            for snapshot_id in (added_snapshot_ids | modified_snapshot_ids | removed_snapshot_ids)
        ],
        key=lambda record: record.display_name,
    )

    sections = [
        ("### Added", [record for record in table_records if record.is_added]),
        ("### Removed", [record for record in table_records if record.is_removed]),
        (
            "### Directly Modified",
            [record for record in table_records if record.is_directly_modified],
        ),
        (
            "### Indirectly Modified",
            [record for record in table_records if record.is_indirectly_modified],
        ),
        (
            "### Metadata Updated",
            [
                record
                for record in table_records
                if record.is_metadata_updated and not record.is_modified
            ],
        ),
    ]

    summary = ""
    for title, records in sections:
        if records:
            summary += f"\n{title}\n"

        for record in records:
            summary += f"{record.as_markdown_list_item}\n"

    return summary


@dataclass
class SnapshotSummaryRecord:
    snapshot_id: SnapshotId
    plan: Plan
    request_term: str = "PR"

    @property
    def snapshot(self) -> Snapshot:
        if self.is_removed:
            raise ValueError("Removed snapshots only have SnapshotTableInfo available")
        return self.plan.snapshots[self.snapshot_id]

    @cached_property
    def snapshot_table_info(self) -> SnapshotTableInfo:
        if self.is_removed:
            return self.plan.modified_snapshots[self.snapshot_id].table_info
        return self.plan.snapshots[self.snapshot_id].table_info

    @property
    def display_name(self) -> str:
        dialect = None if self.is_removed else self.snapshot.node.dialect
        return self.snapshot_table_info.display_name(
            self.plan.environment_naming_info,
            default_catalog=None,
            dialect=dialect,
        )

    @property
    def change_category(self) -> str:
        if self.is_removed:
            return SNAPSHOT_CHANGE_CATEGORY_STR[SnapshotChangeCategory.BREAKING]

        if change_category := self.snapshot.change_category:
            return SNAPSHOT_CHANGE_CATEGORY_STR[change_category]

        return "Uncategorized"

    @property
    def is_added(self) -> bool:
        return self.snapshot_id in self.plan.context_diff.added

    @property
    def is_removed(self) -> bool:
        return self.snapshot_id in self.plan.context_diff.removed_snapshots

    @property
    def is_dev_preview(self) -> bool:
        return not self.plan.deployability_index.is_deployable(self.snapshot_id)

    @property
    def is_directly_modified(self) -> bool:
        return self.plan.context_diff.directly_modified(self.snapshot_table_info.name)

    @property
    def is_indirectly_modified(self) -> bool:
        return self.plan.context_diff.indirectly_modified(self.snapshot_table_info.name)

    @property
    def is_modified(self) -> bool:
        return self.is_directly_modified or self.is_indirectly_modified

    @property
    def is_metadata_updated(self) -> bool:
        return self.plan.context_diff.metadata_updated(self.snapshot_table_info.name)

    @property
    def is_incremental(self) -> bool:
        return self.snapshot_table_info.is_incremental

    @property
    def loaded_intervals(self) -> SnapshotIntervals:
        if self.is_removed:
            raise ValueError("Removed snapshots dont have loaded intervals available")

        return SnapshotIntervals(
            snapshot_id=self.snapshot_id,
            intervals=(
                self.snapshot.dev_intervals
                if self.snapshot.is_forward_only
                else self.snapshot.intervals
            ),
        )

    @property
    def loaded_intervals_rendered(self) -> str:
        if self.is_removed:
            return "REMOVED"

        return self._format_intervals(self.loaded_intervals)

    @property
    def missing_intervals(self) -> t.Optional[SnapshotIntervals]:
        return next(
            (
                intervals
                for intervals in self.plan.missing_intervals
                if intervals.snapshot_id == self.snapshot_id
            ),
            None,
        )

    @property
    def missing_intervals_formatted(self) -> str:
        if not self.is_removed and (intervals := self.missing_intervals):
            return self._format_intervals(intervals)
        return "N/A"

    @property
    def as_markdown_list_item(self) -> str:
        if self.is_removed:
            return f"- `{self.display_name}` ({self.change_category})"

        how_applied = ""
        if not self.is_incremental:
            from sqlmesh.core.console import _format_missing_intervals

            how_applied = _format_missing_intervals(self.snapshot, self.loaded_intervals)

        how_applied_str = f" [{how_applied}]" if how_applied else ""

        item = f"- `{self.display_name}` ({self.change_category})\n"

        if self.snapshot_table_info.model_kind_name:
            item += f"  **Kind:** {self.snapshot_table_info.model_kind_name}{how_applied_str}\n"

        if self.is_incremental:
            item += (
                f"  **Dates loaded in {self.request_term}:** [{self.loaded_intervals_rendered}]\n"
            )
            if self.missing_intervals:
                item += (
                    f"  **Dates *not* loaded in {self.request_term}:** "
                    f"[{self.missing_intervals_formatted}]\n"
                )

        return item

    def _format_intervals(self, intervals: SnapshotIntervals) -> str:
        preview_modifier = " (**preview**)" if self.is_dev_preview else ""
        return f"{intervals.format_intervals(self.snapshot.node.interval_unit)}{preview_modifier}"
