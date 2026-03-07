from __future__ import annotations

import logging
import traceback
import typing as t

import click

from sqlmesh.core.analytics import cli_analytics
from sqlmesh.core.console import MarkdownConsole, set_console
from sqlmesh.integrations.gitlab.cicd.controller import GitLabController, StalePipelineError
from sqlmesh.utils.errors import CICDBotError, LinterError, NoChangesPlanError

logger = logging.getLogger(__name__)


@click.group(no_args_is_help=True)
@click.option(
    "--token",
    type=str,
    envvar="GITLAB_TOKEN",
    help="The GitLab token used to update merge request notes.",
)
@click.option(
    "--full-logs",
    is_flag=True,
    help="Whether to print all logs or only capture them for the SQLMesh MR note.",
)
@click.pass_context
def gitlab(ctx: click.Context, token: str, full_logs: bool = False) -> None:
    """GitLab CI/CD Bot. See https://sqlmesh.readthedocs.io/en/stable/integrations/gitlab/ for details."""
    set_console(
        MarkdownConsole(
            width=1000,
            warning_capture_only=not full_logs,
            error_capture_only=not full_logs,
        )
    )
    ctx.obj["gitlab"] = GitLabController(
        paths=ctx.obj["paths"],
        token=token,
        config=ctx.obj["config"],
    )


def _update_note(
    controller: GitLabController,
    *,
    stage_statuses: t.Optional[t.Mapping[str, str]] = None,
    merge_request_environment_summary: t.Optional[str] = None,
    prod_plan_summary: t.Optional[str] = None,
    details: t.Optional[t.Mapping[str, t.Optional[str]]] = None,
) -> None:
    note_state = controller.get_merge_request_note_state()
    resolved_stage_statuses = dict(note_state.stage_statuses)
    if stage_statuses:
        resolved_stage_statuses.update(stage_statuses)
    resolved_details = dict(note_state.details)
    if details is not None:
        for stage, detail in details.items():
            if detail:
                resolved_details[stage] = detail
            else:
                resolved_details.pop(stage, None)

    controller.upsert_sqlmesh_mr_note(
        controller.render_merge_request_note(
            stage_statuses=resolved_stage_statuses,
            merge_request_environment_summary=(
                note_state.merge_request_environment_summary
                if merge_request_environment_summary is None
                else merge_request_environment_summary
            ),
            prod_plan_summary=(
                note_state.prod_plan_summary if prod_plan_summary is None else prod_plan_summary
            ),
            details=resolved_details,
        )
    )


def _run_tests(controller: GitLabController) -> t.Tuple[bool, str]:
    try:
        result, output = controller.run_tests()
        return result.wasSuccessful(), output
    except Exception:
        logger.exception("Error occurred when running tests")
        return False, traceback.format_exc()


def _run_linter(controller: GitLabController) -> t.Tuple[bool, str]:
    try:
        controller.run_linter()
        linter_output = (
            f"{controller._console.consume_captured_warnings()}"
            f"{controller._console.consume_captured_output()}"
        ).strip()
        return True, linter_output
    except LinterError:
        logger.exception("Error occurred when running linter")
        return False, controller._console.consume_captured_errors() or "Linter failed."
    except Exception:
        logger.exception("Unexpected error occurred when running linter")
        return False, traceback.format_exc()


def _run_linter_stage(controller: GitLabController) -> bool:
    _update_note(controller, stage_statuses={"Linter": "in_progress"})
    linter_passed, linter_output = _run_linter(controller)
    stage_statuses = {"Linter": "success" if linter_passed else "failure"}
    detail_updates: t.Dict[str, t.Optional[str]] = {
        "Linter": linter_output.strip() if linter_output else None
    }
    note_kwargs: t.Dict[str, t.Any] = {
        "merge_request_environment_summary": "",
        "prod_plan_summary": "",
    }
    if linter_passed:
        stage_statuses.update(
            {
                "Unit Tests": "queued",
                "MR Environment": "queued",
                "Prod Plan Preview": "queued",
            }
        )
        detail_updates.update(
            {
                "Unit Tests": None,
                "MR Environment": None,
                "Prod Plan Preview": None,
            }
        )
    else:
        stage_statuses.update(
            {
                "Unit Tests": "skipped",
                "MR Environment": "skipped",
                "Prod Plan Preview": "skipped",
            }
        )
        detail_updates.update(
            {
                "Unit Tests": None,
                "MR Environment": None,
                "Prod Plan Preview": None,
            }
        )
    _update_note(
        controller,
        stage_statuses=stage_statuses,
        details=detail_updates,
        **note_kwargs,
    )
    return linter_passed


def _run_tests_stage(controller: GitLabController) -> bool:
    _update_note(controller, stage_statuses={"Unit Tests": "in_progress"})
    tests_passed, test_output = _run_tests(controller)
    stage_statuses = {"Unit Tests": "success" if tests_passed else "failure"}
    detail_updates: t.Dict[str, t.Optional[str]] = {
        "Unit Tests": None if tests_passed else test_output.strip()
    }
    note_kwargs: t.Dict[str, t.Any] = {
        "merge_request_environment_summary": "",
        "prod_plan_summary": "",
    }
    if tests_passed:
        stage_statuses.update(
            {
                "MR Environment": "queued",
                "Prod Plan Preview": "queued",
            }
        )
        detail_updates.update(
            {
                "MR Environment": None,
                "Prod Plan Preview": None,
            }
        )
    else:
        stage_statuses.update(
            {
                "MR Environment": "skipped",
                "Prod Plan Preview": "skipped",
            }
        )
        detail_updates.update(
            {
                "MR Environment": None,
                "Prod Plan Preview": None,
            }
        )
    _update_note(
        controller,
        stage_statuses=stage_statuses,
        details=detail_updates,
        **note_kwargs,
    )
    return tests_passed


def _update_mr_environment(controller: GitLabController) -> bool:
    _update_note(controller, stage_statuses={"MR Environment": "in_progress"})
    try:
        summary = controller.update_merge_request_environment()
        _update_note(
            controller,
            stage_statuses={
                "MR Environment": "success",
                "Prod Plan Preview": "queued",
            },
            merge_request_environment_summary=summary,
            prod_plan_summary="",
            details={
                "MR Environment": None,
                "Prod Plan Preview": None,
            },
        )
        return True
    except NoChangesPlanError as ex:
        _update_note(
            controller,
            stage_statuses={
                "MR Environment": "skipped",
                "Prod Plan Preview": "skipped",
            },
            merge_request_environment_summary=controller.get_merge_request_environment_summary(
                exception=ex
            ),
            prod_plan_summary="",
            details={
                "MR Environment": str(ex),
                "Prod Plan Preview": None,
            },
        )
        return True
    except StalePipelineError as ex:
        _update_note(
            controller,
            stage_statuses={
                "MR Environment": "skipped",
                "Prod Plan Preview": "skipped",
            },
            merge_request_environment_summary="",
            prod_plan_summary="",
            details={
                "MR Environment": str(ex),
                "Prod Plan Preview": None,
            },
        )
        return True
    except Exception as ex:
        _update_note(
            controller,
            stage_statuses={
                "MR Environment": "failure",
                "Prod Plan Preview": "skipped",
            },
            merge_request_environment_summary=controller.get_merge_request_environment_summary(
                exception=ex
            ),
            prod_plan_summary="",
            details={
                "MR Environment": str(ex),
                "Prod Plan Preview": None,
            },
        )
        return False


def _gen_prod_plan(controller: GitLabController) -> bool:
    _update_note(controller, stage_statuses={"Prod Plan Preview": "in_progress"})
    try:
        plan_summary = controller.get_plan_summary(controller.prod_plan)
        _update_note(
            controller,
            stage_statuses={"Prod Plan Preview": "success"},
            prod_plan_summary=plan_summary,
            details={"Prod Plan Preview": None},
        )
        return True
    except Exception as ex:
        _update_note(
            controller,
            stage_statuses={"Prod Plan Preview": "failure"},
            prod_plan_summary=str(ex),
            details={"Prod Plan Preview": str(ex)},
        )
        return False


@gitlab.command()
@click.pass_context
@cli_analytics
def run_linter(ctx: click.Context) -> None:
    """Runs the SQLMesh linter."""
    if not _run_linter_stage(ctx.obj["gitlab"]):
        raise CICDBotError("Failed to run the linter.")


@gitlab.command()
@click.pass_context
@cli_analytics
def run_tests(ctx: click.Context) -> None:
    """Runs the unit tests."""
    if not _run_tests_stage(ctx.obj["gitlab"]):
        raise CICDBotError("Failed to run tests.")


@gitlab.command()
@click.pass_context
@cli_analytics
def update_mr_environment(ctx: click.Context) -> None:
    """Creates or updates the merge request environment."""
    if not _update_mr_environment(ctx.obj["gitlab"]):
        raise CICDBotError("Failed to update merge request environment.")


@gitlab.command()
@click.pass_context
@cli_analytics
def gen_prod_plan(ctx: click.Context) -> None:
    """Generates the production plan preview."""
    if not _gen_prod_plan(ctx.obj["gitlab"]):
        raise CICDBotError("Failed to generate the production plan preview.")


def _run_all(controller: GitLabController) -> None:
    click.echo(f"SQLMesh Version: {controller.version_info}")

    statuses = {
        "Linter": "queued",
        "Unit Tests": "queued",
        "MR Environment": "queued",
        "Prod Plan Preview": "queued",
    }
    details: t.Dict[str, str] = {}
    merge_request_environment_summary = ""
    prod_plan_summary = ""
    _update_note(
        controller,
        stage_statuses=statuses,
        merge_request_environment_summary="",
        prod_plan_summary="",
        details={stage: None for stage in controller.PIPELINE_STAGE_LABELS},
    )

    statuses["Linter"] = "in_progress"
    _update_note(controller, stage_statuses=statuses)
    linter_passed, linter_output = _run_linter(controller)
    statuses["Linter"] = "success" if linter_passed else "failure"
    if linter_output:
        details["Linter"] = linter_output.strip()
    else:
        details.pop("Linter", None)
    _update_note(controller, stage_statuses=statuses, details=details)
    if not linter_passed:
        statuses["Unit Tests"] = "skipped"
        statuses["MR Environment"] = "skipped"
        statuses["Prod Plan Preview"] = "skipped"
        _update_note(controller, stage_statuses=statuses, details=details)
        raise CICDBotError("Linter failed.")

    statuses["Unit Tests"] = "in_progress"
    _update_note(controller, stage_statuses=statuses, details=details)
    tests_passed, test_output = _run_tests(controller)
    statuses["Unit Tests"] = "success" if tests_passed else "failure"
    if test_output and not tests_passed:
        details["Unit Tests"] = test_output.strip()
    else:
        details.pop("Unit Tests", None)
    _update_note(controller, stage_statuses=statuses, details=details)
    if not tests_passed:
        statuses["MR Environment"] = "skipped"
        statuses["Prod Plan Preview"] = "skipped"
        _update_note(controller, stage_statuses=statuses, details=details)
        raise CICDBotError("Unit tests failed.")

    statuses["MR Environment"] = "in_progress"
    _update_note(controller, stage_statuses=statuses, details=details)
    stale_pipeline = False
    try:
        merge_request_environment_summary = controller.update_merge_request_environment()
        statuses["MR Environment"] = "success"
    except Exception as ex:
        logger.exception("Error occurred when updating MR environment")
        merge_request_environment_summary = controller.get_merge_request_environment_summary(
            exception=ex
        )
        statuses["MR Environment"] = (
            "skipped" if isinstance(ex, (NoChangesPlanError, StalePipelineError)) else "failure"
        )
        if isinstance(ex, StalePipelineError):
            statuses["Prod Plan Preview"] = "skipped"
            stale_pipeline = True
        details["MR Environment"] = str(ex)
    else:
        details.pop("MR Environment", None)
    _update_note(
        controller,
        stage_statuses=statuses,
        merge_request_environment_summary=merge_request_environment_summary,
        prod_plan_summary=prod_plan_summary,
        details=details,
    )
    if stale_pipeline:
        return

    statuses["Prod Plan Preview"] = "in_progress"
    _update_note(
        controller,
        stage_statuses=statuses,
        merge_request_environment_summary=merge_request_environment_summary,
        details=details,
    )
    try:
        prod_plan_summary = controller.get_plan_summary(controller.prod_plan)
        statuses["Prod Plan Preview"] = "success"
    except Exception as ex:
        logger.exception("Error occurred generating prod plan")
        prod_plan_summary = str(ex)
        statuses["Prod Plan Preview"] = "failure"
        details["Prod Plan Preview"] = str(ex)
    else:
        details.pop("Prod Plan Preview", None)
    _update_note(
        controller,
        stage_statuses=statuses,
        merge_request_environment_summary=merge_request_environment_summary,
        prod_plan_summary=prod_plan_summary,
        details=details,
    )

    if "failure" in statuses.values():
        raise CICDBotError("GitLab SQLMesh bot failed. See the merge request note for details.")


@gitlab.command(name="run-all")
@click.pass_context
@cli_analytics
def run_all(ctx: click.Context) -> None:
    """Runs the GitLab CI/CD bot workflow."""
    _run_all(ctx.obj["gitlab"])
