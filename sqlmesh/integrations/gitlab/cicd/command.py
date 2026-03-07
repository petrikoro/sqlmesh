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

RUN_LINTER_NOTE = GitLabController.RUN_LINTER_NOTE
RUN_TESTS_NOTE = GitLabController.RUN_TESTS_NOTE
UPDATE_MR_ENVIRONMENT_NOTE = GitLabController.UPDATE_MR_ENVIRONMENT_NOTE
GEN_PROD_PLAN_NOTE = GitLabController.GEN_PROD_PLAN_NOTE

LINTER_STAGE = "Linter"
UNIT_TESTS_STAGE = "Unit Tests"
MR_ENVIRONMENT_STAGE = "MR Environment"
PROD_PLAN_PREVIEW_STAGE = "Prod Plan Preview"


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
    note_type: str,
    *,
    stage_statuses: t.Optional[t.Mapping[str, str]] = None,
    summary: t.Optional[str] = None,
    details: t.Optional[t.Mapping[str, t.Optional[str]]] = None,
) -> None:
    note_state = controller.get_merge_request_note_state(note_type)
    resolved_stage_statuses = dict(note_state.stage_statuses)
    resetting_stages: t.Set[str] = set()
    if stage_statuses:
        resolved_stage_statuses.update(stage_statuses)
        resetting_stages = {
            stage for stage, status in stage_statuses.items() if status in {"queued", "in_progress"}
        }
    resolved_details = dict(note_state.details)
    for stage in resetting_stages:
        resolved_details.pop(stage, None)
    if details is not None:
        for stage, detail in details.items():
            if detail:
                resolved_details[stage] = detail
            else:
                resolved_details.pop(stage, None)
    resolved_summary = note_state.summary if summary is None else summary
    if resetting_stages and summary is None:
        resolved_summary = ""

    controller.upsert_sqlmesh_mr_note(
        note_type,
        controller.render_merge_request_note(
            note_type=note_type,
            stage_statuses=resolved_stage_statuses,
            summary=resolved_summary,
            details=resolved_details,
        ),
    )


def _run_tests(controller: GitLabController) -> t.Tuple[bool, str, t.Optional[str]]:
    try:
        result, output = controller.run_tests()
        test_summary = controller.get_test_summary(result).strip()
        rendered_output = output.strip()
        summary = test_summary or rendered_output
        if not summary:
            summary = "Tests Passed" if result.wasSuccessful() else "Tests Failed"
        details = rendered_output if rendered_output and rendered_output != summary else None
        return result.wasSuccessful(), summary, details
    except Exception:
        logger.exception("Error occurred when running tests")
        return False, traceback.format_exc().strip(), None


def _consume_linter_output(controller: GitLabController) -> str:
    return (
        f"{controller._console.consume_captured_warnings()}"
        f"{controller._console.consume_captured_errors()}"
        f"{controller._console.consume_captured_output()}"
    ).strip()


def _run_linter(controller: GitLabController) -> t.Tuple[bool, str, t.Optional[str]]:
    try:
        controller.run_linter()
        linter_output = _consume_linter_output(controller)
        return True, linter_output or "Linter Success", None
    except LinterError:
        logger.exception("Error occurred when running linter")
        return False, _consume_linter_output(controller) or "Linter failed.", None
    except Exception as ex:
        logger.exception("Unexpected error occurred when running linter")
        linter_output = _consume_linter_output(controller)
        traceback_output = traceback.format_exc().strip()
        summary = f"{linter_output}\n\n{traceback_output}" if linter_output else traceback_output
        return (
            False,
            summary,
            None,
        )


def _run_linter_stage(controller: GitLabController) -> bool:
    _update_note(controller, RUN_LINTER_NOTE, stage_statuses={LINTER_STAGE: "in_progress"})
    linter_passed, linter_summary, linter_details = _run_linter(controller)
    stage_statuses = {LINTER_STAGE: "success" if linter_passed else "failure"}
    _update_note(
        controller,
        RUN_LINTER_NOTE,
        stage_statuses=stage_statuses,
        summary=linter_summary,
        details={LINTER_STAGE: linter_details},
    )
    return linter_passed


def _run_tests_stage(controller: GitLabController) -> bool:
    _update_note(controller, RUN_TESTS_NOTE, stage_statuses={UNIT_TESTS_STAGE: "in_progress"})
    tests_passed, test_summary, test_details = _run_tests(controller)
    stage_statuses = {UNIT_TESTS_STAGE: "success" if tests_passed else "failure"}
    _update_note(
        controller,
        RUN_TESTS_NOTE,
        stage_statuses=stage_statuses,
        summary=test_summary,
        details={UNIT_TESTS_STAGE: test_details},
    )
    return tests_passed


def _update_mr_environment(controller: GitLabController) -> bool:
    _update_note(
        controller,
        UPDATE_MR_ENVIRONMENT_NOTE,
        stage_statuses={MR_ENVIRONMENT_STAGE: "in_progress"},
    )
    try:
        summary = controller.update_merge_request_environment()
        _update_note(
            controller,
            UPDATE_MR_ENVIRONMENT_NOTE,
            stage_statuses={MR_ENVIRONMENT_STAGE: "success"},
            summary=summary,
            details={MR_ENVIRONMENT_STAGE: None},
        )
        return True
    except NoChangesPlanError as ex:
        _update_note(
            controller,
            UPDATE_MR_ENVIRONMENT_NOTE,
            stage_statuses={MR_ENVIRONMENT_STAGE: "skipped"},
            summary=controller.get_merge_request_environment_summary(exception=ex),
            details={MR_ENVIRONMENT_STAGE: None},
        )
        return True
    except StalePipelineError as ex:
        _update_note(
            controller,
            UPDATE_MR_ENVIRONMENT_NOTE,
            stage_statuses={MR_ENVIRONMENT_STAGE: "skipped"},
            summary=str(ex),
            details={MR_ENVIRONMENT_STAGE: None},
        )
        return True
    except Exception as ex:
        _update_note(
            controller,
            UPDATE_MR_ENVIRONMENT_NOTE,
            stage_statuses={MR_ENVIRONMENT_STAGE: "failure"},
            summary=controller.get_merge_request_environment_summary(exception=ex),
            details={MR_ENVIRONMENT_STAGE: None},
        )
        return False


def _gen_prod_plan(controller: GitLabController) -> bool:
    _update_note(
        controller,
        GEN_PROD_PLAN_NOTE,
        stage_statuses={PROD_PLAN_PREVIEW_STAGE: "in_progress"},
    )
    try:
        plan_summary = controller.get_prod_plan_preview_summary(controller.prod_plan)
        _update_note(
            controller,
            GEN_PROD_PLAN_NOTE,
            stage_statuses={PROD_PLAN_PREVIEW_STAGE: "success"},
            summary=plan_summary,
            details={PROD_PLAN_PREVIEW_STAGE: None},
        )
        return True
    except Exception as ex:
        _update_note(
            controller,
            GEN_PROD_PLAN_NOTE,
            stage_statuses={PROD_PLAN_PREVIEW_STAGE: "failure"},
            summary=str(ex),
            details={PROD_PLAN_PREVIEW_STAGE: None},
        )
        return False


def _run_linter_command(controller: GitLabController) -> None:
    if not _run_linter_stage(controller):
        raise CICDBotError("Failed to run the linter.")


def _run_tests_command(controller: GitLabController) -> None:
    if not _run_tests_stage(controller):
        raise CICDBotError("Failed to run tests.")


def _update_mr_environment_command(controller: GitLabController) -> None:
    if not _update_mr_environment(controller):
        raise CICDBotError("Failed to update merge request environment.")


def _gen_prod_plan_command(controller: GitLabController) -> None:
    if not _gen_prod_plan(controller):
        raise CICDBotError("Failed to generate the production plan preview.")


@gitlab.command()
@click.pass_context
@cli_analytics
def run_linter(ctx: click.Context) -> None:
    """Runs the SQLMesh linter."""
    _run_linter_command(ctx.obj["gitlab"])


@gitlab.command()
@click.pass_context
@cli_analytics
def run_tests(ctx: click.Context) -> None:
    """Runs the unit tests."""
    _run_tests_command(ctx.obj["gitlab"])


@gitlab.command()
@click.pass_context
@cli_analytics
def update_mr_environment(ctx: click.Context) -> None:
    """Creates or updates the merge request environment."""
    _update_mr_environment_command(ctx.obj["gitlab"])


@gitlab.command()
@click.pass_context
@cli_analytics
def gen_prod_plan(ctx: click.Context) -> None:
    """Generates the production plan preview."""
    _gen_prod_plan_command(ctx.obj["gitlab"])


def _run_all(controller: GitLabController) -> None:
    click.echo(f"SQLMesh Version: {controller.version_info}")
    _run_linter_command(controller)
    _run_tests_command(controller)
    _update_mr_environment_command(controller)
    _gen_prod_plan_command(controller)


@gitlab.command(name="run-all")
@click.pass_context
@cli_analytics
def run_all(ctx: click.Context) -> None:
    """Runs the GitLab CI/CD bot workflow."""
    _run_all(ctx.obj["gitlab"])
