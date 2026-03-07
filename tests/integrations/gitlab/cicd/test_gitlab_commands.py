# type: ignore
from unittest.result import TestResult

import typing as t

import pytest

from sqlmesh.integrations.gitlab.cicd import command
from sqlmesh.utils.errors import CICDBotError, LinterError, NoChangesPlanError
from tests.integrations.gitlab.cicd.conftest import MockMergeRequestNote

TestResult.__test__ = False

pytestmark = pytest.mark.gitlab

NOTE_TYPE_MARKER_PREFIX = "<!-- sqlmesh-gitlab-note-type:"


def _get_note_by_type(client, note_type: str) -> MockMergeRequestNote:
    marker = f"{NOTE_TYPE_MARKER_PREFIX}{note_type} -->"
    return next(note for note in client.notes if marker in note.body)


def _has_note_by_type(client, note_type: str) -> bool:
    marker = f"{NOTE_TYPE_MARKER_PREFIX}{note_type} -->"
    return any(marker in note.body for note in client.notes)


def _make_typed_note(
    note_id: int, note_type: str, body: str, *, pipeline_id: t.Optional[int] = None
) -> MockMergeRequestNote:
    markers = [
        "<!-- sqlmesh-gitlab-bot-note -->",
        f"{NOTE_TYPE_MARKER_PREFIX}{note_type} -->",
    ]
    if pipeline_id is not None:
        markers.append(f"<!-- sqlmesh-gitlab-pipeline-id:{pipeline_id} -->")
    markers.append(body)
    return MockMergeRequestNote(note_id, "\n".join(markers))


def _assert_note_uses_check_like_output(body: str, expected_title: str) -> None:
    assert expected_title in body


def test_run_all_calls_individual_command_behaviors_in_order(
    make_gitlab_client, make_controller, mocker
):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    calls: t.List[str] = []

    mocker.patch.object(
        command, "_run_linter_command", side_effect=lambda _: calls.append("run-linter")
    )
    mocker.patch.object(
        command, "_run_tests_command", side_effect=lambda _: calls.append("run-tests")
    )
    mocker.patch.object(
        command,
        "_update_mr_environment_command",
        side_effect=lambda _: calls.append("update-mr-environment"),
    )
    mocker.patch.object(
        command, "_gen_prod_plan_command", side_effect=lambda _: calls.append("gen-prod-plan")
    )

    command._run_all(controller)

    assert calls == [
        "run-linter",
        "run-tests",
        "update-mr-environment",
        "gen-prod-plan",
    ]


def test_run_all_creates_four_typed_sticky_notes(make_gitlab_client, make_controller, mocker):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )

    command._run_all(controller)

    assert len(client.notes) == 4
    assert len(client.created_notes) == 4
    assert len(client.updated_notes) == 4

    run_linter_note = _get_note_by_type(client, "run-linter")
    _assert_note_uses_check_like_output(run_linter_note.body, "Linter results")
    assert (
        controller.get_merge_request_note_state("run-linter").stage_statuses["Linter"] == "success"
    )

    run_tests_note = _get_note_by_type(client, "run-tests")
    _assert_note_uses_check_like_output(run_tests_note.body, "Tests Passed")
    assert (
        controller.get_merge_request_note_state("run-tests").stage_statuses["Unit Tests"]
        == "success"
    )

    mr_environment_note = _get_note_by_type(client, "update-mr-environment")
    _assert_note_uses_check_like_output(
        mr_environment_note.body, "MR Virtual Data Environment: hello_world_42"
    )
    assert (
        controller.get_merge_request_note_state("update-mr-environment").stage_statuses[
            "MR Environment"
        ]
        == "success"
    )
    assert (
        "Dates loaded in MR" in mr_environment_note.body
        or "No models were modified in this MR" in mr_environment_note.body
    )

    prod_plan_note = _get_note_by_type(client, "gen-prod-plan")
    _assert_note_uses_check_like_output(prod_plan_note.body, "Prod Plan Preview")
    assert (
        controller.get_merge_request_note_state("gen-prod-plan").stage_statuses["Prod Plan Preview"]
        == "success"
    )
    assert "This is a preview that shows the differences between this MR environment" in (
        prod_plan_note.body
    )
    assert "```diff" in prod_plan_note.body or "**Added Models:**" in prod_plan_note.body
    assert "Breaking" in prod_plan_note.body or "Non-breaking" in prod_plan_note.body
    assert "**Change categories:**" not in prod_plan_note.body
    assert (
        "**Models needing backfill:**" in prod_plan_note.body
        or "No changes to apply." in prod_plan_note.body
    )


def test_run_all_stops_after_linter_failure(make_gitlab_client, make_controller, mocker):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context.lint_models = mocker.MagicMock(side_effect=LinterError("lint failed"))

    with pytest.raises(CICDBotError, match="Failed to run the linter."):
        command._run_all(controller)

    assert len(client.notes) == 1

    run_linter_note = _get_note_by_type(client, "run-linter")
    _assert_note_uses_check_like_output(run_linter_note.body, "Linter results")
    assert (
        controller.get_merge_request_note_state("run-linter").stage_statuses["Linter"] == "failure"
    )
    assert not _has_note_by_type(client, "run-tests")
    assert not _has_note_by_type(client, "update-mr-environment")
    assert not _has_note_by_type(client, "gen-prod-plan")


def test_run_all_stops_after_tests_failure(make_gitlab_client, make_controller, mocker):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context._run_tests = mocker.MagicMock(side_effect=ValueError("tests exploded"))

    with pytest.raises(CICDBotError, match="Failed to run tests."):
        command._run_all(controller)

    assert len(client.notes) == 2

    run_linter_note = _get_note_by_type(client, "run-linter")
    _assert_note_uses_check_like_output(run_linter_note.body, "Linter results")
    assert (
        controller.get_merge_request_note_state("run-linter").stage_statuses["Linter"] == "success"
    )

    run_tests_note = _get_note_by_type(client, "run-tests")
    _assert_note_uses_check_like_output(run_tests_note.body, "Tests Failed")
    assert (
        controller.get_merge_request_note_state("run-tests").stage_statuses["Unit Tests"]
        == "failure"
    )

    assert not _has_note_by_type(client, "update-mr-environment")
    assert not _has_note_by_type(client, "gen-prod-plan")


def test_run_all_stops_before_prod_plan_on_mr_environment_failure(
    make_gitlab_client, make_controller, mocker
):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )
    controller.update_merge_request_environment = mocker.MagicMock(side_effect=ValueError("boom"))
    controller.get_merge_request_environment_summary = mocker.MagicMock(
        return_value="Failed summary"
    )
    controller.get_prod_plan_preview_summary = mocker.MagicMock(return_value="Should not run")

    with pytest.raises(CICDBotError, match="Failed to update merge request environment."):
        command._run_all(controller)

    assert len(client.notes) == 3

    mr_environment_note = _get_note_by_type(client, "update-mr-environment")
    _assert_note_uses_check_like_output(
        mr_environment_note.body, "MR Virtual Data Environment: hello_world_42"
    )
    assert (
        controller.get_merge_request_note_state("update-mr-environment").stage_statuses[
            "MR Environment"
        ]
        == "failure"
    )
    assert "Failed summary" in mr_environment_note.body

    assert not _has_note_by_type(client, "gen-prod-plan")
    controller.get_prod_plan_preview_summary.assert_not_called()


def test_run_linter_stage_updates_only_own_note(make_gitlab_client, make_controller):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    assert command._run_linter_stage(controller)

    assert len(client.notes) == 1
    note = _get_note_by_type(client, "run-linter")
    _assert_note_uses_check_like_output(note.body, "Linter results")
    assert (
        controller.get_merge_request_note_state("run-linter").stage_statuses["Linter"] == "success"
    )
    assert not _has_note_by_type(client, "run-tests")
    assert not _has_note_by_type(client, "update-mr-environment")
    assert not _has_note_by_type(client, "gen-prod-plan")


def test_run_linter_stage_surfaces_warning_details(make_gitlab_client, make_controller, mocker):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    mocker.patch.object(
        controller,
        "run_linter",
        side_effect=lambda: controller._console.log_warning("lint warning"),
    )

    assert command._run_linter_stage(controller)

    note = _get_note_by_type(client, "run-linter")
    _assert_note_uses_check_like_output(note.body, "Linter results")
    assert "lint warning" in note.body


def test_run_linter_stage_failure_updates_only_own_note(
    make_gitlab_client, make_controller, mocker
):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context.lint_models = mocker.MagicMock(side_effect=ValueError("lint exploded"))

    assert not command._run_linter_stage(controller)

    assert len(client.notes) == 1
    note = _get_note_by_type(client, "run-linter")
    _assert_note_uses_check_like_output(note.body, "Linter results")
    assert (
        controller.get_merge_request_note_state("run-linter").stage_statuses["Linter"] == "failure"
    )
    assert not _has_note_by_type(client, "run-tests")
    assert not _has_note_by_type(client, "update-mr-environment")
    assert not _has_note_by_type(client, "gen-prod-plan")


def test_run_linter_stage_failure_keeps_error_summary_and_traceback_details(
    make_gitlab_client, make_controller, mocker
):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    def _raise_linter_error() -> None:
        controller._console.log_warning("lint warning")
        raise ValueError("lint exploded")

    mocker.patch.object(controller, "run_linter", side_effect=_raise_linter_error)

    assert not command._run_linter_stage(controller)

    note = _get_note_by_type(client, "run-linter")
    _assert_note_uses_check_like_output(note.body, "Linter results")
    assert "lint warning" in note.body
    assert "ValueError: lint exploded" in note.body
    assert controller.get_merge_request_note_state("run-linter").details == {}


def test_run_tests_stage_surfaces_rendered_summary(make_gitlab_client, make_controller, mocker):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )
    mocker.patch.object(
        controller,
        "get_test_summary",
        return_value="**Successfully Ran `3` Tests Against `duckdb`**",
    )

    assert command._run_tests_stage(controller)

    note = _get_note_by_type(client, "run-tests")
    _assert_note_uses_check_like_output(note.body, "Tests Passed")
    assert "**Successfully Ran `3` Tests Against `duckdb`**" in note.body


def test_run_linter_stage_does_not_modify_existing_other_notes(make_gitlab_client, make_controller):
    run_tests_note = _make_typed_note(
        1,
        "run-tests",
        "## Run Tests\n\nExisting tests summary",
    )
    update_mr_environment_note = _make_typed_note(
        2,
        "update-mr-environment",
        "## Update MR Environment\n\nExisting MR summary",
    )
    gen_prod_plan_note = _make_typed_note(
        3,
        "gen-prod-plan",
        "## Generate Prod Plan\n\nExisting prod plan",
    )
    run_tests_body = run_tests_note.body
    update_mr_environment_body = update_mr_environment_note.body
    gen_prod_plan_body = gen_prod_plan_note.body
    client = make_gitlab_client([run_tests_note, update_mr_environment_note, gen_prod_plan_note])
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    assert command._run_linter_stage(controller)

    assert len(client.notes) == 4
    assert _get_note_by_type(client, "run-tests").body == run_tests_body
    assert _get_note_by_type(client, "update-mr-environment").body == update_mr_environment_body
    assert _get_note_by_type(client, "gen-prod-plan").body == gen_prod_plan_body


def test_run_tests_stage_updates_only_own_note(make_gitlab_client, make_controller, mocker):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )

    assert command._run_tests_stage(controller)

    assert len(client.notes) == 1
    note = _get_note_by_type(client, "run-tests")
    _assert_note_uses_check_like_output(note.body, "Tests Passed")
    assert (
        controller.get_merge_request_note_state("run-tests").stage_statuses["Unit Tests"]
        == "success"
    )
    assert not _has_note_by_type(client, "run-linter")
    assert not _has_note_by_type(client, "update-mr-environment")
    assert not _has_note_by_type(client, "gen-prod-plan")


def test_run_tests_stage_failure_updates_only_own_note(make_gitlab_client, make_controller, mocker):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context._run_tests = mocker.MagicMock(side_effect=ValueError("tests exploded"))

    assert not command._run_tests_stage(controller)

    assert len(client.notes) == 1
    note = _get_note_by_type(client, "run-tests")
    _assert_note_uses_check_like_output(note.body, "Tests Failed")
    assert (
        controller.get_merge_request_note_state("run-tests").stage_statuses["Unit Tests"]
        == "failure"
    )
    assert not _has_note_by_type(client, "run-linter")
    assert not _has_note_by_type(client, "update-mr-environment")
    assert not _has_note_by_type(client, "gen-prod-plan")


def test_run_tests_stage_failure_keeps_error_summary_and_traceback_details(
    make_gitlab_client, make_controller, mocker
):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context._run_tests = mocker.MagicMock(side_effect=ValueError("tests exploded"))

    assert not command._run_tests_stage(controller)

    note = _get_note_by_type(client, "run-tests")
    _assert_note_uses_check_like_output(note.body, "Tests Failed")
    assert "ValueError: tests exploded" in note.body
    assert controller.get_merge_request_note_state("run-tests").details == {}


def test_update_note_clears_stale_summary_when_stage_restarts(make_gitlab_client, make_controller):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    client.notes = [
        MockMergeRequestNote(
            1,
            controller.render_merge_request_note(
                note_type="run-tests",
                stage_statuses={command.UNIT_TESTS_STAGE: "failure"},
                summary="Old failure summary",
                details={command.UNIT_TESTS_STAGE: "Old traceback"},
            ),
        )
    ]

    command._update_note(
        controller,
        "run-tests",
        stage_statuses={command.UNIT_TESTS_STAGE: "in_progress"},
    )

    note = _get_note_by_type(client, "run-tests")
    _assert_note_uses_check_like_output(note.body, "Running Tests")
    assert "Old failure summary" not in note.body
    state = controller.get_merge_request_note_state("run-tests")
    assert state.summary == ""
    assert state.details == {}


def test_run_tests_stage_does_not_modify_existing_other_notes(
    make_gitlab_client, make_controller, mocker
):
    run_linter_note = _make_typed_note(
        1,
        "run-linter",
        "## Run Linter\n\nExisting linter details",
    )
    update_mr_environment_note = _make_typed_note(
        2,
        "update-mr-environment",
        "## Update MR Environment\n\nExisting MR summary",
    )
    gen_prod_plan_note = _make_typed_note(
        3,
        "gen-prod-plan",
        "## Generate Prod Plan\n\nExisting prod plan",
    )
    run_linter_body = run_linter_note.body
    update_mr_environment_body = update_mr_environment_note.body
    gen_prod_plan_body = gen_prod_plan_note.body
    client = make_gitlab_client([run_linter_note, update_mr_environment_note, gen_prod_plan_note])
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )

    assert command._run_tests_stage(controller)

    assert len(client.notes) == 4
    assert _get_note_by_type(client, "run-linter").body == run_linter_body
    assert _get_note_by_type(client, "update-mr-environment").body == update_mr_environment_body
    assert _get_note_by_type(client, "gen-prod-plan").body == gen_prod_plan_body


def test_update_mr_environment_success_updates_only_its_note(
    make_gitlab_client, make_controller, mocker
):
    run_linter_note = _make_typed_note(
        1,
        "run-linter",
        "## Run Linter\n\nExisting linter",
    )
    gen_prod_plan_note = _make_typed_note(
        2,
        "gen-prod-plan",
        "## Generate Prod Plan\n\nExisting prod plan",
    )
    run_linter_body = run_linter_note.body
    gen_prod_plan_body = gen_prod_plan_note.body
    client = make_gitlab_client([run_linter_note, gen_prod_plan_note])
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller.update_merge_request_environment = mocker.MagicMock(return_value="MR updated")

    assert command._update_mr_environment(controller)

    assert len(client.notes) == 3
    note = _get_note_by_type(client, "update-mr-environment")
    _assert_note_uses_check_like_output(note.body, "MR Virtual Data Environment: hello_world_42")
    assert (
        controller.get_merge_request_note_state("update-mr-environment").stage_statuses[
            "MR Environment"
        ]
        == "success"
    )
    assert "MR updated" in note.body
    assert _get_note_by_type(client, "run-linter").body == run_linter_body
    assert _get_note_by_type(client, "gen-prod-plan").body == gen_prod_plan_body


def test_update_mr_environment_skips_when_no_changes(make_gitlab_client, make_controller, mocker):
    prod_plan_note = _make_typed_note(
        1,
        "gen-prod-plan",
        "## Generate Prod Plan\n\nExisting prod plan",
    )
    prod_plan_body = prod_plan_note.body
    client = make_gitlab_client([prod_plan_note])
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller.update_merge_request_environment = mocker.MagicMock(
        side_effect=NoChangesPlanError("no changes")
    )
    controller.get_merge_request_environment_summary = mocker.MagicMock(
        return_value="No changes were detected compared to the prod environment."
    )

    assert command._update_mr_environment(controller)

    assert len(client.notes) == 2
    note = _get_note_by_type(client, "update-mr-environment")
    _assert_note_uses_check_like_output(note.body, "MR Virtual Data Environment: hello_world_42")
    assert (
        controller.get_merge_request_note_state("update-mr-environment").stage_statuses[
            "MR Environment"
        ]
        == "skipped"
    )
    assert "No changes were detected compared to the prod environment." in note.body
    assert _get_note_by_type(client, "gen-prod-plan").body == prod_plan_body


def test_update_mr_environment_failure_does_not_modify_existing_prod_preview(
    make_gitlab_client, make_controller, mocker
):
    update_mr_environment_note = _make_typed_note(
        1,
        "update-mr-environment",
        "## Update MR Environment\n\nOld MR summary",
    )
    gen_prod_plan_note = _make_typed_note(
        2,
        "gen-prod-plan",
        "## Generate Prod Plan\n\nOld prod plan",
    )
    gen_prod_plan_body = gen_prod_plan_note.body
    client = make_gitlab_client([update_mr_environment_note, gen_prod_plan_note])
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller.update_merge_request_environment = mocker.MagicMock(side_effect=ValueError("boom"))
    controller.get_merge_request_environment_summary = mocker.MagicMock(
        return_value="Failed summary"
    )

    assert not command._update_mr_environment(controller)

    mr_environment_note = _get_note_by_type(client, "update-mr-environment")
    _assert_note_uses_check_like_output(
        mr_environment_note.body, "MR Virtual Data Environment: hello_world_42"
    )
    assert (
        controller.get_merge_request_note_state("update-mr-environment").stage_statuses[
            "MR Environment"
        ]
        == "failure"
    )
    assert "Failed summary" in mr_environment_note.body

    assert _get_note_by_type(client, "gen-prod-plan").body == gen_prod_plan_body


def test_update_mr_environment_skips_when_newer_different_note_type_exists(
    monkeypatch: pytest.MonkeyPatch, make_gitlab_client, make_controller
):
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    prod_plan_note = _make_typed_note(
        1,
        "gen-prod-plan",
        "## Generate Prod Plan\n\nExisting prod plan",
        pipeline_id=11,
    )
    prod_plan_body = prod_plan_note.body
    client = make_gitlab_client([prod_plan_note])
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    assert command._update_mr_environment(controller)

    assert len(client.notes) == 2
    mr_environment_note = _get_note_by_type(client, "update-mr-environment")
    _assert_note_uses_check_like_output(
        mr_environment_note.body, "MR Virtual Data Environment: hello_world_42"
    )
    assert (
        controller.get_merge_request_note_state("update-mr-environment").stage_statuses[
            "MR Environment"
        ]
        == "skipped"
    )
    assert _get_note_by_type(client, "gen-prod-plan").body == prod_plan_body
    assert not controller._context.apply.called


def test_update_mr_environment_skips_stale_pipeline_for_same_note_type(
    monkeypatch: pytest.MonkeyPatch, make_gitlab_client, make_controller
):
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    mr_environment_note = _make_typed_note(
        1,
        "update-mr-environment",
        "## Update MR Environment\n\nExisting MR summary",
        pipeline_id=11,
    )
    mr_environment_body = mr_environment_note.body
    client = make_gitlab_client([mr_environment_note])
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    assert command._update_mr_environment(controller)

    assert len(client.notes) == 1
    assert _get_note_by_type(client, "update-mr-environment").body == mr_environment_body
    assert not controller._context.apply.called


def test_gen_prod_plan_updates_only_plan_preview_note(make_gitlab_client, make_controller, mocker):
    run_linter_note = _make_typed_note(
        1,
        "run-linter",
        "## Run Linter\n\nExisting linter",
    )
    mr_environment_note = _make_typed_note(
        2,
        "update-mr-environment",
        "## Update MR Environment\n\nExisting MR summary",
    )
    run_linter_body = run_linter_note.body
    mr_environment_body = mr_environment_note.body
    client = make_gitlab_client([run_linter_note, mr_environment_note])
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    mocker.patch.object(
        controller, "get_prod_plan_preview_summary", return_value="No changes to apply."
    )

    assert command._gen_prod_plan(controller)

    assert _get_note_by_type(client, "run-linter").body == run_linter_body
    assert _get_note_by_type(client, "update-mr-environment").body == mr_environment_body

    prod_plan_note = _get_note_by_type(client, "gen-prod-plan")
    _assert_note_uses_check_like_output(prod_plan_note.body, "Prod Plan Preview")
    assert (
        controller.get_merge_request_note_state("gen-prod-plan").stage_statuses["Prod Plan Preview"]
        == "success"
    )
    assert "No changes to apply." in prod_plan_note.body
