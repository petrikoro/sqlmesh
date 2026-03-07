# type: ignore
from unittest.result import TestResult

import pytest

from sqlmesh.integrations.gitlab.cicd import command
from sqlmesh.utils.errors import CICDBotError, LinterError, NoChangesPlanError
from tests.integrations.gitlab.cicd.conftest import MockMergeRequestNote

TestResult.__test__ = False

pytestmark = pytest.mark.gitlab


def test_run_all_updates_single_sticky_note(make_gitlab_client, make_controller, mocker):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )

    command._run_all(controller)

    assert len(client.notes) == 1
    assert len(client.created_notes) == 1
    assert len(client.updated_notes) > 0
    assert "## Pipeline Status" in client.notes[0].body
    assert "MR Environment Summary" in client.notes[0].body
    assert "Prod Plan Preview" in client.notes[0].body
    assert (
        "Dates loaded in MR" in client.notes[0].body
        or "No models were modified in this MR" in client.notes[0].body
    )


def test_run_all_marks_remaining_stages_skipped_on_linter_failure(
    make_gitlab_client, make_controller, mocker
):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context.lint_models = mocker.MagicMock(side_effect=LinterError("lint failed"))

    with pytest.raises(CICDBotError, match="Linter failed"):
        command._run_all(controller)

    assert len(client.notes) == 1
    assert "**Linter:** failure" in client.notes[0].body
    assert "**Unit Tests:** skipped" in client.notes[0].body
    assert "**MR Environment:** skipped" in client.notes[0].body
    assert "**Prod Plan Preview:** skipped" in client.notes[0].body


def test_run_linter_stage_updates_sticky_note(make_gitlab_client, make_controller):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    assert command._run_linter_stage(controller)

    assert len(client.notes) == 1
    assert "**Linter:** success" in client.notes[0].body


def test_run_linter_stage_surfaces_warning_details(make_gitlab_client, make_controller, mocker):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    mocker.patch.object(
        controller,
        "run_linter",
        side_effect=lambda: controller._console.log_warning("lint warning"),
    )

    assert command._run_linter_stage(controller)

    assert "**Linter:** success" in client.notes[0].body
    assert "lint warning" in client.notes[0].body


def test_run_linter_stage_failure_skips_downstream_stages(
    make_gitlab_client, make_controller, mocker
):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context.lint_models = mocker.MagicMock(side_effect=ValueError("lint exploded"))

    assert not command._run_linter_stage(controller)

    assert "**Linter:** failure" in client.notes[0].body
    assert "**Unit Tests:** skipped" in client.notes[0].body
    assert "**MR Environment:** skipped" in client.notes[0].body
    assert "**Prod Plan Preview:** skipped" in client.notes[0].body
    assert "MR Environment Summary" not in client.notes[0].body
    assert "Prod Plan Preview</summary>" not in client.notes[0].body


def test_run_tests_stage_updates_sticky_note(make_gitlab_client, make_controller, mocker):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )

    assert command._run_tests_stage(controller)

    assert len(client.notes) == 1
    assert "**Unit Tests:** success" in client.notes[0].body


def test_update_mr_environment_skips_when_no_changes(make_gitlab_client, make_controller, mocker):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller.update_merge_request_environment = mocker.MagicMock(
        side_effect=NoChangesPlanError("no changes")
    )
    controller.get_merge_request_environment_summary = mocker.MagicMock(
        return_value="No changes were detected compared to the prod environment."
    )

    command._update_mr_environment(controller)

    assert len(client.notes) == 1
    assert "**MR Environment:** skipped" in client.notes[0].body
    assert "**Linter:** queued" in client.notes[0].body
    assert "**Unit Tests:** queued" in client.notes[0].body


def test_update_mr_environment_failure_clears_stale_prod_preview(
    make_gitlab_client, make_controller, mocker
):
    client = make_gitlab_client(
        [
            mocker.MagicMock(
                id=1,
                body="""<!-- sqlmesh-gitlab-bot-note -->
:robot: **SQLMesh Bot Info** :robot:
- Merge request: `group/hello-world!42`
- Merge request URL: https://gitlab.com/group/hello-world/-/merge_requests/42
- MR environment: `hello_world_42`

## Pipeline Status
- :white_check_mark: **Linter:** success
- :white_check_mark: **Unit Tests:** success
- :white_check_mark: **MR Environment:** success
- :white_check_mark: **Prod Plan Preview:** success

<details>
  <summary>:ship: Prod Plan Preview</summary>

Old prod plan
</details>""",
            )
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    controller.update_merge_request_environment = mocker.MagicMock(side_effect=ValueError("boom"))
    controller.get_merge_request_environment_summary = mocker.MagicMock(
        return_value="Failed summary"
    )

    assert not command._update_mr_environment(controller)

    assert "**MR Environment:** failure" in client.notes[0].body
    assert "**Prod Plan Preview:** skipped" in client.notes[0].body
    assert "<summary>:ship: Prod Plan Preview</summary>" not in client.notes[0].body


def test_update_mr_environment_skips_stale_pipeline(
    monkeypatch: pytest.MonkeyPatch, make_gitlab_client, make_controller
):
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    client = make_gitlab_client(
        [
            MockMergeRequestNote(
                1,
                """<!-- sqlmesh-gitlab-bot-note -->
<!-- sqlmesh-gitlab-pipeline-id:11 -->
:robot: **SQLMesh Bot Info** :robot:
- Merge request: `group/hello-world!42`
- Merge request URL: https://gitlab.com/group/hello-world/-/merge_requests/42
- MR environment: `hello_world_42`

## Pipeline Status
- :white_check_mark: **Linter:** success
- :white_check_mark: **Unit Tests:** success
- :white_check_mark: **MR Environment:** success
- :white_check_mark: **Prod Plan Preview:** success""",
            )
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    assert command._update_mr_environment(controller)

    assert "**MR Environment:** success" in client.notes[0].body
    assert not controller._context.apply.called


def test_gen_prod_plan_preserves_existing_stage_statuses(
    make_gitlab_client, make_controller, mocker
):
    existing_note = make_gitlab_client(
        [
            mocker.MagicMock(
                id=1,
                body="""<!-- sqlmesh-gitlab-bot-note -->
:robot: **SQLMesh Bot Info** :robot:
- Merge request: `group/hello-world!42`
- Merge request URL: https://gitlab.com/group/hello-world/-/merge_requests/42
- MR environment: `hello_world_42`

## Pipeline Status
- :white_check_mark: **Linter:** success
- :x: **Unit Tests:** failure
- :hourglass_flowing_sand: **MR Environment:** queued
- :hourglass_flowing_sand: **Prod Plan Preview:** queued""",
            )
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", existing_note)
    mocker.patch.object(controller, "get_plan_summary", return_value="No changes to apply.")

    command._gen_prod_plan(controller)

    assert "**Linter:** success" in existing_note.notes[0].body
    assert "**Unit Tests:** failure" in existing_note.notes[0].body
    assert "**MR Environment:** queued" in existing_note.notes[0].body
    assert "**Prod Plan Preview:** success" in existing_note.notes[0].body
