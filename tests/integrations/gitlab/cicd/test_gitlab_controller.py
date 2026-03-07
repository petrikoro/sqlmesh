import pathlib
import typing as t

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot.errors import SqlglotError

from sqlmesh.cicd.config import CICDBotConfig
from sqlmesh.core.config import Config, ModelDefaultsConfig, load_config_from_paths
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig, MergeMethod
from sqlmesh.integrations.gitlab.cicd.config import GitLabCICDBotConfig
from sqlmesh.integrations.gitlab.cicd.controller import RequestsGitLabAPIClient
from sqlmesh.utils.errors import CICDBotError, NotFoundError
from tests.integrations.gitlab.cicd.conftest import MockGitLabClient, MockMergeRequestNote
from tests.utils.test_filesystem import create_temp_file

pytestmark = pytest.mark.gitlab

NOTE_TYPE_MARKER_PREFIX = "<!-- sqlmesh-gitlab-note-type:"


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


@pytest.mark.parametrize(
    "existing_notes, expected_notes_count, expected_created_count, expected_updated_count",
    [
        ([], 1, 1, 0),
        (
            [
                _make_typed_note(
                    1,
                    "run-linter",
                    "## Run Linter\nOld merge request note",
                )
            ],
            1,
            0,
            1,
        ),
    ],
)
def test_upsert_sqlmesh_mr_note(
    existing_notes: t.List[MockMergeRequestNote],
    expected_notes_count: int,
    expected_created_count: int,
    expected_updated_count: int,
    make_gitlab_client,
    make_controller,
):
    client = make_gitlab_client(existing_notes)
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    note = controller.upsert_sqlmesh_mr_note("run-linter", "## Run Linter\nNew merge request note")

    assert "<!-- sqlmesh-gitlab-bot-note -->" in note.body
    assert f"{NOTE_TYPE_MARKER_PREFIX}run-linter -->" in note.body
    assert note.body.endswith("New merge request note")
    assert len(client.notes) == expected_notes_count
    assert len(client.created_notes) == expected_created_count
    assert len(client.updated_notes) == expected_updated_count


def test_upsert_sqlmesh_mr_note_skips_older_pipeline_update(
    monkeypatch: pytest.MonkeyPatch, make_gitlab_client, make_controller
):
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    client = make_gitlab_client(
        [
            _make_typed_note(
                1,
                "update-mr-environment",
                "## Update MR Environment\nExisting note",
                pipeline_id=11,
            )
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    note = controller.upsert_sqlmesh_mr_note(
        "update-mr-environment",
        "## Update MR Environment\nOlder note",
    )

    assert note.body.endswith("Existing note")
    assert len(client.updated_notes) == 0
    assert len(client.created_notes) == 0


def test_upsert_sqlmesh_mr_note_ignores_newer_other_note_type(
    monkeypatch: pytest.MonkeyPatch, make_gitlab_client, make_controller
):
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    client = make_gitlab_client(
        [
            _make_typed_note(
                1,
                "gen-prod-plan",
                "## Generate Prod Plan\nExisting note",
                pipeline_id=11,
            )
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    note = controller.upsert_sqlmesh_mr_note(
        "update-mr-environment",
        "## Update MR Environment\nNew note",
    )

    assert note.body.endswith("New note")
    assert len(client.updated_notes) == 0
    assert len(client.created_notes) == 1


def test_has_newer_pipeline_note_ignores_unknown_typed_note(
    monkeypatch: pytest.MonkeyPatch, make_gitlab_client, make_controller
):
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    client = make_gitlab_client(
        [
            MockMergeRequestNote(
                1,
                "\n".join(
                    [
                        "<!-- sqlmesh-gitlab-bot-note -->",
                        f"{NOTE_TYPE_MARKER_PREFIX}experimental-note -->",
                        "<!-- sqlmesh-gitlab-pipeline-id:11 -->",
                        "## Experimental Note",
                        "Ignore me",
                    ]
                ),
            )
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    assert not controller.has_newer_pipeline_note()


def test_upsert_sqlmesh_mr_note_recreates_missing_note(make_controller):
    class MissingNoteOnUpdateClient(MockGitLabClient):
        def update_merge_request_note(
            self, project_id: int, merge_request_iid: int, note_id: int, body: str
        ) -> MockMergeRequestNote:
            self.notes = [note for note in self.notes if note.id != note_id]
            raise NotFoundError("GitLab note no longer exists.")

    client = MissingNoteOnUpdateClient(
        [
            _make_typed_note(
                1,
                "run-linter",
                "## Run Linter\nOld merge request note",
            )
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    note = controller.upsert_sqlmesh_mr_note("run-linter", "## Run Linter\nNew merge request note")

    assert f"{NOTE_TYPE_MARKER_PREFIX}run-linter -->" in note.body
    assert note.body.endswith("New merge request note")
    assert len(client.notes) == 1
    assert len(client.created_notes) == 1
    assert len(client.updated_notes) == 0


def test_get_sqlmesh_mr_note_deletes_duplicate_bot_notes(make_gitlab_client, make_controller):
    client = make_gitlab_client(
        [
            _make_typed_note(
                1,
                "run-linter",
                "## Run Linter\nOld",
            ),
            _make_typed_note(
                2,
                "run-linter",
                "## Run Linter\nNew",
            ),
            _make_typed_note(
                3,
                "update-mr-environment",
                "## Update MR Environment\nLeave me alone",
            ),
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    note = controller._get_sqlmesh_mr_note("run-linter")

    assert note is not None
    assert note.id == 2
    assert client.deleted_note_ids == [1]
    assert len(client.notes) == 2
    assert any("Leave me alone" in note.body for note in client.notes)


def test_get_sqlmesh_mr_note_prefers_newest_pipeline_marker(make_gitlab_client, make_controller):
    client = make_gitlab_client(
        [
            _make_typed_note(
                1,
                "gen-prod-plan",
                "## Generate Prod Plan\nNewer pipeline note",
                pipeline_id=11,
            ),
            _make_typed_note(
                2,
                "gen-prod-plan",
                "## Generate Prod Plan\nOlder pipeline note",
                pipeline_id=10,
            ),
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    note = controller._get_sqlmesh_mr_note("gen-prod-plan")

    assert note is not None
    assert note.id == 1
    assert client.deleted_note_ids == [2]


def test_upsert_sqlmesh_mr_note_deletes_untyped_note(make_gitlab_client, make_controller):
    client = make_gitlab_client(
        [
            MockMergeRequestNote(
                1,
                "<!-- sqlmesh-gitlab-bot-note -->\n## Run Linter\nUntyped note",
            )
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    note = controller.upsert_sqlmesh_mr_note(
        "update-mr-environment",
        "## Update MR Environment\nNew typed note",
    )

    assert len(client.notes) == 1
    assert client.deleted_note_ids == [1]
    assert f"{NOTE_TYPE_MARKER_PREFIX}update-mr-environment -->" in note.body


def test_upsert_sqlmesh_mr_note_keeps_notes_of_different_type(make_gitlab_client, make_controller):
    client = make_gitlab_client(
        [
            MockMergeRequestNote(
                1,
                "\n".join(
                    [
                        "<!-- sqlmesh-gitlab-bot-note -->",
                        f"{NOTE_TYPE_MARKER_PREFIX}run-linter -->",
                        "## Run Linter",
                        "Existing note",
                    ]
                ),
            )
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    note = controller.upsert_sqlmesh_mr_note(
        "update-mr-environment",
        "## Update MR Environment\nNew typed note",
    )

    assert len(client.notes) == 2
    assert client.deleted_note_ids == []
    assert f"{NOTE_TYPE_MARKER_PREFIX}update-mr-environment -->" in note.body


def test_get_mr_environment_summary_uses_mr_wording(make_gitlab_client, make_controller):
    controller = make_controller(
        "tests/fixtures/gitlab/merge_request_open.json",
        make_gitlab_client(),
        mock_out_context=False,
    )

    summary = controller.get_merge_request_environment_summary()

    assert "MR environment" in summary
    assert "Dates loaded in MR" in summary or "No models were modified in this MR" in summary


def test_get_mr_environment_summary_references_gen_prod_plan_output(
    make_gitlab_client, make_controller, mocker
):
    controller = make_controller(
        "tests/fixtures/gitlab/merge_request_open.json",
        make_gitlab_client(),
        config=Config(
            model_defaults=ModelDefaultsConfig(dialect="duckdb"),
            cicd_bot=GitLabCICDBotConfig(skip_pr_backfill=True),
        ),
    )
    fake_plan = mocker.MagicMock()
    fake_plan.has_changes = True
    fake_plan.user_provided_flags = {}
    controller._prod_plan_with_gaps_builder = mocker.MagicMock()
    controller._prod_plan_with_gaps_builder.build.return_value = fake_plan
    mocker.patch(
        "sqlmesh.integrations.gitlab.cicd.controller.generate_request_environment_summary_list",
        return_value="",
    )

    summary = controller._get_merge_request_environment_summary_success()

    assert "`Prod Plan Preview` output from `gen-prod-plan`" in summary
    assert "`Prod Plan Preview` note" not in summary


def test_get_prod_plan_preview_summary_reuses_shared_plan_summary(
    make_gitlab_client, make_controller, mocker
):
    controller = make_controller(
        "tests/fixtures/gitlab/merge_request_open.json", make_gitlab_client()
    )
    rich_plan_summary = """**Directly Modified:**
* `memory.sushi.waiter_revenue_by_day` (Non-breaking)
  
  ```diff
  - old_line
  + new_line
  ```"""
    mocker.patch.object(controller, "get_plan_summary", return_value=rich_plan_summary)

    summary = controller.get_prod_plan_preview_summary(mocker.MagicMock())

    assert summary == (
        "This is a preview that shows the differences between this MR environment "
        "`hello_world_42` and `prod`.\n\n"
        "These are the changes that would be deployed.\n\n"
        f"{rich_plan_summary}"
    )


def test_render_merge_request_note_round_trips_rich_markdown_sections(
    make_gitlab_client, make_controller
):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    rich_summary = """**Directly Modified:**
* `memory.sushi.waiter_revenue_by_day` (Non-breaking)

> [!IMPORTANT]
> Review the diff before deploying.

| Item | Value |
| --- | --- |
| Diff | Rich |

```diff
- old_line
+ new_line
```"""
    rich_details = """**Plan diagnostics**

### Added
- `memory.sushi.orders` (Breaking)

<details>

<summary>Plan flags</summary>

- `skip_tests` = `True`

</details>"""

    note = controller.render_merge_request_note(
        note_type="gen-prod-plan",
        stage_statuses={"Prod Plan Preview": "success"},
        summary=rich_summary,
        details={"Prod Plan Preview": rich_details},
    )
    client.notes = [MockMergeRequestNote(1, note)]

    _assert_note_uses_check_like_output(note, "Prod Plan Preview")
    assert rich_summary in note

    state = controller.get_merge_request_note_state("gen-prod-plan")

    assert state.summary == rich_summary
    assert state.details == {"Prod Plan Preview": rich_details}


def test_render_merge_request_note_persists_hidden_state_without_details(
    make_gitlab_client, make_controller
):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    summary = "Canonical hidden summary"
    note = controller.render_merge_request_note(
        note_type="run-tests",
        stage_statuses={"Unit Tests": "success"},
        summary=summary,
    )

    hidden_state = controller._extract_note_state_marker(note)

    assert hidden_state is not None
    assert hidden_state.stage_statuses == {"Unit Tests": "success"}
    assert hidden_state.summary == summary
    assert hidden_state.details == {}

    client.notes = [MockMergeRequestNote(1, note.replace(summary, "Visible summary", 1))]

    state = controller.get_merge_request_note_state("run-tests")

    assert state.stage_statuses["Unit Tests"] == "success"
    assert state.summary == summary
    assert state.details == {}


def test_get_merge_request_note_state_uses_state_marker(make_gitlab_client, make_controller):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    note_body = controller.render_merge_request_note(
        note_type="run-tests",
        stage_statuses={"Unit Tests": "success"},
        summary="Hidden summary",
        details={"Unit Tests": "Hidden details"},
    )
    client.notes = [MockMergeRequestNote(1, note_body)]

    state = controller.get_merge_request_note_state("run-tests")

    assert state.stage_statuses["Unit Tests"] == "success"
    assert state.summary == "Hidden summary"
    assert state.details == {"Unit Tests": "Hidden details"}


def test_get_merge_request_note_state_falls_back_when_hidden_state_is_truncated(
    make_gitlab_client, make_controller, mocker
):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    mocker.patch.object(controller, "MAX_NOTE_LENGTH", 350)
    summary = "Truncated summaries should stay visible."
    details = {"Prod Plan Preview": "x" * 800}

    controller.upsert_sqlmesh_mr_note(
        "gen-prod-plan",
        controller.render_merge_request_note(
            note_type="gen-prod-plan",
            stage_statuses={"Prod Plan Preview": "success"},
            summary=summary,
            details=details,
        ),
    )

    note = client.notes[0]
    _assert_note_uses_check_like_output(note.body, "Prod Plan Preview")
    assert summary in note.body

    state = controller.get_merge_request_note_state("gen-prod-plan")

    assert state.stage_statuses["Prod Plan Preview"] == "success"
    assert state.summary == summary
    assert state.details == {}


def test_get_merge_request_note_state_recovers_truncated_visible_summary(
    make_gitlab_client, make_controller, mocker
):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    mocker.patch.object(controller, "MAX_NOTE_LENGTH", 260)
    summary = "Summary chunk " * 40

    controller.upsert_sqlmesh_mr_note(
        "gen-prod-plan",
        controller.render_merge_request_note(
            note_type="gen-prod-plan",
            stage_statuses={"Prod Plan Preview": "success"},
            summary=summary,
        ),
    )

    note = client.notes[0]
    _assert_note_uses_check_like_output(note.body, "Prod Plan Preview")
    assert controller.BOT_NOTE_SUMMARY_END_MARKER not in note.body

    state = controller.get_merge_request_note_state("gen-prod-plan")

    assert state.stage_statuses["Prod Plan Preview"] == "success"
    assert state.summary
    assert state.summary.startswith("Summary chunk")


def test_get_merge_request_note_state_ignores_partial_summary_end_marker(
    make_gitlab_client, make_controller
):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    note = controller.render_merge_request_note(
        note_type="gen-prod-plan",
        stage_statuses={"Prod Plan Preview": "success"},
        summary="Summary chunk " * 20,
    )
    summary_end_marker_index = note.index(controller.BOT_NOTE_SUMMARY_END_MARKER)
    truncated_note = note[: summary_end_marker_index + 12]
    client.notes = [MockMergeRequestNote(1, truncated_note)]

    state = controller.get_merge_request_note_state("gen-prod-plan")

    assert state.stage_statuses["Prod Plan Preview"] == "success"
    assert state.summary.startswith("Summary chunk")
    assert "<!--" not in state.summary


@pytest.mark.parametrize(
    ("note_type", "stage", "detail_key", "detail_value", "summary"),
    [
        ("run-linter", "Linter", "Linter", "First line\nSecond line", ""),
        ("run-tests", "Unit Tests", "Unit Tests", "Test details", "Test summary"),
        (
            "update-mr-environment",
            "MR Environment",
            "MR Environment",
            "Warning details",
            "MR summary",
        ),
        (
            "gen-prod-plan",
            "Prod Plan Preview",
            "Prod Plan Preview",
            "Plan details",
            "Plan summary",
        ),
    ],
)
def test_get_merge_request_note_state_parses_typed_note(
    note_type: str,
    stage: str,
    detail_key: str,
    detail_value: str,
    summary: str,
    make_gitlab_client,
    make_controller,
):
    client = make_gitlab_client()
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)
    note = controller.render_merge_request_note(
        note_type=note_type,
        stage_statuses={stage: "success"},
        details={detail_key: detail_value},
        summary=summary,
    )
    client.notes = [MockMergeRequestNote(1, note)]

    state = controller.get_merge_request_note_state(note_type)

    expected_title = {
        "run-linter": "Linter results",
        "run-tests": "Tests Passed",
        "update-mr-environment": "MR Virtual Data Environment: hello_world_42",
        "gen-prod-plan": "Prod Plan Preview",
    }[note_type]
    _assert_note_uses_check_like_output(note, expected_title)
    assert state.stage_statuses[stage] == "success"
    assert state.details == {detail_key: detail_value}
    assert state.summary == summary


def test_list_merge_request_notes_fetches_all_pages(mocker: MockerFixture):
    session = mocker.MagicMock()
    first_response = mocker.MagicMock()
    first_response.ok = True
    first_response.json.return_value = [{"id": 1, "body": "first page"}]
    first_response.headers = {"X-Next-Page": "2"}
    second_response = mocker.MagicMock()
    second_response.ok = True
    second_response.json.return_value = [{"id": 2, "body": "second page"}]
    second_response.headers = {"X-Next-Page": ""}
    session.request.side_effect = [first_response, second_response]

    client = RequestsGitLabAPIClient(
        api_v4_url="https://gitlab.example.com/api/v4",
        token="abc",
        session=session,
    )

    notes = client.list_merge_request_notes(project_id=1, merge_request_iid=2)

    assert [note.id for note in notes] == [1, 2]
    assert session.request.call_args_list[0].kwargs["params"] == {"page": 1, "per_page": 100}
    assert session.request.call_args_list[1].kwargs["params"] == {"page": 2, "per_page": 100}


def test_list_merge_request_notes_ignores_extra_api_fields(mocker: MockerFixture):
    session = mocker.MagicMock()
    response = mocker.MagicMock()
    response.ok = True
    response.json.return_value = [
        {
            "id": 1,
            "body": "bot note",
            "type": None,
            "attachment": None,
            "author": {"id": 99, "username": "sqlmesh-bot"},
            "created_at": "2026-03-07T00:00:00.000Z",
            "updated_at": "2026-03-07T00:00:00.000Z",
            "system": False,
            "resolvable": False,
            "confidential": False,
            "internal": False,
            "noteable_id": 2,
            "noteable_iid": 3,
            "noteable_type": "MergeRequest",
            "project_id": 1,
        }
    ]
    response.headers = {"X-Next-Page": ""}
    session.request.return_value = response

    client = RequestsGitLabAPIClient(
        api_v4_url="https://gitlab.example.com/api/v4",
        token="abc",
        session=session,
    )

    notes = client.list_merge_request_notes(project_id=1, merge_request_iid=2)

    assert len(notes) == 1
    assert notes[0].id == 1
    assert notes[0].body == "bot note"


def test_update_merge_request_note_raises_not_found_for_404(mocker: MockerFixture):
    session = mocker.MagicMock()
    response = mocker.MagicMock()
    response.ok = False
    response.status_code = 404
    response.text = "404 Note Not Found"
    session.request.return_value = response

    client = RequestsGitLabAPIClient(
        api_v4_url="https://gitlab.example.com/api/v4",
        token="abc",
        session=session,
    )

    with pytest.raises(NotFoundError, match="404 Note Not Found"):
        client.update_merge_request_note(
            project_id=1, merge_request_iid=2, note_id=3, body="updated"
        )


def test_server_url_override_updates_merge_request_link(make_gitlab_client, make_controller):
    controller = make_controller(
        "tests/fixtures/gitlab/merge_request_open.json",
        make_gitlab_client(),
        config=Config(
            model_defaults=ModelDefaultsConfig(dialect="duckdb"),
            cicd_bot=GitLabCICDBotConfig(server_url="https://gitlab.internal.example"),
        ),
    )

    assert controller.server_url == "https://gitlab.internal.example"
    assert controller.api_v4_url == "https://gitlab.internal.example/api/v4"
    assert (
        controller.merge_request_url
        == "https://gitlab.internal.example/group/hello-world/-/merge_requests/42"
    )


def test_gitlab_controller_converts_legacy_shared_bot_config(
    tmp_path, make_gitlab_client, make_controller
):
    create_temp_file(
        tmp_path,
        pathlib.Path("config.yaml"),
        """
cicd_bot:
  pr_environment_name: shared_env
model_defaults:
  dialect: duckdb
""",
    )
    config = load_config_from_paths(Config, project_paths=[tmp_path / "config.yaml"])

    controller = make_controller(
        "tests/fixtures/gitlab/merge_request_open.json",
        make_gitlab_client(),
        config=config,
    )

    assert controller.bot_config.type_ == "gitlab"
    assert controller.bot_config.pr_environment_name == "shared_env"


def test_gitlab_controller_accepts_public_legacy_cicd_bot_config(
    make_gitlab_client, make_controller
):
    controller = make_controller(
        "tests/fixtures/gitlab/merge_request_open.json",
        make_gitlab_client(),
        config=Config(
            model_defaults=ModelDefaultsConfig(dialect="duckdb"),
            cicd_bot=CICDBotConfig(pr_environment_name="shared_env"),
        ),
    )

    assert controller.bot_config.type_ == "gitlab"
    assert controller.bot_config.pr_environment_name == "shared_env"


def test_gitlab_controller_default_bot_config_inherits_context_auto_categorization(
    make_gitlab_client, make_controller
):
    controller = make_controller(
        "tests/fixtures/gitlab/merge_request_open.json",
        make_gitlab_client(),
    )

    assert isinstance(controller.bot_config, GitLabCICDBotConfig)
    assert (
        controller.bot_config.auto_categorize_changes == controller._context.auto_categorize_changes
    )


def test_gitlab_controller_rejects_github_specific_bot_config(make_gitlab_client, make_controller):
    with pytest.raises(CICDBotError, match="GitHub-only `cicd_bot` options"):
        make_controller(
            "tests/fixtures/gitlab/merge_request_open.json",
            make_gitlab_client(),
            config=Config(
                model_defaults=ModelDefaultsConfig(dialect="duckdb"),
                cicd_bot=GithubCICDBotConfig(
                    enable_deploy_command=True,
                    merge_method=MergeMethod.SQUASH,
                ),
            ),
        )


def test_gitlab_controller_rejects_legacy_config_with_github_only_options(
    make_gitlab_client, make_controller
):
    with pytest.raises(CICDBotError, match="GitHub-only `cicd_bot` options"):
        make_controller(
            "tests/fixtures/gitlab/merge_request_open.json",
            make_gitlab_client(),
            config=Config(
                model_defaults=ModelDefaultsConfig(dialect="duckdb"),
                cicd_bot=CICDBotConfig(
                    enable_deploy_command=True,
                    merge_method=MergeMethod.SQUASH,
                ),
            ),
        )


def test_gitlab_controller_rejects_explicit_github_bot_config(make_gitlab_client, make_controller):
    with pytest.raises(CICDBotError, match="GitHub `cicd_bot` config"):
        make_controller(
            "tests/fixtures/gitlab/merge_request_open.json",
            make_gitlab_client(),
            config=Config(
                model_defaults=ModelDefaultsConfig(dialect="duckdb"),
                cicd_bot=GithubCICDBotConfig(pr_environment_name="shared_env"),
            ),
        )


def test_merge_request_summary_formats_sqlglot_error(make_gitlab_client, make_controller):
    controller = make_controller(
        "tests/fixtures/gitlab/merge_request_open.json", make_gitlab_client()
    )

    summary = controller.get_merge_request_environment_summary(exception=SqlglotError("bad sql"))

    assert summary == "**Error:** bad sql"
