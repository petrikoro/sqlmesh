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
from sqlmesh.utils.errors import CICDBotError
from tests.integrations.gitlab.cicd.conftest import MockMergeRequestNote
from tests.utils.test_filesystem import create_temp_file

pytestmark = pytest.mark.gitlab


@pytest.mark.parametrize(
    "existing_notes, expected_notes_count, expected_created_count, expected_updated_count",
    [
        ([], 1, 1, 0),
        (
            [
                MockMergeRequestNote(
                    1,
                    "<!-- sqlmesh-gitlab-bot-note -->\n:robot: **SQLMesh Bot Info** :robot:\nOld merge request note",
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

    note = controller.upsert_sqlmesh_mr_note(
        ":robot: **SQLMesh Bot Info** :robot:\nNew merge request note"
    )

    assert (
        note.body
        == "<!-- sqlmesh-gitlab-bot-note -->\n:robot: **SQLMesh Bot Info** :robot:\nNew merge request note"
    )
    assert len(client.notes) == expected_notes_count
    assert len(client.created_notes) == expected_created_count
    assert len(client.updated_notes) == expected_updated_count


def test_upsert_sqlmesh_mr_note_skips_older_pipeline_update(
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
Existing note""",
            )
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    note = controller.upsert_sqlmesh_mr_note(":robot: **SQLMesh Bot Info** :robot:\nOlder note")

    assert note.body.endswith("Existing note")
    assert len(client.updated_notes) == 0


def test_get_sqlmesh_mr_note_deletes_duplicate_bot_notes(make_gitlab_client, make_controller):
    client = make_gitlab_client(
        [
            MockMergeRequestNote(
                1,
                "<!-- sqlmesh-gitlab-bot-note -->\n:robot: **SQLMesh Bot Info** :robot:\nOld",
            ),
            MockMergeRequestNote(
                2,
                "<!-- sqlmesh-gitlab-bot-note -->\n:robot: **SQLMesh Bot Info** :robot:\nNew",
            ),
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    note = controller._get_sqlmesh_mr_note()

    assert note is not None
    assert note.id == 2
    assert client.deleted_note_ids == [1]


def test_get_sqlmesh_mr_note_prefers_newest_pipeline_marker(make_gitlab_client, make_controller):
    client = make_gitlab_client(
        [
            MockMergeRequestNote(
                1,
                """<!-- sqlmesh-gitlab-bot-note -->
<!-- sqlmesh-gitlab-pipeline-id:11 -->
:robot: **SQLMesh Bot Info** :robot:
Newer pipeline note""",
            ),
            MockMergeRequestNote(
                2,
                """<!-- sqlmesh-gitlab-bot-note -->
<!-- sqlmesh-gitlab-pipeline-id:10 -->
:robot: **SQLMesh Bot Info** :robot:
Older pipeline note""",
            ),
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    note = controller._get_sqlmesh_mr_note()

    assert note is not None
    assert note.id == 1
    assert client.deleted_note_ids == [2]


def test_get_mr_environment_summary_uses_mr_wording(make_gitlab_client, make_controller):
    controller = make_controller(
        "tests/fixtures/gitlab/merge_request_open.json",
        make_gitlab_client(),
        mock_out_context=False,
    )

    summary = controller.get_merge_request_environment_summary()

    assert "MR environment" in summary
    assert "Dates loaded in MR" in summary or "No models were modified in this MR" in summary


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
        controller.render_merge_request_note(stage_statuses={"Linter": "queued"})
        .splitlines()[3]
        .endswith("https://gitlab.internal.example/group/hello-world/-/merge_requests/42")
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


def test_note_state_preserves_multiline_details(make_gitlab_client, make_controller):
    client = make_gitlab_client(
        [
            MockMergeRequestNote(
                1,
                """<!-- sqlmesh-gitlab-bot-note -->
:robot: **SQLMesh Bot Info** :robot:
- Merge request: `group/hello-world!42`
- Merge request URL: https://gitlab.com/group/hello-world/-/merge_requests/42
- MR environment: `hello_world_42`

## Pipeline Status
- :x: **Linter:** failure
- :hourglass_flowing_sand: **Unit Tests:** queued
- :hourglass_flowing_sand: **MR Environment:** queued
- :hourglass_flowing_sand: **Prod Plan Preview:** queued

## Notes
- Linter: First line
  Second line
- Prod Plan Preview: Another detail""",
            )
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    state = controller.get_merge_request_note_state()

    assert state.details == {
        "Linter": "First line\nSecond line",
        "Prod Plan Preview": "Another detail",
    }


def test_note_state_normalizes_in_progress_status(make_gitlab_client, make_controller):
    client = make_gitlab_client(
        [
            MockMergeRequestNote(
                1,
                """<!-- sqlmesh-gitlab-bot-note -->
:robot: **SQLMesh Bot Info** :robot:
- Merge request: `group/hello-world!42`
- Merge request URL: https://gitlab.com/group/hello-world/-/merge_requests/42
- MR environment: `hello_world_42`

## Pipeline Status
- :rocket: **Linter:** in progress
- :hourglass_flowing_sand: **Unit Tests:** queued
- :hourglass_flowing_sand: **MR Environment:** queued
- :hourglass_flowing_sand: **Prod Plan Preview:** queued""",
            )
        ]
    )
    controller = make_controller("tests/fixtures/gitlab/merge_request_open.json", client)

    state = controller.get_merge_request_note_state()

    assert state.stage_statuses["Linter"] == "in_progress"


def test_merge_request_summary_formats_sqlglot_error(make_gitlab_client, make_controller):
    controller = make_controller(
        "tests/fixtures/gitlab/merge_request_open.json", make_gitlab_client()
    )

    summary = controller.get_merge_request_environment_summary(exception=SqlglotError("bad sql"))

    assert summary == "**Error:** bad sql"
