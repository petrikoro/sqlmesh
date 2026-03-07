import pytest

from sqlmesh.integrations.gitlab.cicd.controller import GitLabEventContext

pytestmark = pytest.mark.gitlab


def test_merge_request_context_from_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CI_API_V4_URL", "https://gitlab.example.com/api/v4")
    monkeypatch.setenv("CI_PROJECT_ID", "456")
    monkeypatch.setenv("CI_PROJECT_PATH", "analytics/sqlmesh")
    monkeypatch.setenv("CI_MERGE_REQUEST_IID", "7")
    monkeypatch.setenv("CI_MERGE_REQUEST_SOURCE_BRANCH_NAME", "feature/self-managed")
    monkeypatch.setenv("CI_MERGE_REQUEST_TARGET_BRANCH_NAME", "master")

    event = GitLabEventContext.from_env()

    assert event.api_v4_url == "https://gitlab.example.com/api/v4"
    assert event.merge_request_info.project_id == 456
    assert event.merge_request_info.project_path == "analytics/sqlmesh"
    assert event.merge_request_info.merge_request_iid == 7
    assert event.merge_request_info.source_branch == "feature/self-managed"
    assert event.merge_request_info.target_branch == "master"


@pytest.mark.parametrize(
    "fixture_path, project_path, merge_request_iid, api_v4_url",
    [
        (
            "tests/fixtures/gitlab/merge_request_open.json",
            "group/hello-world",
            42,
            "https://gitlab.com/api/v4",
        ),
        (
            "tests/fixtures/gitlab/merge_request_open_self_managed.json",
            "analytics/sqlmesh",
            7,
            "https://gitlab.example.com/api/v4",
        ),
    ],
)
def test_merge_request_event_from_fixture(
    fixture_path: str,
    project_path: str,
    merge_request_iid: int,
    api_v4_url: str,
    make_event_from_fixture,
):
    event = make_event_from_fixture(fixture_path)

    assert event.is_merge_request
    assert event.merge_request_info.project_path == project_path
    assert event.merge_request_info.merge_request_iid == merge_request_iid
    assert event.api_v4_url == api_v4_url


def test_merge_request_event_accepts_string_iid():
    event = GitLabEventContext.from_obj(
        {
            "object_kind": "merge_request",
            "event_type": "merge_request",
            "api_v4_url": "https://gitlab.example.com/api/v4",
            "project": {
                "id": 456,
                "path_with_namespace": "analytics/sqlmesh",
                "web_url": "https://gitlab.example.com/analytics/sqlmesh",
            },
            "object_attributes": {
                "iid": "7",
                "source_branch": "feature/self-managed",
                "target_branch": "master",
                "url": "https://gitlab.example.com/analytics/sqlmesh/-/merge_requests/7",
            },
        }
    )

    assert event.is_merge_request
    assert event.merge_request_info.merge_request_iid == 7


def test_merge_request_note_event_from_fixture(make_event_from_fixture):
    event = make_event_from_fixture("tests/fixtures/gitlab/merge_request_note.json")

    assert event.is_note
    assert event.merge_request_note_body == "example note"
    assert event.merge_request_info.merge_request_iid == 42
