import typing as t
from pathlib import Path

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.core.config import Config
from sqlmesh.core.console import MarkdownConsole, get_console, set_console
from sqlmesh.integrations.gitlab.cicd.controller import (
    GitLabAPIClient,
    GitLabController,
    GitLabEventContext,
    GitLabMergeRequestNote,
)
from sqlglot.helper import ensure_list


class MockMergeRequestNote(GitLabMergeRequestNote):
    def __init__(self, note_id: int, body: str):
        super().__init__(id=note_id, body=body)


class MockGitLabClient(GitLabAPIClient):
    def __init__(self, notes: t.Optional[t.Sequence[GitLabMergeRequestNote]] = None):
        self.notes: t.List[GitLabMergeRequestNote] = list(notes or [])
        self.created_notes: t.List[GitLabMergeRequestNote] = []
        self.updated_notes: t.List[GitLabMergeRequestNote] = []
        self.deleted_note_ids: t.List[int] = []

    def list_merge_request_notes(
        self, project_id: int, merge_request_iid: int
    ) -> t.List[GitLabMergeRequestNote]:
        return list(self.notes)

    def create_merge_request_note(
        self, project_id: int, merge_request_iid: int, body: str
    ) -> GitLabMergeRequestNote:
        note = MockMergeRequestNote(note_id=len(self.notes) + 1, body=body)
        self.notes.append(note)
        self.created_notes.append(note)
        return note

    def update_merge_request_note(
        self, project_id: int, merge_request_iid: int, note_id: int, body: str
    ) -> GitLabMergeRequestNote:
        note = next(note for note in self.notes if note.id == note_id)
        note.body = body
        self.updated_notes.append(note)
        return note

    def delete_merge_request_note(
        self, project_id: int, merge_request_iid: int, note_id: int
    ) -> None:
        self.notes = [note for note in self.notes if note.id != note_id]
        self.deleted_note_ids.append(note_id)


@pytest.fixture
def make_gitlab_client() -> t.Callable[..., MockGitLabClient]:
    def _make_function(
        notes: t.Optional[t.Sequence[GitLabMergeRequestNote]] = None,
    ) -> MockGitLabClient:
        return MockGitLabClient(notes=notes)

    return _make_function


@pytest.fixture
def sqlmesh_repo_root_path() -> Path:
    return next(p for p in Path(__file__).parents if str(p).endswith("tests")).parent


@pytest.fixture
def make_controller(
    mocker: MockerFixture,
    copy_to_temp_path: t.Callable,
    monkeypatch: pytest.MonkeyPatch,
    sqlmesh_repo_root_path: Path,
) -> t.Callable:
    def _make_function(
        event_path: t.Union[str, Path, t.Dict],
        client: GitLabAPIClient,
        *,
        mock_out_context: bool = True,
        config: t.Optional[t.Union[Config, str]] = None,
        paths: t.Optional[t.Union[Path, t.List[Path]]] = None,
    ) -> GitLabController:
        if mock_out_context:
            mocker.patch("sqlmesh.core.context.Context.apply", mocker.MagicMock())
            mocker.patch("sqlmesh.core.context.Context._run_plan_tests", mocker.MagicMock())
            mocker.patch("sqlmesh.core.context.Context._run_tests", mocker.MagicMock())

        if paths is None:
            paths = copy_to_temp_path(sqlmesh_repo_root_path / "examples" / "sushi")

        paths = ensure_list(paths)

        if isinstance(event_path, str):
            as_path = Path(event_path)
            if not as_path.is_absolute():
                event_path = sqlmesh_repo_root_path / as_path

        monkeypatch.chdir(paths[0])

        orig_console = get_console()
        try:
            set_console(MarkdownConsole(warning_capture_only=True, error_capture_only=True))

            return GitLabController(
                paths=paths,
                token="abc",
                event=(
                    GitLabEventContext.from_path(event_path)
                    if isinstance(event_path, (str, Path))
                    else GitLabEventContext.from_obj(event_path)
                ),
                client=client,
                config=config,
            )
        finally:
            set_console(orig_console)

    return _make_function


@pytest.fixture
def make_event_from_fixture() -> t.Callable[[str], GitLabEventContext]:
    def _make_function(fixture_path: str) -> GitLabEventContext:
        return GitLabEventContext.from_path(fixture_path)

    return _make_function
