from __future__ import annotations

import json
import logging
import os
import pathlib
import traceback
import typing as t
from dataclasses import dataclass, field
from pathlib import Path

import requests
from sqlglot.errors import SqlglotError

from sqlmesh.cicd.summary import (
    generate_plan_flags_section,
    generate_request_environment_summary_intro,
    generate_request_environment_summary_list,
    get_plan_summary,
)
from sqlmesh.core import constants as c
from sqlmesh.core.config import Config
from sqlmesh.core.console import MarkdownConsole, get_console
from sqlmesh.core.context import Context
from sqlmesh.core.environment import Environment
from sqlmesh.core.plan import Plan, PlanBuilder
from sqlmesh.core.plan.definition import UserProvidedFlags
from sqlmesh.core.test.result import ModelTextTestResult
from sqlmesh.integrations.gitlab.cicd.config import GitLabCICDBotConfig
from sqlmesh.utils import Verbosity
from sqlmesh.utils.errors import (
    CICDBotError,
    NoChangesPlanError,
    PlanError,
    SQLMeshError,
    UncategorizedPlanError,
)
from sqlmesh.utils.pydantic import PydanticModel

logger = logging.getLogger(__name__)


class TestFailure(Exception):
    pass


class StalePipelineError(CICDBotError):
    pass


class GitLabMergeRequestInfo(PydanticModel):
    api_v4_url: str
    project_id: int
    project_path: str
    merge_request_iid: int
    source_branch: str
    target_branch: str
    merge_request_url: t.Optional[str] = None
    server_url: t.Optional[str] = None

    @property
    def project_name(self) -> str:
        return self.project_path.split("/")[-1]

    @property
    def full_merge_request_path(self) -> str:
        return f"{self.project_path}!{self.merge_request_iid}"

    @property
    def resolved_server_url(self) -> str:
        if self.server_url:
            return self.server_url.rstrip("/")
        return self.api_v4_url.removesuffix("/api/v4").rstrip("/")

    @property
    def resolved_merge_request_url(self) -> str:
        if self.merge_request_url:
            return self.merge_request_url
        return f"{self.resolved_server_url}/{self.project_path}/-/merge_requests/{self.merge_request_iid}"


class GitLabEventContext:
    """A normalized wrapper for GitLab CI and webhook merge request metadata."""

    def __init__(self, payload: t.Dict[str, t.Any]) -> None:
        self.payload = payload
        self._merge_request_info: t.Optional[GitLabMergeRequestInfo] = None

    @classmethod
    def from_obj(cls, obj: t.Dict[str, t.Any]) -> GitLabEventContext:
        return cls(payload=obj)

    @classmethod
    def from_path(cls, path: t.Union[str, pathlib.Path]) -> GitLabEventContext:
        with open(path, "r", encoding="utf-8") as payload_file:
            return cls.from_obj(json.load(payload_file))

    @classmethod
    def from_env(cls) -> GitLabEventContext:
        api_v4_url = os.environ.get("CI_API_V4_URL")
        merge_request_iid = os.environ.get("CI_MERGE_REQUEST_IID")
        if not api_v4_url or not merge_request_iid:
            raise CICDBotError(
                "Unable to determine the GitLab merge request context from the environment."
            )

        project_id = os.environ.get("CI_MERGE_REQUEST_PROJECT_ID") or os.environ.get(
            "CI_PROJECT_ID"
        )
        project_path = os.environ.get("CI_MERGE_REQUEST_PROJECT_PATH") or os.environ.get(
            "CI_PROJECT_PATH"
        )
        source_branch = os.environ.get("CI_MERGE_REQUEST_SOURCE_BRANCH_NAME")
        target_branch = os.environ.get("CI_MERGE_REQUEST_TARGET_BRANCH_NAME")
        if not project_id or not project_path or not source_branch or not target_branch:
            raise CICDBotError(
                "Missing required GitLab merge request environment variables for SQLMesh bot."
            )

        server_url = os.environ.get("CI_SERVER_URL") or api_v4_url.removesuffix("/api/v4")
        merge_request_url = (
            os.environ.get("CI_MERGE_REQUEST_PROJECT_URL")
            or os.environ.get("CI_PROJECT_URL")
            or f"{server_url.rstrip('/')}/{project_path}"
        )
        merge_request_url = f"{merge_request_url}/-/merge_requests/{merge_request_iid}"

        return cls.from_obj(
            {
                "object_kind": "merge_request",
                "event_type": "merge_request",
                "api_v4_url": api_v4_url,
                "server_url": server_url,
                "project": {
                    "id": int(project_id),
                    "path_with_namespace": project_path,
                    "web_url": os.environ.get("CI_PROJECT_URL")
                    or os.environ.get("CI_MERGE_REQUEST_PROJECT_URL")
                    or f"{server_url.rstrip('/')}/{project_path}",
                },
                "object_attributes": {
                    "iid": int(merge_request_iid),
                    "source_branch": source_branch,
                    "target_branch": target_branch,
                    "url": merge_request_url,
                },
            }
        )

    @property
    def is_merge_request(self) -> bool:
        return self.payload.get("object_kind") == "merge_request"

    @property
    def is_note(self) -> bool:
        attributes = self.payload.get("object_attributes") or {}
        return self.payload.get("object_kind") == "note" and (
            attributes.get("noteable_type") == "MergeRequest" or "merge_request" in self.payload
        )

    @property
    def api_v4_url(self) -> str:
        if api_v4_url := self.payload.get("api_v4_url"):
            return str(api_v4_url).rstrip("/")

        project_web_url = (self.payload.get("project") or {}).get(
            "web_url"
        ) or self.merge_request_info.resolved_server_url
        return f"{str(project_web_url).rstrip('/').removesuffix(self.merge_request_info.project_path.rstrip('/'))}/api/v4".replace(
            "//api/v4", "/api/v4"
        )

    @property
    def server_url(self) -> str:
        if server_url := self.payload.get("server_url"):
            return str(server_url).rstrip("/")
        project_web_url = (self.payload.get("project") or {}).get("web_url")
        if project_web_url:
            return str(project_web_url).removesuffix(f"/{self.merge_request_info.project_path}")
        return self.api_v4_url.removesuffix("/api/v4").rstrip("/")

    @property
    def merge_request_note_body(self) -> t.Optional[str]:
        if self.is_note:
            return (self.payload.get("object_attributes") or {}).get("note")
        return None

    @property
    def merge_request_info(self) -> GitLabMergeRequestInfo:
        if not self._merge_request_info:
            project = self.payload.get("project") or {}
            attributes = self.payload.get("object_attributes") or {}
            merge_request = self.payload.get("merge_request") or {}
            api_v4_url = str(self.payload.get("api_v4_url") or "").rstrip("/")
            project_web_url = str(project.get("web_url") or "").rstrip("/")
            server_url = str(self.payload.get("server_url") or "").rstrip("/")
            if not api_v4_url:
                server_url = server_url or project_web_url.removesuffix(
                    f"/{project.get('path_with_namespace', '')}"
                )
                api_v4_url = f"{server_url.rstrip('/')}/api/v4"
            merge_request_iid = attributes.get("iid") or merge_request.get("iid")
            if merge_request_iid is None:
                raise CICDBotError(
                    "Unable to determine the merge request IID from the GitLab event payload."
                )

            self._merge_request_info = GitLabMergeRequestInfo(
                api_v4_url=api_v4_url,
                project_id=int(project["id"]),
                project_path=project["path_with_namespace"],
                merge_request_iid=int(t.cast(t.Union[str, int], merge_request_iid)),
                source_branch=attributes.get("source_branch") or merge_request.get("source_branch"),
                target_branch=attributes.get("target_branch") or merge_request.get("target_branch"),
                merge_request_url=attributes.get("url"),
                server_url=server_url or None,
            )
        return self._merge_request_info


class GitLabMergeRequestNote(PydanticModel):
    id: int
    body: str


@dataclass
class GitLabMergeRequestNoteState:
    stage_statuses: t.Dict[str, str]
    merge_request_environment_summary: str = ""
    prod_plan_summary: str = ""
    details: t.Dict[str, str] = field(default_factory=dict)


class GitLabAPIClient:
    def list_merge_request_notes(
        self, project_id: int, merge_request_iid: int
    ) -> t.List[GitLabMergeRequestNote]:
        raise NotImplementedError

    def create_merge_request_note(
        self, project_id: int, merge_request_iid: int, body: str
    ) -> GitLabMergeRequestNote:
        raise NotImplementedError

    def update_merge_request_note(
        self, project_id: int, merge_request_iid: int, note_id: int, body: str
    ) -> GitLabMergeRequestNote:
        raise NotImplementedError

    def delete_merge_request_note(
        self, project_id: int, merge_request_iid: int, note_id: int
    ) -> None:
        raise NotImplementedError


class RequestsGitLabAPIClient(GitLabAPIClient):
    def __init__(
        self,
        *,
        api_v4_url: str,
        token: str,
        session: t.Optional[requests.Session] = None,
    ) -> None:
        self._api_v4_url = api_v4_url.rstrip("/")
        self._token = token
        self._session = session or requests.Session()

    def list_merge_request_notes(
        self, project_id: int, merge_request_iid: int
    ) -> t.List[GitLabMergeRequestNote]:
        path = f"/projects/{project_id}/merge_requests/{merge_request_iid}/notes"
        page = 1
        notes: t.List[GitLabMergeRequestNote] = []

        while True:
            response = self._request_response("get", path, params={"page": page, "per_page": 100})
            notes.extend(GitLabMergeRequestNote.model_validate(note) for note in response.json())

            next_page = response.headers.get("X-Next-Page")
            if not next_page:
                break
            page = int(next_page)

        return notes

    def create_merge_request_note(
        self, project_id: int, merge_request_iid: int, body: str
    ) -> GitLabMergeRequestNote:
        response = self._request_json(
            "post",
            f"/projects/{project_id}/merge_requests/{merge_request_iid}/notes",
            data={"body": body},
        )
        return GitLabMergeRequestNote.model_validate(response)

    def update_merge_request_note(
        self, project_id: int, merge_request_iid: int, note_id: int, body: str
    ) -> GitLabMergeRequestNote:
        response = self._request_json(
            "put",
            f"/projects/{project_id}/merge_requests/{merge_request_iid}/notes/{note_id}",
            data={"body": body},
        )
        return GitLabMergeRequestNote.model_validate(response)

    def delete_merge_request_note(
        self, project_id: int, merge_request_iid: int, note_id: int
    ) -> None:
        self._request_response(
            "delete",
            f"/projects/{project_id}/merge_requests/{merge_request_iid}/notes/{note_id}",
        )

    def _request_json(self, method: str, path: str, **kwargs: t.Any) -> t.Any:
        return self._request_response(method, path, **kwargs).json()

    def _request_response(self, method: str, path: str, **kwargs: t.Any) -> requests.Response:
        response = self._session.request(
            method=method,
            url=f"{self._api_v4_url}{path}",
            headers={"PRIVATE-TOKEN": self._token},
            timeout=30,
            **kwargs,
        )
        if not response.ok:
            raise CICDBotError(
                f"GitLab API request failed with status {response.status_code}: {response.text}"
            )
        return response


class GitLabController:
    BOT_HEADER_MSG = ":robot: **SQLMesh Bot Info** :robot:"
    BOT_NOTE_MARKER = "<!-- sqlmesh-gitlab-bot-note -->"
    BOT_PIPELINE_MARKER_PREFIX = "<!-- sqlmesh-gitlab-pipeline-id:"
    BOT_PIPELINE_MARKER_SUFFIX = " -->"
    MAX_NOTE_LENGTH = 1_000_000
    PIPELINE_STAGE_LABELS = ("Linter", "Unit Tests", "MR Environment", "Prod Plan Preview")

    def __init__(
        self,
        paths: t.Union[Path, t.Iterable[Path]],
        token: str,
        config: t.Optional[t.Union[Config, str]] = None,
        event: t.Optional[GitLabEventContext] = None,
        client: t.Optional[GitLabAPIClient] = None,
        context: t.Optional[Context] = None,
    ) -> None:
        self.config = config
        self._paths = paths
        self._token = token
        self._event = event or GitLabEventContext.from_env()
        self._pr_plan_builder: t.Optional[PlanBuilder] = None
        self._prod_plan_builder: t.Optional[PlanBuilder] = None
        self._prod_plan_with_gaps_builder: t.Optional[PlanBuilder] = None

        if not isinstance(get_console(), MarkdownConsole):
            raise CICDBotError("Console must be a markdown console.")
        self._console = t.cast(MarkdownConsole, get_console())

        self._context = context or Context(paths=self._paths, config=self.config)
        self._client = client or RequestsGitLabAPIClient(
            api_v4_url=self.api_v4_url,
            token=self._token,
        )

        logger.debug("GitLab event: %s", json.dumps(self._event.payload))
        logger.debug("Bot config: %s", self.bot_config.json(indent=2))

    @property
    def merge_request_info(self) -> GitLabMergeRequestInfo:
        return self._event.merge_request_info

    @property
    def api_v4_url(self) -> str:
        if self.bot_config.api_v4_url:
            return self.bot_config.api_v4_url.rstrip("/")
        if self.bot_config.server_url:
            return f"{self.bot_config.server_url.rstrip('/')}/api/v4"
        return self._event.api_v4_url.rstrip("/")

    @property
    def server_url(self) -> str:
        return (
            self.bot_config.server_url
            or self.merge_request_info.server_url
            or self._event.server_url
        ).rstrip("/")

    @property
    def merge_request_url(self) -> str:
        return (
            f"{self.server_url}/{self.merge_request_info.project_path}/-/merge_requests/"
            f"{self.merge_request_info.merge_request_iid}"
        )

    @property
    def pipeline_id(self) -> t.Optional[int]:
        pipeline_id = os.environ.get("CI_PIPELINE_ID")
        if pipeline_id and pipeline_id.isdigit():
            return int(pipeline_id)
        return None

    @property
    def bot_config(self) -> GitLabCICDBotConfig:
        bot_config = self._context.config.cicd_bot or GitLabCICDBotConfig.model_validate(
            {"auto_categorize_changes": self._context.auto_categorize_changes}
        )
        if isinstance(bot_config, GitLabCICDBotConfig):
            return bot_config

        github_specific_fields_set = any(
            (
                getattr(bot_config, "enable_deploy_command", False),
                getattr(bot_config, "merge_method", None) is not None,
                getattr(bot_config, "command_namespace", None) is not None,
                getattr(bot_config, "invalidate_environment_after_deploy", True) is not True,
                getattr(bot_config, "check_if_blocked_on_deploy_to_prod", True) is not True,
            )
        )
        if github_specific_fields_set:
            raise CICDBotError(
                "The GitLab bot cannot use GitHub-only `cicd_bot` options. "
                "Set `cicd_bot.type: gitlab` and remove GitHub-specific settings."
            )

        if not getattr(bot_config, "legacy_providerless_config_", False):
            raise CICDBotError(
                "The GitLab bot cannot use a GitHub `cicd_bot` config. "
                "Set `cicd_bot.type: gitlab` to run the GitLab integration."
            )

        gitlab_field_aliases = {
            field.alias or field_name
            for field_name, field in GitLabCICDBotConfig.model_fields.items()
        }
        gitlab_kwargs = {
            key: value
            for key, value in bot_config.model_dump(by_alias=True, exclude_none=True).items()
            if key in gitlab_field_aliases
        }
        gitlab_kwargs["type"] = "gitlab"
        return GitLabCICDBotConfig.model_validate(gitlab_kwargs)

    @property
    def pr_environment_name(self) -> str:
        return Environment.sanitize_name(
            "_".join(
                [
                    self.bot_config.pr_environment_name or self.merge_request_info.project_name,
                    str(self.merge_request_info.merge_request_iid),
                ]
            )
        )

    @property
    def pr_targets_prod_branch(self) -> bool:
        return self.merge_request_info.target_branch in self.bot_config.prod_branch_names

    @property
    def forward_only_plan(self) -> bool:
        default = self._context.config.plan.forward_only
        return (
            self.merge_request_info.source_branch.endswith(
                self.bot_config.forward_only_branch_suffix
            )
            or default
        )

    @property
    def pr_plan(self) -> Plan:
        if not self._pr_plan_builder:
            self._pr_plan_builder = self._context.plan_builder(
                environment=self.pr_environment_name,
                skip_tests=True,
                skip_linter=True,
                categorizer_config=self.bot_config.auto_categorize_changes,
                start=self.bot_config.default_pr_start,
                min_intervals=self.bot_config.pr_min_intervals,
                skip_backfill=self.bot_config.skip_pr_backfill,
                include_unmodified=self.bot_config.pr_include_unmodified,
                forward_only=self.forward_only_plan,
            )
        assert self._pr_plan_builder
        return self._pr_plan_builder.build()

    @property
    def pr_plan_or_none(self) -> t.Optional[Plan]:
        try:
            return self.pr_plan
        except Exception:
            return None

    @property
    def pr_plan_flags(self) -> t.Optional[t.Dict[str, UserProvidedFlags]]:
        if pr_plan := self.pr_plan_or_none:
            return pr_plan.user_provided_flags
        if self._pr_plan_builder:
            return self._pr_plan_builder._user_provided_flags
        return None

    @property
    def prod_plan(self) -> Plan:
        if not self._prod_plan_builder:
            self._prod_plan_builder = self._context.plan_builder(
                c.PROD,
                no_gaps=True,
                skip_tests=True,
                skip_linter=True,
                categorizer_config=self.bot_config.auto_categorize_changes,
                run=self.bot_config.run_on_deploy_to_prod,
                forward_only=self.forward_only_plan,
            )
        assert self._prod_plan_builder
        return self._prod_plan_builder.build()

    @property
    def prod_plan_with_gaps(self) -> Plan:
        if not self._prod_plan_with_gaps_builder:
            self._prod_plan_with_gaps_builder = self._context.plan_builder(
                c.PROD,
                no_gaps=False,
                skip_tests=True,
                skip_linter=True,
                categorizer_config=self.bot_config.auto_categorize_changes,
                run=self.bot_config.run_on_deploy_to_prod,
                forward_only=self.forward_only_plan,
            )
        assert self._prod_plan_with_gaps_builder
        return self._prod_plan_with_gaps_builder.build()

    @property
    def version_info(self) -> str:
        from sqlmesh.cli.main import _sqlmesh_version

        return _sqlmesh_version()

    def run_tests(self) -> t.Tuple[ModelTextTestResult, str]:
        return self._context._run_tests(verbosity=Verbosity.VERBOSE)

    def run_linter(self) -> None:
        self._console.consume_captured_output()
        self._context.lint_models()

    def get_plan_summary(self, plan: Plan) -> str:
        return get_plan_summary(
            console=self._console,
            plan=plan,
            default_catalog=self._context.default_catalog,
        )

    def get_merge_request_environment_summary(self, exception: t.Optional[Exception] = None) -> str:
        if exception is None:
            summary = self._get_merge_request_environment_summary_success()
        elif isinstance(exception, NoChangesPlanError):
            summary = "No changes were detected compared to the prod environment."
        elif isinstance(exception, UncategorizedPlanError) and (plan := self.pr_plan_or_none):
            summary = "The following models could not be categorized automatically:\n"
            for snapshot in plan.uncategorized:
                summary += f"- {snapshot.name}\n"
            summary += (
                f"\nRun `sqlmesh plan {self.pr_environment_name}` locally to apply these changes."
            )
        elif isinstance(exception, PlanError):
            summary = f"Plan failed to generate. Error: {exception}"
        elif isinstance(exception, (SQLMeshError, SqlglotError, ValueError)):
            summary = f"**Error:** {exception}"
        else:
            summary = (
                "This is an unexpected error.\n\n**Exception:**\n```\n"
                f"{traceback.format_exc()}\n```"
            )

        warnings_block = self._console.consume_captured_warnings()
        errors_block = self._console.consume_captured_errors()
        prefix = ""
        if warnings_block:
            prefix += warnings_block
        if exception is not None and errors_block:
            prefix += errors_block
        if prefix:
            summary = f"{prefix}{summary}"

        return summary.strip()

    def _get_merge_request_environment_summary_success(self) -> str:
        prod_plan = self.prod_plan_with_gaps
        if not prod_plan.has_changes:
            summary = "No models were modified in this MR.\n"
        else:
            summary = generate_request_environment_summary_intro(
                bot_config=self.bot_config,
                environment_name=self.pr_environment_name,
                request_term="MR",
                preview_label="Prod Plan Preview",
            ) + generate_request_environment_summary_list(prod_plan, request_term="MR")

        if prod_plan.user_provided_flags:
            summary += generate_plan_flags_section(prod_plan.user_provided_flags)

        return summary

    def update_merge_request_environment(self) -> str:
        if self.has_newer_pipeline_note():
            raise StalePipelineError(
                "Skipping MR environment update because a newer pipeline note already exists."
            )
        self._console.consume_captured_output()
        self._context.apply(self.pr_plan)
        return self.get_merge_request_environment_summary()

    def _chunk_up_api_message(self, message: str) -> t.List[str]:
        return [
            message[index : index + self.MAX_NOTE_LENGTH]
            for index in range(0, len(message), self.MAX_NOTE_LENGTH)
        ]

    def _get_sqlmesh_mr_note(self) -> t.Optional[GitLabMergeRequestNote]:
        notes = self._client.list_merge_request_notes(
            self.merge_request_info.project_id,
            self.merge_request_info.merge_request_iid,
        )
        bot_notes = sorted(
            [
                t.cast(GitLabMergeRequestNote, note)
                for note in notes
                if self.BOT_NOTE_MARKER in note.body
            ],
            key=lambda note: (self._extract_pipeline_id(note.body) or -1, note.id),
        )
        if not bot_notes:
            return None

        latest_note = bot_notes[-1]
        for note in bot_notes[:-1]:
            self._client.delete_merge_request_note(
                self.merge_request_info.project_id,
                self.merge_request_info.merge_request_iid,
                note.id,
            )
        return latest_note

    def get_merge_request_note_state(self) -> GitLabMergeRequestNoteState:
        note = self._get_sqlmesh_mr_note()
        statuses = {label: "queued" for label in self.PIPELINE_STAGE_LABELS}
        if not note:
            return GitLabMergeRequestNoteState(stage_statuses=statuses)

        for line in note.body.splitlines():
            for label in self.PIPELINE_STAGE_LABELS:
                marker = f"**{label}:** "
                if marker in line:
                    statuses[label] = line.split(marker, 1)[1].strip().replace(" ", "_")

        details = self._extract_note_details(note.body, "## Notes")

        return GitLabMergeRequestNoteState(
            stage_statuses=statuses,
            merge_request_environment_summary=self._extract_details_section(
                note.body, "  <summary>:eyes: MR Environment Summary</summary>"
            ),
            prod_plan_summary=self._extract_details_section(
                note.body, "  <summary>:ship: Prod Plan Preview</summary>"
            ),
            details=details,
        )

    def upsert_sqlmesh_mr_note(self, body: str) -> GitLabMergeRequestNote:
        note_body, *truncated = self._chunk_up_api_message(self._with_note_marker(body))
        if truncated:
            logger.warning("GitLab MR note body exceeded max size and was truncated.")

        note = self._get_sqlmesh_mr_note()
        if note:
            existing_pipeline_id = self._extract_pipeline_id(note.body)
            if (
                self.pipeline_id is not None
                and existing_pipeline_id is not None
                and existing_pipeline_id > self.pipeline_id
            ):
                logger.info(
                    "Skipping GitLab MR note update because a newer pipeline note already exists."
                )
                return note
            return t.cast(
                GitLabMergeRequestNote,
                self._client.update_merge_request_note(
                    self.merge_request_info.project_id,
                    self.merge_request_info.merge_request_iid,
                    note.id,
                    note_body,
                ),
            )

        return t.cast(
            GitLabMergeRequestNote,
            self._client.create_merge_request_note(
                self.merge_request_info.project_id,
                self.merge_request_info.merge_request_iid,
                note_body,
            ),
        )

    def render_merge_request_note(
        self,
        *,
        stage_statuses: t.Mapping[str, str],
        merge_request_environment_summary: str = "",
        prod_plan_summary: str = "",
        details: t.Optional[t.Mapping[str, str]] = None,
    ) -> str:
        status_icon = {
            "queued": ":hourglass_flowing_sand:",
            "in_progress": ":rocket:",
            "success": ":white_check_mark:",
            "failure": ":x:",
            "skipped": ":next_track_button:",
        }

        lines = [
            self.BOT_NOTE_MARKER,
            self.bot_config.note_header,
            f"- Merge request: `{self.merge_request_info.full_merge_request_path}`",
            f"- Merge request URL: {self.merge_request_url}",
            f"- MR environment: `{self.pr_environment_name}`",
            "",
            "## Pipeline Status",
        ]

        for label, status in stage_statuses.items():
            lines.append(
                f"- {status_icon.get(status, ':grey_question:')} **{label}:** {status.replace('_', ' ')}"
            )

        if details:
            lines.extend(["", "## Notes"])
            rendered_detail_keys = set()
            for stage in self.PIPELINE_STAGE_LABELS:
                if stage not in details:
                    continue
                rendered_detail_keys.add(stage)
                detail_lines = details[stage].splitlines() or [details[stage]]
                lines.append(f"- {stage}: {detail_lines[0]}")
                lines.extend(f"  {line}" for line in detail_lines[1:])
            for stage, detail in details.items():
                if stage in rendered_detail_keys:
                    continue
                detail_lines = detail.splitlines() or [detail]
                lines.append(f"- {stage}: {detail_lines[0]}")
                lines.extend(f"  {line}" for line in detail_lines[1:])

        if merge_request_environment_summary:
            lines.extend(
                [
                    "",
                    "<details>",
                    "  <summary>:eyes: MR Environment Summary</summary>",
                    "",
                    merge_request_environment_summary,
                    "</details>",
                ]
            )

        if prod_plan_summary:
            lines.extend(
                [
                    "",
                    "<details>",
                    "  <summary>:ship: Prod Plan Preview</summary>",
                    "",
                    prod_plan_summary,
                    "</details>",
                ]
            )

        return "\n".join(lines).strip()

    def _with_note_marker(self, body: str) -> str:
        parts = []
        if self.BOT_NOTE_MARKER not in body:
            parts.append(self.BOT_NOTE_MARKER)
        if self.pipeline_id is not None and self.BOT_PIPELINE_MARKER_PREFIX not in body:
            parts.append(
                f"{self.BOT_PIPELINE_MARKER_PREFIX}{self.pipeline_id}{self.BOT_PIPELINE_MARKER_SUFFIX}"
            )
        parts.append(body)
        return "\n".join(parts)

    def _extract_pipeline_id(self, body: str) -> t.Optional[int]:
        if self.BOT_PIPELINE_MARKER_PREFIX not in body:
            return None
        marker = body.split(self.BOT_PIPELINE_MARKER_PREFIX, 1)[1].split(
            self.BOT_PIPELINE_MARKER_SUFFIX, 1
        )[0]
        if marker.isdigit():
            return int(marker)
        return None

    def has_newer_pipeline_note(self) -> bool:
        note = self._get_sqlmesh_mr_note()
        if not note or self.pipeline_id is None:
            return False
        existing_pipeline_id = self._extract_pipeline_id(note.body)
        return existing_pipeline_id is not None and existing_pipeline_id > self.pipeline_id

    def _extract_details_section(self, body: str, summary_line: str) -> str:
        lines = body.splitlines()
        try:
            start_index = lines.index(summary_line)
        except ValueError:
            return ""

        extracted_lines: t.List[str] = []
        depth = 1
        for line in lines[start_index + 1 :]:
            stripped_line = line.strip()
            if stripped_line == "<details>":
                depth += 1
            elif stripped_line == "</details>":
                depth -= 1
                if depth == 0:
                    break
            extracted_lines.append(line)

        return "\n".join(extracted_lines).strip()

    def _extract_note_details(self, body: str, heading: str) -> t.Dict[str, str]:
        if heading not in body:
            return {}

        section = body.split(heading, 1)[1].lstrip("\n")
        details: t.Dict[str, str] = {}
        current_key: t.Optional[str] = None
        for line in section.splitlines():
            if not line:
                if details:
                    break
                continue
            if line.startswith("- "):
                detail_key, _, detail_value = line[2:].partition(": ")
                current_key = detail_key if detail_value else None
                if current_key:
                    details[current_key] = detail_value
                continue
            if line.startswith("  ") and current_key:
                details[current_key] = f"{details[current_key]}\n{line[2:]}"
                continue
            if details:
                break
        return details
