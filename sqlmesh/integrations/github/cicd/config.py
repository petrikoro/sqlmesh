import typing as t
from enum import Enum

from pydantic import Field

from sqlmesh.cicd.shared_config import BaseCICDBotConfig
from sqlmesh.utils.pydantic import model_validator


class MergeMethod(str, Enum):
    MERGE = "merge"
    SQUASH = "squash"
    REBASE = "rebase"


class GithubCICDBotConfig(BaseCICDBotConfig):
    type_: t.Literal["github"] = Field(alias="type", default="github")

    invalidate_environment_after_deploy: bool = True
    enable_deploy_command: bool = False
    merge_method: t.Optional[MergeMethod] = None
    command_namespace: t.Optional[str] = None
    check_if_blocked_on_deploy_to_prod: bool = True

    @model_validator(mode="before")
    @classmethod
    def _validate(cls, data: t.Any) -> t.Any:
        if not isinstance(data, dict):
            return data

        if data.get("enable_deploy_command") and not data.get("merge_method"):
            raise ValueError("merge_method must be set if enable_deploy_command is True")
        if data.get("command_namespace") and not data.get("enable_deploy_command"):
            raise ValueError("enable_deploy_command must be set if command_namespace is set")

        return data

    FIELDS_FOR_ANALYTICS: t.ClassVar[t.Set[str]] = BaseCICDBotConfig.FIELDS_FOR_ANALYTICS | {
        "invalidate_environment_after_deploy",
        "enable_deploy_command",
        "merge_method",
        "command_namespace",
    }
