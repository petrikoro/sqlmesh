import pytest
from click.testing import CliRunner

from sqlmesh.cicd.bot import bot

pytestmark = pytest.mark.gitlab


def test_bot_registers_gitlab_command():
    runner = CliRunner()

    result = runner.invoke(bot, ["--help"])
    assert result.exit_code == 0
    assert "gitlab" in result.output

    result = runner.invoke(bot, ["gitlab", "--help"])
    assert result.exit_code == 0
