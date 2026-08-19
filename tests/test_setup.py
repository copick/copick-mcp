import json

import pytest
from click.testing import CliRunner

from copick_mcp.cli import setup as setup_cli


@pytest.mark.parametrize("target", ["desktop", "code-global", "code-project"])
def test_setup_status_and_remove_for_every_target(tmp_path, monkeypatch, target):
    runner = CliRunner()
    config_path = tmp_path / f"{target}.json"
    monkeypatch.setattr(
        setup_cli,
        "get_config_path_for_target",
        lambda selected_target, project_path=None: config_path,
    )
    target_args = ["--target", target]
    if target == "code-project":
        target_args.extend(["--project-path", str(tmp_path)])

    setup = runner.invoke(
        setup_cli.mcp,
        [*target_args, "--server-name", "copick-mcp", "--config-path", "/data/project.json"],
    )
    assert setup.exit_code == 0, setup.output

    config = json.loads(config_path.read_text())
    server = config["mcpServers"]["copick-mcp"]
    assert server["args"] == ["-m", "copick_mcp.main"]
    assert server["env"] == {"COPICK_MCP_DEFAULT_CONFIG": "/data/project.json"}

    status = runner.invoke(setup_cli.mcp_status, target_args)
    assert status.exit_code == 0
    assert "copick-mcp" in status.output

    remove = runner.invoke(setup_cli.mcp_remove, [*target_args, "--server-name", "copick-mcp", "--force"])
    assert remove.exit_code == 0
    assert json.loads(config_path.read_text())["mcpServers"] == {}


def test_setup_preserves_other_client_configuration(tmp_path):
    config_path = tmp_path / ".mcp.json"
    config_path.write_text(json.dumps({"theme": "dark", "mcpServers": {"other": {"command": "other"}}}))

    result = CliRunner().invoke(
        setup_cli.mcp,
        [
            "--target",
            "code-project",
            "--project-path",
            str(tmp_path),
            "--server-name",
            "copick-mcp",
        ],
    )

    assert result.exit_code == 0
    config = json.loads(config_path.read_text())
    assert config["theme"] == "dark"
    assert set(config["mcpServers"]) == {"other", "copick-mcp"}
