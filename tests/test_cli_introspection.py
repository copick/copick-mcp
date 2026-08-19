import json
from pathlib import Path

import click
from copick.cli.ext import PLUGIN_GROUPS

from copick_mcp import cli_introspection


def test_discovery_uses_every_copick_plugin_group():
    commands = cli_introspection.get_all_cli_commands()

    assert list(commands) == PLUGIN_GROUPS
    assert {item["name"] for item in commands["setup"]} == {"mcp", "mcp-remove", "mcp-status"}
    assert {item["name"] for item in commands["download"]} == {"project"}
    assert {item["package"] for item in commands["setup"]} == {"copick-mcp"}
    assert {item["package"] for item in commands["download"]} == {"copick-utils"}


def test_command_details_cover_nested_core_and_new_plugin_groups():
    for command_path in (
        "add.picks",
        "setup.mcp",
        "setup.mcp-status",
        "setup.mcp-remove",
        "download.project",
    ):
        details = cli_introspection.get_command_info(command_path)
        assert details["success"] is True
        json.dumps(details)

    assert cli_introspection.get_command_info("setup.mcp")["package"] == "copick-mcp"
    assert cli_introspection.get_command_info("download.project")["package"] == "copick-utils"
    assert cli_introspection.get_command_info("missing.command")["success"] is False


def test_parameter_metadata_is_json_serializable():
    @click.command()
    @click.option("--path", type=click.Path(path_type=Path), default=Path("project.json"))
    @click.option("--mode", type=click.Choice(("read", "write")), default="read")
    def command(path, mode):
        pass

    parameters = cli_introspection.get_command_parameters(command)
    json.dumps(parameters)
    assert parameters[0]["default"] == "project.json"
    assert parameters[1]["choices"] == ["read", "write"]


def test_validation_captures_help_and_never_invokes_command(monkeypatch, capsys):
    invoked = False

    @click.command("danger")
    @click.option("--required", required=True)
    def danger(required):
        nonlocal invoked
        invoked = True

    monkeypatch.setattr(cli_introspection, "add_core_commands", lambda cli: cli)
    monkeypatch.setattr(cli_introspection, "add_plugin_commands", lambda cli: cli.add_command(danger) or cli)

    help_result = cli_introspection.validate_copick_cli_command("copick danger --help")
    valid_result = cli_introspection.validate_copick_cli_command("copick danger --required value")
    invalid_result = cli_introspection.validate_copick_cli_command("copick danger")

    assert help_result["valid"] is True
    assert "Usage:" in help_result["output"]
    assert valid_result["valid"] is True
    assert invalid_result["valid"] is False
    assert invoked is False
    assert capsys.readouterr().out == ""


def test_validation_returns_structured_usage_errors(capsys):
    result = cli_introspection.validate_copick_cli_command("copick missing-command")

    assert result["success"] is True
    assert result["valid"] is False
    assert "No such command" in result["error"]
    assert capsys.readouterr().out == ""
