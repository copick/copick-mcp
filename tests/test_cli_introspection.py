import json
from pathlib import Path

import click
import pytest
from copick.cli.ext import PLUGIN_GROUPS

from copick_mcp import cli_introspection


@pytest.fixture(autouse=True)
def clear_cli_caches():
    cli_introspection._clear_cli_caches()
    yield
    cli_introspection._clear_cli_caches()


def test_discovery_uses_every_copick_plugin_group():
    commands = cli_introspection.get_all_cli_commands()

    assert list(commands) == PLUGIN_GROUPS
    assert {item["name"] for item in commands["setup"]} >= {"mcp", "mcp-remove", "mcp-status"}
    assert {item["name"] for item in commands["download"]} >= {"project"}
    assert {item["package"] for item in commands["setup"]} >= {"copick-mcp"}
    assert {item["package"] for item in commands["download"]} >= {"copick-utils"}
    assert all("path" in item and "help" not in item for items in commands.values() for item in items)


def test_discovery_includes_full_help_only_when_requested():
    commands = cli_introspection.get_all_cli_commands(include_help=True)

    assert all("help" in item for items in commands.values() for item in items)


def test_command_details_cover_nested_core_and_new_plugin_groups():
    for command_path in (
        "add.picks",
        "setup.mcp",
        "setup.mcp-status",
        "setup.mcp-remove",
        "download.project",
        "convert",
    ):
        details = cli_introspection.get_command_info(command_path)
        assert details["success"] is True
        json.dumps(details)

    assert cli_introspection.get_command_info("setup.mcp")["package"] == "copick-mcp"
    assert cli_introspection.get_command_info("download.project")["package"] == "copick-utils"
    assert cli_introspection.get_command_info("convert")["is_group"] is True
    assert cli_introspection.get_command_info("missing.command")["success"] is False


def test_parameter_metadata_is_json_serializable():
    @click.command()
    @click.option("--path", type=click.Path(path_type=Path), default=Path("project.json"))
    @click.option("--mode", type=click.Choice(("read", "write")), default="read")
    @click.option("--tags", default={"z", "a"})
    def command(path, mode):
        pass

    parameters = cli_introspection.get_command_parameters(command)
    json.dumps(parameters)
    assert parameters[0]["default"] == "project.json"
    assert parameters[1]["choices"] == ["read", "write"]
    assert parameters[2]["default"] == ["a", "z"]


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
    assert invalid_result["command"] == "danger"
    assert invalid_result["message"] == "Parameter validation failed"
    assert "Missing option" in invalid_result["error"]
    assert invoked is False
    assert capsys.readouterr().out == ""


def test_validation_returns_structured_usage_errors(capsys):
    result = cli_introspection.validate_copick_cli_command("copick missing-command")

    assert result["success"] is True
    assert result["valid"] is False
    assert "No such command" in result["error"]
    assert result["message"] == "Command not found or usage error"
    assert capsys.readouterr().out == ""


def test_no_args_help_is_parameter_error_with_help_in_output(monkeypatch):
    @click.command("guided", no_args_is_help=True)
    @click.option("--value", required=True)
    def guided(value):
        pass

    monkeypatch.setattr(cli_introspection, "add_core_commands", lambda cli: cli)
    monkeypatch.setattr(cli_introspection, "add_plugin_commands", lambda cli: cli.add_command(guided) or cli)

    result = cli_introspection.validate_copick_cli_command("copick guided")

    assert result["valid"] is False
    assert result["command"] == "guided"
    assert result["message"] == "Parameter validation failed"
    assert result["error"] == "No arguments supplied for command 'guided'."
    assert "Usage:" in result["output"]


def test_plugin_output_is_captured_and_main_shadow_uses_plugin_object(monkeypatch, capsys):
    @click.group("shadowed")
    def core_group():
        pass

    @core_group.command("core-child")
    def core_child():
        pass

    @click.command("shadowed")
    def plugin_command():
        pass

    def add_core(cli):
        cli.add_command(core_group)
        return cli

    calls = {}

    def load_plugins(group):
        calls[group] = calls.get(group, 0) + 1
        print(f"loading {group}")
        return [(plugin_command, "example-plugin")] if group == "main" else []

    monkeypatch.setattr(cli_introspection, "add_core_commands", add_core)
    monkeypatch.setattr(cli_introspection, "load_plugin_commands", load_plugins)

    first = cli_introspection.get_all_cli_commands()
    second = cli_introspection.get_all_cli_commands()
    shadowed = next(item for item in first["main"] if item["name"] == "shadowed")

    assert shadowed["package"] == "example-plugin"
    assert "subcommands" not in shadowed
    assert first == second
    assert all(count == 1 for count in calls.values())
    assert capsys.readouterr().out == ""
