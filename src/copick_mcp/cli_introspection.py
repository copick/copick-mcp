"""CLI introspection utilities for discovering and analyzing copick CLI commands."""

import json
import shlex
from contextlib import redirect_stderr, redirect_stdout
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import click
from copick.cli.cli import add_core_commands, add_plugin_commands
from copick.cli.ext import PLUGIN_GROUPS, load_plugin_commands


def _core_cli() -> click.Group:
    """Build an isolated Click group containing copick's core commands."""

    @click.group()
    def cli():
        pass

    return add_core_commands(cli)


def _json_safe(value: Any) -> Any:
    """Convert Click metadata to values that MCP result schemas can encode."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return _json_safe(value.value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(item) for item in value]
    if callable(value):
        return getattr(value, "__name__", str(value))

    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return str(value)
    return value


def _command_summary(command: click.Command, package: Optional[str] = None) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "name": command.name,
        "short_help": command.get_short_help_str(limit=120),
        "help": command.help or "",
    }
    if package is not None:
        summary["package"] = package
    return summary


def _command_details(
    command: click.Command,
    group: str,
    package: Optional[str] = None,
) -> Dict[str, Any]:
    details: Dict[str, Any] = {
        "success": True,
        "name": command.name,
        "group": group,
        "help": command.help or "",
        "short_help": command.get_short_help_str(limit=200),
        "parameters": get_command_parameters(command),
    }
    if package is not None:
        details["package"] = package
    if command.help and "Examples:" in command.help:
        details["examples"] = command.help.split("Examples:", maxsplit=1)[1].strip()
    return details


def get_all_cli_commands() -> Dict[str, Any]:
    """Discover core commands and every plugin group declared by copick."""
    commands: Dict[str, Any] = {group: [] for group in PLUGIN_GROUPS}

    try:
        core_cli = _core_cli()
        main_commands = {name: _command_summary(command) for name, command in core_cli.commands.items()}

        # Top-level plugins share the main namespace and override a core command
        # with the same name in the assembled copick CLI.
        for command, package_name in load_plugin_commands("main"):
            main_commands[command.name] = _command_summary(command, package_name)

        for name, summary in main_commands.items():
            command = core_cli.commands.get(name)
            if isinstance(command, click.Group) and command.commands:
                summary["subcommands"] = [
                    {
                        "name": sub_name,
                        "short_help": subcommand.get_short_help_str(limit=120),
                        "path": f"{name}.{sub_name}",
                    }
                    for sub_name, subcommand in command.commands.items()
                ]

        commands["main"] = sorted(main_commands.values(), key=lambda item: item["name"])
    except Exception as exc:
        commands["main"].append({"error": f"Failed to load core commands: {str(exc)}"})

    # PLUGIN_GROUPS is the core-owned CLI contract. New groups appear here
    # automatically rather than requiring a matching MCP code change.
    for group_name in (group for group in PLUGIN_GROUPS if group != "main"):
        try:
            for command, package_name in load_plugin_commands(group_name):
                commands[group_name].append(_command_summary(command, package_name))
            commands[group_name].sort(key=lambda item: item["name"])
        except Exception as exc:
            commands[group_name].append({"error": f"Failed to load {group_name} commands: {str(exc)}"})

    return commands


def get_command_parameters(click_command: click.Command) -> List[Dict[str, Any]]:
    """Extract JSON-serializable metadata for every Click parameter."""
    params = []

    for param in click_command.params:
        param_info = {
            "name": param.name,
            "param_type": type(param).__name__,
            "required": param.required,
            "default": _json_safe(param.default),
            "help": getattr(param, "help", "") or "",
        }

        if hasattr(param, "type"):
            param_info["type"] = str(param.type)

        if isinstance(param, click.Option):
            param_info["opts"] = list(param.opts)
            param_info["is_flag"] = param.is_flag
            if param.multiple:
                param_info["multiple"] = True
            if isinstance(param.type, click.Choice):
                param_info["choices"] = _json_safe(param.type.choices)

        if isinstance(param, click.Argument):
            param_info["is_argument"] = True

        params.append(param_info)

    return params


def get_command_info(command_path: str) -> Dict[str, Any]:
    """Return details for a core, nested-core, or installed plugin command."""
    try:
        parts = command_path.split(".")
        if len(parts) not in (1, 2) or any(not part for part in parts):
            return {"success": False, "error": f"Invalid command path: {command_path}"}

        core_cli = _core_cli()

        if len(parts) == 1:
            command = core_cli.commands.get(parts[0])
            package = None
            for plugin_command, package_name in load_plugin_commands("main"):
                if plugin_command.name == parts[0]:
                    command = plugin_command
                    package = package_name
                    break
            if command is None:
                return {"success": False, "error": f"Command not found: {command_path}"}
            return _command_details(command, "main", package)

        parent_name, command_name = parts
        parent = core_cli.commands.get(parent_name)
        if isinstance(parent, click.Group) and command_name in parent.commands:
            return _command_details(parent.commands[command_name], parent_name)

        if parent_name in PLUGIN_GROUPS and parent_name != "main":
            for command, package_name in load_plugin_commands(parent_name):
                if command.name == command_name:
                    return _command_details(command, parent_name, package_name)

        return {"success": False, "error": f"Command not found: {command_path}"}
    except Exception as exc:
        return {"success": False, "error": f"Failed to get command info: {str(exc)}"}


def _parse_command(args: List[str]) -> Dict[str, Any]:
    """Parse a copick command without invoking its command callback."""

    @click.group()
    def cli():
        pass

    cli = add_core_commands(cli)
    cli = add_plugin_commands(cli)

    parent_context = click.Context(cli, info_name="copick")
    command_name, command, remaining_args = cli.resolve_command(parent_context, args)
    command_path = [command_name]

    if isinstance(command, click.Group) and remaining_args and not remaining_args[0].startswith("-"):
        group_context = click.Context(command, info_name=command_name, parent=parent_context)
        subcommand_name, subcommand, remaining_args = command.resolve_command(group_context, remaining_args)
        command_path.append(subcommand_name)
        command = subcommand
        parent_context = group_context

    command_context = click.Context(command, info_name=command_path[-1], parent=parent_context)
    try:
        command.parse_args(command_context, remaining_args)
    except click.exceptions.Exit as exc:
        if exc.exit_code == 0:
            return {
                "success": True,
                "valid": True,
                "message": "Command help requested; syntax is valid",
                "command": ".".join(command_path),
            }
        return {
            "success": True,
            "valid": False,
            "error": f"Command parsing exited with status {exc.exit_code}",
            "command": ".".join(command_path),
            "message": "Parameter validation failed",
        }

    return {
        "success": True,
        "valid": True,
        "message": "Command syntax is valid",
        "command": ".".join(command_path),
    }


def validate_copick_cli_command(command_string: str) -> Dict[str, Any]:
    """Validate Click syntax without invoking commands or leaking output."""
    try:
        args = shlex.split(command_string)
    except ValueError as exc:
        return {"success": True, "valid": False, "error": str(exc), "message": "Invalid command string"}

    if not args or args[0] != "copick":
        return {"success": False, "error": "Command must start with 'copick'"}
    if len(args) < 2:
        return {"success": False, "error": "No command specified"}

    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        try:
            result = _parse_command(args[1:])
        except click.ClickException as exc:
            result = {
                "success": True,
                "valid": False,
                "error": exc.format_message(),
                "message": "Command not found or usage error",
            }
        except Exception as exc:
            result = {"success": False, "error": f"Failed to validate command: {str(exc)}"}

    output = stdout.getvalue() + stderr.getvalue()
    if output:
        result["output"] = output
    return result
