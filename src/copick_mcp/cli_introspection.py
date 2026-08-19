"""CLI introspection utilities for discovering and analyzing copick CLI commands."""

import json
import shlex
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from enum import Enum
from functools import lru_cache
from io import StringIO
from pathlib import Path
from threading import RLock
from typing import Any, Dict, Iterator, List, Optional, Tuple

import click
from copick.cli.cli import add_core_commands, add_plugin_commands
from copick.cli.ext import PLUGIN_GROUPS, load_plugin_commands

_CLI_IO_LOCK = RLock()


@contextmanager
def _capture_cli_output() -> Iterator[Tuple[StringIO, StringIO]]:
    """Capture Click/plugin output while serializing process-global stream redirects."""
    stdout = StringIO()
    stderr = StringIO()
    with _CLI_IO_LOCK, redirect_stdout(stdout), redirect_stderr(stderr):
        yield stdout, stderr


@lru_cache(maxsize=1)
def _core_cli() -> click.Group:
    """Build an isolated Click group containing copick's core commands."""

    @click.group()
    def cli():
        pass

    return add_core_commands(cli)


@lru_cache(maxsize=None)
def _plugin_commands(group: str) -> Tuple[Tuple[click.Command, str], ...]:
    """Load each installed entry-point group once for the server process."""
    return tuple(load_plugin_commands(group))


@lru_cache(maxsize=1)
def _full_cli() -> click.Group:
    """Build the complete CLI once; parsing creates fresh contexts per request."""

    @click.group()
    def cli():
        pass

    return add_plugin_commands(add_core_commands(cli))


def _clear_cli_caches() -> None:
    """Clear process-lifetime discovery caches for tests."""
    _core_cli.cache_clear()
    _plugin_commands.cache_clear()
    _full_cli.cache_clear()


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
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (set, frozenset)):
        converted = [_json_safe(item) for item in value]
        return sorted(converted, key=lambda item: json.dumps(item, sort_keys=True, default=str))
    if callable(value):
        return getattr(value, "__name__", str(value))

    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return str(value)
    return value


def _command_summary(
    command: click.Command,
    path: str,
    package: Optional[str] = None,
    include_help: bool = False,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "name": command.name,
        "path": path,
        "short_help": command.get_short_help_str(limit=120),
    }
    if include_help:
        summary["help"] = command.help or ""
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


def _get_all_cli_commands(include_help: bool = False) -> Dict[str, Any]:
    commands: Dict[str, Any] = {group: [] for group in PLUGIN_GROUPS}

    try:
        core_cli = _core_cli()
        main_commands = {name: (command, None) for name, command in core_cli.commands.items()}

        # Top-level plugins share the main namespace and override a core command
        # with the same name in the assembled copick CLI.
        for command, package_name in _plugin_commands("main"):
            main_commands[command.name] = (command, package_name)

        summaries = []
        for name, (command, package_name) in main_commands.items():
            summary = _command_summary(command, name, package_name, include_help)
            if isinstance(command, click.Group) and command.commands:
                summary["subcommands"] = [
                    {
                        "name": sub_name,
                        "short_help": subcommand.get_short_help_str(limit=120),
                        "path": f"{name}.{sub_name}",
                    }
                    for sub_name, subcommand in command.commands.items()
                ]
            summaries.append(summary)

        commands["main"] = sorted(summaries, key=lambda item: item["name"])
    except Exception as exc:
        commands["main"].append({"error": f"Failed to load core commands: {str(exc)}"})

    # PLUGIN_GROUPS is the core-owned CLI contract. New groups appear here
    # automatically rather than requiring a matching MCP code change.
    for group_name in (group for group in PLUGIN_GROUPS if group != "main"):
        try:
            for command, package_name in _plugin_commands(group_name):
                commands[group_name].append(
                    _command_summary(command, f"{group_name}.{command.name}", package_name, include_help),
                )
            commands[group_name].sort(key=lambda item: item["name"])
        except Exception as exc:
            commands[group_name].append({"error": f"Failed to load {group_name} commands: {str(exc)}"})

    return commands


def get_all_cli_commands(include_help: bool = False) -> Dict[str, Any]:
    """Discover core commands and every plugin group declared by copick."""
    with _capture_cli_output():
        return _get_all_cli_commands(include_help)


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


def _get_command_info(command_path: str) -> Dict[str, Any]:
    try:
        parts = command_path.split(".")
        if len(parts) not in (1, 2) or any(not part for part in parts):
            return {"success": False, "error": f"Invalid command path: {command_path}"}

        core_cli = _core_cli()

        if len(parts) == 1:
            if parts[0] in PLUGIN_GROUPS and parts[0] != "main":
                subcommands = [
                    _command_summary(command, f"{parts[0]}.{command.name}", package_name)
                    for command, package_name in _plugin_commands(parts[0])
                ]
                return {
                    "success": True,
                    "name": parts[0],
                    "group": parts[0],
                    "is_group": True,
                    "subcommands": sorted(subcommands, key=lambda item: item["name"]),
                }

            command = core_cli.commands.get(parts[0])
            package = None
            for plugin_command, package_name in _plugin_commands("main"):
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
            for command, package_name in _plugin_commands(parent_name):
                if command.name == command_name:
                    return _command_details(command, parent_name, package_name)

        return {"success": False, "error": f"Command not found: {command_path}"}
    except Exception as exc:
        return {"success": False, "error": f"Failed to get command info: {str(exc)}"}


def get_command_info(command_path: str) -> Dict[str, Any]:
    """Return details for a core, nested-core, installed plugin command, or plugin group."""
    with _capture_cli_output():
        return _get_command_info(command_path)


def _parse_command(args: List[str]) -> Dict[str, Any]:
    """Parse a copick command without invoking its command callback."""

    cli = _full_cli()

    parent_context = click.Context(cli, info_name="copick")
    try:
        command_name, command, remaining_args = cli.resolve_command(parent_context, args)
    except click.UsageError as exc:
        return {
            "success": True,
            "valid": False,
            "error": exc.format_message(),
            "message": "Command not found or usage error",
        }
    command_path = [command_name]

    if isinstance(command, click.Group) and remaining_args and not remaining_args[0].startswith("-"):
        group_context = click.Context(command, info_name=command_name, parent=parent_context)
        attempted_subcommand = remaining_args[0]
        try:
            subcommand_name, subcommand, remaining_args = command.resolve_command(group_context, remaining_args)
        except click.UsageError as exc:
            return {
                "success": True,
                "valid": False,
                "error": exc.format_message(),
                "command": f"{command_name}.{attempted_subcommand}",
                "message": "Command not found or usage error",
            }
        command_path.append(subcommand_name)
        command = subcommand
        parent_context = group_context

    command_context = click.Context(command, info_name=command_path[-1], parent=parent_context)
    try:
        command.parse_args(command_context, remaining_args)
    except click.exceptions.NoArgsIsHelpError as exc:
        path = ".".join(command_path)
        return {
            "success": True,
            "valid": False,
            "error": f"No arguments supplied for command '{path}'.",
            "command": path,
            "message": "Parameter validation failed",
            "output": exc.format_message(),
        }
    except click.UsageError as exc:
        return {
            "success": True,
            "valid": False,
            "error": exc.format_message(),
            "command": ".".join(command_path),
            "message": "Parameter validation failed",
        }
    except click.ClickException as exc:
        return {
            "success": True,
            "valid": False,
            "error": exc.format_message(),
            "command": ".".join(command_path),
            "message": "Parameter validation failed",
        }
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

    with _capture_cli_output() as (stdout, stderr):
        try:
            result = _parse_command(args[1:])
        except Exception as exc:
            result = {"success": False, "error": f"Failed to validate command: {str(exc)}"}

    output = stdout.getvalue() + stderr.getvalue()
    if output:
        result["output"] = result.get("output", "") + output
    return result
