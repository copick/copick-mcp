"""CLI introspection utilities for discovering and analyzing copick CLI commands."""

import shlex
from typing import Any, Dict, List

import click
from copick.cli.cli import (
    add_core_commands,
    add_plugin_commands,
    convert,
    evaluation,
    inference,
    logical,
    process,
    training,
)
from copick.cli.ext import load_plugin_commands


def get_all_cli_commands() -> Dict[str, Any]:
    """Discover all copick CLI commands using load_plugin_commands().

    Returns:
        Dictionary containing hierarchical structure of all commands with metadata.
    """
    commands = {
        "main": [],
        "inference": [],
        "training": [],
        "evaluation": [],
        "process": [],
        "convert": [],
        "logical": [],
    }

    # Get main commands (core commands)
    try:
        # Create a temporary group to get core commands
        @click.group()
        def temp_cli():
            pass

        temp_cli = add_core_commands(temp_cli)

        for cmd_name in temp_cli.commands:
            cmd = temp_cli.commands[cmd_name]
            commands["main"].append(
                {
                    "name": cmd_name,
                    "short_help": cmd.get_short_help_str(limit=120)
                    if hasattr(cmd, "get_short_help_str")
                    else cmd.short_help,
                    "help": cmd.help,
                },
            )
    except Exception as e:
        commands["main"].append({"error": f"Failed to load core commands: {str(e)}"})

    # Get plugin commands for each group
    groups = {
        "inference": inference,
        "training": training,
        "evaluation": evaluation,
        "process": process,
        "convert": convert,
        "logical": logical,
    }

    for group_name, _group_cmd in groups.items():
        try:
            plugin_commands = load_plugin_commands(group_name)
            if plugin_commands:
                for command, package_name in plugin_commands:
                    commands[group_name].append(
                        {
                            "name": command.name,
                            "short_help": (
                                command.get_short_help_str(limit=120)
                                if hasattr(command, "get_short_help_str")
                                else command.short_help
                            ),
                            "help": command.help,
                            "package": package_name,
                        },
                    )
        except Exception as e:
            commands[group_name].append({"error": f"Failed to load {group_name} commands: {str(e)}"})

    return commands


def get_command_parameters(click_command: click.Command) -> List[Dict[str, Any]]:
    """Extract all parameters from a Click command.

    Args:
        click_command: The Click command object to extract parameters from.

    Returns:
        List of dictionaries containing parameter information.
    """
    params = []

    for param in click_command.params:
        param_info = {
            "name": param.name,
            "param_type": type(param).__name__,
            "required": param.required,
            "default": param.default if param.default is not None else None,
            "help": param.help if param.help else "",
        }

        # Add type information
        if hasattr(param, "type"):
            param_info["type"] = str(param.type)

        # Add option flags for options
        if isinstance(param, click.Option):
            param_info["opts"] = param.opts
            param_info["is_flag"] = param.is_flag
            if param.multiple:
                param_info["multiple"] = True
            if hasattr(param, "type") and isinstance(param.type, click.Choice):
                param_info["choices"] = param.type.choices

        # Add argument information
        if isinstance(param, click.Argument):
            param_info["is_argument"] = True

        params.append(param_info)

    return params


def get_command_info(command_path: str) -> Dict[str, Any]:
    """Get detailed information about a specific copick CLI command.

    Args:
        command_path: Path to the command (e.g., "convert.picks2seg" or "add").

    Returns:
        Dictionary containing command information including parameters, help text, etc.
    """
    try:
        # Parse the command path
        parts = command_path.split(".")
        if len(parts) == 1:
            # Main command
            group_name = "main"
            cmd_name = parts[0]
        elif len(parts) == 2:
            # Subcommand (e.g., convert.picks2seg)
            group_name = parts[0]
            cmd_name = parts[1]
        else:
            return {"success": False, "error": f"Invalid command path: {command_path}"}

        # Get the command object
        command = None

        if group_name == "main":
            # Get core command
            @click.group()
            def temp_cli():
                pass

            temp_cli = add_core_commands(temp_cli)
            if cmd_name in temp_cli.commands:
                command = temp_cli.commands[cmd_name]
        else:
            # Get plugin command
            groups = {
                "inference": "inference",
                "training": "training",
                "evaluation": "evaluation",
                "process": "process",
                "convert": "convert",
                "logical": "logical",
            }

            if group_name in groups:
                plugin_commands = load_plugin_commands(group_name)
                for cmd, _package_name in plugin_commands:
                    if cmd.name == cmd_name:
                        command = cmd
                        break

        if command is None:
            return {"success": False, "error": f"Command not found: {command_path}"}

        # Extract command information
        command_info = {
            "success": True,
            "name": command.name,
            "group": group_name,
            "help": command.help if command.help else "",
            "short_help": (
                command.get_short_help_str(limit=200) if hasattr(command, "get_short_help_str") else command.short_help
            ),
            "parameters": get_command_parameters(command),
        }

        # Add usage example if available in help text
        if command.help and "Examples:" in command.help:
            parts = command.help.split("Examples:")
            if len(parts) > 1:
                command_info["examples"] = parts[1].strip()

        return command_info

    except Exception as e:
        return {"success": False, "error": f"Failed to get command info: {str(e)}"}


def validate_copick_cli_command(command_string: str) -> Dict[str, Any]:
    """Validate a copick CLI command string using Click's parsing.

    Args:
        command_string: Full CLI command string (e.g., "copick convert picks2seg --config ...")

    Returns:
        Dictionary containing validation status and any error messages.
    """
    try:
        # Parse the command string
        args = shlex.split(command_string)

        if not args or args[0] != "copick":
            return {"success": False, "error": "Command must start with 'copick'"}

        if len(args) < 2:
            return {"success": False, "error": "No command specified"}

        # Build the CLI
        @click.group()
        def cli():
            pass

        cli = add_core_commands(cli)
        cli = add_plugin_commands(cli)

        # Create a test context
        ctx = click.Context(cli)

        # Try to parse the command
        try:
            # Remove 'copick' from args
            remaining_args = args[1:]

            # Parse the command and its arguments
            cmd_name, cmd, remaining_args = cli.resolve_command(ctx, remaining_args)

            if isinstance(cmd, click.Group) and remaining_args:
                # It's a group command with a subcommand
                sub_ctx = click.Context(cmd, parent=ctx)
                sub_cmd_name, sub_cmd, sub_remaining_args = cmd.resolve_command(sub_ctx, remaining_args)

                # Try to parse the parameters
                try:
                    # Create a context for the subcommand
                    final_ctx = click.Context(sub_cmd, parent=sub_ctx)
                    sub_cmd.parse_args(final_ctx, sub_remaining_args)

                    return {
                        "success": True,
                        "valid": True,
                        "message": "Command syntax is valid",
                        "command": f"{cmd_name}.{sub_cmd_name}",
                    }
                except click.ClickException as e:
                    return {
                        "success": True,
                        "valid": False,
                        "error": str(e),
                        "command": f"{cmd_name}.{sub_cmd_name}",
                        "message": "Parameter validation failed",
                    }
            else:
                # It's a direct command
                try:
                    final_ctx = click.Context(cmd, parent=ctx)
                    cmd.parse_args(final_ctx, remaining_args)

                    return {
                        "success": True,
                        "valid": True,
                        "message": "Command syntax is valid",
                        "command": cmd_name,
                    }
                except click.ClickException as e:
                    return {
                        "success": True,
                        "valid": False,
                        "error": str(e),
                        "command": cmd_name,
                        "message": "Parameter validation failed",
                    }

        except click.UsageError as e:
            return {"success": True, "valid": False, "error": str(e), "message": "Command not found or usage error"}

    except Exception as e:
        return {"success": False, "error": f"Failed to validate command: {str(e)}"}
