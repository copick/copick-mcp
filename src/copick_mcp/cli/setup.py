"""Setup subcommands for Copick MCP server configuration.

These commands are registered under the 'copick setup' group defined in the core copick CLI.
"""

import json
import os
import platform
import sys
from pathlib import Path
from typing import Optional

import click


def get_claude_code_global_config_path() -> Path:
    """Get the Claude Code global configuration file path."""
    return Path.home() / ".claude.json"


def get_claude_code_project_config_path(project_path: Optional[Path] = None) -> Path:
    """Get the Claude Code project-specific MCP configuration file path."""
    if project_path is None:
        project_path = Path.cwd()
    return project_path / ".mcp.json"


def get_config_path_for_target(target: str, project_path: Optional[Path] = None) -> Path:
    """Get the configuration file path based on target.

    Args:
        target: One of 'desktop', 'code-global', or 'code-project'
        project_path: Path for project-specific config (only used with 'code-project')

    Returns:
        Path to the configuration file
    """
    if target == "desktop":
        return get_claude_desktop_config_path()
    elif target == "code-global":
        return get_claude_code_global_config_path()
    elif target == "code-project":
        return get_claude_code_project_config_path(project_path)
    else:
        raise ValueError(f"Unknown target: {target}")


def get_target_display_name(target: str) -> str:
    """Get human-readable name for target."""
    names = {
        "desktop": "Claude Desktop",
        "code-global": "Claude Code (global)",
        "code-project": "Claude Code (project)",
    }
    return names.get(target, target)


def get_claude_desktop_config_path() -> Path:
    """Get the Claude Desktop configuration file path for the current platform."""
    system = platform.system()

    if system == "Darwin":  # macOS
        return Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
    elif system == "Windows":
        appdata = os.getenv("APPDATA")
        if appdata:
            return Path(appdata) / "Claude" / "claude_desktop_config.json"
        else:
            return Path.home() / "AppData" / "Roaming" / "Claude" / "claude_desktop_config.json"
    else:  # Linux and others
        return Path.home() / ".config" / "Claude" / "claude_desktop_config.json"


# Keep alias for backward compatibility
get_claude_config_path = get_claude_desktop_config_path


@click.command("mcp")
@click.option(
    "--target",
    type=click.Choice(["desktop", "code-global", "code-project"]),
    default="desktop",
    help="Target application: desktop (Claude Desktop), code-global (Claude Code global), code-project (Claude Code project-specific)",
)
@click.option(
    "--project-path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help="Project path for code-project target (defaults to current directory)",
)
@click.option(
    "--server-name",
    default="copick-mcp",
    help="Name for the MCP server (default: copick-mcp)",
)
@click.option(
    "--python-path",
    help="Path to Python executable (defaults to current Python)",
)
@click.option(
    "--config-path",
    help="Default Copick config path (optional, can be provided per-request)",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force overwrite existing configuration",
)
def mcp(
    target: str,
    project_path: Optional[Path],
    server_name: str,
    python_path: Optional[str],
    config_path: Optional[str],
    force: bool,
):
    """Setup Copick MCP server configuration for Claude Desktop or Claude Code."""
    config_file_path = get_config_path_for_target(target, project_path)

    # Ensure directory exists
    config_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Get Python path
    if not python_path:
        python_path = sys.executable

    # Load existing configuration or create new one
    config = {}
    if config_file_path.exists():
        try:
            with open(config_file_path, "r") as f:
                config = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            if not force:
                click.echo(f"Error reading existing config: {e}")
                click.echo("Use --force to overwrite or fix the configuration manually.")
                sys.exit(1)
            click.echo(f"Warning: Existing config has errors, creating new one: {e}")

    # Initialize mcpServers section if it doesn't exist
    if "mcpServers" not in config:
        config["mcpServers"] = {}

    # Check if server already exists
    if server_name in config["mcpServers"] and not force:
        click.echo(f"Server '{server_name}' already exists in configuration.")
        click.echo("Use --force to overwrite or choose a different --server-name.")
        sys.exit(1)

    # Build environment variables for server configuration
    env_vars = {}
    if config_path:
        env_vars["COPICK_MCP_DEFAULT_CONFIG"] = config_path

    # Add Copick MCP server configuration
    config["mcpServers"][server_name] = {"command": python_path, "args": ["-m", "copick_mcp.main"], "env": env_vars}

    # Write configuration
    try:
        with open(config_file_path, "w") as f:
            json.dump(config, f, indent=2)

        target_name = get_target_display_name(target)
        click.echo(f"✅ Successfully configured {target_name} MCP server!")
        click.echo(f"   Server name: {server_name}")
        click.echo(f"   Config file: {config_file_path}")
        click.echo(f"   Python path: {python_path}")
        if env_vars:
            click.echo("   Environment variables:")
            for key, value in env_vars.items():
                click.echo(f"     {key}: {value}")
        else:
            click.echo("   💡 No default config path set - provide config_path in each tool call")
        click.echo()
        click.echo("📋 Next steps:")
        if target == "desktop":
            click.echo("   1. Restart Claude Desktop completely")
            click.echo("   2. The Copick MCP tools should now be available in Claude Desktop")
            click.echo("   💡 Note: The server starts automatically when Claude Desktop connects")
        elif target == "code-global":
            click.echo("   1. Restart Claude Code or start a new session")
            click.echo("   2. The Copick MCP tools should now be available in all Claude Code sessions")
        else:  # code-project
            click.echo(f"   1. Open Claude Code in the project directory: {project_path or Path.cwd()}")
            click.echo("   2. The Copick MCP tools should now be available for this project")
            click.echo("   💡 Note: This configuration only applies to this specific project")

    except OSError as e:
        click.echo(f"❌ Error writing configuration file: {e}")
        sys.exit(1)


@click.command("mcp-status")
@click.option(
    "--target",
    type=click.Choice(["desktop", "code-global", "code-project"]),
    default="desktop",
    help="Target application to check status for",
)
@click.option(
    "--project-path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help="Project path for code-project target (defaults to current directory)",
)
def mcp_status(target: str, project_path: Optional[Path]):
    """Check Copick MCP server configuration status."""
    config_file_path = get_config_path_for_target(target, project_path)
    target_name = get_target_display_name(target)

    click.echo(f"🔍 MCP Configuration Status ({target_name})")
    click.echo(f"   Platform: {platform.system()}")
    click.echo(f"   Config path: {config_file_path}")
    click.echo(f"   Config exists: {'✅ Yes' if config_file_path.exists() else '❌ No'}")

    if config_file_path.exists():
        try:
            with open(config_file_path, "r") as f:
                config = json.load(f)

            mcp_servers = config.get("mcpServers", {})
            click.echo(f"   MCP servers configured: {len(mcp_servers)}")

            # Check for Copick MCP server
            copick_servers = [name for name in mcp_servers if "copick" in name.lower()]
            if copick_servers:
                click.echo(f"   ✅ Copick MCP servers found: {', '.join(copick_servers)}")
                for server_name in copick_servers:
                    server_config = mcp_servers[server_name]
                    click.echo(f"      - {server_name}: {server_config.get('command', 'unknown command')}")
            else:
                click.echo("   ❌ No Copick MCP servers found")

        except (json.JSONDecodeError, OSError) as e:
            click.echo(f"   ❌ Error reading config: {e}")

    click.echo()
    click.echo("💡 To set up MCP server:")
    click.echo(f"   copick setup mcp --target {target}")


@click.command("mcp-remove")
@click.option(
    "--target",
    type=click.Choice(["desktop", "code-global", "code-project"]),
    default="desktop",
    help="Target application to remove server from",
)
@click.option(
    "--project-path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help="Project path for code-project target (defaults to current directory)",
)
@click.option(
    "--server-name",
    help="Name of the MCP server to remove",
    required=True,
)
@click.option(
    "--force",
    is_flag=True,
    help="Force removal without confirmation",
)
def mcp_remove(target: str, project_path: Optional[Path], server_name: str, force: bool):
    """Remove Copick MCP server configuration."""
    config_file_path = get_config_path_for_target(target, project_path)
    target_name = get_target_display_name(target)

    if not config_file_path.exists():
        click.echo(f"❌ No {target_name} configuration found.")
        sys.exit(1)

    try:
        with open(config_file_path, "r") as f:
            config = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        click.echo(f"❌ Error reading configuration: {e}")
        sys.exit(1)

    mcp_servers = config.get("mcpServers", {})
    if server_name not in mcp_servers:
        click.echo(f"❌ Server '{server_name}' not found in configuration.")
        sys.exit(1)

    if not force:
        click.confirm(f"Remove MCP server '{server_name}'?", abort=True)

    # Remove server
    del mcp_servers[server_name]

    # Write updated configuration
    try:
        with open(config_file_path, "w") as f:
            json.dump(config, f, indent=2)

        click.echo(f"✅ Successfully removed MCP server '{server_name}'")
        if target == "desktop":
            click.echo("   Restart Claude Desktop to apply changes.")
        elif target == "code-global":
            click.echo("   Restart Claude Code or start a new session to apply changes.")
        else:
            click.echo("   Start a new Claude Code session in this project to apply changes.")

    except OSError as e:
        click.echo(f"❌ Error writing configuration: {e}")
        sys.exit(1)
