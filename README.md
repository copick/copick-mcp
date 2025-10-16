# Copick MCP Server

A Model Context Protocol (MCP) server for Copick that provides two sets of tools:
1. **Data Exploration Tools** - Browse and query copick project contents (read-only)
2. **CLI Introspection Tools** - Discover and validate copick CLI commands for building processing pipelines

## Features

- **Read-only data exploration** - List and inspect runs, picks, segmentations, meshes, tomograms, and project metadata
- **CLI discovery** - Dynamically discover all available copick CLI commands with full documentation
- **Command validation** - Validate copick CLI command syntax using Click's native parsing
- **Smart caching** - Efficient caching of copick project roots
- **Easy setup** - Simple CLI for registering with Claude Desktop

## Installation

```bash
cd copick-mcp
pip install -e .
```

## Quick Setup

### Register with Claude Desktop

Use the copick CLI to register the MCP server with Claude Desktop:

```bash
# Basic setup (default settings)
copick setup mcp

# Setup with custom server name
copick setup mcp --server-name "my-copick-server"

# Setup with default config path (optional - can be provided per-request)
copick setup mcp --config-path "/path/to/default/config.json"

# Check registration status
copick setup mcp-status
```

After setup:
1. Restart Claude Desktop completely
2. The Copick MCP tools should now be available
3. The server starts automatically when Claude Desktop connects

### Manual Configuration (Optional)

If you prefer manual setup, add this to your Claude Desktop configuration file:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`

**Windows**: `%APPDATA%/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "copick-mcp": {
      "command": "python",
      "args": ["-m", "copick_mcp.main"],
      "env": {}
    }
  }
}
```

## Available Tools

### Data Exploration Tools (Read-Only)

All data exploration tools require a `config_path` parameter pointing to your copick configuration file.

#### `list_runs`
List all runs in a Copick project.
- **Args**: `config_path` (str)
- **Returns**: List of run names

#### `get_run_details`
Get detailed information about a specific run including voxel spacings, picks, meshes, and segmentations.
- **Args**: `config_path` (str), `run_name` (str)
- **Returns**: Comprehensive run details

#### `list_objects`
List all pickable objects defined in the project.
- **Args**: `config_path` (str)
- **Returns**: List of objects with properties (name, type, label, color, radius, etc.)

#### `list_picks`
List picks for a run with optional filtering.
- **Args**: `config_path` (str), `run_name` (str), `object_name` (optional), `user_id` (optional), `session_id` (optional)
- **Returns**: List of picks with point counts and sample coordinates

#### `list_meshes`
List meshes for a run with optional filtering.
- **Args**: `config_path` (str), `run_name` (str), `object_name` (optional), `user_id` (optional), `session_id` (optional)
- **Returns**: List of meshes

#### `list_segmentations`
List segmentations for a run with optional filtering.
- **Args**: `config_path` (str), `run_name` (str), `voxel_size` (optional), `name` (optional), `user_id` (optional), `session_id` (optional), `is_multilabel` (optional)
- **Returns**: List of segmentations with metadata

#### `list_tomograms`
List tomograms for a specific run and voxel spacing.
- **Args**: `config_path` (str), `run_name` (str), `voxel_spacing` (float)
- **Returns**: List of tomograms with feature information

#### `list_voxel_spacings`
List all voxel spacings available for a run.
- **Args**: `config_path` (str), `run_name` (str)
- **Returns**: List of voxel spacings with tomogram counts

#### `get_project_info`
Get general project information and statistics.
- **Args**: `config_path` (str)
- **Returns**: Project metadata and entity counts

#### `get_json_config`
Get the raw JSON configuration of the project.
- **Args**: `config_path` (str)
- **Returns**: Complete configuration dictionary

### CLI Introspection Tools

These tools help LLMs discover and validate copick CLI commands for building processing pipelines.

#### `list_copick_cli_commands`
List all available copick CLI commands hierarchically organized by group.
- **Returns**: Complete command tree including:
  - `main`: Core commands (add, browse, config, deposit, info, new, stats, sync)
  - `inference`: Inference commands (e.g., membrain-seg)
  - `training`: Training commands
  - `evaluation`: Evaluation commands
  - `process`: Processing commands (downsample, fit-spline, hull, skeletonize, etc.)
  - `convert`: Conversion commands (picks2seg, mesh2seg, seg2picks, etc.)
  - `logical`: Logical operations (clipmesh, clippicks, meshop, segop, etc.)

#### `get_copick_cli_command_info`
Get detailed information about a specific command.
- **Args**: `command_path` (str) - e.g., "convert.picks2seg" or "add"
- **Returns**: Full command details including:
  - Help text and short description
  - All parameters with types, defaults, and help text
  - Required vs optional parameters
  - Usage examples (if present in help)
  - Command group

#### `validate_copick_cli_command`
Validate a copick CLI command string using Click's syntax validation.
- **Args**: `command_string` (str) - e.g., "copick convert picks2seg --config /path/to/config.json ..."
- **Returns**: Validation result including:
  - Valid/invalid status
  - Missing required parameters
  - Parameter type errors
  - Helpful error messages from Click

## Usage Examples

### Data Exploration Workflow

```
User: "Show me all runs in my copick project at /data/my_project/config.json"

Assistant uses: list_runs(config_path="/data/my_project/config.json")

User: "What picks are available for run TS_001?"

Assistant uses: list_picks(config_path="/data/my_project/config.json", run_name="TS_001")

User: "Show me only the ribosome picks from user 'annotator1'"

Assistant uses: list_picks(
    config_path="/data/my_project/config.json",
    run_name="TS_001",
    object_name="ribosome",
    user_id="annotator1"
)
```

### CLI Discovery Workflow

```
User: "I want to convert picks to a segmentation. What copick command can do that?"

Assistant uses: list_copick_cli_commands()
# Discovers convert.picks2seg command

Assistant uses: get_copick_cli_command_info(command_path="convert.picks2seg")
# Gets full documentation and parameters
Assistant explains:
"The picks2seg command converts picks to segmentation by painting spheres at pick locations.
It requires:
- --config: Path to copick config
- --input: Picks URI (format: object_name:user_id/session_id)
- --output: Segmentation URI (format: name:user_id/session_id@voxel_spacing)
- --radius: Sphere radius in angstroms (default: 10.0)"

User: "Can you validate this command for me: copick convert picks2seg --config /data/config.json --input 'ribosome:user1/manual-001' --output 'ribosome:picks2seg/painted-001@10.0'"

Assistant uses: validate_copick_cli_command(
    command_string="copick convert picks2seg --config /data/config.json --input 'ribosome:user1/manual-001' --output 'ribosome:picks2seg/painted-001@10.0'"
)# Returns validation result: valid=True if syntax is correct
```

### Pipeline Building Workflow

```
User: "I want to build a pipeline that:
1. Converts ribosome picks to meshes
2. Computes the convex hull of those meshes
3. Converts the hulls to segmentations"

Assistant uses: list_copick_cli_commands()
# Discovers relevant commands in convert and process groups

Assistant uses: get_copick_cli_command_info(command_path="convert.picks2mesh")
Assistant uses: get_copick_cli_command_info(command_path="process.hull")
Assistant uses: get_copick_cli_command_info(command_path="convert.mesh2seg")
# Gets documentation for each command

Assistant suggests the pipeline:
"Here's a three-step pipeline for your workflow:

Step 1: Convert picks to meshes
copick convert picks2mesh --config /path/to/config.json \
    --input 'ribosome:user1/manual-001' \
    --output 'ribosome:pipeline/step1-meshes' \
    --method convex_hull

Step 2: Compute convex hull (if not done in step 1)
copick process hull --config /path/to/config.json \
    --input-mesh 'ribosome:pipeline/step1-meshes' \
    --output-mesh 'ribosome:pipeline/step2-hulls'

Step 3: Convert meshes to segmentation
copick convert mesh2seg --config /path/to/config.json \
    --input 'ribosome:pipeline/step2-hulls' \
    --output 'ribosome:pipeline/final-seg@10.0'"
```

## Management Commands

```bash
# Check MCP server status
copick setup mcp-status

# Remove MCP server configuration
copick setup mcp-remove --server-name "copick-mcp"

# Force removal without confirmation
copick setup mcp-remove --server-name "copick-mcp" --force
```

## Troubleshooting

1. **"MCP server not found"**: Ensure you've restarted Claude Desktop completely after configuration
2. **"Python module not found"**: Verify the package is installed and the Python path is correct in the config
3. **"Permission denied"**: Check that the Claude config directory is writable
4. **"Invalid JSON"**: Use `copick setup mcp-status` to validate your configuration
5. **"Command not found" during CLI introspection**: Ensure copick and all plugin packages (copick-torch, copick-utils) are installed
6. **"setup command not found"**: Make sure copick-mcp is installed (`pip install -e .` from the copick-mcp directory)

## Development

```bash
# Install in development mode
cd copick-mcp
pip install -e ".[dev]"

# Format code
black src/

# Lint
ruff check --fix src/

# Run the server locally for testing
python -m copick_mcp.main
```

## License

MIT License - See LICENSE file for details.

## Links

- [Copick Documentation](https://copick.github.io/copick)
- [Model Context Protocol](https://modelcontextprotocol.io)
- [FastMCP](https://github.com/jlowin/fastmcp)
