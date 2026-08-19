"""Copick MCP Server - FastMCP server providing data exploration and CLI introspection tools."""

import importlib.util
import logging
import os
import sys
from collections import OrderedDict
from pathlib import Path
from threading import RLock
from typing import Any, Dict, Optional

import copick
from fastmcp import FastMCP

# Fix: `import copick` installs a RichHandler on the root logger that writes to
# stdout (via copick.util.log.get_logger). This corrupts the MCP stdio JSON-RPC
# transport. Redirect all root logger handlers to stderr and suppress noisy
# dependency loggers (gql, httpx, etc.) that would otherwise pollute output.
for _h in logging.root.handlers:
    if hasattr(_h, "console") and hasattr(_h.console, "file"):
        _h.console.file = sys.stderr
    elif isinstance(_h, logging.StreamHandler):
        _h.setStream(sys.stderr)

for _noisy_logger in ("gql", "gql.transport", "httpx", "httpcore", "fsspec", "urllib3"):
    logging.getLogger(_noisy_logger).setLevel(logging.WARNING)

# Copick conventions and constraints for LLM context
COPICK_INSTRUCTIONS = """Copick Naming Conventions:
- UNDERSCORES ARE FORBIDDEN in: object_name, user_id, session_id, segmentation names
- Invalid characters (< > : " / \\ | ? * whitespace _) are replaced with dashes

Label Constraints:
- Label 0 is reserved for background (cannot be used for objects)
- Labels must be unique across all objects in a project

Required Fields:
- Picks: object_name (must exist in config), session_id, user_id
- Meshes: object_name (must exist in config), session_id, user_id
- Segmentations: voxel_size, name, session_id, is_multilabel, user_id
- For non-multilabel segmentations, name must match a pickable object

Default Behavior:
- user_id defaults to root.config.user_id if not provided
- Use exist_ok=True for idempotent operations

nnUNet Training:
- For nnUNet training workflows (preparation, training, inference), call get_nnunet_workflow_info().
  That tool checks whether copick-torch is installed and returns the full step-by-step workflow.
"""

# Initialize FastMCP server
mcp = FastMCP("Copick MCP Server", instructions=COPICK_INSTRUCTIONS)

# Configure logging
logger = logging.getLogger("copick-mcp")
handler = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False

DEFAULT_CONFIG_ENV = "COPICK_MCP_DEFAULT_CONFIG"
MAX_CONFIG_SIZE_BYTES = 1024 * 1024
ROOT_CACHE_MAX_SIZE = 16

# Copick root cache: canonical path -> ((mtime_ns, size), root).
_copick_cache: "OrderedDict[str, tuple[tuple[int, int], Any]]" = OrderedDict()
_copick_cache_lock = RLock()


def _resolve_config_path(config_path: Optional[str] = None) -> str:
    """Resolve an explicit config path or the configured server default."""
    selected = config_path or os.getenv(DEFAULT_CONFIG_ENV)
    if not selected:
        raise ValueError(f"config_path is required unless {DEFAULT_CONFIG_ENV} is set")
    return str(Path(selected).expanduser().resolve())


def get_copick_root_from_file(config_path: Optional[str] = None):
    """Get or initialize the Copick root instance from a configuration file.

    Args:
        config_path: Path to the copick configuration file.

    Returns:
        The initialized Copick root instance.
    """
    resolved_path = _resolve_config_path(config_path)

    with _copick_cache_lock:
        stat = Path(resolved_path).stat()
        fingerprint = (stat.st_mtime_ns, stat.st_size)
        cached = _copick_cache.get(resolved_path)
        if cached is not None and cached[0] == fingerprint:
            _copick_cache.move_to_end(resolved_path)
            return cached[1]

        root = copick.from_file(resolved_path)
        _copick_cache[resolved_path] = (fingerprint, root)
        _copick_cache.move_to_end(resolved_path)
        while len(_copick_cache) > ROOT_CACHE_MAX_SIZE:
            _copick_cache.popitem(last=False)
        return root


def _pick_summary(pick: Any, include_point_details: bool, include_sample_points: bool = False) -> Dict[str, Any]:
    """Build a pick summary, isolating optional lazy payload failures."""
    summary = {
        "object_name": pick.pickable_object_name,
        "user_id": pick.user_id,
        "session_id": pick.session_id,
        "num_points": None,
    }
    if not include_point_details:
        return summary

    try:
        points = pick.points or []
    except Exception as exc:
        summary["point_details_error"] = str(exc)
        return summary

    summary["num_points"] = len(points)
    if include_sample_points and points:
        summary["sample_points"] = [
            {"x": point.location.x, "y": point.location.y, "z": point.location.z} for point in points[:3]
        ]
    return summary


def _empty_run_result_message(entity: str, run_name: str, filters: list[str]) -> str:
    """Format an empty-result message with optional filters."""
    message = f"No {entity} found for run '{run_name}'"
    return message + (f" with {', '.join(filters)}" if filters else "")


# ============================================================================
# Data Exploration Tools (Read-Only)
# ============================================================================


@mcp.tool()
def list_runs(config_path: Optional[str] = None) -> Dict[str, Any]:
    """List all runs in a Copick project.

    Args:
        config_path: Path to the Copick configuration file.

    Returns:
        Dictionary containing list of runs or error message.
    """
    try:
        root = get_copick_root_from_file(config_path)
        runs = root.runs

        if not runs:
            return {"success": True, "runs": [], "message": "No runs found in the Copick project"}

        run_list = [{"name": run.name} for run in runs]

        return {"success": True, "runs": run_list, "count": len(run_list)}
    except Exception as e:
        logger.exception(f"Failed to list runs: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_run_details(
    run_name: str,
    config_path: Optional[str] = None,
    include_point_details: bool = False,
) -> Dict[str, Any]:
    """Get detailed information about a specific run.

    Args:
        config_path: Path to the Copick configuration file.
        run_name: Name of the run to get details for.
        include_point_details: Load point counts from pick payloads. Defaults to False.

    Returns:
        Dictionary containing detailed run information or error message.
    """
    try:
        root = get_copick_root_from_file(config_path)
        run = root.get_run(run_name)

        if not run:
            return {"success": False, "error": f"Run '{run_name}' not found"}

        # Get voxel spacings
        voxel_spacings = [{"voxel_size": vs.voxel_size} for vs in run.voxel_spacings]

        # Get picks information
        picks_list = [_pick_summary(pick, include_point_details) for pick in run.picks]

        # Get mesh information
        meshes_list = []
        for mesh in run.meshes:
            meshes_list.append(
                {"object_name": mesh.pickable_object_name, "user_id": mesh.user_id, "session_id": mesh.session_id},
            )

        # Get segmentation information
        segmentations_list = []
        for seg in run.segmentations:
            segmentations_list.append(
                {
                    "name": seg.name,
                    "user_id": seg.user_id,
                    "session_id": seg.session_id,
                    "is_multilabel": seg.is_multilabel,
                    "voxel_size": seg.voxel_size,
                },
            )

        return {
            "success": True,
            "run_name": run.name,
            "voxel_spacings": voxel_spacings,
            "picks": picks_list,
            "meshes": meshes_list,
            "segmentations": segmentations_list,
        }
    except Exception as e:
        logger.exception(f"Failed to get run details: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def list_objects(config_path: Optional[str] = None) -> Dict[str, Any]:
    """List all pickable objects in a Copick project.

    Args:
        config_path: Path to the Copick configuration file.

    Returns:
        Dictionary containing list of pickable objects or error message.
    """
    try:
        root = get_copick_root_from_file(config_path)
        objects = root.pickable_objects

        if not objects:
            return {"success": True, "objects": [], "message": "No pickable objects found"}

        objects_list = []
        for obj in objects:
            obj_dict = {
                "name": obj.name,
                "is_particle": obj.is_particle,
                "label": obj.label,
                "color": obj.color if obj.color else None,
            }
            if obj.radius:
                obj_dict["radius"] = obj.radius
            if obj.pdb_id:
                obj_dict["pdb_id"] = obj.pdb_id
            if obj.emdb_id:
                obj_dict["emdb_id"] = obj.emdb_id
            if obj.identifier:
                obj_dict["identifier"] = obj.identifier

            objects_list.append(obj_dict)

        return {"success": True, "objects": objects_list, "count": len(objects_list)}
    except Exception as e:
        logger.exception(f"Failed to list objects: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def list_tomograms(
    run_name: str,
    voxel_spacing: float,
    config_path: Optional[str] = None,
) -> Dict[str, Any]:
    """List all tomograms for a specific run and voxel spacing.

    Args:
        config_path: Path to the Copick configuration file.
        run_name: Name of the run.
        voxel_spacing: Voxel spacing to filter by.

    Returns:
        Dictionary containing list of tomograms or error message.
    """
    try:
        root = get_copick_root_from_file(config_path)
        run = root.get_run(run_name)

        if not run:
            return {"success": False, "error": f"Run '{run_name}' not found"}

        vs = run.get_voxel_spacing(voxel_spacing)
        if not vs:
            return {"success": False, "error": f"Voxel spacing '{voxel_spacing}' not found in run '{run_name}'"}

        tomograms = vs.tomograms
        if not tomograms:
            return {
                "success": True,
                "tomograms": [],
                "message": f"No tomograms found for run '{run_name}' with voxel spacing '{voxel_spacing}'",
            }

        tomograms_list = []
        for tomo in tomograms:
            features = [{"feature_type": feature.feature_type} for feature in tomo.features]
            tomograms_list.append({"tomo_type": tomo.tomo_type, "features": features})

        return {"success": True, "run_name": run_name, "voxel_spacing": voxel_spacing, "tomograms": tomograms_list}
    except Exception as e:
        logger.exception(f"Failed to list tomograms: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def list_picks(
    run_name: str,
    config_path: Optional[str] = None,
    object_name: Optional[str] = None,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    include_point_details: bool = False,
) -> Dict[str, Any]:
    """List picks for a specific run, optionally filtered by object name, user ID, and session ID.

    Args:
        config_path: Path to the Copick configuration file.
        run_name: Name of the run.
        object_name: Name of the object to filter by (optional).
        user_id: User ID to filter by (optional).
        session_id: Session ID to filter by (optional).
        include_point_details: Load point counts and sample coordinates. Defaults to False.

    Returns:
        Dictionary containing list of picks or error message.
    """
    try:
        root = get_copick_root_from_file(config_path)
        run = root.get_run(run_name)

        if not run:
            return {"success": False, "error": f"Run '{run_name}' not found"}

        picks = run.get_picks(object_name=object_name, user_id=user_id, session_id=session_id)

        if not picks:
            filters = []
            if object_name:
                filters.append(f"object '{object_name}'")
            if user_id:
                filters.append(f"user '{user_id}'")
            if session_id:
                filters.append(f"session '{session_id}'")
            return {
                "success": True,
                "picks": [],
                "message": _empty_run_result_message("picks", run_name, filters),
            }

        picks_list = [_pick_summary(pick, include_point_details, include_sample_points=True) for pick in picks]

        return {"success": True, "run_name": run_name, "picks": picks_list, "count": len(picks_list)}
    except Exception as e:
        logger.exception(f"Failed to list picks: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def list_segmentations(
    run_name: str,
    config_path: Optional[str] = None,
    voxel_size: Optional[float] = None,
    name: Optional[str] = None,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    is_multilabel: Optional[bool] = None,
) -> Dict[str, Any]:
    """List segmentations for a specific run, optionally filtered by various parameters.

    Args:
        config_path: Path to the Copick configuration file.
        run_name: Name of the run.
        voxel_size: Voxel size to filter by (optional).
        name: Name of the segmentation to filter by (optional).
        user_id: User ID to filter by (optional).
        session_id: Session ID to filter by (optional).
        is_multilabel: Filter by multilabel status (optional).

    Returns:
        Dictionary containing list of segmentations or error message.
    """
    try:
        root = get_copick_root_from_file(config_path)
        run = root.get_run(run_name)

        if not run:
            return {"success": False, "error": f"Run '{run_name}' not found"}

        segmentations = run.get_segmentations(
            voxel_size=voxel_size,
            name=name,
            user_id=user_id,
            session_id=session_id,
            is_multilabel=is_multilabel,
        )

        if not segmentations:
            filters = []
            if voxel_size:
                filters.append(f"voxel size '{voxel_size}'")
            if name:
                filters.append(f"name '{name}'")
            if user_id:
                filters.append(f"user '{user_id}'")
            if session_id:
                filters.append(f"session '{session_id}'")
            if is_multilabel is not None:
                filters.append(f"multilabel '{is_multilabel}'")
            return {
                "success": True,
                "segmentations": [],
                "message": _empty_run_result_message("segmentations", run_name, filters),
            }

        segmentations_list = []
        for seg in segmentations:
            segmentations_list.append(
                {
                    "name": seg.name,
                    "user_id": seg.user_id,
                    "session_id": seg.session_id,
                    "is_multilabel": seg.is_multilabel,
                    "voxel_size": seg.voxel_size,
                },
            )

        return {
            "success": True,
            "run_name": run_name,
            "segmentations": segmentations_list,
            "count": len(segmentations_list),
        }
    except Exception as e:
        logger.exception(f"Failed to list segmentations: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def list_voxel_spacings(run_name: str, config_path: Optional[str] = None) -> Dict[str, Any]:
    """List all voxel spacings for a specific run.

    Args:
        config_path: Path to the Copick configuration file.
        run_name: Name of the run.

    Returns:
        Dictionary containing list of voxel spacings or error message.
    """
    try:
        root = get_copick_root_from_file(config_path)
        run = root.get_run(run_name)

        if not run:
            return {"success": False, "error": f"Run '{run_name}' not found"}

        voxel_spacings = run.voxel_spacings
        if not voxel_spacings:
            return {"success": True, "voxel_spacings": [], "message": f"No voxel spacings found for run '{run_name}'"}

        voxel_spacings_list = []
        for vs in voxel_spacings:
            tomo_count = len(vs.tomograms)
            voxel_spacings_list.append({"voxel_size": vs.voxel_size, "tomogram_count": tomo_count})

        return {"success": True, "run_name": run_name, "voxel_spacings": voxel_spacings_list}
    except Exception as e:
        logger.exception(f"Failed to list voxel spacings: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def list_meshes(
    run_name: str,
    config_path: Optional[str] = None,
    object_name: Optional[str] = None,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """List meshes for a specific run, optionally filtered by object name, user ID, and session ID.

    Args:
        config_path: Path to the Copick configuration file.
        run_name: Name of the run.
        object_name: Name of the object to filter by (optional).
        user_id: User ID to filter by (optional).
        session_id: Session ID to filter by (optional).

    Returns:
        Dictionary containing list of meshes or error message.
    """
    try:
        root = get_copick_root_from_file(config_path)
        run = root.get_run(run_name)

        if not run:
            return {"success": False, "error": f"Run '{run_name}' not found"}

        meshes = run.get_meshes(object_name=object_name, user_id=user_id, session_id=session_id)

        if not meshes:
            filters = []
            if object_name:
                filters.append(f"object '{object_name}'")
            if user_id:
                filters.append(f"user '{user_id}'")
            if session_id:
                filters.append(f"session '{session_id}'")
            return {
                "success": True,
                "meshes": [],
                "message": _empty_run_result_message("meshes", run_name, filters),
            }

        meshes_list = []
        for mesh in meshes:
            meshes_list.append(
                {"object_name": mesh.pickable_object_name, "user_id": mesh.user_id, "session_id": mesh.session_id},
            )

        return {"success": True, "run_name": run_name, "meshes": meshes_list, "count": len(meshes_list)}
    except Exception as e:
        logger.exception(f"Failed to list meshes: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_project_info(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Get general information about the Copick project.

    Args:
        config_path: Path to the Copick configuration file.

    Returns:
        Dictionary containing project information or error message.
    """
    try:
        root = get_copick_root_from_file(config_path)

        project_info = {
            "name": root.config.name,
            "description": root.config.description,
            "version": root.config.version,
            "statistics": {
                "total_runs": len(root.runs),
                "total_pickable_objects": len(root.pickable_objects),
            },
        }

        return {"success": True, "project": project_info}
    except Exception as e:
        logger.exception(f"Failed to get project info: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_json_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Get the normalized, validated JSON configuration of a Copick project.

    Args:
        config_path: Path to the Copick configuration file.

    Returns:
        Dictionary containing config data or error message.
    """
    try:
        resolved_path = _resolve_config_path(config_path)
        if Path(resolved_path).stat().st_size > MAX_CONFIG_SIZE_BYTES:
            raise ValueError(f"Copick configuration exceeds the {MAX_CONFIG_SIZE_BYTES}-byte size limit")
        root = get_copick_root_from_file(resolved_path)
        return {"success": True, "config": root.config.model_dump(mode="json")}
    except Exception as e:
        logger.exception(f"Failed to get JSON config: {str(e)}")
        return {"success": False, "error": str(e)}


# ============================================================================
# CLI Introspection Tools
# ============================================================================


@mcp.tool()
def list_copick_cli_commands(include_help: bool = False) -> Dict[str, Any]:
    """List all available copick CLI commands hierarchically.

    Args:
        include_help: Include full command help in the listing. Defaults to compact summaries.

    Returns:
        Dictionary containing the command tree with groups and subcommands.
    """
    try:
        from copick_mcp.cli_introspection import get_all_cli_commands

        commands = get_all_cli_commands(include_help=include_help)
        return {"success": True, "commands": commands}
    except Exception as e:
        logger.exception(f"Failed to list CLI commands: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_copick_cli_command_info(command_path: str) -> Dict[str, Any]:
    """Get full details for a specific copick CLI command.

    Args:
        command_path: Path to the command (e.g., "convert.picks2seg" for subcommands or "add" for main commands).

    Returns:
        Dictionary containing command details including parameters, help text, and examples.
    """
    try:
        from copick_mcp.cli_introspection import get_command_info

        return get_command_info(command_path)
    except Exception as e:
        logger.exception(f"Failed to get CLI command info: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def validate_copick_cli_command(command_string: str) -> Dict[str, Any]:
    """Validate a copick CLI command string using Click's native parsing.

    Args:
        command_string: Full CLI command string (e.g., "copick convert picks2seg --config /path/to/config.json ...").

    Returns:
        Dictionary containing validation status, error messages, and suggestions.
    """
    try:
        from copick_mcp.cli_introspection import validate_copick_cli_command as validate_cmd

        return validate_cmd(command_string)
    except Exception as e:
        logger.exception(f"Failed to validate CLI command: {str(e)}")
        return {"success": False, "error": str(e)}


# ============================================================================
# copick-torch / nnUNet Workflow
# ============================================================================

NNUNET_WORKFLOW_DOCS = """nnUNet Training Workflow (copick-torch)

The workflow has three ordered commands:
1. "copick convert nnunet" prepares an nnUNet raw dataset from Copick tomograms and segmentations.
2. "copick training nnunet" plans, preprocesses, and trains one or more folds.
3. "copick inference nnunet" runs prediction and writes segmentations back to the Copick project.

The installed copick-torch command metadata returned with this guide is the authoritative source for parameters,
choices, required values, and examples.

Training and inference are GPU-intensive. Suggest their commands as copy-pasteable blocks and never execute them
unless the user explicitly asks to run or execute them.
"""

NNUNET_COMMAND_PATHS = {
    "prepare": "convert.nnunet",
    "train": "training.nnunet",
    "inference": "inference.nnunet",
}


@mcp.tool()
def get_nnunet_workflow_info() -> Dict[str, Any]:
    """Get nnUNet workflow guidance and authoritative installed command metadata.

    Returns:
        Dictionary with installation status, workflow ordering, and installed command details.
    """
    installed = importlib.util.find_spec("copick_torch") is not None
    result: Dict[str, Any] = {
        "copick_torch_installed": installed,
        "workflow": NNUNET_WORKFLOW_DOCS,
    }
    if installed:
        from copick_mcp.cli_introspection import get_command_info

        commands = {step: get_command_info(path) for step, path in NNUNET_COMMAND_PATHS.items()}
        result["commands"] = commands
        result["commands_available"] = all(command.get("success") is True for command in commands.values())
        if not result["commands_available"]:
            result["integration_error"] = "One or more installed copick-torch nnUNet commands could not be discovered"
    else:
        result["install_instructions"] = (
            'copick-torch is not installed. Run: pip install "copick-mcp[torch]"\n'
            "copick-torch provides the 'copick convert nnunet', 'copick training nnunet', "
            "and 'copick inference nnunet' commands required for this workflow."
        )
    return result


# Run the MCP server
if __name__ == "__main__":
    mcp.run(transport="stdio")
