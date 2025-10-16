"""Copick MCP Server - FastMCP server providing data exploration and CLI introspection tools."""

import logging
from typing import Any, Dict, Optional

import copick
from fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("Copick MCP Server")

# Configure logging
logger = logging.getLogger("copick-mcp")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Global Copick root cache
_copick_cache: Dict[str, Any] = {}


def get_copick_root_from_file(config_path: str):
    """Get or initialize the Copick root instance from a configuration file.

    Args:
        config_path: Path to the copick configuration file.

    Returns:
        The initialized Copick root instance.
    """
    global _copick_cache
    if config_path not in _copick_cache:
        _copick_cache[config_path] = copick.from_file(config_path)
    return _copick_cache[config_path]


# ============================================================================
# Data Exploration Tools (Read-Only)
# ============================================================================


@mcp.tool()
def list_runs(config_path: str) -> Dict[str, Any]:
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
def get_run_details(config_path: str, run_name: str) -> Dict[str, Any]:
    """Get detailed information about a specific run.

    Args:
        config_path: Path to the Copick configuration file.
        run_name: Name of the run to get details for.

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
        picks_list = []
        for pick in run.picks:
            num_points = len(pick.points) if pick.meta.points else 0
            picks_list.append(
                {
                    "object_name": pick.pickable_object_name,
                    "user_id": pick.user_id,
                    "session_id": pick.session_id,
                    "num_points": num_points,
                },
            )

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
def list_objects(config_path: str) -> Dict[str, Any]:
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
def list_tomograms(config_path: str, run_name: str, voxel_spacing: float) -> Dict[str, Any]:
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
    config_path: str,
    run_name: str,
    object_name: Optional[str] = None,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """List picks for a specific run, optionally filtered by object name, user ID, and session ID.

    Args:
        config_path: Path to the Copick configuration file.
        run_name: Name of the run.
        object_name: Name of the object to filter by (optional).
        user_id: User ID to filter by (optional).
        session_id: Session ID to filter by (optional).

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
            filter_str = ", ".join(filters) if filters else ""
            return {"success": True, "picks": [], "message": f"No picks found for run '{run_name}'{filter_str}"}

        picks_list = []
        for pick in picks:
            num_points = len(pick.points) if pick.meta.points else 0
            pick_dict = {
                "object_name": pick.pickable_object_name,
                "user_id": pick.user_id,
                "session_id": pick.session_id,
                "num_points": num_points,
            }

            # Include first few points if available
            if num_points > 0:
                sample_points = []
                for point in pick.points[:3]:  # First 3 points
                    sample_points.append({"x": point.location.x, "y": point.location.y, "z": point.location.z})
                pick_dict["sample_points"] = sample_points

            picks_list.append(pick_dict)

        return {"success": True, "run_name": run_name, "picks": picks_list, "count": len(picks_list)}
    except Exception as e:
        logger.exception(f"Failed to list picks: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def list_segmentations(
    config_path: str,
    run_name: str,
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
            filter_str = ", ".join(filters) if filters else ""
            return {
                "success": True,
                "segmentations": [],
                "message": f"No segmentations found for run '{run_name}'{filter_str}",
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
def list_voxel_spacings(config_path: str, run_name: str) -> Dict[str, Any]:
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
            tomo_count = len(vs.tomograms) if hasattr(vs, "tomograms") else 0
            voxel_spacings_list.append({"voxel_size": vs.voxel_size, "tomogram_count": tomo_count})

        return {"success": True, "run_name": run_name, "voxel_spacings": voxel_spacings_list}
    except Exception as e:
        logger.exception(f"Failed to list voxel spacings: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def list_meshes(
    config_path: str,
    run_name: str,
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
            filter_str = ", ".join(filters) if filters else ""
            return {"success": True, "meshes": [], "message": f"No meshes found for run '{run_name}'{filter_str}"}

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
def get_project_info(config_path: str) -> Dict[str, Any]:
    """Get general information about the Copick project.

    Args:
        config_path: Path to the Copick configuration file.

    Returns:
        Dictionary containing project information or error message.
    """
    try:
        root = get_copick_root_from_file(config_path)

        project_info = {}

        # Add project metadata
        if hasattr(root.config, "name"):
            project_info["name"] = root.config.name
        if hasattr(root.config, "description"):
            project_info["description"] = root.config.description
        if hasattr(root.config, "version"):
            project_info["version"] = root.config.version

        # Count various entities
        run_count = len(root.runs) if hasattr(root, "runs") else 0
        object_count = len(root.pickable_objects) if hasattr(root, "pickable_objects") else 0

        project_info["statistics"] = {"total_runs": run_count, "total_pickable_objects": object_count}

        return {"success": True, "project": project_info}
    except Exception as e:
        logger.exception(f"Failed to get project info: {str(e)}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_json_config(config_path: str) -> Dict[str, Any]:
    """Get the JSON configuration of a Copick project.

    Args:
        config_path: Path to the Copick configuration file.

    Returns:
        Dictionary containing config data or error message.
    """
    try:
        import json

        with open(config_path, "r") as f:
            config = json.load(f)

        return {"success": True, "config": config}
    except Exception as e:
        logger.exception(f"Failed to get JSON config: {str(e)}")
        return {"success": False, "error": str(e)}


# ============================================================================
# CLI Introspection Tools
# ============================================================================


@mcp.tool()
def list_copick_cli_commands() -> Dict[str, Any]:
    """List all available copick CLI commands hierarchically.

    Returns:
        Dictionary containing complete command tree with groups and subcommands.
    """
    try:
        from copick_mcp.cli_introspection import get_all_cli_commands

        commands = get_all_cli_commands()
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


# Run the MCP server
if __name__ == "__main__":
    mcp.run(transport="stdio")
