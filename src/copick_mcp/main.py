"""Copick MCP Server - FastMCP server providing data exploration and CLI introspection tools."""

import logging
import sys
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


# ============================================================================
# copick-torch / nnUNet Workflow
# ============================================================================

NNUNET_WORKFLOW_DOCS = """nnUNet Training Workflow (copick-torch)

The nnUNet workflow has three steps: prepare → train → inference.

STEP 1 — Prepare: Convert CoPick project to nnUNet format
  Command: copick convert nnunet
  Purpose: Exports tomograms and segmentation masks to nnUNet raw dataset structure (imagesTr/labelsTr/imagesTs .nii.gz files).
  Required params:
    -c / --config PATH           CoPick config.json
    -n / --dataset-name STR      nnUNet dataset name (becomes Dataset{id}_{name})
  Optional params:
    -uri / --tomo-uri STR        Tomogram URI, format "algorithm@voxel_spacing" (default: "wbp@10.0")
    -sinfo / --seg-info STR      Segmentation as "name" or "name,user_id,session_id" (default: "targets")
    -train / --train-run-ids     Comma-separated run IDs for training (default: all except test set)
    -test / --test-run-ids       Comma-separated run IDs for test set
    -id / --dataset-id INT       Numeric dataset ID (default: 1)
    -o / --output PATH           Output directory for nnunet_raw
    -j / --num-workers INT       Parallel workers (default: 4)
  Example:
    copick convert nnunet \\
      -c /path/to/config.json \\
      -n my-segmentation \\
      -uri "wbp@10.0" \\
      -sinfo "targets,root,default" \\
      -train run1,run2,run3 \\
      -test run4,run5 \\
      -o /path/to/nnunet-raw

STEP 2 — Train: Plan, preprocess, and train nnUNet
  Command: copick training nnunet
  Purpose: Runs nnUNetv2 planning + preprocessing, then trains one model per requested fold.
  GPU-intensive (hours) — always suggest as a copy-pasteable command block; NEVER execute directly
  unless the user explicitly says "run it", "go ahead", or "execute it".
  Required params:
    -n / --dataset-name STR      Must match the name used in prepare
    -r / --raw PATH              Path to nnunet_raw directory
    -pre / --preprocessed PATH   Path to nnunet_preprocessed directory
    -o / --output PATH           Path to nnunet_results directory
  Optional params:
    -id / --dataset-id INT       Must match the ID used in prepare (default: 1)
    -cfg / --configuration       "3d_fullres" (default), "3d_lowres", or "3d_cascade_fullres"
    -f / --folds LIST            Folds to train, e.g. "0" or "0,1,2,3,4" (default: "0")
    -m / --model CHOICE          Architecture: "nnunet" (default), "resnecl", "mednext-s/b/m/l"
    -skip / --skip-preprocess    Skip planning/preprocessing if already done
  Example:
    copick training nnunet \\
      -n my-segmentation \\
      -r /path/to/nnunet-raw \\
      -pre /path/to/nnunet-preprocessed \\
      -o /path/to/nnunet-results \\
      -cfg 3d_fullres \\
      -f 0,1,2,3,4 \\
      -m nnunet

STEP 3 — Inference: Run segmentation on CoPick tomograms
  Command: copick inference nnunet
  Purpose: Sliding-window inference on CoPick tomograms; writes predictions back as segmentations.
    Supports fold ensembling (pass multiple -w flags) and multi-GPU batch processing.
  GPU-intensive — always suggest as a copy-pasteable command block; NEVER execute directly
  unless the user explicitly says "run it", "go ahead", or "execute it".
  Required params:
    -c / --config PATH           CoPick config.json
    -p / --plans PATH            Path to nnUNet plans.json (in nnunet_results)
    -d / --dataset PATH          Path to nnUNet dataset.json (in nnunet_results)
    -w / --weights PATH          Checkpoint .pth file (repeat flag for fold ensembling)
  Optional params:
    -turi / --tomo-uri STR       Tomogram to predict, format "algorithm@voxel_spacing" (default: "wbp@10.0")
    --tta BOOL                   Test-time augmentation via mirroring (default: True)
    --run-ids / -runs STR        Comma-separated CoPick run IDs to predict (default: all runs)
    -suri / --seg-uri STR        Output segmentation URI "name:user_id/session_id" (default: "predict:nnunet/1")
  Single-fold example:
    copick inference nnunet \\
      -c /path/to/config.json \\
      -p /path/to/nnunet-results/Dataset001_my-segmentation/nnUNetTrainer__nnUNetPlans__3d_fullres/plans.json \\
      -d /path/to/nnunet-raw/Dataset001_my-segmentation/dataset.json \\
      -w /path/to/nnunet-results/.../fold_0/checkpoint_best.pth \\
      -suri "predictions:nnunet/1"
  Fold-ensembling example (pass -w once per fold):
    copick inference nnunet \\
      -c /path/to/config.json \\
      -p /path/to/plans.json \\
      -d /path/to/dataset.json \\
      -w fold_0/checkpoint_best.pth \\
      -w fold_1/checkpoint_best.pth \\
      -w fold_2/checkpoint_best.pth \\
      -suri "predictions-ensemble:nnunet/1"
"""


@mcp.tool()
def get_nnunet_workflow_info() -> Dict[str, Any]:
    """Get the full nnUNet training workflow documentation for copick-torch, including
    preparation, training, and inference steps with all parameters and examples.
    Also checks whether copick-torch is installed.

    Returns:
        Dictionary with installation status and complete workflow documentation.
    """
    import importlib.util

    installed = importlib.util.find_spec("copick_torch") is not None
    result: Dict[str, Any] = {
        "copick_torch_installed": installed,
        "workflow": NNUNET_WORKFLOW_DOCS,
    }
    if not installed:
        result["install_instructions"] = (
            "copick-torch is not installed. Run: pip install copick-torch\n"
            "copick-torch provides the 'copick convert nnunet', 'copick training nnunet', "
            "and 'copick inference nnunet' commands required for this workflow."
        )
    return result


# Run the MCP server
if __name__ == "__main__":
    mcp.run(transport="stdio")
