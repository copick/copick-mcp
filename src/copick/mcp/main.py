from typing import List, Dict, Any, Optional, Union
import os
import sys
import json
import logging
from pathlib import Path
from mcp.server.fastmcp import FastMCP
import copick

# Initialize FastMCP server
mcp = FastMCP("copick-mcp")

# Configure logging
logger = logging.getLogger("copick-mcp")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Global Copick root instance
_copick_instance = None

def get_copick_root_from_file(config_path: str):
    """Get or initialize the Copick root instance from a configuration file.
    
    Args:
        config_path: Path to the copick configuration file.
        
    Returns:
        The initialized Copick root instance.
    """
    global _copick_instance
    if _copick_instance is None or not hasattr(_copick_instance, 'config_path') or _copick_instance.config_path != config_path:
        _copick_instance = copick.from_file(config_path)
        _copick_instance.config_path = config_path  # Add a reference to the config path
    return _copick_instance

def get_copick_root_from_dataset(dataset_id: int, overlay_root: str, user_id: Optional[str] = None, session_id: Optional[str] = None):
    """Get or initialize the Copick root instance from a CZCDP dataset ID.
    
    Args:
        dataset_id: The CZ cryoET Data Portal dataset ID.
        overlay_root: The root URL for the overlay storage.
        user_id: The user ID to use for the project.
        session_id: The session ID to use for the project.
        
    Returns:
        The initialized Copick root instance.
    """
    global _copick_instance
    if (_copick_instance is None or not hasattr(_copick_instance, 'dataset_id') 
        or _copick_instance.dataset_id != dataset_id
        or _copick_instance.overlay_root != overlay_root):
        _copick_instance = copick.from_czcdp_datasets(
            dataset_ids=[dataset_id],
            overlay_root=overlay_root,
            overlay_fs_args={"auto_mkdir": True},
            user_id=user_id,
            session_id=session_id
        )
        _copick_instance.dataset_id = dataset_id
        _copick_instance.overlay_root = overlay_root
    return _copick_instance

@mcp.tool()
async def list_runs(config_path: Optional[str] = None, dataset_id: Optional[int] = None, 
                   overlay_root: Optional[str] = None) -> str:
    """List all runs in a Copick project.
    
    Args:
        config_path: Path to the Copick configuration file. Optional if dataset_id is provided.
        dataset_id: The CZ cryoET Data Portal dataset ID. Optional if config_path is provided.
        overlay_root: The root URL for the overlay storage. Required if dataset_id is provided.
        
    Returns:
        Formatted list of runs.
    """
    try:
        if config_path:
            root = get_copick_root_from_file(config_path)
        elif dataset_id and overlay_root:
            root = get_copick_root_from_dataset(dataset_id, overlay_root)
        else:
            return "Error: Either config_path or (dataset_id and overlay_root) must be provided."
        
        runs = root.runs
        if not runs:
            return "No runs found in the Copick project."
        
        result = "Available runs:\n\n"
        for run in runs:
            result += f"Run: {run.name}\n"
        
        return result
    except Exception as e:
        logger.error(f"Failed to list runs: {str(e)}", exc_info=True)
        return f"Failed to list runs: {str(e)}"

@mcp.tool()
async def get_run_details(run_name: str, config_path: Optional[str] = None, 
                         dataset_id: Optional[int] = None, overlay_root: Optional[str] = None) -> str:
    """Get detailed information about a specific run.
    
    Args:
        run_name: Name of the run to get details for.
        config_path: Path to the Copick configuration file. Optional if dataset_id is provided.
        dataset_id: The CZ cryoET Data Portal dataset ID. Optional if config_path is provided.
        overlay_root: The root URL for the overlay storage. Required if dataset_id is provided.
        
    Returns:
        Detailed information about the run.
    """
    try:
        if config_path:
            root = get_copick_root_from_file(config_path)
        elif dataset_id and overlay_root:
            root = get_copick_root_from_dataset(dataset_id, overlay_root)
        else:
            return "Error: Either config_path or (dataset_id and overlay_root) must be provided."
        
        run = root.get_run(run_name)
        if not run:
            return f"Run '{run_name}' not found."
        
        result = f"Run: {run.name}\n\n"
        
        # Add voxel spacings information
        result += "Voxel Spacings:\n"
        for vs in run.voxel_spacings:
            result += f"  {vs.voxel_size:.3f}\n"
        
        # Add picks information
        result += "\nPicks:\n"
        for pick in run.picks:
            num_points = len(pick.points) if pick.meta.points else 0
            result += f"  Object: {pick.pickable_object_name}, User: {pick.user_id}, Session: {pick.session_id}, Points: {num_points}\n"
        
        # Add mesh information
        result += "\nMeshes:\n"
        for mesh in run.meshes:
            result += f"  Object: {mesh.pickable_object_name}, User: {mesh.user_id}, Session: {mesh.session_id}\n"
        
        # Add segmentation information
        result += "\nSegmentations:\n"
        for seg in run.segmentations:
            multilabel = "Multilabel" if seg.is_multilabel else "Single label"
            result += f"  Name: {seg.name}, User: {seg.user_id}, Session: {seg.session_id}, Type: {multilabel}, Voxel Size: {seg.voxel_size}\n"
        
        return result
    except Exception as e:
        logger.error(f"Failed to get run details: {str(e)}", exc_info=True)
        return f"Failed to get run details: {str(e)}"

@mcp.tool()
async def list_objects(config_path: Optional[str] = None, dataset_id: Optional[int] = None, 
                      overlay_root: Optional[str] = None) -> str:
    """List all pickable objects in a Copick project.
    
    Args:
        config_path: Path to the Copick configuration file. Optional if dataset_id is provided.
        dataset_id: The CZ cryoET Data Portal dataset ID. Optional if config_path is provided.
        overlay_root: The root URL for the overlay storage. Required if dataset_id is provided.
        
    Returns:
        Formatted list of pickable objects.
    """
    try:
        if config_path:
            root = get_copick_root_from_file(config_path)
        elif dataset_id and overlay_root:
            root = get_copick_root_from_dataset(dataset_id, overlay_root)
        else:
            return "Error: Either config_path or (dataset_id and overlay_root) must be provided."
        
        objects = root.pickable_objects
        if not objects:
            return "No pickable objects found in the Copick project."
        
        result = "Available pickable objects:\n\n"
        for obj in objects:
            particle_type = "Particle" if obj.is_particle else "Segmentation"
            color_str = f"[{obj.color[0]}, {obj.color[1]}, {obj.color[2]}, {obj.color[3]}]" if obj.color else "None"
            result += f"Name: {obj.name}\n"
            result += f"  Type: {particle_type}\n"
            result += f"  Label: {obj.label}\n"
            result += f"  Color: {color_str}\n"
            if obj.radius:
                result += f"  Radius: {obj.radius}\n"
            if obj.pdb_id:
                result += f"  PDB ID: {obj.pdb_id}\n"
            if obj.emdb_id:
                result += f"  EMDB ID: {obj.emdb_id}\n"
            if obj.identifier:
                result += f"  Identifier: {obj.identifier}\n"
            result += "\n"
        
        return result
    except Exception as e:
        logger.error(f"Failed to list objects: {str(e)}", exc_info=True)
        return f"Failed to list objects: {str(e)}"

@mcp.tool()
async def list_tomograms(run_name: str, voxel_spacing: float, config_path: Optional[str] = None, 
                        dataset_id: Optional[int] = None, overlay_root: Optional[str] = None) -> str:
    """List all tomograms for a specific run and voxel spacing.
    
    Args:
        run_name: Name of the run.
        voxel_spacing: Voxel spacing to filter by.
        config_path: Path to the Copick configuration file. Optional if dataset_id is provided.
        dataset_id: The CZ cryoET Data Portal dataset ID. Optional if config_path is provided.
        overlay_root: The root URL for the overlay storage. Required if dataset_id is provided.
        
    Returns:
        Formatted list of tomograms.
    """
    try:
        if config_path:
            root = get_copick_root_from_file(config_path)
        elif dataset_id and overlay_root:
            root = get_copick_root_from_dataset(dataset_id, overlay_root)
        else:
            return "Error: Either config_path or (dataset_id and overlay_root) must be provided."
        
        run = root.get_run(run_name)
        if not run:
            return f"Run '{run_name}' not found."
        
        vs = run.get_voxel_spacing(voxel_spacing)
        if not vs:
            return f"Voxel spacing '{voxel_spacing}' not found in run '{run_name}'."
        
        tomograms = vs.tomograms
        if not tomograms:
            return f"No tomograms found for run '{run_name}' with voxel spacing '{voxel_spacing}'."
        
        result = f"Tomograms for run '{run_name}' with voxel spacing '{voxel_spacing}':\n\n"
        for tomo in tomograms:
            result += f"Type: {tomo.tomo_type}\n"
            # Include number of feature maps
            result += f"  Features: {len(tomo.features)}\n"
            for feature in tomo.features:
                result += f"    - {feature.feature_type}\n"
        
        return result
    except Exception as e:
        logger.error(f"Failed to list tomograms: {str(e)}", exc_info=True)
        return f"Failed to list tomograms: {str(e)}"

@mcp.tool()
async def list_picks(run_name: str, object_name: Optional[str] = None, user_id: Optional[str] = None,
                    session_id: Optional[str] = None, config_path: Optional[str] = None, 
                    dataset_id: Optional[int] = None, overlay_root: Optional[str] = None) -> str:
    """List picks for a specific run, optionally filtered by object name, user ID, and session ID.
    
    Args:
        run_name: Name of the run.
        object_name: Name of the object to filter by. Optional.
        user_id: User ID to filter by. Optional.
        session_id: Session ID to filter by. Optional.
        config_path: Path to the Copick configuration file. Optional if dataset_id is provided.
        dataset_id: The CZ cryoET Data Portal dataset ID. Optional if config_path is provided.
        overlay_root: The root URL for the overlay storage. Required if dataset_id is provided.
        
    Returns:
        Formatted list of picks.
    """
    try:
        if config_path:
            root = get_copick_root_from_file(config_path)
        elif dataset_id and overlay_root:
            root = get_copick_root_from_dataset(dataset_id, overlay_root)
        else:
            return "Error: Either config_path or (dataset_id and overlay_root) must be provided."
        
        run = root.get_run(run_name)
        if not run:
            return f"Run '{run_name}' not found."
        
        picks = run.get_picks(object_name=object_name, user_id=user_id, session_id=session_id)
        if not picks:
            filter_msg = ""
            if object_name:
                filter_msg += f", object '{object_name}'"
            if user_id:
                filter_msg += f", user '{user_id}'"
            if session_id:
                filter_msg += f", session '{session_id}'"
            return f"No picks found for run '{run_name}'{filter_msg}."
        
        result = f"Picks for run '{run_name}':\n\n"
        for pick in picks:
            num_points = len(pick.points) if pick.meta.points else 0
            result += f"Object: {pick.pickable_object_name}, User: {pick.user_id}, Session: {pick.session_id}, Points: {num_points}\n"
            if num_points > 0:
                result += f"  First 3 points (if available):\n"
                for i, point in enumerate(pick.points[:3]):
                    result += f"    {i+1}: ({point.location.x:.1f}, {point.location.y:.1f}, {point.location.z:.1f})\n"
        
        return result
    except Exception as e:
        logger.error(f"Failed to list picks: {str(e)}", exc_info=True)
        return f"Failed to list picks: {str(e)}"

@mcp.tool()
async def list_segmentations(run_name: str, voxel_size: Optional[float] = None, name: Optional[str] = None,
                           user_id: Optional[str] = None, session_id: Optional[str] = None, 
                           is_multilabel: Optional[bool] = None, config_path: Optional[str] = None, 
                           dataset_id: Optional[int] = None, overlay_root: Optional[str] = None) -> str:
    """List segmentations for a specific run, optionally filtered by various parameters.
    
    Args:
        run_name: Name of the run.
        voxel_size: Voxel size to filter by. Optional.
        name: Name of the segmentation to filter by. Optional.
        user_id: User ID to filter by. Optional.
        session_id: Session ID to filter by. Optional.
        is_multilabel: Filter by multilabel status. Optional.
        config_path: Path to the Copick configuration file. Optional if dataset_id is provided.
        dataset_id: The CZ cryoET Data Portal dataset ID. Optional if config_path is provided.
        overlay_root: The root URL for the overlay storage. Required if dataset_id is provided.
        
    Returns:
        Formatted list of segmentations.
    """
    try:
        if config_path:
            root = get_copick_root_from_file(config_path)
        elif dataset_id and overlay_root:
            root = get_copick_root_from_dataset(dataset_id, overlay_root)
        else:
            return "Error: Either config_path or (dataset_id and overlay_root) must be provided."
        
        run = root.get_run(run_name)
        if not run:
            return f"Run '{run_name}' not found."
        
        segmentations = run.get_segmentations(
            voxel_size=voxel_size, name=name, user_id=user_id, 
            session_id=session_id, is_multilabel=is_multilabel
        )
        
        if not segmentations:
            filter_msg = ""
            if voxel_size:
                filter_msg += f", voxel size '{voxel_size}'"
            if name:
                filter_msg += f", name '{name}'"
            if user_id:
                filter_msg += f", user '{user_id}'"
            if session_id:
                filter_msg += f", session '{session_id}'"
            if is_multilabel is not None:
                filter_msg += f", multilabel '{is_multilabel}'"
            return f"No segmentations found for run '{run_name}'{filter_msg}."
        
        result = f"Segmentations for run '{run_name}':\n\n"
        for seg in segmentations:
            multilabel = "Multilabel" if seg.is_multilabel else "Single label"
            result += f"Name: {seg.name}, User: {seg.user_id}, Session: {seg.session_id}, Type: {multilabel}, Voxel Size: {seg.voxel_size}\n"
        
        return result
    except Exception as e:
        logger.error(f"Failed to list segmentations: {str(e)}", exc_info=True)
        return f"Failed to list segmentations: {str(e)}"

@mcp.tool()
async def list_voxel_spacings(run_name: str, config_path: Optional[str] = None, 
                             dataset_id: Optional[int] = None, overlay_root: Optional[str] = None) -> str:
    """List all voxel spacings for a specific run.
    
    Args:
        run_name: Name of the run.
        config_path: Path to the Copick configuration file. Optional if dataset_id is provided.
        dataset_id: The CZ cryoET Data Portal dataset ID. Optional if config_path is provided.
        overlay_root: The root URL for the overlay storage. Required if dataset_id is provided.
        
    Returns:
        Formatted list of voxel spacings.
    """
    try:
        if config_path:
            root = get_copick_root_from_file(config_path)
        elif dataset_id and overlay_root:
            root = get_copick_root_from_dataset(dataset_id, overlay_root)
        else:
            return "Error: Either config_path or (dataset_id and overlay_root) must be provided."
        
        run = root.get_run(run_name)
        if not run:
            return f"Run '{run_name}' not found."
        
        voxel_spacings = run.voxel_spacings
        if not voxel_spacings:
            return f"No voxel spacings found for run '{run_name}'."
        
        result = f"Voxel spacings for run '{run_name}':\n\n"
        for vs in voxel_spacings:
            result += f"Voxel spacing: {vs.voxel_size:.3f}\n"
            # Include counts of tomograms
            tomo_count = len(vs.tomograms) if hasattr(vs, 'tomograms') else 0
            result += f"  Tomograms: {tomo_count}\n"
        
        return result
    except Exception as e:
        logger.error(f"Failed to list voxel spacings: {str(e)}", exc_info=True)
        return f"Failed to list voxel spacings: {str(e)}"

@mcp.tool()
async def list_meshes(run_name: str, object_name: Optional[str] = None, user_id: Optional[str] = None,
                     session_id: Optional[str] = None, config_path: Optional[str] = None, 
                     dataset_id: Optional[int] = None, overlay_root: Optional[str] = None) -> str:
    """List meshes for a specific run, optionally filtered by object name, user ID, and session ID.
    
    Args:
        run_name: Name of the run.
        object_name: Name of the object to filter by. Optional.
        user_id: User ID to filter by. Optional.
        session_id: Session ID to filter by. Optional.
        config_path: Path to the Copick configuration file. Optional if dataset_id is provided.
        dataset_id: The CZ cryoET Data Portal dataset ID. Optional if config_path is provided.
        overlay_root: The root URL for the overlay storage. Required if dataset_id is provided.
        
    Returns:
        Formatted list of meshes.
    """
    try:
        if config_path:
            root = get_copick_root_from_file(config_path)
        elif dataset_id and overlay_root:
            root = get_copick_root_from_dataset(dataset_id, overlay_root)
        else:
            return "Error: Either config_path or (dataset_id and overlay_root) must be provided."
        
        run = root.get_run(run_name)
        if not run:
            return f"Run '{run_name}' not found."
        
        meshes = run.get_meshes(object_name=object_name, user_id=user_id, session_id=session_id)
        if not meshes:
            filter_msg = ""
            if object_name:
                filter_msg += f", object '{object_name}'"
            if user_id:
                filter_msg += f", user '{user_id}'"
            if session_id:
                filter_msg += f", session '{session_id}'"
            return f"No meshes found for run '{run_name}'{filter_msg}."
        
        result = f"Meshes for run '{run_name}':\n\n"
        for mesh in meshes:
            result += f"Object: {mesh.pickable_object_name}, User: {mesh.user_id}, Session: {mesh.session_id}\n"
        
        return result
    except Exception as e:
        logger.error(f"Failed to list meshes: {str(e)}", exc_info=True)
        return f"Failed to list meshes: {str(e)}"

@mcp.tool()
async def get_project_info(config_path: Optional[str] = None, dataset_id: Optional[int] = None, 
                          overlay_root: Optional[str] = None) -> str:
    """Get general information about the Copick project.
    
    Args:
        config_path: Path to the Copick configuration file. Optional if dataset_id is provided.
        dataset_id: The CZ cryoET Data Portal dataset ID. Optional if config_path is provided.
        overlay_root: The root URL for the overlay storage. Required if dataset_id is provided.
        
    Returns:
        Formatted project information.
    """
    try:
        if config_path:
            root = get_copick_root_from_file(config_path)
        elif dataset_id and overlay_root:
            root = get_copick_root_from_dataset(dataset_id, overlay_root)
        else:
            return "Error: Either config_path or (dataset_id and overlay_root) must be provided."
        
        result = "Copick Project Information:\n\n"
        
        # Add project metadata
        if hasattr(root.config, 'name'):
            result += f"Project Name: {root.config.name}\n"
        if hasattr(root.config, 'description'):
            result += f"Description: {root.config.description}\n"
        if hasattr(root.config, 'version'):
            result += f"Version: {root.config.version}\n"
        
        # Count various entities
        run_count = len(root.runs) if hasattr(root, 'runs') else 0
        object_count = len(root.pickable_objects) if hasattr(root, 'pickable_objects') else 0
        
        result += f"\nStatistics:\n"
        result += f"  Total Runs: {run_count}\n"
        result += f"  Total Pickable Objects: {object_count}\n"
        
        return result
    except Exception as e:
        logger.error(f"Failed to get project info: {str(e)}", exc_info=True)
        return f"Failed to get project info: {str(e)}"

@mcp.tool()
async def create_new_picks(run_name: str, object_name: str, session_id: str, user_id: Optional[str] = None,
                          config_path: Optional[str] = None, dataset_id: Optional[int] = None, 
                          overlay_root: Optional[str] = None) -> str:
    """Create a new picks set for a specific run, object, session, and user.
    
    Args:
        run_name: Name of the run.
        object_name: Name of the object.
        session_id: Session ID for the picks.
        user_id: User ID for the picks. Optional if set in the project config.
        config_path: Path to the Copick configuration file. Optional if dataset_id is provided.
        dataset_id: The CZ cryoET Data Portal dataset ID. Optional if config_path is provided.
        overlay_root: The root URL for the overlay storage. Required if dataset_id is provided.
        
    Returns:
        Status message.
    """
    try:
        if config_path:
            root = get_copick_root_from_file(config_path)
        elif dataset_id and overlay_root:
            root = get_copick_root_from_dataset(dataset_id, overlay_root)
        else:
            return "Error: Either config_path or (dataset_id and overlay_root) must be provided."
        
        run = root.get_run(run_name)
        if not run:
            return f"Run '{run_name}' not found."
        
        # If user_id not provided, use the project's user_id if available
        if not user_id:
            user_id = root.user_id
            if not user_id:
                return "Error: user_id must be provided or set in the project config."
        
        # Check if the object exists
        obj = root.get_object(object_name)
        if not obj:
            return f"Object '{object_name}' not found."
        
        # Check if the picks already exist
        existing_picks = run.get_picks(object_name=object_name, user_id=user_id, session_id=session_id)
        if existing_picks:
            return f"Picks for object '{object_name}', user '{user_id}', and session '{session_id}' already exist."
        
        # Create new picks
        picks = run.new_picks(object_name=object_name, user_id=user_id, session_id=session_id)
        return f"Successfully created new picks for object '{object_name}', user '{user_id}', and session '{session_id}'."
    except Exception as e:
        logger.error(f"Failed to create new picks: {str(e)}", exc_info=True)
        return f"Failed to create new picks: {str(e)}"

@mcp.tool()
async def get_json_config(config_path: str) -> str:
    """Get the JSON configuration of a Copick project.
    
    Args:
        config_path: Path to the Copick configuration file.
        
    Returns:
        JSON configuration as a string.
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        return json.dumps(config, indent=2)
    except Exception as e:
        logger.error(f"Failed to get JSON config: {str(e)}", exc_info=True)
        return f"Failed to get JSON config: {str(e)}"

# Run the MCP server
if __name__ == "__main__":
    mcp.run(transport='stdio')