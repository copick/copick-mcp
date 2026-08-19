import json
from pathlib import Path

import pytest
import trimesh

from copick_mcp import main


def _write_entity_directory(path: Path) -> None:
    path.mkdir(parents=True)


def _build_project(path: Path) -> Path:
    run = path / "ExperimentRuns" / "run-001"
    picks = run / "Picks"
    meshes = run / "Meshes"
    picks.mkdir(parents=True)
    meshes.mkdir()

    pick = {
        "pickable_object_name": "ribosome",
        "user_id": "alice",
        "session_id": "manual",
        "run_name": "run-001",
        "voxel_spacing": 10.0,
        "unit": "angstrom",
        "points": [{"location": {"x": 1.0, "y": 2.0, "z": 3.0}}],
        "trust_orientation": False,
    }
    (picks / "alice_manual_ribosome.json").write_text(json.dumps(pick))
    mesh = trimesh.Trimesh(vertices=[[0, 0, 0], [1, 0, 0], [0, 1, 0]], faces=[[0, 1, 2]], process=False)
    mesh.export(str(meshes / "alice_manual_ribosome.glb"), file_type="glb")

    voxel_spacing = run / "VoxelSpacing10.000"
    _write_entity_directory(voxel_spacing / "wbp.zarr")
    _write_entity_directory(voxel_spacing / "wbp_edge_features.zarr")
    _write_entity_directory(run / "Segmentations" / "10.000_alice_manual_ribosome.zarr")

    config = {
        "config_type": "filesystem",
        "name": "mcp-entity-discovery",
        "description": "Minimal metadata-only MCP fixture",
        "version": "1.0.0",
        "user_id": "alice",
        "session_id": "manual",
        "pickable_objects": [
            {
                "name": "ribosome",
                "is_particle": True,
                "label": 1,
                "color": [255, 0, 0, 255],
                "radius": 120.0,
            },
        ],
        "overlay_root": f"local://{path}",
        "overlay_fs_args": {"auto_mkdir": True},
    }
    config_path = path / "config.json"
    config_path.write_text(json.dumps(config))
    return config_path


@pytest.fixture
def project_configs(tmp_path):
    return {
        "first": _build_project(tmp_path / "project-first"),
        "second": _build_project(tmp_path / "project-second"),
    }


@pytest.fixture(autouse=True)
def clear_copick_cache():
    main._copick_cache.clear()
    yield
    main._copick_cache.clear()
