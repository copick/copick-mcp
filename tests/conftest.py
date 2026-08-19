import json
from pathlib import Path

import pytest

from copick_mcp import main


def _write_zarr_root(path: Path, zarr_format: int) -> None:
    path.mkdir(parents=True)
    if zarr_format == 2:
        (path / ".zgroup").write_text(json.dumps({"zarr_format": 2}))
        (path / ".zattrs").write_text(
            json.dumps({"multiscales": [{"version": "0.4", "datasets": [{"path": "0"}]}]}),
        )
    else:
        (path / "zarr.json").write_text(
            json.dumps(
                {
                    "zarr_format": 3,
                    "node_type": "group",
                    "attributes": {"ome": {"version": "0.5", "multiscales": [{"datasets": [{"path": "0"}]}]}},
                },
            ),
        )


def _build_project(path: Path, zarr_format: int) -> Path:
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
    (meshes / "alice_manual_ribosome.glb").write_bytes(b"")

    voxel_spacing = run / "VoxelSpacing10.000"
    _write_zarr_root(voxel_spacing / "wbp.zarr", zarr_format)
    _write_zarr_root(voxel_spacing / "wbp_edge_features.zarr", zarr_format)
    _write_zarr_root(run / "Segmentations" / "10.000_alice_manual_ribosome.zarr", zarr_format)

    config = {
        "config_type": "filesystem",
        "name": "mcp-format-parity",
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
        "v2": _build_project(tmp_path / "project-v2", 2),
        "v3": _build_project(tmp_path / "project-v3", 3),
    }


@pytest.fixture(autouse=True)
def clear_copick_cache():
    main._copick_cache.clear()
    yield
    main._copick_cache.clear()
