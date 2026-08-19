from copy import deepcopy

from copick_mcp import main


def _explore(config_path):
    path = str(config_path)
    return {
        "runs": main.list_runs(path),
        "run": main.get_run_details(path, "run-001"),
        "objects": main.list_objects(path),
        "tomograms": main.list_tomograms(path, "run-001", 10.0),
        "picks": main.list_picks(path, "run-001"),
        "segmentations": main.list_segmentations(path, "run-001"),
        "voxel_spacings": main.list_voxel_spacings(path, "run-001"),
        "meshes": main.list_meshes(path, "run-001"),
        "project": main.get_project_info(path),
        "config": main.get_json_config(path),
    }


def test_metadata_tools_match_for_legacy_and_v3_projects(project_configs):
    legacy = _explore(project_configs["v2"])
    migrated = _explore(project_configs["v3"])

    assert all(result["success"] for result in legacy.values())
    assert all(result["success"] for result in migrated.values())

    # The project roots are intentionally different; all logical metadata is equal.
    normalized_legacy = deepcopy(legacy)
    normalized_migrated = deepcopy(migrated)
    normalized_legacy["config"]["config"]["overlay_root"] = "local://PROJECT"
    normalized_migrated["config"]["config"]["overlay_root"] = "local://PROJECT"
    assert normalized_legacy == normalized_migrated

    assert legacy["runs"]["runs"] == [{"name": "run-001"}]
    assert legacy["picks"]["picks"][0]["sample_points"] == [{"x": 1.0, "y": 2.0, "z": 3.0}]
    assert legacy["tomograms"]["tomograms"] == [{"tomo_type": "wbp", "features": [{"feature_type": "edge"}]}]


def test_metadata_tools_return_structured_errors(project_configs, tmp_path):
    config_path = str(project_configs["v3"])

    assert main.get_run_details(config_path, "missing") == {"success": False, "error": "Run 'missing' not found"}
    assert main.list_tomograms(config_path, "run-001", 99.0) == {
        "success": False,
        "error": "Voxel spacing '99.0' not found in run 'run-001'",
    }
    assert main.list_runs(str(tmp_path / "missing.json"))["success"] is False


def test_root_cache_is_scoped_by_config_path(project_configs):
    legacy_path = str(project_configs["v2"])
    migrated_path = str(project_configs["v3"])

    legacy = main.get_copick_root_from_file(legacy_path)
    assert main.get_copick_root_from_file(legacy_path) is legacy
    assert main.get_copick_root_from_file(migrated_path) is not legacy
