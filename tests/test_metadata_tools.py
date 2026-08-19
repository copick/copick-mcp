import json
from copy import deepcopy
from pathlib import Path

from copick_mcp import main


def _explore(config_path):
    path = str(config_path)
    return {
        "runs": main.list_runs(config_path=path),
        "run": main.get_run_details(run_name="run-001", config_path=path),
        "objects": main.list_objects(config_path=path),
        "tomograms": main.list_tomograms(run_name="run-001", voxel_spacing=10.0, config_path=path),
        "picks": main.list_picks(run_name="run-001", config_path=path),
        "segmentations": main.list_segmentations(run_name="run-001", config_path=path),
        "voxel_spacings": main.list_voxel_spacings(run_name="run-001", config_path=path),
        "meshes": main.list_meshes(run_name="run-001", config_path=path),
        "project": main.get_project_info(config_path=path),
        "config": main.get_json_config(config_path=path),
    }


def test_metadata_tools_report_equivalent_filesystem_entities(project_configs):
    first = _explore(project_configs["first"])
    second = _explore(project_configs["second"])

    assert all(result["success"] for result in first.values())
    assert all(result["success"] for result in second.values())

    # The project roots are intentionally different; all logical metadata is equal.
    normalized_first = deepcopy(first)
    normalized_second = deepcopy(second)
    normalized_first["config"]["config"]["overlay_root"] = "local://PROJECT"
    normalized_second["config"]["config"]["overlay_root"] = "local://PROJECT"
    assert normalized_first == normalized_second

    assert first["runs"]["runs"] == [{"name": "run-001"}]
    assert first["picks"]["picks"][0]["num_points"] is None
    assert first["tomograms"]["tomograms"] == [{"tomo_type": "wbp", "features": [{"feature_type": "edge"}]}]


def test_point_details_are_opt_in_and_failures_are_isolated(project_configs):
    config_path = project_configs["first"]
    picks_path = config_path.parent / "ExperimentRuns" / "run-001" / "Picks"
    (picks_path / "bob_manual_ribosome.json").write_text("not-json")

    summary = main.list_picks(run_name="run-001", config_path=str(config_path))
    detailed = main.list_picks(run_name="run-001", config_path=str(config_path), include_point_details=True)
    run_details = main.get_run_details(
        run_name="run-001",
        config_path=str(config_path),
        include_point_details=True,
    )

    assert summary["success"] is True
    assert all(pick["num_points"] is None and "point_details_error" not in pick for pick in summary["picks"])
    assert detailed["success"] is True
    assert len(detailed["picks"]) == 2
    assert next(pick for pick in detailed["picks"] if pick["user_id"] == "alice")["sample_points"] == [
        {"x": 1.0, "y": 2.0, "z": 3.0},
    ]
    assert "point_details_error" in next(pick for pick in detailed["picks"] if pick["user_id"] == "bob")
    assert run_details["success"] is True
    assert run_details["meshes"]


def test_metadata_tools_return_structured_errors(project_configs, tmp_path):
    config_path = str(project_configs["second"])

    assert main.get_run_details(run_name="missing", config_path=config_path) == {
        "success": False,
        "error": "Run 'missing' not found",
    }
    assert main.list_tomograms(run_name="run-001", voxel_spacing=99.0, config_path=config_path) == {
        "success": False,
        "error": "Voxel spacing '99.0' not found in run 'run-001'",
    }
    assert main.list_runs(config_path=str(tmp_path / "missing.json"))["success"] is False


def test_default_config_environment_and_normalized_config(project_configs, monkeypatch):
    config_path = str(project_configs["first"])
    monkeypatch.setenv(main.DEFAULT_CONFIG_ENV, config_path)

    assert main.list_runs()["runs"] == [{"name": "run-001"}]
    config = main.get_json_config()
    assert config["success"] is True
    assert config["config"]["config_type"] == "filesystem"
    assert config["config"]["name"] == "mcp-entity-discovery"

    explicit = main.get_json_config(config_path=str(project_configs["second"]))
    assert explicit["config"]["overlay_root"].endswith("project-second")

    monkeypatch.delenv(main.DEFAULT_CONFIG_ENV)
    missing = main.list_runs()
    assert missing["success"] is False
    assert main.DEFAULT_CONFIG_ENV in missing["error"]


def test_get_json_config_rejects_non_copick_and_oversized_json(tmp_path):
    credentials = tmp_path / "credentials.json"
    credentials.write_text(json.dumps({"access_token": "secret"}))
    assert main.get_json_config(config_path=str(credentials))["success"] is False

    oversized = tmp_path / "oversized.json"
    oversized.write_text(" " * (main.MAX_CONFIG_SIZE_BYTES + 1))
    result = main.get_json_config(config_path=str(oversized))
    assert result["success"] is False
    assert "size limit" in result["error"]


def test_root_cache_is_scoped_by_config_path(project_configs):
    first_path = str(project_configs["first"])
    second_path = str(project_configs["second"])

    first = main.get_copick_root_from_file(first_path)
    assert main.get_copick_root_from_file(first_path) is first
    assert main.get_copick_root_from_file(second_path) is not first


def test_root_cache_invalidates_changed_configs(project_configs):
    config_path = Path(project_configs["first"])
    first = main.get_copick_root_from_file(str(config_path))
    config = json.loads(config_path.read_text())
    config["description"] = "Changed metadata description"
    config_path.write_text(json.dumps(config))

    changed = main.get_copick_root_from_file(str(config_path))
    assert changed is not first
    assert changed.config.description == "Changed metadata description"


def test_root_cache_evicts_least_recently_used_config(tmp_path, monkeypatch):
    paths = []
    for index in range(main.ROOT_CACHE_MAX_SIZE + 1):
        path = tmp_path / f"config-{index}.json"
        path.write_text("{}")
        paths.append(path)

    monkeypatch.setattr(main.copick, "from_file", lambda path: object())
    for path in paths:
        main.get_copick_root_from_file(str(path))

    assert len(main._copick_cache) == main.ROOT_CACHE_MAX_SIZE
    assert str(paths[0].resolve()) not in main._copick_cache


def test_filtered_empty_results_have_readable_messages(project_configs):
    config_path = str(project_configs["first"])
    result = main.list_picks(run_name="run-001", config_path=config_path, object_name="missing")
    assert result["message"] == "No picks found for run 'run-001' with object 'missing'"
