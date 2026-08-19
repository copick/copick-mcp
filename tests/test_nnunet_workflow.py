from copick_mcp import cli_introspection, main


def test_nnunet_workflow_reports_missing_optional_package(monkeypatch):
    monkeypatch.setattr(main.importlib.util, "find_spec", lambda name: None)

    result = main.get_nnunet_workflow_info()

    assert result["copick_torch_installed"] is False
    assert "install_instructions" in result
    assert "commands" not in result


def test_nnunet_workflow_uses_installed_command_metadata(monkeypatch):
    monkeypatch.setattr(main.importlib.util, "find_spec", lambda name: object())
    monkeypatch.setattr(
        cli_introspection,
        "get_command_info",
        lambda path: {"success": True, "path": path, "parameters": []},
    )

    result = main.get_nnunet_workflow_info()

    assert result["copick_torch_installed"] is True
    assert result["commands_available"] is True
    assert {step: details["path"] for step, details in result["commands"].items()} == main.NNUNET_COMMAND_PATHS
    assert "mednext-" not in result["workflow"]
    assert "--run-ids" not in result["workflow"]
