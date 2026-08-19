import asyncio

from fastmcp import Client

from copick_mcp.main import mcp

EXPECTED_TOOLS = {
    "list_runs",
    "get_run_details",
    "list_objects",
    "list_tomograms",
    "list_picks",
    "list_segmentations",
    "list_voxel_spacings",
    "list_meshes",
    "get_project_info",
    "get_json_config",
    "list_copick_cli_commands",
    "get_copick_cli_command_info",
    "validate_copick_cli_command",
    "get_nnunet_workflow_info",
}


def test_fastmcp_registration_and_serialization(project_configs):
    async def exercise_server():
        async with Client(mcp) as client:
            tools = await client.list_tools()
            assert {tool.name for tool in tools} == EXPECTED_TOOLS

            runs = await client.call_tool("list_runs", {"config_path": str(project_configs["v3"])})
            assert runs.data["runs"] == [{"name": "run-001"}]

            commands = await client.call_tool("list_copick_cli_commands", {})
            assert {item["name"] for item in commands.data["commands"]["setup"]} == {
                "mcp",
                "mcp-remove",
                "mcp-status",
            }

            validation = await client.call_tool(
                "validate_copick_cli_command",
                {"command_string": "copick setup mcp --help"},
            )
            assert validation.data["valid"] is True

    asyncio.run(exercise_server())
