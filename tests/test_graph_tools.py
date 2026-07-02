"""Integration test: the copick-graph pipeline tools are registered on the copick MCP server and
return well-formed JSON. Importing ``copick_mcp.main`` registers the tools; we call each tool's
underlying function via ``mcp.get_tool(name).fn`` (fastmcp 3.x)."""

import asyncio

import copick_mcp.main as m

TOOLS = [
    "graph_list_ops",
    "graph_get_op",
    "graph_schema",
    "graph_examples",
    "graph_validate",
    "graph_plan",
    "graph_render",
    "graph_visualize",
    "graph_run",
]


def _get_tool(name):
    return asyncio.run(m.mcp.get_tool(name))  # get_tool is async in fastmcp 3.x


def _fn(name):
    tool = _get_tool(name)
    assert tool is not None, f"{name} is not registered on the MCP server"
    return tool.fn


def _example_graph():
    return _fn("graph_examples")()["examples"]["simple_segop_clipseg"]


def test_all_graph_tools_registered():
    for name in TOOLS:
        assert _get_tool(name) is not None, name


def test_list_ops_schema_get_op():
    ops = _fn("graph_list_ops")()
    assert ops["success"] and len(ops["ops"]) == 12
    assert all(o.get("help") for o in ops["ops"])
    sch = _fn("graph_schema")()
    assert sch["success"] and "$defs" in sch["schema"]
    op = _fn("graph_get_op")("logical.segop")
    assert op["success"] and op["op"]["op_id"] == "logical.segop"


def test_authoring_flow_offline():
    g = _example_graph()
    v = _fn("graph_validate")(g)
    assert v["success"] and v["valid"], v
    p = _fn("graph_plan")(g)
    assert p["success"] and p["plan"]["tasks"]
    rb = _fn("graph_render")(g, backend="bash")
    assert rb["success"] and "copick logical segop" in rb["script"]
    rn = _fn("graph_render")(g, backend="nextflow")
    assert rn["success"] and "process p_" in rn["script"] and "slurm" in rn["nextflow_config"]
    viz = _fn("graph_visualize")(g)
    assert viz["success"] and viz["mermaid"].startswith("graph TD")


def test_graph_run_requires_config_and_errors_cleanly():
    r = _fn("graph_run")(_example_graph(), "")  # empty config path
    assert r["success"] is False and "config" in r["error"].lower()


def test_validate_reports_bad_graph_without_crashing():
    r = _fn("graph_validate")("op: nope\nproject: {config: x}")  # not a valid GraphSpec
    assert r["success"] is True and r["valid"] is False and r["errors"]
