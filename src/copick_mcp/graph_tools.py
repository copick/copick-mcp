"""copick-dag pipeline tools for the Copick MCP server (M5).

Thin `@mcp.tool()` wrappers over ``copick_dag.api`` so an LLM can author, validate, inspect,
render, and run copick-dag pipelines. A ``graph`` argument is a YAML/JSON **string** (how the
assistant authors it); ``config_path`` is an optional copick config (offline validate/plan/render
works when the graph's ``sweep``/``runs`` supply the run-set; execution requires it).

Call ``register(mcp, logger)`` from ``main.py`` after the server is created (no circular import —
this module imports ``copick_dag.api``, not ``copick_mcp.main``).
"""

from __future__ import annotations

from typing import Any, Dict


def register(mcp, logger=None) -> None:
    """Register the copick-dag pipeline tools on the given FastMCP server."""
    import copick_dag.api as cg

    def _fail(exc: Exception) -> Dict[str, Any]:
        if logger is not None:
            logger.exception(str(exc))
        return {"success": False, "error": str(exc)}

    @mcp.tool()
    def graph_list_ops() -> Dict[str, Any]:
        """List all copick-dag pipeline operations available for building a pipeline.

        Each op has an id (e.g. 'logical.segop', 'process.combine'), a scope (per_run or barrier),
        input/output URI slots, parameters, and CLI help. Use these plus graph_schema() to author a
        pipeline graph.
        """
        try:
            return {"success": True, **cg.describe_ops()}
        except Exception as e:  # noqa: BLE001
            return _fail(e)

    @mcp.tool()
    def graph_get_op(op_id: str) -> Dict[str, Any]:
        """Get detailed metadata + CLI help for one copick-dag operation (e.g. 'logical.clipseg')."""
        try:
            return {"success": True, "op": cg.describe_op(op_id)}
        except Exception as e:  # noqa: BLE001
            return _fail(e)

    @mcp.tool()
    def graph_schema() -> Dict[str, Any]:
        """JSON schema for a copick-dag pipeline (GraphSpec): nodes, the sweep matrix, CSV expand,
        switch output-routing, coalesce inputs, and the finalize block. Use to author a valid graph.
        """
        try:
            return {"success": True, "schema": cg.graph_schema()}
        except Exception as e:  # noqa: BLE001
            return _fail(e)

    @mcp.tool()
    def graph_examples() -> Dict[str, Any]:
        """Return example pipeline graphs (YAML) to use as authoring templates."""
        try:
            return {"success": True, "examples": cg.list_examples()}
        except Exception as e:  # noqa: BLE001
            return _fail(e)

    @mcp.tool()
    def graph_validate(graph: str, config_path: str = "") -> Dict[str, Any]:
        """Validate a pipeline graph (YAML/JSON string) against the schema, op registry, and planner.

        Returns {valid, errors, warnings, task_count, runs, barriers}. config_path is optional
        (validation runs offline when the graph's sweep/runs supply the run-set).
        """
        try:
            return {"success": True, **cg.validate_graph(graph, config_path or None)}
        except Exception as e:  # noqa: BLE001
            return _fail(e)

    @mcp.tool()
    def graph_plan(graph: str, config_path: str = "") -> Dict[str, Any]:
        """Compile a pipeline graph to its concrete DAG.

        Returns the resolved tasks (op, run, voxel_spacing, inputs/outputs, deps, scope, optional)
        plus a summary. config_path is optional (offline if the graph supplies its run-set).
        """
        try:
            return {"success": True, "plan": cg.plan_graph(graph, config_path or None)}
        except Exception as e:  # noqa: BLE001
            return _fail(e)

    @mcp.tool()
    def graph_render(graph: str, config_path: str = "", backend: str = "bash") -> Dict[str, Any]:
        """Render a pipeline graph to a runnable artifact.

        backend='bash' returns a `copick` CLI script; backend='nextflow' returns a DSL2 workflow plus
        a nextflow.config (local + slurm profiles). config_path is optional.
        """
        try:
            return {"success": True, **cg.render_graph(graph, config_path or None, backend)}
        except Exception as e:  # noqa: BLE001
            return _fail(e)

    @mcp.tool()
    def graph_visualize(graph: str, config_path: str = "") -> Dict[str, Any]:
        """Visualize a pipeline graph's dependency DAG as a Mermaid flowchart + a text listing."""
        try:
            return {"success": True, **cg.visualize_graph(graph, config_path or None)}
        except Exception as e:  # noqa: BLE001
            return _fail(e)

    @mcp.tool()
    def graph_run(graph: str, config_path: str, backend: str = "memory", workers: int = 1) -> Dict[str, Any]:
        """EXECUTE a pipeline graph, WRITING results (segmentations) into the copick project.

        This mutates data. backend='memory' runs in-process (write-through to zarr); backend='bash'
        runs the emitted `copick` CLI script. config_path is required. For 'nextflow', use
        graph_render(backend='nextflow') to emit the workflow and run it with the nextflow binary.
        """
        try:
            return {"success": True, **cg.run_graph(graph, config_path, backend, workers)}
        except Exception as e:  # noqa: BLE001
            return _fail(e)
