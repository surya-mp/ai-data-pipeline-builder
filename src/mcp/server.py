from __future__ import annotations

import asyncio

from mcp.server.fastmcp import FastMCP

from src.models import DataContract, JoinCondition
from src.service import PipelineService

mcp = FastMCP("AI Data Pipeline Builder")
service = PipelineService()


@mcp.tool()
def create_pipeline(request: str, output: str | None = None, sql: str | None = None, joins: list[dict] | None = None, workspace_id: str = "default") -> dict:
    """Create a validated pipeline from analytical intent; optional SQL overrides generated SQL."""
    mappings = [JoinCondition.model_validate(join) for join in joins] if joins else None
    return service.create_pipeline(request, output, sql, mappings, workspace_id).model_dump(mode="json")


@mcp.tool()
def list_pipelines() -> list[dict]:
    """List saved pipelines and their review state."""
    return service.list_pipelines()


@mcp.tool()
def preview_sql(pipeline_id: str) -> str:
    """Return the materialization SQL for a created pipeline."""
    return service.preview_sql(pipeline_id)


@mcp.tool()
def edit_pipeline_sql(pipeline_id: str, sql: str) -> dict:
    """Validate and persist a reviewed SQL edit while the plan is pending approval."""
    return service.edit_pipeline_sql(pipeline_id, sql)


@mcp.tool()
def get_query_cost(pipeline_id: str) -> dict:
    """Return the database EXPLAIN plan for a pending pipeline SQL statement."""
    return service.query_cost(pipeline_id)


@mcp.tool()
def validate_pipeline(pipeline_id: str) -> list[str]:
    """Verify SQL safety and the pipeline's dependency graph."""
    return service.validate_pipeline(service.load_definition(pipeline_id))


@mcp.tool()
def run_pipeline(pipeline_id: str) -> dict:
    """Execute or resume the pipeline, including quality checks and EDA."""
    return asyncio.run(service.run_pipeline(pipeline_id)).model_dump(mode="json")


@mcp.tool()
def cancel_pipeline(pipeline_id: str) -> dict:
    """Request cooperative cancellation of a running pipeline."""
    service.cancel_pipeline(pipeline_id)
    return {"pipeline_id": pipeline_id, "cancellation_requested": True}


@mcp.tool()
def get_semantic_plan(pipeline_id: str) -> dict:
    """Return plan assumptions, target grain, metrics, and current human review state."""
    return service.semantic_plan(pipeline_id)


@mcp.tool()
def review_pipeline(pipeline_id: str, reviewer: str, approved: bool = True, note: str | None = None) -> dict:
    """Approve or reject a semantic plan. Only approved plans may execute."""
    return service.approve_pipeline(pipeline_id, reviewer, note, approved).model_dump(mode="json")


@mcp.tool()
def save_data_contract(contract: dict) -> dict:
    """Save a schema contract for a source asset before pipeline planning or execution."""
    return service.save_contract(DataContract.model_validate(contract)).model_dump(mode="json")


@mcp.tool()
def get_schema_drift(pipeline_id: str) -> list[dict]:
    """Compare live source schemas and contracts against the pipeline planning snapshot."""
    return [report.model_dump(mode="json") for report in service.schema_drift(service.load_definition(pipeline_id))]


@mcp.tool()
def get_pipeline_status(pipeline_id: str) -> dict:
    """Return persisted pipeline/task status."""
    return service.status(pipeline_id).model_dump(mode="json")


@mcp.tool()
def get_pipeline_lineage(pipeline_id: str) -> dict:
    """Return input tables, task dependencies, and output table."""
    return service.lineage(pipeline_id)


@mcp.tool()
def get_impact_analysis(source_table: str, source_column: str | None = None) -> dict:
    """Find materialized datasets and columns impacted by a source table or column change."""
    return service.impact_analysis(source_table, source_column)


@mcp.tool()
def get_pipeline_observability(pipeline_id: str) -> dict:
    """Return structured events and duration/task metrics for a pipeline run."""
    return service.observability(pipeline_id)


@mcp.tool()
def get_eda_report(pipeline_id: str) -> str:
    """Return the filesystem path to the completed HTML EDA report."""
    return str(service.report_path(pipeline_id))


def main() -> None:
    mcp.run(transport="stdio")
