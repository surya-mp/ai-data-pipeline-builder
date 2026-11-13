from __future__ import annotations

from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from src.models import DataContract, JoinCondition
from src.service import PipelineService


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.service = PipelineService()
    yield


app = FastAPI(title="AI Data Pipeline Builder", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def api_key_auth(request: Request, call_next):
    """Opt-in API-key gate; local development remains frictionless when unset."""
    expected = getattr(request.app.state, "service", None)
    api_key = expected.settings.api_key if expected else None
    if api_key and request.url.path != "/health" and request.headers.get("X-API-Key") != api_key:
        return JSONResponse(status_code=401, content={"detail": "Invalid or missing X-API-Key."})
    return await call_next(request)


class CreatePipelineRequest(BaseModel):
    request: str = Field(min_length=3, examples=["Create a customer analytical dataset using customers and orders"])
    output: str | None = Field(default=None, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    sql: str | None = None
    joins: list[JoinCondition] = Field(default_factory=list)
    workspace_id: str = Field(default="default", pattern=r"^[A-Za-z0-9_-]+$")


class ReviewPipelineRequest(BaseModel):
    reviewer: str = Field(min_length=1)
    note: str | None = None
    approved: bool = True


class ScheduleRequest(BaseModel):
    schedule: str | None = None


class SqlEditRequest(BaseModel):
    sql: str = Field(min_length=1)


def service() -> PipelineService:
    return app.state.service


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/schemas")
def schemas():
    return service().database.schemas()


@app.post("/pipelines", status_code=201)
def create_pipeline(payload: CreatePipelineRequest):
    try:
        return service().create_pipeline(payload.request, payload.output, payload.sql, payload.joins, payload.workspace_id)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.get("/pipelines")
def list_pipelines(workspace_id: str | None = None):
    return service().list_pipelines(workspace_id)


@app.get("/pipelines/{pipeline_id}")
def pipeline_definition(pipeline_id: str):
    try:
        return service().load_definition(pipeline_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/pipelines/{pipeline_id}/sql")
def preview_sql(pipeline_id: str):
    try:
        return {"sql": service().preview_sql(pipeline_id)}
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/pipelines/{pipeline_id}/cost")
def query_cost(pipeline_id: str):
    try:
        return service().query_cost(pipeline_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.put("/pipelines/{pipeline_id}/sql")
def edit_sql(pipeline_id: str, payload: SqlEditRequest):
    try:
        return service().edit_pipeline_sql(pipeline_id, payload.sql)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.post("/pipelines/{pipeline_id}/validate")
def validate_pipeline(pipeline_id: str):
    try:
        return {"messages": service().validate_pipeline(service().load_definition(pipeline_id))}
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.post("/pipelines/{pipeline_id}/run")
async def run_pipeline(pipeline_id: str):
    try:
        return await service().run_pipeline(pipeline_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(422, str(exc)) from exc


@app.post("/pipelines/{pipeline_id}/cancel")
def cancel_pipeline(pipeline_id: str):
    try:
        service().cancel_pipeline(pipeline_id)
        return {"pipeline_id": pipeline_id, "cancellation_requested": True}
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.put("/pipelines/{pipeline_id}/schedule")
def set_schedule(pipeline_id: str, payload: ScheduleRequest):
    try:
        return service().set_schedule(pipeline_id, payload.schedule)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.get("/pipelines/{pipeline_id}/semantic-plan")
def semantic_plan(pipeline_id: str):
    try:
        return service().semantic_plan(pipeline_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.post("/pipelines/{pipeline_id}/review")
def review_pipeline(pipeline_id: str, payload: ReviewPipelineRequest):
    try:
        return service().approve_pipeline(pipeline_id, payload.reviewer, payload.note, payload.approved)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/pipelines/{pipeline_id}/schema-drift")
def schema_drift(pipeline_id: str):
    try:
        return service().schema_drift(service().load_definition(pipeline_id))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/contracts")
def list_contracts():
    return service().contracts.list()


@app.put("/contracts/{asset}")
def save_contract(asset: str, contract: DataContract):
    if contract.asset != asset:
        raise HTTPException(422, "The contract asset must match the URL asset.")
    return service().save_contract(contract)


@app.get("/pipelines/{pipeline_id}/status")
def pipeline_status(pipeline_id: str):
    try:
        return service().status(pipeline_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/pipelines/{pipeline_id}/lineage")
def pipeline_lineage(pipeline_id: str):
    try:
        return service().lineage(pipeline_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/impact")
def impact_analysis(source_table: str, source_column: str | None = None):
    return service().impact_analysis(source_table, source_column)


@app.get("/pipelines/{pipeline_id}/observability")
def pipeline_observability(pipeline_id: str):
    try:
        return service().observability(pipeline_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/pipelines/{pipeline_id}/runs")
def run_history(pipeline_id: str):
    try:
        return service().run_history(pipeline_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/pipelines/{pipeline_id}/eda")
def eda_report(pipeline_id: str):
    try:
        path = service().report_path(pipeline_id)
        return FileResponse(path, media_type="text/html", filename=path.name)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc


def run() -> None:
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)
