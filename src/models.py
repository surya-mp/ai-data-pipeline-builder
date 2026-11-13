from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class Transformation(BaseModel):
    name: str
    description: str
    sql: str | None = None


class AggregateMetric(BaseModel):
    name: str
    function: str = Field(pattern=r"^(SUM|AVG|COUNT|MIN|MAX)$")
    column: str


class JoinCondition(BaseModel):
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_type: str = Field(default="LEFT", pattern=r"^(INNER|LEFT)$")


class FilterDefinition(BaseModel):
    column: str
    operator: str = Field(pattern=r"^(=|!=|<|<=|>|>=|IN|IS NOT NULL|IS NULL)$")
    value: str | int | float | bool | None = None


class PipelinePlan(BaseModel):
    objective: str
    source_tables: list[str] = Field(min_length=1)
    transformations: list[Transformation] = Field(default_factory=list)
    output: str
    group_by: list[str] = Field(default_factory=list, description="Qualified source columns, e.g. customers.customer_id")
    metrics: list[AggregateMetric] = Field(default_factory=list)
    joins: list[JoinCondition] = Field(default_factory=list)
    filters: list[FilterDefinition] = Field(default_factory=list)
    expected_grain: str | None = None

    @field_validator("source_tables", mode="before")
    @classmethod
    def unique_tables(cls, value: list[str]) -> list[str]:
        return list(dict.fromkeys(value))


class ColumnInfo(BaseModel):
    name: str
    data_type: str
    nullable: bool = True


class TableSchema(BaseModel):
    name: str
    columns: list[ColumnInfo]
    foreign_keys: list[dict[str, object]] = Field(default_factory=list)


class SqlValidationResult(BaseModel):
    valid: bool
    errors: list[str] = Field(default_factory=list)
    referenced_tables: list[str] = Field(default_factory=list)
    referenced_columns: list[str] = Field(default_factory=list)


class TaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class TaskDefinition(BaseModel):
    id: str
    kind: str
    depends_on: list[str] = Field(default_factory=list)
    sql: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    retries: int = Field(default=1, ge=0, le=5)


class ColumnLineage(BaseModel):
    output_column: str
    source_columns: list[str]
    expression: str


class ContractColumn(BaseModel):
    name: str
    data_type: str | None = None
    nullable: bool | None = None
    required: bool = True


class DataContract(BaseModel):
    asset: str
    version: str = "1"
    columns: list[ContractColumn]
    allow_additive_columns: bool = True
    freshness_column: str | None = None
    max_age_hours: int | None = Field(default=None, ge=1)
    primary_key: list[str] = Field(default_factory=list)
    null_thresholds: dict[str, float] = Field(default_factory=dict)
    accepted_values: dict[str, list[str | int | float | bool]] = Field(default_factory=dict)
    ranges: dict[str, tuple[float | None, float | None]] = Field(default_factory=dict)
    custom_assertions: list[str] = Field(default_factory=list)


class ContractIssue(BaseModel):
    severity: str = Field(pattern=r"^(warning|blocking)$")
    kind: str
    detail: str


class ContractReport(BaseModel):
    asset: str
    valid: bool
    issues: list[ContractIssue] = Field(default_factory=list)


class ReviewStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class SemanticReview(BaseModel):
    status: ReviewStatus = ReviewStatus.PENDING
    reviewer: str | None = None
    note: str | None = None
    reviewed_at: datetime | None = None


class PipelineDefinition(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    plan: PipelinePlan
    tasks: list[TaskDefinition]
    column_lineage: list[ColumnLineage] = Field(default_factory=list)
    source_schema_snapshot: dict[str, TableSchema] = Field(default_factory=dict)
    semantic_review: SemanticReview = Field(default_factory=SemanticReview)
    materialized_output: str | None = None
    workspace_id: str = "default"
    schedule: str | None = None
    max_concurrency: int = Field(default=1, ge=1, le=16)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class TaskRun(BaseModel):
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


class RunEvent(BaseModel):
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    event: str
    task_id: str | None = None
    detail: str | None = None


class RunMetrics(BaseModel):
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int = 0
    tasks_succeeded: int = 0
    tasks_failed: int = 0
    task_duration_ms: dict[str, int] = Field(default_factory=dict)


class PipelineRunState(BaseModel):
    pipeline_id: str
    status: TaskStatus = TaskStatus.PENDING
    tasks: dict[str, TaskRun]
    report_path: str | None = None
    quality_results: list[dict[str, Any]] = Field(default_factory=list)
    events: list[RunEvent] = Field(default_factory=list)
    metrics: RunMetrics = Field(default_factory=RunMetrics)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
