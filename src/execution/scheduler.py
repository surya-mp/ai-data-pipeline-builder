from __future__ import annotations

import asyncio
import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.analytics.eda import EDAReporter
from src.contracts import ContractStore
from src.database import Database
from src.execution.dag import DAG
from src.execution.history import RunHistoryStore
from src.models import PipelineDefinition, PipelineRunState, TaskRun, TaskStatus
from src.models import RunEvent
from src.observability import emit
from src.quality.checks import DataQualityEngine
from src.sql.validator import SQLValidator


class PipelineExecutor:
    def __init__(self, database: Database, state_dir: Path, report_dir: Path, eda_max_rows: int = 100000) -> None:
        self.database = database
        self.state_dir = state_dir
        self.reporter = EDAReporter(database, report_dir, eda_max_rows)
        self.quality = DataQualityEngine(database)
        self.contracts = ContractStore(state_dir)
        self.cancelled: set[str] = set()
        self.history = RunHistoryStore(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._state_write_lock = threading.Lock()

    def state_path(self, pipeline_id: str) -> Path:
        return self.state_dir / f"{pipeline_id}.json"

    def load_state(self, definition: PipelineDefinition) -> PipelineRunState:
        path = self.state_path(definition.id)
        if path.exists():
            return PipelineRunState.model_validate_json(path.read_text(encoding="utf-8"))
        return PipelineRunState(pipeline_id=definition.id, tasks={task.id: TaskRun() for task in definition.tasks})

    def save_state(self, state: PipelineRunState) -> None:
        state.updated_at = datetime.now(timezone.utc)
        with self._state_write_lock:
            self.state_path(state.pipeline_id).write_text(state.model_dump_json(indent=2), encoding="utf-8")

    def record_event(self, state: PipelineRunState, event: str, task_id: str | None = None, detail: str | None = None) -> None:
        state.events.append(RunEvent(event=event, task_id=task_id, detail=detail))
        # Keep state artifacts bounded while logs retain the full event stream.
        state.events = state.events[-200:]
        emit(event, state.pipeline_id, task_id=task_id, detail=detail)

    async def run(self, definition: PipelineDefinition) -> PipelineRunState:
        dag = DAG(definition.tasks)
        state = self.load_state(definition)
        # A prior failure is retried on resume; successful tasks are preserved.
        for run in state.tasks.values():
            if run.status in (TaskStatus.FAILED, TaskStatus.RUNNING):
                run.status, run.error = TaskStatus.PENDING, None
        state.status = TaskStatus.RUNNING
        run_id = str(uuid4())
        self.history.started(run_id, definition.id)
        state.metrics.started_at = datetime.now(timezone.utc)
        self.record_event(state, "pipeline.started", detail="Execution started or resumed.")
        self.save_state(state)
        completed = {task_id for task_id, run in state.tasks.items() if run.status == TaskStatus.SUCCESS}
        semaphore = asyncio.Semaphore(definition.max_concurrency)
        while len(completed) < len(definition.tasks):
            if definition.id in self.cancelled:
                self._finish_cancelled(state, run_id)
                raise RuntimeError("Pipeline execution was cancelled.")
            ready = dag.ready(completed, set())
            if not ready:
                state.status = TaskStatus.FAILED
                state.metrics.finished_at = datetime.now(timezone.utc)
                self._finalize_metrics(state)
                self.record_event(state, "pipeline.failed", detail="No executable tasks remain.")
                self.save_state(state)
                self.history.finished(run_id, "failed", state.metrics.model_dump(), "No executable tasks remain.")
                raise RuntimeError("No executable tasks remain; dependencies may have failed.")
            results = await asyncio.gather(*(self._run_task(task, state, definition, semaphore) for task in ready), return_exceptions=True)
            if definition.id in self.cancelled:
                self._finish_cancelled(state, run_id)
                raise RuntimeError("Pipeline execution was cancelled.")
            failed = [result for result in results if isinstance(result, Exception)]
            completed = {task_id for task_id, run in state.tasks.items() if run.status == TaskStatus.SUCCESS}
            if failed:
                state.status = TaskStatus.FAILED
                state.metrics.finished_at = datetime.now(timezone.utc)
                self._finalize_metrics(state)
                self.record_event(state, "pipeline.failed", detail=str(failed[0]))
                self.save_state(state)
                self.history.finished(run_id, "failed", state.metrics.model_dump(), str(failed[0]))
                raise RuntimeError(str(failed[0])) from failed[0]
        state.status = TaskStatus.SUCCESS
        state.metrics.finished_at = datetime.now(timezone.utc)
        self._finalize_metrics(state)
        self.record_event(state, "pipeline.succeeded", detail="All tasks completed.")
        self.save_state(state)
        self.history.finished(run_id, "success", state.metrics.model_dump())
        return state

    def cancel(self, pipeline_id: str) -> None:
        self.cancelled.add(pipeline_id)

    def _finish_cancelled(self, state: PipelineRunState, run_id: str) -> None:
        self.cancelled.discard(state.pipeline_id)
        state.status = TaskStatus.CANCELLED
        state.metrics.finished_at = datetime.now(timezone.utc)
        self._finalize_metrics(state)
        self.record_event(state, "pipeline.cancelled", detail="Cancellation requested.")
        self.save_state(state)
        self.history.finished(run_id, "cancelled", state.metrics.model_dump(), "Cancellation requested.")

    async def _run_task(self, task, state: PipelineRunState, definition: PipelineDefinition, semaphore: asyncio.Semaphore) -> None:
        async with semaphore:
            await self._run_task_limited(task, state, definition)

    async def _run_task_limited(self, task, state: PipelineRunState, definition: PipelineDefinition) -> None:
        run = state.tasks[task.id]
        run.status, run.started_at = TaskStatus.RUNNING, datetime.now(timezone.utc)
        self.record_event(state, "task.started", task.id)
        self.save_state(state)
        for attempt in range(task.retries + 1):
            run.attempts += 1
            try:
                task_result = await asyncio.to_thread(self._execute_task, task, definition)
                if task_result:
                    if "report_path" in task_result:
                        state.report_path = task_result["report_path"]
                    if "quality_results" in task_result:
                        state.quality_results = task_result["quality_results"]
                run.status, run.error, run.finished_at = TaskStatus.SUCCESS, None, datetime.now(timezone.utc)
                self._record_task_duration(state, task.id)
                state.metrics.tasks_succeeded += 1
                self.record_event(state, "task.succeeded", task.id)
                self.save_state(state)
                return
            except Exception as exc:
                run.error = str(exc)
                if attempt == task.retries:
                    run.status, run.finished_at = TaskStatus.FAILED, datetime.now(timezone.utc)
                    self._record_task_duration(state, task.id)
                    state.metrics.tasks_failed += 1
                    self.record_event(state, "task.failed", task.id, str(exc))
                    self.save_state(state)
                    raise

    @staticmethod
    def _record_task_duration(state: PipelineRunState, task_id: str) -> None:
        run = state.tasks[task_id]
        if run.started_at and run.finished_at:
            state.metrics.task_duration_ms[task_id] = int((run.finished_at - run.started_at).total_seconds() * 1000)

    @staticmethod
    def _finalize_metrics(state: PipelineRunState) -> None:
        if state.metrics.started_at and state.metrics.finished_at:
            state.metrics.duration_ms = int((state.metrics.finished_at - state.metrics.started_at).total_seconds() * 1000)

    def _execute_task(self, task, definition: PipelineDefinition) -> dict[str, object] | None:
        output = definition.materialized_output or definition.plan.output
        if task.kind == "sql":
            self.database.execute(task.sql or "")
        elif task.kind == "quality":
            contract = self.contracts.get(str(task.config.get("contract_asset", "")))
            checks = self.quality.run(output, int(task.config.get("expected_min_rows", 1)), contract)
            self.quality.require_success(checks)
            return {"quality_results": [{"name": check.name, "passed": check.passed, "detail": check.detail, "metric": check.metric} for check in checks]}
        elif task.kind == "eda":
            return {"report_path": str(self.reporter.generate(output, definition.id))}
        elif task.kind == "assertion":
            result = self.database.query_dataframe(task.sql or "SELECT 0 AS failed")
            failed = int(result.iloc[0, 0]) if not result.empty else 1
            if failed:
                raise ValueError(f"Assertion '{task.id}' reported {failed} failures.")
        elif task.kind == "notification":
            emit("pipeline.notification", definition.id, task_id=task.id, detail=str(task.config.get("message", "Pipeline event")))
        elif task.kind == "export":
            destination = self.state_dir / "exports" / f"{definition.id}-{task.id}.csv"
            destination.parent.mkdir(parents=True, exist_ok=True)
            self.database.query_dataframe(f"SELECT * FROM {SQLValidator.quote_identifier(output)}").to_csv(destination, index=False)
        elif task.kind == "ingestion":
            # Ingestion SQL is still routed through the same database transaction; external connectors should create a dedicated task adapter.
            self.database.execute(task.sql or "")
        else:
            raise ValueError(f"Unsupported task type '{task.kind}'.")
        return None
