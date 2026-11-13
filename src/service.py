from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
import asyncio
import difflib

import sqlglot
from sqlglot import exp

from sqlalchemy import text

from src.config import Settings
from src.contracts import ContractStore, ContractValidator
from src.database import Database
from src.execution.dag import DAG
from src.execution.cron import validate_cron
from src.execution.scheduler import PipelineExecutor
from src.models import ContractIssue, ContractReport, DataContract, JoinCondition, PipelineDefinition, PipelinePlan, ReviewStatus, TaskDefinition, TaskStatus
from src.observability import emit
from src.planner.llm_planner import LLMPlanner
from src.sql.generator import SQLGenerator
from src.sql.validator import SQLValidator


class PipelineService:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()
        self.settings.prepare_directories()
        self.database = Database(self.settings.database_url, self.settings.query_timeout_seconds)
        self.contracts = ContractStore(self.settings.state_dir)
        self.planner = LLMPlanner(self.settings.gemini_api_key)
        self.generator = SQLGenerator()
        self.validator = SQLValidator(set(self.settings.source_table_allowlist) or None, self.settings.allow_pii)
        self.executor = PipelineExecutor(self.database, self.settings.state_dir, self.settings.report_dir, self.settings.eda_max_rows)
        self._run_locks: dict[str, asyncio.Lock] = {}

    def create_pipeline(self, request: str, output: str | None = None, sql: str | None = None, joins: list[JoinCondition] | None = None, workspace_id: str = "default") -> PipelineDefinition:
        schemas = self.database.schemas()
        plan = self.planner.create_plan(request, schemas)
        if output:
            plan.output = output
        if joins:
            plan.joins = joins
        pipeline_id = str(uuid4())
        materialized_output = f"{plan.output}__v_{pipeline_id.replace('-', '')[:12]}"
        execution_plan = plan.model_copy(update={"output": materialized_output})
        generated_sql = sql.replace("{{output}}", materialized_output) if sql else self.generator.generate(execution_plan, schemas)
        validation = self.validator.validate(generated_sql, schemas, allowed_output=materialized_output)
        if not validation.valid:
            raise ValueError("Generated SQL is not valid: " + "; ".join(validation.errors))
        if not generated_sql.lstrip().upper().startswith("CREATE TABLE"):
            raise ValueError("Pipeline SQL must use CREATE TABLE ... AS SELECT to materialize the requested output.")
        contract_reports = self.contract_reports_for_tables(plan.source_tables, schemas)
        self.require_contract_success(contract_reports)
        definition = PipelineDefinition(
            id=pipeline_id,
            plan=plan,
            tasks=[
                TaskDefinition(id="materialize", kind="sql", sql=generated_sql, retries=1),
                TaskDefinition(id="quality", kind="quality", depends_on=["materialize"], config={"contract_asset": plan.output}, retries=0),
                TaskDefinition(id="eda", kind="eda", depends_on=["quality"], retries=0),
            ],
            column_lineage=self.generator.build_column_lineage(plan, schemas),
            source_schema_snapshot={name: schemas[name] for name in plan.source_tables},
            materialized_output=materialized_output,
            workspace_id=workspace_id,
        )
        self.save_definition(definition)
        emit("pipeline.created", definition.id, sources=plan.source_tables, output=plan.output)
        return definition

    def validate_pipeline(self, definition: PipelineDefinition) -> list[str]:
        DAG(definition.tasks)
        schemas = self.database.schemas()
        for task in definition.tasks:
            if task.kind == "sql":
                result = self.validator.validate(task.sql or "", schemas, definition.materialized_output or definition.plan.output)
                if not result.valid:
                    raise ValueError("; ".join(result.errors))
        return ["DAG is acyclic and SQL tasks passed validation."]

    def preview_sql(self, pipeline_id: str) -> str:
        definition = self.load_definition(pipeline_id)
        return next(task.sql for task in definition.tasks if task.kind == "sql") or ""

    def edit_pipeline_sql(self, pipeline_id: str, sql: str) -> dict[str, object]:
        definition = self.load_definition(pipeline_id)
        if definition.semantic_review.status != ReviewStatus.PENDING:
            raise ValueError("SQL can only be edited while semantic review is pending.")
        rendered = sql.replace("{{output}}", definition.materialized_output or definition.plan.output)
        result = self.validator.validate(rendered, self.database.schemas(), definition.materialized_output or definition.plan.output)
        if not result.valid:
            raise ValueError("Edited SQL is invalid: " + "; ".join(result.errors))
        task = next(task for task in definition.tasks if task.kind == "sql")
        before = task.sql or ""
        task.sql = rendered
        self.save_definition(definition)
        return {"sql": rendered, "diff": "".join(difflib.unified_diff(before.splitlines(keepends=True), rendered.splitlines(keepends=True), fromfile="previous.sql", tofile="edited.sql"))}

    def query_cost(self, pipeline_id: str) -> dict[str, object]:
        return {"pipeline_id": pipeline_id, "plan": self.database.explain(self.preview_sql(pipeline_id))}

    async def run_pipeline(self, pipeline_id: str):
        definition = self.load_definition(pipeline_id)
        if definition.semantic_review.status != ReviewStatus.APPROVED:
            raise ValueError("Pipeline requires semantic-plan approval before execution.")
        self.validate_pipeline(definition)
        drift_reports = self.schema_drift(definition)
        self.require_contract_success(drift_reports)
        lock = self._run_locks.setdefault(pipeline_id, asyncio.Lock())
        if lock.locked():
            raise ValueError("Pipeline is already running; concurrent executions are not allowed.")
        async with lock:
            return await self.executor.run(definition)

    async def run_scheduled(self, pipeline_id: str):
        """Create a fresh immutable output version for a scheduled trigger."""
        definition = self.load_definition(pipeline_id)
        if definition.semantic_review.status != ReviewStatus.APPROVED:
            raise ValueError("Scheduled pipeline requires prior semantic approval.")
        version_id = str(uuid4())
        materialized = f"{definition.plan.output}__v_{version_id.replace('-', '')[:12]}"
        previous_sql = next(task.sql for task in definition.tasks if task.kind == "sql") or ""
        sql = self._retarget_materialization(previous_sql, materialized)
        validation = self.validator.validate(sql, self.database.schemas(), materialized)
        if not validation.valid:
            raise ValueError("Reviewed SQL could not be retargeted safely: " + "; ".join(validation.errors))
        tasks = [task.model_copy(deep=True) for task in definition.tasks]
        next(task for task in tasks if task.kind == "sql").sql = sql
        scheduled = definition.model_copy(update={"id": version_id, "tasks": tasks, "materialized_output": materialized, "created_at": datetime.now(timezone.utc)})
        self.save_definition(scheduled)
        return await self.run_pipeline(scheduled.id)

    def _retarget_materialization(self, sql: str, target: str) -> str:
        dialect = "postgres" if self.database.engine.dialect.name == "postgresql" else "sqlite"
        statement = sqlglot.parse_one(sql, read=dialect)
        if not isinstance(statement, exp.Create) or statement.args.get("kind") != "TABLE":
            raise ValueError("Scheduled runs require reviewed CREATE TABLE ... AS SELECT SQL.")
        statement.set("this", exp.Table(this=exp.Identifier(this=target, quoted=True)))
        return statement.sql(dialect=dialect)

    def cancel_pipeline(self, pipeline_id: str) -> None:
        definition = self.load_definition(pipeline_id)
        state = self.executor.load_state(definition)
        if state.status != TaskStatus.RUNNING:
            raise ValueError("Only a currently running pipeline can be cancelled.")
        self.executor.cancel(pipeline_id)
        emit("pipeline.cancellation_requested", pipeline_id)

    def set_schedule(self, pipeline_id: str, schedule: str | None) -> PipelineDefinition:
        definition = self.load_definition(pipeline_id)
        if schedule:
            validate_cron(schedule)
        definition.schedule = schedule
        self.save_definition(definition)
        return definition

    def status(self, pipeline_id: str):
        definition = self.load_definition(pipeline_id)
        return self.executor.load_state(definition)

    def list_pipelines(self, workspace_id: str | None = None) -> list[dict[str, object]]:
        definitions = []
        for path in self.settings.state_dir.glob("*.definition.json"):
            definition = PipelineDefinition.model_validate_json(path.read_text(encoding="utf-8"))
            if workspace_id and definition.workspace_id != workspace_id:
                continue
            definitions.append({"id": definition.id, "objective": definition.plan.objective, "output": definition.plan.output, "materialized_output": definition.materialized_output, "review_status": definition.semantic_review.status, "created_at": definition.created_at})
        return sorted(definitions, key=lambda item: str(item["created_at"]), reverse=True)

    def lineage(self, pipeline_id: str) -> dict[str, object]:
        definition = self.load_definition(pipeline_id)
        return {
            "pipeline_id": definition.id,
            "sources": definition.plan.source_tables,
            "output": definition.plan.output,
            "materialized_output": definition.materialized_output or definition.plan.output,
            "tasks": DAG(definition.tasks).lineage(),
            "columns": [item.model_dump() for item in definition.column_lineage],
        }

    def impact_analysis(self, source_table: str, source_column: str | None = None) -> dict[str, object]:
        """Find persisted datasets and output columns affected by a source asset change."""
        source = f"{source_table}.{source_column}" if source_column else None
        impacts: list[dict[str, object]] = []
        for definition_path in self.settings.state_dir.glob("*.definition.json"):
            definition = PipelineDefinition.model_validate_json(definition_path.read_text(encoding="utf-8"))
            affected_columns = [
                item.model_dump()
                for item in definition.column_lineage
                if any(
                    input_column == source if source else input_column.split(".", 1)[0] == source_table
                    for input_column in item.source_columns
                )
            ]
            if affected_columns or (source_column is None and source_table in definition.plan.source_tables):
                impacts.append(
                    {
                        "pipeline_id": definition.id,
                        "objective": definition.plan.objective,
                        "output": definition.plan.output,
                        "affected_columns": affected_columns,
                    }
                )
        return {"source": source or source_table, "impacted_pipelines": impacts, "count": len(impacts)}

    def observability(self, pipeline_id: str) -> dict[str, object]:
        state = self.status(pipeline_id)
        return {"pipeline_id": pipeline_id, "status": state.status, "metrics": state.metrics.model_dump(), "events": [event.model_dump() for event in state.events]}

    def run_history(self, pipeline_id: str) -> list[dict[str, object]]:
        self.load_definition(pipeline_id)
        return self.executor.history.list(pipeline_id)

    def approve_pipeline(self, pipeline_id: str, reviewer: str, note: str | None = None, approved: bool = True) -> PipelineDefinition:
        definition = self.load_definition(pipeline_id)
        definition.semantic_review.status = ReviewStatus.APPROVED if approved else ReviewStatus.REJECTED
        definition.semantic_review.reviewer = reviewer
        definition.semantic_review.note = note
        definition.semantic_review.reviewed_at = datetime.now(timezone.utc)
        self.save_definition(definition)
        emit("pipeline.reviewed", pipeline_id, reviewer=reviewer, status=definition.semantic_review.status)
        return definition

    def semantic_plan(self, pipeline_id: str) -> dict[str, object]:
        definition = self.load_definition(pipeline_id)
        return {
            "pipeline_id": pipeline_id,
            "objective": definition.plan.objective,
            "sources": definition.plan.source_tables,
            "grain": definition.plan.group_by or ["unaggregated rows"],
            "metrics": [metric.model_dump() for metric in definition.plan.metrics],
            "output": definition.plan.output,
            "materialized_output": definition.materialized_output or definition.plan.output,
            "review": definition.semantic_review.model_dump(),
        }

    def save_contract(self, contract: DataContract) -> DataContract:
        return self.contracts.save(contract)

    def contract_reports_for_tables(self, tables: list[str], schemas: dict[str, object] | None = None) -> list[ContractReport]:
        all_schemas = schemas or self.database.schemas()
        reports: list[ContractReport] = []
        for table in tables:
            contract = self.contracts.get(table)
            if contract and table in all_schemas:
                report = ContractValidator.validate(contract, all_schemas[table])
                self._append_freshness_issue(report, contract)
                reports.append(report)
        return reports

    def _append_freshness_issue(self, report: ContractReport, contract: DataContract) -> None:
        if not contract.freshness_column or not contract.max_age_hours or not report.valid:
            return
        table = SQLValidator.quote_identifier(contract.asset)
        column = SQLValidator.quote_identifier(contract.freshness_column)
        with self.database.engine.connect() as connection:
            latest = connection.execute(text(f"SELECT MAX({column}) FROM {table}")).scalar_one()
        if latest is None:
            report.issues.append(ContractIssue(severity="blocking", kind="stale_data", detail=f"No value found in freshness column '{contract.freshness_column}'."))
        else:
            try:
                observed = latest if isinstance(latest, datetime) else datetime.fromisoformat(str(latest).replace("Z", "+00:00"))
            except (TypeError, ValueError):
                report.issues.append(ContractIssue(severity="blocking", kind="invalid_freshness_value", detail=f"Freshness column '{contract.freshness_column}' does not contain a parseable timestamp."))
                report.valid = False
                return
            if observed.tzinfo is None:
                observed = observed.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - observed.astimezone(timezone.utc)).total_seconds() / 3600
            if age_hours > contract.max_age_hours:
                report.issues.append(ContractIssue(severity="blocking", kind="stale_data", detail=f"Latest '{contract.freshness_column}' is {age_hours:.1f} hours old; limit is {contract.max_age_hours}."))
        report.valid = not any(issue.severity == "blocking" for issue in report.issues)

    def schema_drift(self, definition: PipelineDefinition) -> list[ContractReport]:
        current = self.database.schemas()
        reports: list[ContractReport] = []
        for name, snapshot in definition.source_schema_snapshot.items():
            if name not in current:
                reports.append(ContractReport(asset=name, valid=False, issues=[{"severity": "blocking", "kind": "missing_table", "detail": f"Source table '{name}' no longer exists."}]))
            else:
                reports.append(ContractValidator.drift(snapshot, current[name]))
        reports.extend(self.contract_reports_for_tables(definition.plan.source_tables, current))
        return reports

    @staticmethod
    def require_contract_success(reports: list[ContractReport]) -> None:
        blockers = [f"{report.asset}: {issue.detail}" for report in reports for issue in report.issues if issue.severity == "blocking"]
        if blockers:
            raise ValueError("Data contract/schema drift gate failed: " + "; ".join(blockers))

    def report_path(self, pipeline_id: str) -> Path:
        state = self.status(pipeline_id)
        if not state.report_path:
            raise FileNotFoundError("No EDA report has been created for this pipeline.")
        return Path(state.report_path)

    def definition_path(self, pipeline_id: str) -> Path:
        return self.settings.state_dir / f"{pipeline_id}.definition.json"

    def save_definition(self, definition: PipelineDefinition) -> None:
        self.definition_path(definition.id).write_text(definition.model_dump_json(indent=2), encoding="utf-8")

    def load_definition(self, pipeline_id: str) -> PipelineDefinition:
        path = self.definition_path(pipeline_id)
        if not path.exists():
            raise KeyError(f"Pipeline '{pipeline_id}' was not found.")
        return PipelineDefinition.model_validate_json(path.read_text(encoding="utf-8"))
