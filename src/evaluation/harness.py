from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from src.models import AggregateMetric, ColumnInfo, PipelinePlan, TableSchema
from src.planner.llm_planner import LLMPlanner
from src.sql.generator import SQLGenerationError, SQLGenerator


class ExpectedPlan(BaseModel):
    source_tables: list[str]
    group_by: list[str] = Field(default_factory=list)
    metrics: list[AggregateMetric] = Field(default_factory=list)
    output: str | None = None
    require_output_name: bool = False


class PlannerBenchmarkCase(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: str
    request: str
    source_schema: dict[str, list[ColumnInfo]] = Field(alias="schema")
    foreign_keys: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    expected: ExpectedPlan

    def schemas(self) -> dict[str, TableSchema]:
        return {name: TableSchema(name=name, columns=columns, foreign_keys=self.foreign_keys.get(name, [])) for name, columns in self.source_schema.items()}


class CaseScore(BaseModel):
    id: str
    table_precision: float
    table_recall: float
    table_f1: float
    group_by_exact: bool
    metric_f1: float
    output_exact: bool | None
    sql_valid: bool
    score: float
    generated_plan: PipelinePlan
    sql: str | None = None
    errors: list[str] = Field(default_factory=list)


class EvaluationReport(BaseModel):
    generated_at: datetime
    planner: str
    cases: list[CaseScore]
    aggregate: dict[str, float]


class PlannerEvaluationHarness:
    """Runs versioned planner cases without requiring a live database or LLM provider."""

    def __init__(self, planner: LLMPlanner | None = None, generator: SQLGenerator | None = None) -> None:
        self.planner = planner or LLMPlanner()
        self.generator = generator or SQLGenerator()

    def evaluate(self, cases: list[PlannerBenchmarkCase]) -> EvaluationReport:
        results = [self.evaluate_case(case) for case in cases]
        aggregate = {
            "case_count": float(len(results)),
            "mean_score": self._mean([result.score for result in results]),
            "mean_table_f1": self._mean([result.table_f1 for result in results]),
            "metric_exact_rate": self._mean([result.metric_f1 for result in results]),
            "group_by_exact_rate": self._mean([float(result.group_by_exact) for result in results]),
            "sql_valid_rate": self._mean([float(result.sql_valid) for result in results]),
            "failing_case_count": float(sum(result.score < 1 for result in results)),
        }
        return EvaluationReport(
            generated_at=datetime.now(timezone.utc),
            planner=type(self.planner).__name__,
            cases=results,
            aggregate=aggregate,
        )

    def evaluate_case(self, case: PlannerBenchmarkCase) -> CaseScore:
        schemas = case.schemas()
        errors: list[str] = []
        try:
            plan = self.planner.create_plan(case.request, schemas)
        except Exception as exc:
            # Preserve failure as a scored result rather than hiding it in test output.
            plan = PipelinePlan(objective=case.request, source_tables=[next(iter(schemas))], output="planner_failure")
            errors.append(f"Planning failed: {exc}")
        precision, recall, f1 = self._set_scores(set(plan.source_tables), set(case.expected.source_tables))
        expected_group_by = set(case.expected.group_by)
        group_by_exact = set(plan.group_by) == expected_group_by
        predicted_metrics = {self._metric_key(metric) for metric in plan.metrics}
        expected_metrics = {self._metric_key(metric) for metric in case.expected.metrics}
        _, _, metric_f1 = self._set_scores(predicted_metrics, expected_metrics)
        output_exact = plan.output == case.expected.output if case.expected.output is not None else None
        sql: str | None = None
        sql_valid = False
        try:
            sql = self.generator.generate(plan, schemas)
            sql_valid = True
        except (SQLGenerationError, ValueError) as exc:
            errors.append(f"SQL generation/validation failed: {exc}")
        dimensions = [f1, float(group_by_exact), metric_f1, float(sql_valid)]
        # Output names are normally a convention, not a semantic correctness signal. Cases
        # that explicitly ask for a named output can opt into exact-name scoring.
        if output_exact is not None and case.expected.require_output_name:
            dimensions.append(float(output_exact))
        return CaseScore(
            id=case.id,
            table_precision=precision,
            table_recall=recall,
            table_f1=f1,
            group_by_exact=group_by_exact,
            metric_f1=metric_f1,
            output_exact=output_exact,
            sql_valid=sql_valid,
            score=sum(dimensions) / len(dimensions),
            generated_plan=plan,
            sql=sql,
            errors=errors,
        )

    @staticmethod
    def _metric_key(metric: AggregateMetric) -> tuple[str, str, str]:
        return metric.name, metric.function, metric.column

    @staticmethod
    def _set_scores(predicted: set[Any], expected: set[Any]) -> tuple[float, float, float]:
        if not predicted and not expected:
            return 1.0, 1.0, 1.0
        intersection = len(predicted & expected)
        precision = intersection / len(predicted) if predicted else 0.0
        recall = intersection / len(expected) if expected else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        return precision, recall, f1

    @staticmethod
    def _mean(values: list[float]) -> float:
        return sum(values) / len(values) if values else 0.0


def load_cases(path: Path) -> list[PlannerBenchmarkCase]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return [PlannerBenchmarkCase.model_validate(item) for item in raw]


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate natural-language pipeline planning against benchmark cases.")
    parser.add_argument("--cases", type=Path, default=Path("benchmarks/planner_cases.json"))
    parser.add_argument("--output", type=Path, default=Path("reports/planner-evaluation.json"))
    parser.add_argument("--min-score", type=float, default=1.0, help="Fail with a non-zero exit code below this aggregate score.")
    args = parser.parse_args()
    if not 0 <= args.min_score <= 1:
        parser.error("--min-score must be between 0 and 1")
    report = PlannerEvaluationHarness().evaluate(load_cases(args.cases))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    print(json.dumps(report.aggregate, indent=2))
    if report.aggregate["mean_score"] < args.min_score:
        raise SystemExit(1)
