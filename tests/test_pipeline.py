from __future__ import annotations

import asyncio
from pathlib import Path

from src.config import Settings
from src.database import Database
from src.evaluation.harness import PlannerEvaluationHarness, load_cases
from src.execution.cron import cron_matches, validate_cron
from src.models import ContractColumn, DataContract, FilterDefinition, JoinCondition, PipelinePlan
from src.sql.generator import SQLGenerator
from src.service import PipelineService
from src.sql.validator import SQLValidator


def seed(database: Database) -> None:
    database.execute("CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    database.execute("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL REFERENCES customers(customer_id), amount REAL NOT NULL)")
    database.execute("INSERT INTO customers VALUES (1, 'Ada'), (2, 'Grace')")
    database.execute("INSERT INTO orders VALUES (10, 1, 15.0), (11, 2, 25.0)")


def test_validator_rejects_destructive_sql():
    schemas = {"customers": None}
    result = SQLValidator().validate("DROP TABLE customers", schemas)
    assert not result.valid
    assert any("Only SELECT" in error for error in result.errors)


def test_pipeline_runs_and_persists_artifacts(tmp_path):
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        state_dir=tmp_path / "state",
        report_dir=tmp_path / "reports",
    )
    service = PipelineService(settings)
    seed(service.database)
    definition = service.create_pipeline(
        "Create a customer dataset using customers and orders",
        output="customer_analytics",
    )
    assert "CREATE TABLE" in service.preview_sql(definition.id)
    assert service.semantic_plan(definition.id)["review"]["status"] == "pending"
    service.approve_pipeline(definition.id, reviewer="analytics@example.com", note="Verified customer grain and join.")
    state = asyncio.run(service.run_pipeline(definition.id))
    assert state.status == "success"
    assert service.database.query_dataframe(f"SELECT * FROM {definition.materialized_output}").shape[0] == 2
    assert service.report_path(definition.id).exists()
    lineage = service.lineage(definition.id)
    assert lineage["sources"] == ["customers", "orders"]
    assert any(column["output_column"] == "orders_customer_id" for column in lineage["columns"])
    impact = service.impact_analysis("orders", "amount")
    assert impact["count"] == 1
    assert impact["impacted_pipelines"][0]["output"] == "customer_analytics"
    observability = service.observability(definition.id)
    assert observability["metrics"]["tasks_succeeded"] == 3
    assert any(event["event"] == "pipeline.succeeded" for event in observability["events"])


def test_planner_generates_customer_aggregation(tmp_path):
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'aggregation.db'}", state_dir=tmp_path / "state", report_dir=tmp_path / "reports")
    service = PipelineService(settings)
    seed(service.database)
    definition = service.create_pipeline(
        "Create a customer-level dataset with total purchase amount, number of orders, and average order value using customers and orders",
    )
    sql = service.preview_sql(definition.id)
    assert "SUM(" in sql and "AVG(" in sql and "COUNT(DISTINCT" in sql
    service.approve_pipeline(definition.id, reviewer="analytics@example.com")
    state = asyncio.run(service.run_pipeline(definition.id))
    assert state.status == "success"
    frame = service.database.query_dataframe(f"SELECT * FROM {definition.materialized_output} ORDER BY customer_id")
    assert frame["total_amount"].tolist() == [15.0, 25.0]
    assert frame["order_count"].tolist() == [1, 1]
    assert service.impact_analysis("orders", "amount")["impacted_pipelines"][0]["affected_columns"][0]["output_column"] in {"total_amount", "average_amount"}


def test_offline_planner_benchmark_is_perfect_for_baseline_cases():
    cases = load_cases(Path("benchmarks/planner_cases.json"))
    report = PlannerEvaluationHarness().evaluate(cases)
    assert report.aggregate["case_count"] == 4
    assert report.aggregate["mean_score"] == 1.0
    assert report.aggregate["sql_valid_rate"] == 1.0


def test_contract_blocks_schema_drift_and_unapproved_runs(tmp_path):
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'contract.db'}", state_dir=tmp_path / "state", report_dir=tmp_path / "reports")
    service = PipelineService(settings)
    seed(service.database)
    service.save_contract(
        DataContract(
            asset="orders",
            columns=[
                ContractColumn(name="order_id", data_type="INTEGER", nullable=False),
                ContractColumn(name="customer_id", data_type="INTEGER", nullable=False),
                ContractColumn(name="amount", data_type="REAL", nullable=False),
            ],
            allow_additive_columns=False,
        )
    )
    definition = service.create_pipeline("Create a dataset using customers and orders")
    try:
        asyncio.run(service.run_pipeline(definition.id))
        assert False, "Unapproved pipelines must not run"
    except ValueError as exc:
        assert "approval" in str(exc)
    service.approve_pipeline(definition.id, reviewer="owner")
    service.database.execute("ALTER TABLE orders ADD COLUMN unexpected TEXT")
    drift = service.schema_drift(service.load_definition(definition.id))
    assert any(not report.valid for report in drift)
    try:
        asyncio.run(service.run_pipeline(definition.id))
        assert False, "Contract drift must block runs"
    except ValueError as exc:
        assert "contract/schema drift" in str(exc)


def test_cron_validation_rejects_invalid_fields_and_never_raises_for_valid_schedule():
    validate_cron("0 8 * * 1-5")
    assert cron_matches("* * * * *", __import__("datetime").datetime.now())
    try:
        validate_cron("99 25 * * *")
        assert False, "Out-of-range cron values must be rejected"
    except ValueError:
        pass


def test_filters_are_compiled_before_grouping_and_reviewed_sql_can_be_retargeted(tmp_path):
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'filters.db'}", state_dir=tmp_path / "state", report_dir=tmp_path / "reports")
    service = PipelineService(settings)
    seed(service.database)
    schemas = service.database.schemas()
    plan = PipelinePlan(objective="Filtered orders", source_tables=["orders"], output="filtered_orders", filters=[FilterDefinition(column="orders.amount", operator=">=", value=20)])
    sql = SQLGenerator().generate(plan, schemas)
    assert " WHERE " in sql and " GROUP BY " not in sql
    definition = service.create_pipeline("Create a dataset using customers")
    edited = service.edit_pipeline_sql(definition.id, 'CREATE TABLE {{output}} AS SELECT "customers"."customer_id" FROM "customers" WHERE "customers"."customer_id" = 1')
    retargeted = service._retarget_materialization(edited["sql"], "customers__v_new")
    assert '"customers__v_new"' in retargeted
    assert "WHERE" in retargeted
    try:
        service.cancel_pipeline(definition.id)
        assert False, "Idle pipeline cancellation must be rejected"
    except ValueError:
        pass


def test_left_join_direction_is_preserved_even_when_source_list_order_differs(tmp_path):
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'joins.db'}", state_dir=tmp_path / "state", report_dir=tmp_path / "reports")
    service = PipelineService(settings)
    seed(service.database)
    plan = PipelinePlan(
        objective="Preserve orders",
        source_tables=["customers", "orders"],
        output="order_customer",
        joins=[JoinCondition(left_table="orders", left_column="customer_id", right_table="customers", right_column="customer_id", join_type="LEFT")],
    )
    sql = SQLGenerator().generate(plan, service.database.schemas())
    assert 'FROM "orders" LEFT JOIN "customers"' in sql
