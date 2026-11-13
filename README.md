# AI Data Pipeline Builder

A local-first system that turns an analytical request into a safe, executable data pipeline. It discovers the connected schema, produces a structured plan, validates SQL with SQLGlot, materializes a dataset, runs data-quality checks, and writes an HTML EDA report. A FastAPI service and an MCP server expose the same workflow.

## Quick start

Requires Python 3.11+.

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
copy .env.example .env
uvicorn src.api:app --reload
```

The default database is SQLite at `pipeline.db`. Set `DATABASE_URL` to use PostgreSQL. Docker Compose starts Postgres and the API:

```bash
docker compose up --build
```

For a zero-setup SQLite walkthrough, bootstrap sources, contracts, an approved sample plan, and its EDA report in one command:

```bash
pipeline-demo
# Only create local sample sources/contracts:
pipeline-demo --bootstrap-only
```

Run persisted approved cron schedules with the lightweight local scheduler:

```bash
pipeline-scheduler --poll-seconds 15
```

Create source tables first, then create and execute a pipeline. Supplying `sql` is useful when business logic requires explicit aggregations or a join key cannot be inferred; it must be one `CREATE TABLE requested_output AS SELECT ...` statement.

```bash
curl -X POST http://localhost:8000/pipelines ^
  -H "Content-Type: application/json" ^
  -d "{\"request\":\"Create customer analytics using customers and orders\",\"output\":\"customer_analytics\"}"

curl -X POST http://localhost:8000/pipelines/PIPELINE_ID/run
```

## HTTP API

| Endpoint | Purpose |
| --- | --- |
| `GET /schemas` | Discover source tables and columns |
| `POST /pipelines` | Create and validate a pipeline |
| `GET /pipelines/{id}/sql` | Preview guarded SQL |
| `PUT /pipelines/{id}/sql` | Validate an editable pending-plan SQL draft and return a unified diff |
| `GET /pipelines/{id}/cost` | Return database `EXPLAIN`/query-plan output |
| `POST /pipelines/{id}/validate` | Validate SQL and DAG |
| `POST /pipelines/{id}/run` | Run or resume tasks |
| `POST /pipelines/{id}/cancel` | Request cancellation between tasks |
| `PUT /pipelines/{id}/schedule` | Save a five-field cron schedule |
| `GET /pipelines/{id}/runs` | Read durable execution history |
| `GET /pipelines/{id}/semantic-plan` | Review inferred grain, metrics, and approval state |
| `POST /pipelines/{id}/review` | Approve or reject a semantic plan before it can run |
| `GET /pipelines/{id}/schema-drift` | Compare current sources to planning snapshots and contracts |
| `GET /contracts`, `PUT /contracts/{asset}` | List or save source data contracts |
| `GET /pipelines/{id}/status` | Read persisted state |
| `GET /pipelines/{id}/lineage` | Read inputs, outputs, and task dependencies |
| `GET /impact?source_table=orders&source_column=amount` | Find impacted datasets and output columns |
| `GET /pipelines/{id}/observability` | Read structured events and run/task timing metrics |
| `GET /pipelines/{id}/eda` | Download the HTML report |

## Safety model

SQL is parsed rather than trusted. The validator permits exactly one `SELECT` or `CREATE TABLE ... AS SELECT` statement; it rejects multi-statement payloads and destructive/write operations, verifies source tables and qualified columns against the discovered schema, applies optional source allowlists, blocks PII-like projections unless explicitly enabled, and binds materialization to a versioned output name. Identifiers are quoted only after a strict identifier check.

The built-in generator only uses discovered foreign keys or explicitly supplied join mappings; it does not silently infer joins from shared names. It recognizes common total/average/count requests and builds grouped metrics when the schema supports them. Every pipeline writes to an immutable `output__v_<id>` table, preserving reproducibility and avoiding output-name collisions. Use `{{output}}` in an explicit SQL draft to have the versioned output identifier substituted safely.

## Planning and execution

With `GEMINI_API_KEY`, the planner asks Gemini for a constrained JSON plan and validates table choices locally. Without it, a deterministic schema-aware planner provides local operation. The executor writes task state under `.pipeline-state`, skips completed tasks on restart, retries failures according to each task's retry policy, and runs independent ready tasks concurrently.

To serve MCP tools over stdio:

```bash
pipeline-mcp
```

Available MCP tools: `create_pipeline`, `list_pipelines`, `preview_sql`, `edit_pipeline_sql`, `get_query_cost`, `validate_pipeline`, `run_pipeline`, `cancel_pipeline`, `get_semantic_plan`, `review_pipeline`, `save_data_contract`, `get_schema_drift`, `get_pipeline_status`, `get_pipeline_lineage`, `get_impact_analysis`, `get_pipeline_observability`, and `get_eda_report`.

## Data contracts and semantic approval

Source contracts define expected columns, type families, nullability expectations, additive-column policy, and optional freshness metadata. Save a contract before creating a pipeline:

```json
{
  "asset": "orders",
  "version": "1",
  "allow_additive_columns": false,
  "columns": [
    {"name": "order_id", "data_type": "INTEGER", "nullable": false},
    {"name": "customer_id", "data_type": "INTEGER", "nullable": false},
    {"name": "amount", "data_type": "REAL", "nullable": false}
  ]
}
```

Contracts are checked at planning and again just before execution. The service also saves each source schema as it existed when the pipeline was planned, then treats removed columns and type changes as blocking drift; new columns and weaker nullability are reported as warnings. `GET /pipelines/{id}/schema-drift` gives a preview without running anything.

New pipelines begin in `pending` semantic-review state. Read the inferred objective, sources, grain, and metrics from `GET /pipelines/{id}/semantic-plan`, then approve it with `POST /pipelines/{id}/review`:

```json
{"reviewer": "analytics@example.com", "note": "Verified customer grain and metric definitions.", "approved": true}
```

Execution is blocked until approval. This makes ambiguous join and aggregation assumptions explicit, auditable decisions rather than hidden LLM behavior.

## Lineage, impact, and observability

Every created pipeline persists column-level lineage alongside its reviewed plan. For example, `customer_analytics.total_amount` records `SUM(orders.amount)`, while direct projections retain their source table and column. `GET /impact` scans saved definitions to identify every output dataset and column affected by a source-table or source-column change.

Pipeline runs also emit newline-delimited JSON logs to standard output, persist the most recent 200 events with pipeline state, and write a durable SQLite run ledger. The observability endpoint includes pipeline duration, per-task duration, task success/failure counts, and lifecycle events. EDA samples tables above `EDA_MAX_ROWS` rather than loading an unbounded dataset into memory.

## Planner evaluation harness

`benchmarks/planner_cases.json` is a versioned, offline benchmark suite. Each case contains a source schema, a natural-language request, and expected source tables, grain, metrics, and output name. The harness scores table precision/recall/F1, exact grouping, metric F1, output naming, and whether generated SQL passes the safety validator.

```bash
pipeline-evaluate
# Or choose case and result locations:
python -m src.evaluation.harness --cases benchmarks/planner_cases.json --output reports/planner-evaluation.json
```

The command exits non-zero if the aggregate score is below 1.0; use `--min-score 0.9` for a deliberately relaxed gate. The included GitHub Actions workflow runs this benchmark and uploads its JSON report for every planner-related pull request. Add difficult cases before modifying prompts, planner logic, or model versions; review the generated artifact in CI to understand regression dimensions rather than relying only on a pass/fail signal.

## Tests

```bash
pytest
```
