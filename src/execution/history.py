from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


class RunHistoryStore:
    """Durable run ledger separate from transient per-pipeline state files."""

    def __init__(self, state_dir: Path) -> None:
        self.path = state_dir / "run-history.db"
        with sqlite3.connect(self.path) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS pipeline_runs (run_id TEXT PRIMARY KEY, pipeline_id TEXT NOT NULL, status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT, metrics_json TEXT, error TEXT)")

    def started(self, run_id: str, pipeline_id: str) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute("INSERT OR REPLACE INTO pipeline_runs (run_id, pipeline_id, status, started_at) VALUES (?, ?, ?, ?)", (run_id, pipeline_id, "running", datetime.now(timezone.utc).isoformat()))

    def finished(self, run_id: str, status: str, metrics: dict, error: str | None = None) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute("UPDATE pipeline_runs SET status = ?, finished_at = ?, metrics_json = ?, error = ? WHERE run_id = ?", (status, datetime.now(timezone.utc).isoformat(), json.dumps(metrics, default=str), error, run_id))

    def list(self, pipeline_id: str) -> list[dict[str, object]]:
        with sqlite3.connect(self.path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute("SELECT * FROM pipeline_runs WHERE pipeline_id = ? ORDER BY started_at DESC", (pipeline_id,)).fetchall()
        return [{**dict(row), "metrics": json.loads(row["metrics_json"]) if row["metrics_json"] else None} for row in rows]
