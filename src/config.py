from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./pipeline.db")
    state_dir: Path = Path(os.getenv("PIPELINE_STATE_DIR", ".pipeline-state"))
    report_dir: Path = Path(os.getenv("EDA_REPORT_DIR", "reports"))
    gemini_api_key: str | None = os.getenv("GEMINI_API_KEY")
    api_key: str | None = os.getenv("PIPELINE_API_KEY")
    eda_max_rows: int = int(os.getenv("EDA_MAX_ROWS", "100000"))
    query_timeout_seconds: int = int(os.getenv("QUERY_TIMEOUT_SECONDS", "60"))
    source_table_allowlist: tuple[str, ...] = tuple(filter(None, os.getenv("SOURCE_TABLE_ALLOWLIST", "").split(",")))
    allow_pii: bool = os.getenv("ALLOW_PII", "false").lower() == "true"

    def prepare_directories(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)
