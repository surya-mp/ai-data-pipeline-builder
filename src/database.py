from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine

from src.models import ColumnInfo, TableSchema


class Database:
    def __init__(self, url: str, timeout_seconds: int = 60) -> None:
        connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
        self.engine: Engine = create_engine(url, future=True, connect_args=connect_args)
        self.timeout_ms = max(1, timeout_seconds) * 1000

    @contextmanager
    def begin(self):
        with self.engine.begin() as connection:
            if self.engine.dialect.name == "postgresql":
                connection.execute(text(f"SET LOCAL statement_timeout = {self.timeout_ms}"))
            yield connection

    def schemas(self) -> dict[str, TableSchema]:
        inspector = inspect(self.engine)
        schemas: dict[str, TableSchema] = {}
        for table in inspector.get_table_names():
            columns = [
                ColumnInfo(name=column["name"], data_type=str(column["type"]), nullable=column["nullable"])
                for column in inspector.get_columns(table)
            ]
            schemas[table] = TableSchema(name=table, columns=columns, foreign_keys=inspector.get_foreign_keys(table))
        return schemas

    def execute(self, sql: str, connection: Connection | None = None) -> None:
        if connection is not None:
            connection.execute(text(sql))
            return
        with self.begin() as conn:
            conn.execute(text(sql))

    def query_dataframe(self, sql: str):
        import pandas as pd
        with self.begin() as connection:
            return pd.read_sql_query(text(sql), connection)

    def explain(self, sql: str) -> list[dict[str, object]]:
        statement = f"EXPLAIN QUERY PLAN {sql}" if self.engine.dialect.name == "sqlite" else f"EXPLAIN {sql}"
        with self.engine.connect() as connection:
            if self.engine.dialect.name == "postgresql":
                connection.execute(text(f"SET statement_timeout = {self.timeout_ms}"))
            result = connection.execute(text(statement))
            return [dict(row._mapping) for row in result]
