from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import inspect, text

from src.database import Database
from src.models import DataContract
from src.sql.validator import SQLValidator


@dataclass
class QualityCheck:
    name: str
    passed: bool
    detail: str
    metric: Any = None


class DataQualityEngine:
    def __init__(self, database: Database) -> None:
        self.database = database

    def run(self, table: str, expected_min_rows: int = 1, contract: DataContract | None = None) -> list[QualityCheck]:
        quoted = SQLValidator.quote_identifier(table)
        with self.database.begin() as connection:
            row_count = connection.execute(text(f"SELECT COUNT(*) FROM {quoted}")).scalar_one()
            columns = inspect(self.database.engine).get_columns(table)
            checks = [
                QualityCheck("schema_validation", bool(columns), f"Found {len(columns)} output columns.", len(columns)),
                QualityCheck("expected_row_count", row_count >= expected_min_rows, f"Found {row_count} rows; expected at least {expected_min_rows}.", row_count),
            ]
            for column in columns:
                name = SQLValidator.quote_identifier(column["name"])
                nulls = connection.execute(text(f"SELECT COUNT(*) FROM {quoted} WHERE {name} IS NULL")).scalar_one()
                threshold = contract.null_thresholds.get(column["name"], 1.0) if contract else 1.0
                fraction = nulls / row_count if row_count else 0.0
                checks.append(QualityCheck(f"null_check:{column['name']}", fraction <= threshold, f"{nulls} null values ({fraction:.2%}); threshold {threshold:.2%}.", nulls))
            keys = contract.primary_key if contract and contract.primary_key else [column["name"] for column in columns if column.get("primary_key")]
            if keys:
                key_list = ", ".join(SQLValidator.quote_identifier(key) for key in keys)
                # Count duplicate groups separately from duplicate rows to preserve portable SQL.
                duplicate_groups = connection.execute(text(f"SELECT COUNT(*) FROM (SELECT {key_list} FROM {quoted} GROUP BY {key_list} HAVING COUNT(*) > 1)" )).scalar_one()
                checks.append(QualityCheck(f"uniqueness:{','.join(keys)}", duplicate_groups == 0, f"{duplicate_groups} duplicate key groups.", duplicate_groups))
            if contract:
                for column, values in contract.accepted_values.items():
                    name = SQLValidator.quote_identifier(column)
                    literals = ", ".join(self._literal(value) for value in values)
                    invalid = connection.execute(text(f"SELECT COUNT(*) FROM {quoted} WHERE {name} IS NOT NULL AND {name} NOT IN ({literals})")).scalar_one()
                    checks.append(QualityCheck(f"accepted_values:{column}", invalid == 0, f"{invalid} values outside the declared set.", invalid))
                for column, bounds in contract.ranges.items():
                    lower, upper = bounds
                    name = SQLValidator.quote_identifier(column)
                    clauses = []
                    if lower is not None:
                        clauses.append(f"{name} < {lower}")
                    if upper is not None:
                        clauses.append(f"{name} > {upper}")
                    invalid = connection.execute(text(f"SELECT COUNT(*) FROM {quoted} WHERE {' OR '.join(clauses)}")).scalar_one() if clauses else 0
                    checks.append(QualityCheck(f"range:{column}", invalid == 0, f"{invalid} values outside declared range.", invalid))
                for index, assertion in enumerate(contract.custom_assertions, start=1):
                    query = assertion.replace("{{table}}", quoted).strip()
                    if not query.upper().startswith("SELECT") or ";" in query:
                        checks.append(QualityCheck(f"custom_assertion:{index}", False, "Custom assertions must be a single SELECT that returns zero failures."))
                        continue
                    failures = connection.execute(text(query)).scalar_one()
                    checks.append(QualityCheck(f"custom_assertion:{index}", int(failures or 0) == 0, f"Assertion returned {failures} failures.", failures))
            foreign_keys = inspect(self.database.engine).get_foreign_keys(table)
            orphaned = 0
            for foreign_key in foreign_keys:
                constrained = foreign_key.get("constrained_columns", [])
                referred = foreign_key.get("referred_columns", [])
                referred_table = foreign_key.get("referred_table")
                if not constrained or len(constrained) != len(referred) or not referred_table:
                    continue
                predicates = " AND ".join(
                    f'child.{SQLValidator.quote_identifier(child_column)} = parent.{SQLValidator.quote_identifier(parent_column)}'
                    for child_column, parent_column in zip(constrained, referred)
                )
                non_null = " AND ".join(f"child.{SQLValidator.quote_identifier(column)} IS NOT NULL" for column in constrained)
                query = f"SELECT COUNT(*) FROM {quoted} child WHERE {non_null} AND NOT EXISTS (SELECT 1 FROM {SQLValidator.quote_identifier(referred_table)} parent WHERE {predicates})"
                orphaned += connection.execute(text(query)).scalar_one()
            checks.append(QualityCheck("referential_integrity", orphaned == 0, f"{orphaned} orphaned references across {len(foreign_keys)} declared foreign keys.", orphaned))
        column_list = ", ".join(SQLValidator.quote_identifier(column["name"]) for column in columns)
        with self.database.begin() as connection:
            distinct_rows = connection.execute(text(f"SELECT COUNT(*) FROM (SELECT DISTINCT {column_list} FROM {quoted})" )).scalar_one() if columns else 0
        duplicate_rows = row_count - distinct_rows
        checks.append(QualityCheck("duplicate_rows", duplicate_rows == 0, f"{duplicate_rows} fully duplicated rows.", duplicate_rows))
        numeric_columns = [column for column in columns if any(kind in str(column["type"]).lower() for kind in ("int", "real", "float", "double", "numeric", "decimal"))]
        for column in numeric_columns:
            name = SQLValidator.quote_identifier(column["name"])
            with self.database.begin() as connection:
                missing = connection.execute(text(f"SELECT COUNT(*) FROM {quoted} WHERE {name} IS NULL")).scalar_one()
            threshold = contract.null_thresholds.get(column["name"], 1.0) if contract else 1.0
            fraction = missing / row_count if row_count else 0.0
            checks.append(
                QualityCheck(
                    f"aggregation_consistency:{column['name']}",
                    fraction <= threshold,
                    f"{missing} missing numeric values ({fraction:.2%}); threshold {threshold:.2%}.",
                    missing,
                )
            )
        return checks

    @staticmethod
    def _literal(value: object) -> str:
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        return "'" + str(value).replace("'", "''") + "'"

    @staticmethod
    def require_success(checks: list[QualityCheck]) -> None:
        failures = [f"{check.name}: {check.detail}" for check in checks if not check.passed]
        if failures:
            raise ValueError("Data quality checks failed: " + "; ".join(failures))
