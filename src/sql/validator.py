from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

from src.models import SqlValidationResult, TableSchema


class SQLValidator:
    """Parse SQL and restrict materialization to one safe CREATE TABLE AS SELECT."""

    ALLOWED_ROOTS = (exp.Select, exp.Create)

    def __init__(self, allowed_tables: set[str] | None = None, allow_pii: bool = False) -> None:
        self.allowed_tables = allowed_tables
        self.allow_pii = allow_pii

    def validate(
        self,
        sql: str,
        schemas: dict[str, TableSchema],
        allowed_output: str | None = None,
        dialect: str = "sqlite",
    ) -> SqlValidationResult:
        errors: list[str] = []
        tables: list[str] = []
        columns: list[str] = []
        try:
            statements = sqlglot.parse(sql, read=dialect)
        except Exception as exc:
            return SqlValidationResult(valid=False, errors=[f"Invalid SQL: {exc}"])
        if len(statements) != 1:
            return SqlValidationResult(valid=False, errors=["Exactly one SQL statement is allowed."])
        statement = statements[0]
        if not isinstance(statement, self.ALLOWED_ROOTS):
            errors.append("Only SELECT or CREATE TABLE ... AS SELECT statements are allowed.")
        if isinstance(statement, exp.Create):
            if statement.args.get("kind") != "TABLE" or not statement.find(exp.Select):
                errors.append("Only CREATE TABLE ... AS SELECT is allowed for materialization.")
            target = statement.this.name if statement.this else ""
            if allowed_output and target != allowed_output:
                errors.append(f"Output table must be '{allowed_output}'.")
        forbidden = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Alter, exp.Command, exp.Merge)
        if any(statement.find(kind) for kind in forbidden):
            errors.append("SQL includes a forbidden write or DDL operation.")
        for table in statement.find_all(exp.Table):
            name = table.name
            # The CREATE target is intentionally excluded.
            if isinstance(statement, exp.Create) and table is statement.this:
                continue
            tables.append(name)
            if name not in schemas:
                errors.append(f"Unknown source table '{name}'.")
            elif self.allowed_tables is not None and name not in self.allowed_tables:
                errors.append(f"Source table '{name}' is not allowlisted for execution.")
        source_column_names = {
            column.name
            for table_name in tables
            if isinstance(schemas.get(table_name), TableSchema)
            for column in schemas[table_name].columns
        }
        table_aliases = {
            table.alias_or_name: table.name
            for table in statement.find_all(exp.Table)
            if not (isinstance(statement, exp.Create) and table is statement.this)
        }
        aliases = {alias.alias for alias in statement.find_all(exp.Alias) if alias.alias}
        for column in statement.find_all(exp.Column):
            label = f"{column.table}.{column.name}" if column.table else column.name
            columns.append(label)
            source_table = table_aliases.get(column.table, column.table)
            if column.table and isinstance(schemas.get(source_table), TableSchema):
                names = {item.name for item in schemas[source_table].columns}
                if column.name not in names:
                    errors.append(f"Unknown column '{label}'.")
            elif column.table:
                errors.append(f"Unknown table or alias '{column.table}' in column '{label}'.")
            elif not column.table and column.name not in source_column_names | aliases:
                errors.append(f"Unknown column '{column.name}'.")
            if not self.allow_pii and self._looks_like_pii(column.name):
                errors.append(f"PII-like column '{label}' requires explicit ALLOW_PII=true policy.")
        if not self.allow_pii and any(statement.find(kind) for kind in (exp.Star,)):
            pii_sources = [table for table in tables if isinstance(schemas.get(table), TableSchema) and any(self._looks_like_pii(column.name) for column in schemas[table].columns)]
            if pii_sources:
                errors.append(f"Wildcard projection from PII-bearing table(s) is not permitted: {', '.join(pii_sources)}.")
        return SqlValidationResult(
            valid=not errors,
            errors=list(dict.fromkeys(errors)),
            referenced_tables=list(dict.fromkeys(tables)),
            referenced_columns=list(dict.fromkeys(columns)),
        )

    @staticmethod
    def quote_identifier(identifier: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError(f"Unsafe identifier: {identifier!r}")
        return f'"{identifier}"'

    @staticmethod
    def _looks_like_pii(column: str) -> bool:
        return any(token in column.lower() for token in ("email", "phone", "ssn", "social_security", "address", "birth", "dob"))
