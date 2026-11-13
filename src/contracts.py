from __future__ import annotations

import re
from pathlib import Path

from src.models import ContractIssue, ContractReport, DataContract, TableSchema


class ContractStore:
    """File-backed contract store; each contract carries an explicit version identifier."""

    def __init__(self, root: Path) -> None:
        self.root = root / "contracts"
        self.root.mkdir(parents=True, exist_ok=True)

    def save(self, contract: DataContract) -> DataContract:
        self.path(contract.asset).write_text(contract.model_dump_json(indent=2), encoding="utf-8")
        return contract

    def get(self, asset: str) -> DataContract | None:
        path = self.path(asset)
        return DataContract.model_validate_json(path.read_text(encoding="utf-8")) if path.exists() else None

    def list(self) -> list[DataContract]:
        return [DataContract.model_validate_json(path.read_text(encoding="utf-8")) for path in self.root.glob("*.json")]

    def path(self, asset: str) -> Path:
        safe_name = re.sub(r"[^A-Za-z0-9_.-]", "_", asset)
        return self.root / f"{safe_name}.json"


class ContractValidator:
    @staticmethod
    def validate(contract: DataContract, schema: TableSchema) -> ContractReport:
        issues: list[ContractIssue] = []
        actual = {column.name: column for column in schema.columns}
        expected = {column.name: column for column in contract.columns}
        for name, column in expected.items():
            observed = actual.get(name)
            if not observed:
                if column.required:
                    issues.append(ContractIssue(severity="blocking", kind="missing_column", detail=f"Required column '{name}' is missing."))
                continue
            if column.data_type and ContractValidator._type_family(column.data_type) != ContractValidator._type_family(observed.data_type):
                issues.append(ContractIssue(severity="blocking", kind="type_change", detail=f"Column '{name}' expected {column.data_type}, found {observed.data_type}."))
            if column.nullable is False and observed.nullable:
                issues.append(ContractIssue(severity="warning", kind="nullability_weakened", detail=f"Column '{name}' is nullable in the physical schema."))
        unexpected = set(actual) - set(expected)
        if unexpected and not contract.allow_additive_columns:
            issues.append(ContractIssue(severity="blocking", kind="unexpected_column", detail=f"Unexpected columns: {', '.join(sorted(unexpected))}."))
        if contract.freshness_column and contract.freshness_column not in actual:
            issues.append(ContractIssue(severity="blocking", kind="missing_freshness_column", detail=f"Freshness column '{contract.freshness_column}' is missing."))
        return ContractReport(asset=contract.asset, valid=not any(issue.severity == "blocking" for issue in issues), issues=issues)

    @staticmethod
    def drift(previous: TableSchema, current: TableSchema) -> ContractReport:
        issues: list[ContractIssue] = []
        before = {column.name: column for column in previous.columns}
        after = {column.name: column for column in current.columns}
        for name, old_column in before.items():
            new_column = after.get(name)
            if not new_column:
                issues.append(ContractIssue(severity="blocking", kind="dropped_column", detail=f"Column '{name}' was removed since planning."))
            elif ContractValidator._type_family(old_column.data_type) != ContractValidator._type_family(new_column.data_type):
                issues.append(ContractIssue(severity="blocking", kind="type_change", detail=f"Column '{name}' changed from {old_column.data_type} to {new_column.data_type}."))
            elif not old_column.nullable and new_column.nullable:
                issues.append(ContractIssue(severity="warning", kind="nullability_weakened", detail=f"Column '{name}' became nullable."))
        additions = set(after) - set(before)
        if additions:
            issues.append(ContractIssue(severity="warning", kind="added_columns", detail=f"New columns detected: {', '.join(sorted(additions))}."))
        return ContractReport(asset=previous.name, valid=not any(issue.severity == "blocking" for issue in issues), issues=issues)

    @staticmethod
    def _type_family(data_type: str) -> str:
        normalized = data_type.lower()
        if any(item in normalized for item in ("int", "serial")):
            return "integer"
        if any(item in normalized for item in ("real", "float", "double", "numeric", "decimal")):
            return "number"
        if any(item in normalized for item in ("char", "text", "string", "varchar")):
            return "string"
        if "bool" in normalized:
            return "boolean"
        if "date" in normalized or "time" in normalized:
            return "datetime"
        return normalized
