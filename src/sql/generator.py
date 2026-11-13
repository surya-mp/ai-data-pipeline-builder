from __future__ import annotations

from src.models import ColumnLineage, PipelinePlan, TableSchema
from src.sql.validator import SQLValidator


class SQLGenerationError(ValueError):
    pass


class SQLGenerator:
    """Schema-driven SQL generator; ambiguity is surfaced instead of guessed unsafely."""

    def __init__(self, validator: SQLValidator | None = None) -> None:
        self.validator = validator or SQLValidator()

    def generate(self, plan: PipelinePlan, schemas: dict[str, TableSchema]) -> str:
        sources = plan.source_tables
        missing = set(sources) - set(schemas)
        if missing:
            raise SQLGenerationError(f"Plan uses unknown tables: {', '.join(sorted(missing))}")
        output = self.validator.quote_identifier(plan.output)
        first, ordered_joins = self._ordered_join_plan(sources, plan)
        select_parts = self._select_parts(plan, schemas)
        joins: list[str] = []
        for join in ordered_joins:
            left_table, left_column, right_column = join.left_table, join.left_column, join.right_column
            source_q = self.validator.quote_identifier(join.right_table)
            left_q = self.validator.quote_identifier(left_table)
            left_column_q = self.validator.quote_identifier(left_column)
            right_column_q = self.validator.quote_identifier(right_column)
            joins.append(f"{join.join_type} JOIN {source_q} ON {left_q}.{left_column_q} = {source_q}.{right_column_q}")
        group_by = ""
        where = ""
        if plan.filters:
            where = " WHERE " + " AND ".join(self._filter_expression(item, schemas, set(plan.source_tables)) for item in plan.filters)
        if plan.metrics:
            group_by = " GROUP BY " + ", ".join(self._qualified_column(column, schemas) for column in plan.group_by)
        sql = f"CREATE TABLE {output} AS SELECT {', '.join(select_parts)} FROM {self.validator.quote_identifier(first)} {' '.join(joins)}{where}{group_by}"
        result = self.validator.validate(sql, schemas, allowed_output=plan.output)
        if not result.valid:
            raise SQLGenerationError("; ".join(result.errors))
        return sql

    @staticmethod
    def _ordered_join_plan(sources: list[str], plan: PipelinePlan):
        """Topologically order joins without reversing LEFT JOIN row-preservation semantics."""
        if len(sources) == 1:
            return sources[0], []
        source_set = set(sources)
        joins = [join for join in plan.joins if join.left_table in source_set and join.right_table in source_set]
        if len(joins) < len(sources) - 1:
            raise SQLGenerationError("Every multi-table pipeline needs explicit foreign-key or user-supplied join mappings.")
        right_side_of_left = {join.right_table for join in joins if join.join_type == "LEFT"}
        roots = [source for source in sources if source not in right_side_of_left]
        if not roots:
            raise SQLGenerationError("LEFT JOIN mappings form a cycle and cannot preserve declared row semantics.")
        first = roots[0]
        joined = {first}
        ordered = []
        pending = list(joins)
        while pending:
            next_join = next((join for join in pending if join.left_table in joined and join.right_table not in joined), None)
            if not next_join:
                # INNER JOIN direction is equivalent, but LEFT JOIN direction is not.
                reversible = next((join for join in pending if join.join_type == "INNER" and join.right_table in joined and join.left_table not in joined), None)
                if reversible:
                    next_join = reversible.model_copy(update={"left_table": reversible.right_table, "left_column": reversible.right_column, "right_table": reversible.left_table, "right_column": reversible.left_column})
                    pending.remove(reversible)
                else:
                    raise SQLGenerationError("Join mappings are disconnected or require reversing a LEFT JOIN; reorder/define mappings explicitly.")
            else:
                pending.remove(next_join)
            ordered.append(next_join)
            joined.add(next_join.right_table)
        if joined != source_set:
            raise SQLGenerationError("Join mappings do not connect all selected source tables.")
        return first, ordered

    def _select_parts(self, plan: PipelinePlan, schemas: dict[str, TableSchema]) -> list[str]:
        if plan.metrics:
            if not plan.group_by:
                raise SQLGenerationError("Aggregated metrics require at least one group_by column.")
            parts = [self._qualified_column(column, schemas) for column in plan.group_by]
            for metric in plan.metrics:
                column = self._qualified_column(metric.column, schemas)
                alias = self.validator.quote_identifier(metric.name)
                distinct = "DISTINCT " if metric.function == "COUNT" else ""
                parts.append(f"{metric.function}({distinct}{column}) AS {alias}")
            return parts
        parts: list[str] = []
        emitted_names: set[str] = set()
        for source in plan.source_tables:
            source_q = self.validator.quote_identifier(source)
            for column in schemas[source].columns:
                column_q = self.validator.quote_identifier(column.name)
                output_name = column.name
                if output_name in emitted_names:
                    output_name, suffix = f"{source}_{column.name}", 2
                    while output_name in emitted_names:
                        output_name, suffix = f"{source}_{column.name}_{suffix}", suffix + 1
                emitted_names.add(output_name)
                alias = "" if output_name == column.name else f" AS {self.validator.quote_identifier(output_name)}"
                parts.append(f"{source_q}.{column_q}{alias}")
        return parts

    def build_column_lineage(self, plan: PipelinePlan, schemas: dict[str, TableSchema]) -> list[ColumnLineage]:
        """Return source-to-output mappings without depending on database query history."""
        if plan.metrics:
            lineage = []
            for reference in plan.group_by:
                _, column = reference.split(".", 1)
                lineage.append(ColumnLineage(output_column=column, source_columns=[reference], expression=reference))
            for metric in plan.metrics:
                lineage.append(
                    ColumnLineage(
                        output_column=metric.name,
                        source_columns=[metric.column],
                        expression=f"{metric.function}({metric.column})",
                    )
                )
            return lineage
        lineage: list[ColumnLineage] = []
        emitted_names: set[str] = set()
        for source in plan.source_tables:
            for column in schemas[source].columns:
                output_name = column.name
                if output_name in emitted_names:
                    output_name, suffix = f"{source}_{column.name}", 2
                    while output_name in emitted_names:
                        output_name, suffix = f"{source}_{column.name}_{suffix}", suffix + 1
                emitted_names.add(output_name)
                lineage.append(ColumnLineage(output_column=output_name, source_columns=[f"{source}.{column.name}"], expression=f"{source}.{column.name}"))
        return lineage

    def _qualified_column(self, reference: str, schemas: dict[str, TableSchema]) -> str:
        try:
            table, column = reference.split(".", 1)
        except ValueError as exc:
            raise SQLGenerationError(f"Column reference must be table.column, received '{reference}'.") from exc
        if table not in schemas or column not in {item.name for item in schemas[table].columns}:
            raise SQLGenerationError(f"Unknown column reference '{reference}'.")
        return f"{self.validator.quote_identifier(table)}.{self.validator.quote_identifier(column)}"

    def _filter_expression(self, filter_definition, schemas: dict[str, TableSchema], allowed_tables: set[str]) -> str:
        table = filter_definition.column.split(".", 1)[0]
        if table not in allowed_tables:
            raise SQLGenerationError(f"Filter references non-source table '{table}'.")
        column = self._qualified_column(filter_definition.column, schemas)
        if filter_definition.operator in {"IS NULL", "IS NOT NULL"}:
            return f"{column} {filter_definition.operator}"
        value = filter_definition.value
        if value is None:
            raise SQLGenerationError(f"Filter '{filter_definition.column} {filter_definition.operator}' requires a value.")
        if isinstance(value, bool):
            literal = "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            literal = str(value)
        else:
            literal = "'" + str(value).replace("'", "''") + "'"
        return f"{column} {filter_definition.operator} ({literal})" if filter_definition.operator == "IN" else f"{column} {filter_definition.operator} {literal}"

