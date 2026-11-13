from __future__ import annotations

import json
import re

from src.models import AggregateMetric, JoinCondition, PipelinePlan, TableSchema, Transformation


class PlanningError(ValueError):
    pass


class LLMPlanner:
    """Plans with Gemini when available, with a schema-aware offline fallback."""

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key

    def create_plan(self, request: str, schemas: dict[str, TableSchema]) -> PipelinePlan:
        if not schemas:
            raise PlanningError("No source tables found. Create or connect source tables before planning.")
        if self.api_key:
            try:
                return self._plan_with_gemini(request, schemas)
            except Exception:
                # A deterministic plan keeps the product available during provider outages.
                pass
        return self._fallback_plan(request, schemas)

    def _plan_with_gemini(self, request: str, schemas: dict[str, TableSchema]) -> PipelinePlan:
        from google import genai

        schema_text = "\n".join(
            f"{name}: {', '.join(c.name + ' (' + c.data_type + ')' for c in table.columns)}"
            for name, table in schemas.items()
        )
        prompt = f"""Turn the request into JSON matching this exact shape:
{{"objective": str, "source_tables": [str], "transformations": [{{"name": str, "description": str}}], "output": str, "joins": [{{"left_table": str, "left_column": str, "right_table": str, "right_column": str, "join_type": "LEFT|INNER"}}], "expected_grain": str, "group_by": ["table.column"], "metrics": [{{"name": str, "function": "SUM|AVG|COUNT|MIN|MAX", "column": "table.column"}}]}}
Only select tables listed in this schema. Use a lowercase snake_case output name.
Schema:\n{schema_text}\nRequest: {request}"""
        client = genai.Client(api_key=self.api_key)
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        data = json.loads(response.text.removeprefix("```json").removesuffix("```").strip())
        plan = PipelinePlan.model_validate(data)
        unknown = set(plan.source_tables) - set(schemas)
        if unknown:
            raise PlanningError(f"Planner referenced unavailable tables: {', '.join(sorted(unknown))}")
        return plan

    @staticmethod
    def _fallback_plan(request: str, schemas: dict[str, TableSchema]) -> PipelinePlan:
        lower_request = request.lower()
        selected = [name for name in schemas if re.search(rf"\b{re.escape(name.lower().rstrip('s'))}s?\b", lower_request)]
        selected = selected or list(schemas)[:3]
        output_match = re.search(r"(?:called|named|as)\s+([a-zA-Z][\w ]{1,48})", lower_request)
        output = output_match.group(1).strip().replace(" ", "_") if output_match else ("customer_analytics" if "customer" in lower_request else "analytical_dataset")
        objective = request.strip().rstrip(".")
        transforms = [Transformation(name="build_dataset", description="Join source tables and create the analytical dataset.")]
        if any(word in lower_request for word in ("total", "average", "count", "aggregate")):
            transforms.append(Transformation(name="aggregate_metrics", description="Aggregate requested metrics at the target grain."))
        group_by: list[str] = []
        metrics: list[AggregateMetric] = []
        if any(word in lower_request for word in ("total", "average", "count", "number of", "aggregate")):
            dimension = next((name for name in selected if "customer" in name), selected[0])
            dimension_columns = {column.name for column in schemas[dimension].columns}
            key = next((name for name in ("customer_id", "id") if name in dimension_columns), None)
            if key:
                group_by.append(f"{dimension}.{key}")
            numeric = [
                f"{table}.{column.name}"
                for table in selected
                for column in schemas[table].columns
                if any(kind in column.data_type.lower() for kind in ("int", "real", "float", "double", "numeric", "decimal"))
                and not column.name.endswith("_id") and column.name != "id"
            ]
            amount = next((column for column in numeric if any(word in column.lower() for word in ("amount", "price", "total", "value"))), numeric[0] if numeric else None)
            order_key = next((f"{table}.{column.name}" for table in selected for column in schemas[table].columns if "order" in table and column.name in {"order_id", "id"}), None)
            if "total" in lower_request and amount:
                metrics.append(AggregateMetric(name="total_amount", function="SUM", column=amount))
            if any(word in lower_request for word in ("average", "avg")) and amount:
                metrics.append(AggregateMetric(name="average_amount", function="AVG", column=amount))
            if any(word in lower_request for word in ("number of", "count")) and order_key:
                metrics.append(AggregateMetric(name="order_count", function="COUNT", column=order_key))
        joins: list[JoinCondition] = []
        joined = {selected[0]}
        for source in selected[1:]:
            join = LLMPlanner._foreign_key_join(source, joined, schemas)
            if join:
                joins.append(join)
                joined.add(source)
        grain = f"one row per {group_by[0]}" if group_by else "unaggregated source rows"
        return PipelinePlan(objective=objective, source_tables=selected, transformations=transforms, output=output, group_by=group_by, metrics=metrics, joins=joins, expected_grain=grain)

    @staticmethod
    def _foreign_key_join(source: str, joined: set[str], schemas: dict[str, TableSchema]) -> JoinCondition | None:
        for foreign_key in schemas[source].foreign_keys:
            target = str(foreign_key.get("referred_table") or "")
            constrained = foreign_key.get("constrained_columns") or []
            referred = foreign_key.get("referred_columns") or []
            if target in joined and len(constrained) == len(referred) == 1:
                return JoinCondition(left_table=target, left_column=str(referred[0]), right_table=source, right_column=str(constrained[0]))
        for joined_table in joined:
            for foreign_key in schemas[joined_table].foreign_keys:
                target = str(foreign_key.get("referred_table") or "")
                constrained = foreign_key.get("constrained_columns") or []
                referred = foreign_key.get("referred_columns") or []
                if target == source and len(constrained) == len(referred) == 1:
                    return JoinCondition(left_table=source, left_column=str(referred[0]), right_table=joined_table, right_column=str(constrained[0]))
        return None
