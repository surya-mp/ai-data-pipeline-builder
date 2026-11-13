from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from jinja2 import BaseLoader, Environment

from src.database import Database
from src.sql.validator import SQLValidator


class EDAReporter:
    def __init__(self, database: Database, report_dir: Path, max_rows: int = 100000) -> None:
        self.database = database
        self.report_dir = report_dir
        self.max_rows = max_rows

    def generate(self, table: str, pipeline_id: str) -> Path:
        self.report_dir.mkdir(parents=True, exist_ok=True)
        quoted = SQLValidator.quote_identifier(table)
        total_rows = int(self.database.query_dataframe(f"SELECT COUNT(*) AS count FROM {quoted}").iloc[0]["count"])
        frame = self.database.query_dataframe(f"SELECT * FROM {quoted} LIMIT {self.max_rows}")
        missing = frame.isna().sum().rename("missing").to_frame()
        numeric = frame.select_dtypes(include="number")
        summaries = numeric.describe().round(2).to_html(classes="dataframe", border=0) if not numeric.empty else "<p>No numeric columns.</p>"
        missing_html = missing.to_html(classes="dataframe", border=0)
        figures: list[str] = []
        for column in list(numeric.columns)[:6]:
            try:
                figures.append(px.histogram(frame, x=column, title=f"Distribution: {column}").to_html(full_html=False, include_plotlyjs="cdn"))
            except Exception as exc:
                figures.append(f"<p>Could not render distribution for {column}: {type(exc).__name__}.</p>")
        if len(numeric.columns) >= 2:
            try:
                correlation = numeric.corr()
                heatmap = go.Figure(
                    data=go.Heatmap(
                        z=correlation.values,
                        x=list(correlation.columns),
                        y=list(correlation.index),
                        colorscale="RdBu",
                        zmin=-1,
                        zmax=1,
                        colorbar={"title": "Correlation"},
                    )
                )
                heatmap.update_layout(title="Numeric correlations")
                figures.append(heatmap.to_html(full_html=False, include_plotlyjs=False))
            except Exception as exc:
                figures.append(f"<p>Could not render correlations: {type(exc).__name__}.</p>")
        insights = self._insights(frame, numeric)
        template = Environment(loader=BaseLoader()).from_string("""<!doctype html><html><head><meta charset='utf-8'><title>EDA - {{ table }}</title><style>body{font-family:system-ui;margin:2rem;max-width:1200px}.dataframe{border-collapse:collapse}.dataframe td,.dataframe th{padding:.4rem;border:1px solid #ddd}h1{color:#172554}</style></head><body><h1>EDA report: {{ table }}</h1><p>Rows: {{ rows }} · Columns: {{ columns }}</p><h2>Business insights</h2><ul>{% for insight in insights %}<li>{{ insight }}</li>{% endfor %}</ul><h2>Missing values</h2>{{ missing }}<h2>Numerical summary</h2>{{ summary }}<h2>Distributions and correlations</h2>{% for figure in figures %}{{ figure }}{% endfor %}</body></html>""")
        output = self.report_dir / f"{pipeline_id}.html"
        output.write_text(template.render(table=table, rows=total_rows, columns=len(frame.columns), insights=insights + ([f"Report calculations use a {len(frame):,}-row sample."] if total_rows > len(frame) else []), missing=missing_html, summary=summaries, figures=figures), encoding="utf-8")
        return output

    @staticmethod
    def _insights(frame: pd.DataFrame, numeric: pd.DataFrame) -> list[str]:
        insights = [f"Dataset contains {len(frame):,} rows and {len(frame.columns):,} columns."]
        missing = int(frame.isna().sum().sum())
        if missing:
            insights.append(f"There are {missing:,} missing values across the dataset.")
        if not numeric.empty:
            for column in numeric.columns[:3]:
                q1, q3 = numeric[column].quantile([0.25, 0.75])
                iqr = q3 - q1
                outliers = ((numeric[column] < q1 - 1.5 * iqr) | (numeric[column] > q3 + 1.5 * iqr)).sum() if iqr else 0
                insights.append(f"{column}: mean {numeric[column].mean():.2f}; {outliers:,} IQR-based outliers.")
        return insights
