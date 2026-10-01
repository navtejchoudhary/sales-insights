"""The sales metric view: load it, check it, run it locally, publish it on Databricks.

The YAML in sql/metric_views/ is the single definition of every KPI. On Databricks it becomes a
Unity Catalog metric view (CREATE VIEW ... WITH METRICS LANGUAGE YAML). Plain local Spark has no
metric views, so `compile_query` turns a request ("net_revenue by province and month") into the
ordinary SQL a metric view would run: the fact table LEFT JOINed to its dimensions, the field
expressions grouped, the measure expressions aggregated, MEASURE(x) replaced by x's own expression.
That lets the tests prove locally that the metric view and the hand-written KPI SQL give the same numbers.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from string import Template

import yaml

from sales_insights.common.config import REPO_ROOT

METRIC_VIEWS = REPO_ROOT / "sql" / "metric_views"
GOLD_TABLES = ["fact_sales", "dim_customer", "dim_product", "dim_region", "dim_channel", "dim_date"]
MEASURE_REF = re.compile(r"MEASURE\((\w+)\)")


@dataclass(frozen=True)
class MetricView:
    name: str
    text: str  # raw YAML with ${table} placeholders
    spec: dict

    @property
    def fields(self) -> dict[str, str]:
        return {f["name"]: f["expr"] for f in self.spec.get("fields", self.spec.get("dimensions", []))}

    @property
    def measures(self) -> dict[str, str]:
        return {m["name"]: m["expr"] for m in self.spec["measures"]}

    def render(self, tables: dict[str, str]) -> str:
        """The YAML with real table names filled in (what gets published on Databricks)."""
        return Template(self.text).substitute(tables)


def load(name: str = "sales_metrics", folder: Path = METRIC_VIEWS) -> MetricView:
    text = (folder / f"{name}.yaml").read_text(encoding="utf-8")
    return MetricView(name=name, text=text, spec=yaml.safe_load(text))


def expand(mv: MetricView, measure: str, seen: tuple[str, ...] = ()) -> str:
    """A measure's expression with every MEASURE(x) replaced by x's expression (recursively)."""
    if measure in seen:
        raise ValueError(f"measure cycle: {' -> '.join((*seen, measure))}")
    if measure not in mv.measures:
        raise KeyError(f"unknown measure {measure!r}")
    return MEASURE_REF.sub(lambda m: f"({expand(mv, m.group(1), (*seen, measure))})", mv.measures[measure])


def compile_query(mv: MetricView, tables: dict[str, str], fields: list[str], measures: list[str]) -> str:
    """Plain Spark SQL equivalent of: SELECT <fields>, MEASURE(<measures>) FROM metric_view GROUP BY ALL."""
    spec = mv.spec
    sel = [f"{mv.fields[f]} AS `{f}`" for f in fields] + [f"{expand(mv, m)} AS `{m}`" for m in measures]
    sql = [f"SELECT {', '.join(sel)}", f"FROM {Template(spec['source']).substitute(tables)} AS source"]
    for j in spec.get("joins", []):
        on = j.get("on", j.get(True))  # PyYAML reads an unquoted `on:` key as the boolean True
        sql.append(f"LEFT JOIN {Template(j['source']).substitute(tables)} AS {j['name']} ON {on}")
    if spec.get("filter"):
        sql.append(f"WHERE {spec['filter']}")
    if fields:
        sql.append("GROUP BY " + ", ".join(str(i + 1) for i in range(len(fields))))
    return "\n".join(sql)


def table_names(lake) -> dict[str, str]:
    return {t: lake.sql_name("gold", t) for t in GOLD_TABLES}


def query(spark, lake, fields: list[str], measures: list[str], mv: MetricView | None = None):
    """Run the metric view locally (or anywhere) as plain SQL."""
    mv = mv or load()
    return spark.sql(compile_query(mv, table_names(lake), fields, measures))


def publish(spark, cfg, lake, mv: MetricView | None = None) -> str | None:
    """Create the Unity Catalog metric view on Databricks. Locally there is nothing to publish."""
    if lake.by_path:
        return None
    mv = mv or load()
    target = cfg.table("gold", mv.name)
    spark.sql(f"CREATE OR REPLACE VIEW {target} WITH METRICS LANGUAGE YAML AS $$\n{mv.render(table_names(lake))}\n$$")
    return target
