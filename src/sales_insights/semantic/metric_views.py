"""Metric views: load them, check them, run them locally, publish them on Databricks.

The YAML files in sql/metric_views/ are the single definition of every KPI (sales_metrics for sales,
inventory_metrics and sales_and_stock for the inventory area). On Databricks each becomes a Unity Catalog
metric view (CREATE VIEW ... WITH METRICS LANGUAGE YAML). Plain local Spark has no metric views, so
`compile_query` turns a request ("net_revenue by province and month") into the ordinary SQL a metric view
would run: the fact table LEFT JOINed to its dimensions, the field expressions grouped, the measure
expressions aggregated, MEASURE(x) replaced by x's own expression. That lets the tests prove locally that
the metric view and the hand-written KPI SQL give the same numbers.

Window measures (stock is a level, not a flow): a measure with
    window: [{order: snapshot_date, range: current, semiadditive: last}]
gives the value on the LAST day of each group instead of a sum over days (stock at month end, not the sum
of 30 daily stocks). Locally this is compiled as: find the last day per group (a subquery), then aggregate
only the rows of that day. Only `range: current` + `semiadditive: last` is compiled locally; anything else
is refused rather than guessed. Docs: https://docs.databricks.com/aws/en/metric-views/data-modeling/window-measures
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
# Gold tables each non-sales area owns; the shared dimensions above stay in the sales catalog (for now)
AREA_GOLD_TABLES = {"inventory": ["fact_stock_daily", "fact_stock_movements", "dim_location", "fact_sales_stock_daily"]}
MEASURE_REF = re.compile(r"MEASURE\((\w+)\)")
AGGREGATE = re.compile(r"\b(SUM|COUNT_IF|COUNT|AVG|MIN|MAX)\s*\(", re.IGNORECASE)
LAST = "__last"  # alias of the subquery holding the last day of every group


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

    @property
    def windows(self) -> dict[str, dict]:
        """Window measures: name -> their (single) window spec."""
        out = {}
        for m in self.spec["measures"]:
            if not m.get("window"):
                continue
            (w,) = m["window"]  # one window per measure is all we use
            if w.get("range") != "current" or w.get("semiadditive") != "last" or "offset" in w:
                raise NotImplementedError(f"{m['name']}: only range: current + semiadditive: last is compiled locally")
            if w["order"] not in self.fields:
                raise ValueError(f"{m['name']}: window order {w['order']!r} is not a field")
            out[m["name"]] = w
        return out

    def render(self, tables: dict[str, str]) -> str:
        """The YAML with real table names filled in (what gets published on Databricks)."""
        return Template(self.text).substitute(tables)


def load(name: str = "sales_metrics", folder: Path = METRIC_VIEWS) -> MetricView:
    text = (folder / f"{name}.yaml").read_text(encoding="utf-8")
    return MetricView(name=name, text=text, spec=yaml.safe_load(text))


def _closing_paren(expr: str, start: int) -> int:
    """Index of the ) that closes the ( just before `start` (quotes respected)."""
    depth, quote = 1, None
    for i in range(start, len(expr)):
        c = expr[i]
        if quote:
            quote = None if c == quote else quote
        elif c in "'\"`":
            quote = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i
    raise ValueError(f"unbalanced parentheses in {expr!r}")


def only_on_last_day(expr: str, order_expr: str, last_col: str) -> str:
    """Make every aggregate in `expr` look only at the rows of the group's last day:
    SUM(x) -> SUM(CASE WHEN <order> = <last> THEN x END); COUNT(DISTINCT x) keeps its DISTINCT."""
    cond = f"{order_expr} = {last_col}"
    out, pos = [], 0
    while m := AGGREGATE.search(expr, pos):
        close = _closing_paren(expr, m.end())
        arg = expr[m.end() : close].strip()
        distinct = re.match(r"DISTINCT\s+", arg, re.IGNORECASE)
        prefix, arg = (distinct.group(0), arg[distinct.end() :]) if distinct else ("", arg)
        arg = "1" if arg == "*" else arg
        out.append(f"{expr[pos : m.end()]}{prefix}CASE WHEN {cond} THEN {arg} END)")
        pos = close + 1
    return "".join(out) + expr[pos:]


def expand(mv: MetricView, measure: str, seen: tuple[str, ...] = (), windows: bool = False) -> str:
    """A measure's expression with every MEASURE(x) replaced by x's expression (recursively).
    windows=True also limits window measures to their group's last day (see compile_query)."""
    if measure in seen:
        raise ValueError(f"measure cycle: {' -> '.join((*seen, measure))}")
    if measure not in mv.measures:
        raise KeyError(f"unknown measure {measure!r}")
    expr = mv.measures[measure]
    if windows and measure in mv.windows:
        order = mv.windows[measure]["order"]
        expr = only_on_last_day(expr, mv.fields[order], f"{LAST}.`{order}`")
    return MEASURE_REF.sub(lambda m: f"({expand(mv, m.group(1), (*seen, measure), windows)})", expr)


def uses(mv: MetricView, measure: str) -> set[str]:
    """The measure and every measure it reaches through MEASURE()."""
    out = {measure}
    for ref in MEASURE_REF.findall(mv.measures[measure]):
        out |= uses(mv, ref)
    return out


def field_condition(mv: MetricView, condition: str) -> str:
    """A condition written with field names ("is_last_complete_month AND location_type = 'Depot'") rewritten
    with the fields' expressions, so it runs on the joined tables. Text in quotes is left alone."""
    names = sorted(mv.fields, key=len, reverse=True)
    pattern = re.compile(r"'[^']*'|\b(" + "|".join(map(re.escape, names)) + r")\b")
    return pattern.sub(lambda m: f"({mv.fields[m.group(1)]})" if m.group(1) else m.group(0), condition)


def compile_query(
    mv: MetricView, tables: dict[str, str], fields: list[str], measures: list[str], where: str | None = None
) -> str:
    """Plain Spark SQL equivalent of: SELECT <fields>, MEASURE(<measures>) FROM metric_view [WHERE <where>]
    GROUP BY ALL. `where` uses field names, as a WHERE on the metric view itself does; it also limits the rows
    the window measures pick their last day from."""
    spec = mv.spec
    body = [f"FROM {Template(spec['source']).substitute(tables)} AS source"]
    for j in spec.get("joins", []):
        on = j.get("on", j.get(True))  # PyYAML reads an unquoted `on:` key as the boolean True
        body.append(f"LEFT JOIN {Template(j['source']).substitute(tables)} AS {j['name']} ON {on}")
    conditions = [f"({c})" for c in (spec.get("filter"), where and field_condition(mv, where)) if c]
    where = [f"WHERE {' AND '.join(conditions)}"] if conditions else []
    group = ["GROUP BY " + ", ".join(str(i + 1) for i in range(len(fields)))] if fields else []

    sel = [f"{mv.fields[f]} AS `{f}`" for f in fields] + [f"{expand(mv, m, windows=True)} AS `{m}`" for m in measures]
    sql = [f"SELECT {', '.join(sel)}", *body]
    orders = sorted({mv.windows[w]["order"] for m in measures for w in uses(mv, m) if w in mv.windows})
    if orders:  # the last day of every group, from the same rows the outer query reads
        keys = [f"{mv.fields[f]} AS `__k{i}`" for i, f in enumerate(fields)]
        lasts = [f"MAX({mv.fields[o]}) AS `{o}`" for o in orders]
        inner = "\n".join([f"SELECT {', '.join(keys + lasts)}", *body, *where, *group])
        if fields:
            on = " AND ".join(f"{mv.fields[f]} <=> {LAST}.`__k{i}`" for i, f in enumerate(fields))
            sql.append(f"LEFT JOIN (\n{inner}\n) AS {LAST} ON {on}")
        else:
            sql.append(f"CROSS JOIN (\n{inner}\n) AS {LAST}")
    return "\n".join(sql + where + group)


def table_names(lake, sales_lake=None) -> dict[str, str]:
    """Placeholder -> real table name. With only `lake`: the sales gold tables (as always).
    With `sales_lake` too: the shared tables from sales gold plus the gold tables `lake`'s area owns."""
    if sales_lake is None:
        return {t: lake.sql_name("gold", t) for t in GOLD_TABLES}
    names = {t: sales_lake.sql_name("gold", t) for t in GOLD_TABLES}
    return names | {t: lake.sql_name("gold", t) for t in AREA_GOLD_TABLES.get(lake.cfg.area, [])}


def query(
    spark,
    lake,
    fields: list[str],
    measures: list[str],
    mv: MetricView | None = None,
    tables: dict | None = None,
    where: str | None = None,
):
    """Run the metric view locally (or anywhere) as plain SQL."""
    mv = mv or load()
    return spark.sql(compile_query(mv, tables or table_names(lake), fields, measures, where))


def publish(spark, cfg, lake, mv: MetricView | None = None, tables: dict | None = None) -> str | None:
    """Create the Unity Catalog metric view on Databricks (in cfg's area). Locally there is nothing to publish."""
    if lake.by_path:
        return None
    mv = mv or load()
    target = cfg.table("gold", mv.name)
    yaml_text = mv.render(tables or table_names(lake))
    spark.sql(f"CREATE OR REPLACE VIEW {target} WITH METRICS LANGUAGE YAML AS $$\n{yaml_text}\n$$")
    return target
