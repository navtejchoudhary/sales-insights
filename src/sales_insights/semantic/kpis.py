"""KPI SQL views: load, run, check against the metric view, publish on Databricks.

    uv run python -m sales_insights.semantic.kpis

Each file in sql/kpis/ is one SELECT over the gold tables, with ${fact_sales}-style placeholders.
  - locally:     run as temporary views (plain Spark has no permanent catalog)
  - Databricks:  CREATE OR REPLACE VIEW sales_dev.gold.<kpi> AS <select>, plus the metric view

The plan's rule: "a KPI is done only when the local SQL and the metric view give identical numbers".
`check_against_metric_view` runs both on the same gold data and compares every value; results go
to ops.dq_results (layer "semantic") and the command exits with code 1 if anything differs.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from string import Template

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_insights.common.config import REPO_ROOT, Config
from sales_insights.common.lake import Lake
from sales_insights.pipeline import bronze as b
from sales_insights.semantic import metric_views as mvs

KPI_SQL = REPO_ROOT / "sql" / "kpis"


@dataclass(frozen=True)
class Agreement:
    """Which KPI columns must equal which metric-view measures, and on which keys."""

    kpi: str
    keys: dict[str, str]  # KPI column -> metric view field
    measures: list[str]  # same name in the KPI view and the metric view


AGREEMENTS = [
    Agreement(
        "kpi_sales_monthly",
        {"month": "month"},
        [
            "net_revenue",
            "invoiced_revenue",
            "returns_value",
            "cancellations_value",
            "price_corrections_value",
            "units",
            "invoice_count",
            "active_customers",
            "average_invoice_value",
            "gross_margin",
            "gross_margin_pct",
            "returns_rate_pct",
        ],
    ),
    Agreement(
        "kpi_sales_daily",
        {"invoice_date": "invoice_date"},
        [
            "net_revenue",
            "invoiced_revenue",
            "returns_value",
            "units",
            "invoice_count",
            "active_customers",
            "gross_margin",
        ],
    ),
    Agreement(
        "kpi_sales_by_province_monthly",
        {"month": "month", "province": "province"},
        ["net_revenue", "units", "invoice_count", "active_customers", "gross_margin_pct"],
    ),
    Agreement(
        "kpi_product_monthly",
        {"month": "month", "product": "product"},
        ["net_revenue", "invoiced_revenue", "returns_value", "units", "returns_rate_pct"],
    ),
]


def names(folder: Path = KPI_SQL) -> list[str]:
    return sorted(p.stem for p in folder.glob("kpi_*.sql"))


def render(name: str, tables: dict[str, str], folder: Path = KPI_SQL) -> str:
    return Template((folder / f"{name}.sql").read_text(encoding="utf-8")).substitute(tables)


def kpi(spark: SparkSession, lake: Lake, name: str) -> DataFrame:
    return spark.sql(render(name, mvs.table_names(lake)))


def compare(kpi_df: DataFrame, mv_df: DataFrame, keys: dict[str, str], measures: list[str]) -> DataFrame:
    """Rows where any measure differs after rounding both sides to 2 decimals (KPI outputs are rounded)."""
    k = kpi_df.select(*[F.col(c).alias(f"k_{c}") for c in [*keys, *measures]])
    m = mv_df.select(*[F.col(c).alias(f"m_{c}") for c in [*keys.values(), *measures]])
    cond = None
    for kc, mc in keys.items():
        c = F.col(f"k_{kc}").eqNullSafe(F.col(f"m_{mc}"))
        cond = c if cond is None else cond & c
    j = k.join(m, cond, "full_outer")
    differs = None
    for c in measures:
        a, e = F.round(F.col(f"k_{c}").cast("double"), 2), F.round(F.col(f"m_{c}").cast("double"), 2)
        d = ~(a.eqNullSafe(e) | (F.abs(a - e) <= 0.011))
        differs = d if differs is None else differs | d
    return j.filter(differs)


def check_against_metric_view(spark: SparkSession, lake: Lake) -> list[tuple[str, int, int]]:
    """(kpi, rows, mismatching rows) for every agreement."""
    out = []
    for a in AGREEMENTS:
        k = kpi(spark, lake, a.kpi)
        m = mvs.query(spark, lake, list(a.keys.values()), a.measures)
        out.append((a.kpi, k.count(), compare(k, m, a.keys, a.measures).count()))
    return out


def publish(spark: SparkSession, cfg: Config, lake: Lake) -> list[str]:
    """Databricks: permanent views gold.kpi_* and the metric view gold.sales_metrics. Locally: temp views."""
    tables = mvs.table_names(lake)
    done = []
    for name in names():
        sql = render(name, tables)
        if lake.by_path:
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {name} AS {sql}")
        else:
            spark.sql(f"CREATE OR REPLACE VIEW {cfg.table('gold', name)} AS {sql}")
        done.append(name)
    target = mvs.publish(spark, cfg, lake)
    if target:
        done.append(target)
    return done


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    argparse.ArgumentParser(description="Run KPI SQL and prove it matches the metric view.").parse_args(argv)
    cfg = load_config()
    spark = get_spark(cfg, app_name="kpis")
    lake = Lake(spark, cfg)
    published = publish(spark, cfg, lake)
    print(f"  views: {', '.join(published)}")

    print("  last 6 months (kpi_sales_monthly):")
    cols = ["month", "net_revenue", "units", "invoice_count", "gross_margin_pct", "returns_rate_pct", "mom_growth_pct",
            "yoy_growth_pct"]  # fmt: skip
    kpi(spark, lake, "kpi_sales_monthly").orderBy(F.col("month").desc()).select(*cols).show(6, truncate=False)

    run_id = f"kpis-{datetime.now(UTC):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}"
    now = datetime.now(UTC)
    results = check_against_metric_view(spark, lake)
    dq = []
    for name, rows, bad in results:
        status = "PASS" if bad == 0 and rows > 0 else "FAIL"
        print(f"  {status}  {name:<32} {rows:>6,} rows, {bad} differ from the metric view")
        dq.append(
            (run_id, now, "semantic", "kpi_matches_metric_view", f"gold.{name}", None, None, "0", str(bad), status)
        )
    lake.append(spark.createDataFrame(dq, b.DQ_SCHEMA), *b.DQ_RESULTS)
    if any(r[-1] == "FAIL" for r in dq):
        print("  KPI CHECK FAILED: KPI SQL and metric view disagree. See ops.dq_results.")
        sys.exit(1)
    print("  every KPI matches the metric view.")


if __name__ == "__main__":
    main()
