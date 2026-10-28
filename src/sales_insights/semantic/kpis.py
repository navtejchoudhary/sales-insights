"""KPI SQL views: load, run, check against the metric view, publish on Databricks.

    uv run python -m sales_insights.semantic.kpis
    uv run python -m sales_insights.semantic.kpis --area inventory
    uv run python -m sales_insights.semantic.kpis --area client      # SAP billing data: the sales KPIs

Each file in sql/kpis/ (sales) or sql/kpis/inventory/ is one SELECT over the gold tables, with
${fact_sales}-style placeholders.
  - locally:     run as temporary views (plain Spark has no permanent catalog)
  - Databricks:  CREATE OR REPLACE VIEW <catalog>.gold.<kpi> AS <select>, plus the area's metric views
                 (sales: sales_metrics; inventory: inventory_metrics and sales_and_stock)

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
    metric_view: str = "sales_metrics"


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


INVENTORY_AGREEMENTS = [
    Agreement(
        "kpi_stock_daily",
        {"snapshot_date": "snapshot_date", "province": "province"},
        [
            "stock_on_hand",
            "stock_value",
            "in_transit",
            "avg_daily_issued_28d",
            "days_of_cover",
            "stock_out_positions",
            "low_cover_positions",
            "active_positions",
            "stock_out_rate_pct",
            "issued_quantity",
        ],
        "inventory_metrics",
    ),
    Agreement(
        "kpi_stock_month_end",
        {"month": "month", "location_id": "location_id", "product_id": "product_id"},
        [
            "stock_on_hand",
            "stock_value",
            "in_transit",
            "avg_daily_issued_28d",
            "days_of_cover",
            "issued_quantity",
            "stock_out_days",
        ],
        "inventory_metrics",
    ),
    Agreement(
        "kpi_sales_and_stock_monthly",
        {"month": "month", "province": "province"},
        [
            "net_revenue",
            "units_sold",
            "units_invoiced",
            "issued_quantity",
            "stock_out_days",
            "depot_stock",
            "depot_in_transit",
            "stock_out_positions",
            "depot_days_of_cover",
        ],
        "sales_and_stock",
    ),
]


@dataclass(frozen=True)
class AreaSemantic:
    """Where an area's KPI SQL lives, which metric views it publishes, and what must agree.

    own_gold: the area has the whole sales-shaped gold model in its own catalog (sales; client = SAP billing
    data), so every table name comes from that area. Otherwise it reads the shared dimensions in sales gold.
    """

    folder: Path
    metric_views: tuple[str, ...]
    agreements: tuple[Agreement, ...]
    own_gold: bool = False


AREAS = {
    "sales": AreaSemantic(KPI_SQL, ("sales_metrics",), tuple(AGREEMENTS), own_gold=True),
    "inventory": AreaSemantic(
        KPI_SQL / "inventory", ("inventory_metrics", "sales_and_stock"), tuple(INVENTORY_AGREEMENTS)
    ),
    # The same KPI SQL and metric view as sales (one definition per KPI), published in the client catalog
    "client": AreaSemantic(KPI_SQL, ("sales_metrics",), tuple(AGREEMENTS), own_gold=True),
}


def names(folder: Path = KPI_SQL) -> list[str]:
    return sorted(p.stem for p in folder.glob("kpi_*.sql"))


def render(name: str, tables: dict[str, str], folder: Path = KPI_SQL) -> str:
    return Template((folder / f"{name}.sql").read_text(encoding="utf-8")).substitute(tables)


def area_tables(lake: Lake, sales_lake: Lake | None = None) -> dict[str, str]:
    """Placeholder -> table name for lake's area. Areas without their own gold model also read the shared
    tables in sales gold."""
    if AREAS[lake.cfg.area].own_gold:
        return mvs.table_names(lake)
    if sales_lake is None:
        raise ValueError(f"the {lake.cfg.area} area reads the shared dimensions in sales gold: pass sales_lake")
    return mvs.table_names(lake, sales_lake)


def kpi(spark: SparkSession, lake: Lake, name: str, sales_lake: Lake | None = None) -> DataFrame:
    return spark.sql(render(name, area_tables(lake, sales_lake), AREAS[lake.cfg.area].folder))


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


def check_against_metric_view(
    spark: SparkSession, lake: Lake, sales_lake: Lake | None = None
) -> list[tuple[str, int, int]]:
    """(kpi, rows, mismatching rows) for every agreement of lake's area."""
    area, tables = AREAS[lake.cfg.area], area_tables(lake, sales_lake)
    out = []
    for a in area.agreements:
        k = spark.sql(render(a.kpi, tables, area.folder))
        m = mvs.query(spark, lake, list(a.keys.values()), a.measures, mvs.load(a.metric_view), tables)
        out.append((a.kpi, k.count(), compare(k, m, a.keys, a.measures).count()))
    return out


def publish(spark: SparkSession, cfg: Config, lake: Lake, sales_lake: Lake | None = None) -> list[str]:
    """Databricks: permanent views gold.kpi_* and the area's metric views. Locally: temp views."""
    area, tables = AREAS[cfg.area], area_tables(lake, sales_lake)
    done = []
    for name in names(area.folder):
        sql = render(name, tables, area.folder)
        if lake.by_path:
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {name} AS {sql}")
        else:
            spark.sql(f"CREATE OR REPLACE VIEW {cfg.table('gold', name)} AS {sql}")
        done.append(name)
    for mv in area.metric_views:
        target = mvs.publish(spark, cfg, lake, mvs.load(mv), tables)
        if target:
            done.append(target)
    return done


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    parser = argparse.ArgumentParser(description="Run KPI SQL and prove it matches the metric view.")
    parser.add_argument("--area", default="sales", choices=sorted(AREAS), help="business area (default sales)")
    args = parser.parse_args(argv)
    cfg = load_config(area=args.area)
    spark = get_spark(cfg, app_name="kpis")
    lake = Lake(spark, cfg)
    sales_lake = None if AREAS[cfg.area].own_gold else Lake(spark, cfg.for_area("sales"))
    published = publish(spark, cfg, lake, sales_lake)
    print(f"  views: {', '.join(published)}")

    if AREAS[cfg.area].own_gold:
        print("  last 6 months (kpi_sales_monthly):")
        cols = ["month", "net_revenue", "units", "invoice_count", "gross_margin_pct", "returns_rate_pct",
                "mom_growth_pct", "yoy_growth_pct"]  # fmt: skip
        kpi(spark, lake, "kpi_sales_monthly").orderBy(F.col("month").desc()).select(*cols).show(6, truncate=False)
    else:
        print("  latest 3 days (kpi_stock_daily, all provinces):")
        daily = kpi(spark, lake, "kpi_stock_daily", sales_lake).groupBy("snapshot_date")
        daily.agg(F.sum("stock_value").alias("stock_value"), F.sum("stock_out_positions").alias("stock_outs")).orderBy(
            F.col("snapshot_date").desc()
        ).show(3, truncate=False)

    run_id = f"kpis-{datetime.now(UTC):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}"
    now = datetime.now(UTC)
    results = check_against_metric_view(spark, lake, sales_lake)
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
