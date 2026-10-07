"""Gold: the business-ready star schema that dashboards, metric views and Genie read.

    uv run python -m sales_insights.pipeline.gold

    gold.fact_sales     one row per invoice line, with clear money and quantity measures
    gold.dim_customer   gold.dim_product   gold.dim_region   gold.dim_channel   gold.dim_date

Like silver, gold is rebuilt in full on every run (seconds at this size; see decisions log).
Every table and column carries the plain-English description from gold_model.py.
Afterwards run reconciliation (pipeline/reconcile.py) to prove gold matches the source's own totals.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import date, timedelta

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_insights.common.config import Config
from sales_insights.common.lake import Lake
from sales_insights.insights import periods as pr
from sales_insights.pipeline import gold_model as gm

DOCUMENT_TYPES = {
    "ZAOR": "Invoice",
    "ZFOC": "Free of charge",
    "ZARE": "Return",
    "S1": "Cancellation",
    "ZACR": "Price correction",
}
SALES_TYPES = ["ZAOR", "ZFOC"]
HISTORY = "history"
MONEY = "DECIMAL(18,2)"
QTY = "DECIMAL(18,3)"


@dataclass
class GoldRun:
    rows: dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Fact
# ---------------------------------------------------------------------------


def source_kind(source_file: Column, business_date: Column) -> Column:
    """history / orders / changes, from the landing path of the file the row came from."""
    return (
        F.when(business_date.isNull(), F.lit(HISTORY))
        .when(source_file.contains("/orders_"), F.lit("orders"))
        .when(source_file.contains("/changes_"), F.lit("changes"))
    )


def _only(condition: Column, value: Column, sql_type: str) -> Column:
    """value where condition holds, else 0 (so the measure sums cleanly)."""
    return F.when(condition, value).otherwise(F.lit(0)).cast(sql_type)


def build_fact_sales(lines: DataFrame, products: DataFrame) -> DataFrame:
    t, qty, rev = F.col("invoice_type"), F.col("quantity"), F.col("revenue")
    doc_type = F.coalesce(*[F.when(t == code, F.lit(label)) for code, label in DOCUMENT_TYPES.items()])
    rep = F.col("sales_rep_id") if "sales_rep_id" in lines.columns else F.lit(None).cast("string")
    cost = products.select("product_id", F.col("standard_cost").alias("_unit_cost"))
    f = lines.join(F.broadcast(cost), "product_id", "left")
    cost_amount = (qty * F.coalesce(F.col("_unit_cost"), F.lit(0))).cast(MONEY)
    return f.select(
        "invoice_number",
        "invoice_item",
        "invoice_type",
        doc_type.alias("document_type"),
        "invoice_date",
        "fiscal_year",
        "fiscal_period",
        "customer_id",
        "product_id",
        F.col("region").alias("district_code"),
        F.col("sales_office").alias("sales_office_code"),
        F.col("distribution_channel").alias("distribution_channel_code"),
        F.col("plant").alias("plant_code"),
        "company_code",
        rep.alias("sales_rep_id"),
        qty.cast(QTY).alias("quantity"),
        _only(t.isin(SALES_TYPES), qty, QTY).alias("sold_quantity"),
        _only(t == "ZARE", qty, QTY).alias("returned_quantity"),
        rev.cast(MONEY).alias("net_revenue_amount"),
        _only(t.isin(SALES_TYPES), rev, MONEY).alias("invoiced_revenue_amount"),
        _only(t == "ZARE", rev, MONEY).alias("returns_amount"),
        _only(t == "S1", rev, MONEY).alias("cancellations_amount"),
        _only(t == "ZACR", rev, MONEY).alias("price_corrections_amount"),
        F.col("gross_revenue").cast(MONEY).alias("gross_revenue_amount"),
        F.col("tax_amount").cast(MONEY),
        cost_amount.alias("cost_amount"),
        (rev - cost_amount).cast(MONEY).alias("margin_amount"),
        (t == "ZFOC").alias("is_free_of_charge"),
        F.coalesce(F.col("is_cancelled"), F.lit(False)).alias("is_cancelled"),
        F.col("cancelled_by").alias("cancelled_by_invoice_number"),
        "reference_invoice_number",
        source_kind(F.col("_source_file"), F.col("_business_date")).alias("source_kind"),
        F.col("_business_date").alias("loaded_business_date"),
        "arrival_delay_days",
    )


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------


def build_dim_customer(customers: DataFrame, sales_offices: DataFrame) -> DataFrame:
    offices = sales_offices.select("sales_office", "sales_office_name")
    c = customers.join(F.broadcast(offices), "sales_office", "left")
    return c.select(
        "customer_id",
        "customer_name",
        "customer_full_name",
        F.col("customer_group").alias("customer_group_code"),
        "customer_group_name",
        "city",
        F.col("region").alias("district_code"),
        "district_name",
        "province",
        F.col("sales_office").alias("sales_office_code"),
        "sales_office_name",
        F.col("distribution_channel").alias("distribution_channel_code"),
        "payer_customer_id",
        "created_date",
        F.col("city_mapped").alias("city_is_canonical"),
    )


def build_dim_product(products: DataFrame) -> DataFrame:
    return products.select(
        "product_id",
        "product_description",
        F.col("product_group").alias("product_group_code"),
        "product_group_name",
        F.col("product_type").alias("product_type_code"),
        "product_type_name",
        F.col("division").alias("division_code"),
        "division_name",
        "company_code",
        F.col("plant").alias("plant_code"),
        "base_unit",
        F.col("list_price").alias("list_price_amount"),
        F.col("standard_cost").alias("standard_cost_amount"),
        "launch_date",
    )


def build_dim_region(regions: DataFrame) -> DataFrame:
    return regions.select(F.col("region").alias("district_code"), "district_name", "province", "country")


def build_dim_channel(channels: DataFrame) -> DataFrame:
    return channels.select(
        F.col("distribution_channel").alias("distribution_channel_code"), "distribution_channel_name"
    )


def fiscal_year_start(d: date, start_month: int) -> date:
    return date(d.year if d.month >= start_month else d.year - 1, start_month, 1)


def fiscal_bounds(first: date, last: date, start_month: int) -> tuple[date, date]:
    """Widen a date range to whole fiscal years (so a year-to-date view never runs off the calendar)."""
    end_start = fiscal_year_start(last, start_month)
    return fiscal_year_start(first, start_month), date(end_start.year + 1, start_month, 1) - timedelta(days=1)


def relative_periods(as_of: date, start_month: int) -> dict[str, tuple[date, date]]:
    """The named periods people ask about, as (first day, last day), relative to the latest data date.

    Becomes one True/False column per period in dim_date (is_<name>), so "last month", "this month so far"
    or "year to date" is a plain filter for Genie and the dashboard instead of date arithmetic it can get
    wrong. Same windows as the insights job (insights/periods.py): every comparison is like-for-like.
    """
    mtd, mtd_ly = pr.month_to_date(as_of), pr.month_to_date_last_year(as_of)
    week = pr.last_7_days(as_of)
    month_ly, month_prev = pr.complete_month_vs_last_year(as_of), pr.complete_month_vs_prev_month(as_of)
    fy = fiscal_year_start(as_of, start_month)
    fy_prev = date(fy.year - 1, fy.month, 1)
    return {
        "latest_day": (as_of, as_of),
        "month_to_date": (mtd.current.start, mtd.current.end),
        "same_days_last_month": (mtd.previous.start, mtd.previous.end),
        "same_days_last_year": (mtd_ly.previous.start, mtd_ly.previous.end),
        "last_7_days": (week.current.start, week.current.end),
        "previous_7_days": (week.previous.start, week.previous.end),
        "last_complete_month": (month_ly.current.start, month_ly.current.end),
        "month_before_last_complete_month": (month_prev.previous.start, month_prev.previous.end),
        "last_complete_month_last_year": (month_ly.previous.start, month_ly.previous.end),
        "fiscal_year_to_date": (fy, as_of),
        "same_period_last_fiscal_year": (fy_prev, pr.shift_months(as_of, -12)),
        "current_fiscal_year": (fy, date(fy.year + 1, fy.month, 1) - timedelta(days=1)),
        "previous_fiscal_year": (fy_prev, fy - timedelta(days=1)),
    }


def build_dim_date(spark: SparkSession, first: date, last: date, business: dict, as_of: date) -> DataFrame:
    """One row per day from `first` to `last`; the is_<period> flags are relative to `as_of` (latest data date)."""
    m0 = int(business.get("fiscal_year_start_month", 4))
    seasons = business.get("seasons", {})
    month_to_season = {m: name for name, months in seasons.items() for m in months}
    season_map = F.create_map(*[x for m, name in sorted(month_to_season.items()) for x in (F.lit(m), F.lit(name))])
    gap = business.get("season_gap_label", "Inter-season")

    d = F.col("date")
    month = F.month(d)
    fiscal_period = ((month - F.lit(m0) + F.lit(12)) % F.lit(12) + F.lit(1)).cast("int")
    fiscal_year = F.when(month >= m0, F.year(d)).otherwise(F.year(d) - 1).cast("int")
    days = spark.sql(f"SELECT explode(sequence(DATE'{first}', DATE'{last}', INTERVAL 1 DAY)) AS date")
    return days.select(
        d,
        F.year(d).alias("year"),
        F.quarter(d).alias("quarter"),
        month.alias("month"),
        F.date_format(d, "MMMM").alias("month_name"),
        F.trunc(d, "month").alias("month_start_date"),
        F.date_trunc("week", d).cast("date").alias("week_start_date"),
        ((F.dayofweek(d) + F.lit(5)) % F.lit(7) + F.lit(1)).cast("int").alias("day_of_week"),
        F.date_format(d, "EEEE").alias("day_name"),
        F.dayofweek(d).isin(1, 7).alias("is_weekend"),
        fiscal_year.alias("fiscal_year"),
        F.concat(F.lit("FY"), fiscal_year.cast("string")).alias("fiscal_year_label"),
        fiscal_period.alias("fiscal_period"),
        (((fiscal_period - F.lit(1)) / F.lit(3)).cast("int") + F.lit(1)).alias("fiscal_quarter"),
        F.coalesce(season_map[month], F.lit(gap)).alias("cultivation_season"),
        *[
            d.between(F.lit(start), F.lit(end)).alias(f"is_{name}")
            for name, (start, end) in relative_periods(as_of, m0).items()
        ],
    )


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


def write(lake: Lake, df: DataFrame, table: str) -> None:
    lake.overwrite(gm.with_comments(df, table), "gold", table)
    lake.describe("gold", table, gm.TABLES[table]["description"], gm.TABLES[table]["columns"])


def run_gold(spark: SparkSession, cfg: Config, lake: Lake | None = None) -> GoldRun:
    lake = lake or Lake(spark, cfg)
    run = GoldRun()
    products = lake.read("silver", "products")

    builds = {
        "fact_sales": build_fact_sales(lake.read("silver", "invoice_lines"), products),
        "dim_customer": build_dim_customer(lake.read("silver", "customers"), lake.read("silver", "sales_offices")),
        "dim_product": build_dim_product(products),
        "dim_region": build_dim_region(lake.read("silver", "regions")),
        "dim_channel": build_dim_channel(lake.read("silver", "distribution_channels")),
    }
    for table, df in builds.items():
        write(lake, df, table)

    first, last = lake.read("gold", "fact_sales").agg(F.min("invoice_date"), F.max("invoice_date")).collect()[0]
    m0 = int(cfg.business.get("fiscal_year_start_month", 4))
    write(lake, build_dim_date(spark, *fiscal_bounds(first, last, m0), cfg.business, as_of=last), "dim_date")

    for table in gm.TABLES:
        run.rows[table] = lake.read("gold", table).count()
    return run


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    argparse.ArgumentParser(description="Rebuild gold tables from silver.").parse_args(argv)
    cfg = load_config()
    run = run_gold(get_spark(cfg, app_name="gold"), cfg)
    for table, rows in run.rows.items():
        print(f"    gold.{table:<14} {rows:,} rows")
    print("  next: uv run python -m sales_insights.pipeline.reconcile")


if __name__ == "__main__":
    main()
