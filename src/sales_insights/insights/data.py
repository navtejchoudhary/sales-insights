"""Spark side of the insights job: reduce gold to the small aggregates insights/analysis.py needs.

Formulas match the metric view (net revenue, paid non-cancelled invoices, active customers, margin %,
returns rate), so an insight never disagrees with the dashboard or Genie.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from sales_insights.common.lake import Lake
from sales_insights.insights.periods import Comparison, Window

DIMENSIONS = {  # analysis name -> column in the enriched fact
    "province": "province",
    "product_group": "product_group",
    "product": "product",
    "customer_group": "customer_group",
    "channel": "channel",
}


def enriched_fact(lake: Lake) -> DataFrame:
    """fact_sales with the names people use: province, product, product group, customer group, channel."""
    fact = lake.read("gold", "fact_sales")
    cust = lake.read("gold", "dim_customer").select(
        "customer_id", "province", F.col("customer_group_name").alias("customer_group")
    )
    prod = lake.read("gold", "dim_product").select(
        "product_id", F.col("product_description").alias("product"), F.col("product_group_name").alias("product_group")
    )
    chan = lake.read("gold", "dim_channel").select(
        "distribution_channel_code", F.col("distribution_channel_name").alias("channel")
    )
    return (
        fact.join(F.broadcast(cust), "customer_id", "left")
        .join(F.broadcast(prod), "product_id", "left")
        .join(F.broadcast(chan), "distribution_channel_code", "left")
    )


def _in(w: Window) -> Column:
    return F.col("invoice_date").between(F.lit(w.start), F.lit(w.end))


def _measures(prefix: str, cond: Column) -> list[Column]:
    paid = (F.col("invoice_type") == "ZAOR") & ~F.col("is_cancelled")
    return [
        F.sum(F.when(cond, F.col("net_revenue_amount"))).alias(f"{prefix}net_revenue"),
        F.sum(F.when(cond, F.col("quantity"))).alias(f"{prefix}units"),
        F.countDistinct(F.when(cond & paid, F.col("invoice_number"))).alias(f"{prefix}invoice_count"),
        F.countDistinct(F.when(cond & paid, F.col("customer_id"))).alias(f"{prefix}active_customers"),
        F.sum(F.when(cond, F.col("margin_amount"))).alias(f"{prefix}gross_margin"),
        F.sum(F.when(cond, F.col("invoiced_revenue_amount"))).alias(f"{prefix}invoiced_revenue"),
        (-F.sum(F.when(cond, F.col("returns_amount")))).alias(f"{prefix}returns_value"),
    ]


def _finish(raw: dict, prefix: str) -> dict:
    v = {k[len(prefix) :]: (float(x) if x is not None else None) for k, x in raw.items() if k.startswith(prefix)}
    rev, inv = v.get("net_revenue"), v.get("invoiced_revenue")
    v["gross_margin_pct"] = v["gross_margin"] / rev * 100 if rev and v.get("gross_margin") is not None else None
    v["returns_rate_pct"] = (v["returns_value"] or 0) / inv * 100 if inv else None
    return v


def period_totals(fact: DataFrame, comp: Comparison) -> tuple[dict, dict]:
    raw = fact.agg(*_measures("c_", _in(comp.current)), *_measures("p_", _in(comp.previous))).first().asDict()
    return _finish(raw, "c_"), _finish(raw, "p_")


def period_by(fact: DataFrame, comp: Comparison, dimension: str) -> list[tuple[str, float, float]]:
    col = DIMENSIONS[dimension]
    rows = (
        fact.filter(_in(comp.current) | _in(comp.previous))
        .groupBy(col)
        .agg(
            F.sum(F.when(_in(comp.current), F.col("net_revenue_amount"))).alias("cur"),
            F.sum(F.when(_in(comp.previous), F.col("net_revenue_amount"))).alias("prev"),
        )
        .collect()
    )
    return [(r[col], float(r["cur"] or 0), float(r["prev"] or 0)) for r in rows]


def daily_revenue(fact: DataFrame, start: date, end: date) -> dict[date, float]:
    rows = (
        fact.filter(F.col("invoice_date").between(F.lit(start), F.lit(end)))
        .groupBy("invoice_date")
        .agg(F.sum("net_revenue_amount").alias("r"))
        .collect()
    )
    return {r["invoice_date"]: float(r["r"] or 0) for r in rows}


def daily_units_by_product_province(
    fact: DataFrame, start: date, end: date
) -> dict[tuple[str, str], dict[date, float]]:
    rows = (
        fact.filter(F.col("invoice_date").between(F.lit(start), F.lit(end)) & (F.col("invoice_type") == "ZAOR"))
        .groupBy("product", "province", "invoice_date")
        .agg(F.sum("quantity").alias("u"))
        .collect()
    )
    out: dict[tuple[str, str], dict[date, float]] = {}
    for r in rows:
        out.setdefault((r["product"], r["province"]), {})[r["invoice_date"]] = float(r["u"] or 0)
    return out


def monthly_revenue(fact: DataFrame, first_month: date, last_month: date) -> list[tuple[date, float]]:
    rows = (
        fact.withColumn("m", F.trunc("invoice_date", "month"))
        .filter(F.col("m").between(F.lit(first_month), F.lit(last_month)))
        .groupBy("m")
        .agg(F.sum("net_revenue_amount").alias("r"))
        .orderBy("m")
        .collect()
    )
    return [(r["m"], float(r["r"] or 0)) for r in rows]


def data_until(fact: DataFrame) -> date:
    return fact.agg(F.max("invoice_date")).first()[0]
