"""Spark side of the stock insights: one small row per stock position the rules in insights/stock.py may pick.

Reads the inventory gold (fact_stock_daily, dim_location) and the shared product master in the sales gold.
Only positions on the latest stock date that are out of stock, low on cover, or above the overstock level
are collected (tens of rows), so the rules run in plain Python.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import functions as F

from sales_insights.common.lake import Lake
from sales_insights.insights.stock import OVERSTOCK_DAYS, Position


def latest_positions(lake: Lake, sales_lake: Lake) -> tuple[date | None, list[Position]]:
    daily = lake.read("gold", "fact_stock_daily")
    as_of = daily.agg(F.max("snapshot_date")).first()[0]
    if as_of is None:
        return None, []
    loc = lake.read("gold", "dim_location").select(
        "location_id", "location_name", "location_type", "province", "supplying_location_id"
    )
    names = lake.read("gold", "dim_location").select(
        F.col("location_id").alias("supplying_location_id"), F.col("location_name").alias("supplier")
    )
    products = sales_lake.read("gold", "dim_product").select(
        "product_id", F.col("product_description").alias("product"), F.col("standard_cost_amount").alias("unit_cost")
    )
    today = daily.filter(F.col("snapshot_date") == F.lit(as_of))
    supplier_stock = today.select(
        F.col("location_id").alias("supplying_location_id"),
        "product_id",
        F.col("stock_quantity").alias("supplier_stock"),
    )
    last_in_stock = (
        daily.filter(F.col("stock_quantity") > 0)
        .groupBy("location_id", "product_id")
        .agg(F.max("snapshot_date").alias("last_in_stock"))
    )
    rate_on_day = daily.select(
        "location_id",
        "product_id",
        F.col("snapshot_date").alias("out_since"),
        F.col("avg_daily_issued_quantity_28d").alias("avg_before"),
    )
    avg = F.col("avg_daily_issued_quantity_28d")
    picked = today.filter(
        F.col("is_stock_out") | F.col("is_low_cover") | ((avg > 0) & (F.col("stock_quantity") > avg * OVERSTOCK_DAYS))
    )
    rows = (
        picked.join(F.broadcast(loc), "location_id", "left")
        .join(F.broadcast(names), "supplying_location_id", "left")
        .join(F.broadcast(products), "product_id", "left")
        .join(supplier_stock, ["supplying_location_id", "product_id"], "left")
        .join(last_in_stock, ["location_id", "product_id"], "left")
        .withColumn("out_since", F.when(F.col("is_stock_out"), F.date_add("last_in_stock", 1)))
        .join(rate_on_day, ["location_id", "product_id", "out_since"], "left")
        .collect()
    )

    def num(v) -> float:
        return float(v) if v is not None else 0.0

    positions = [
        Position(
            location=r["location_name"] or r["location_id"],
            location_type=r["location_type"] or "",
            province=r["province"] or "",
            product=r["product"] or r["product_id"],
            stock=num(r["stock_quantity"]),
            avg_daily=num(r["avg_daily_issued_quantity_28d"]),
            unit_cost=num(r["unit_cost"]),
            is_stock_out=bool(r["is_stock_out"]),
            is_low_cover=bool(r["is_low_cover"]),
            out_since=r["out_since"],
            avg_before=float(r["avg_before"]) if r["avg_before"] is not None else None,
            supplier=r["supplier"],
            supplier_stock=float(r["supplier_stock"]) if r["supplier_stock"] is not None else None,
        )
        for r in rows
    ]
    return as_of, positions
