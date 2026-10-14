"""Inventory gold: stock tables that dashboards, metric views and Genie read.

    uv run python -m sales_insights.pipeline.inventory_gold

    gold.fact_stock_daily       one row per location, product and day: stock, value, issues, days of cover
    gold.fact_stock_movements   one row per material document line
    gold.dim_location           one row per location
    gold.fact_sales_stock_daily one row per day, sales office and product: sales next to depot stock

Products, dates and regions are the shared dimensions in the sales catalog (dim_product, dim_date, dim_region).
Rebuilt in full on every run. Every column carries its description from pipeline/inventory_model.py.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_insights.common.config import Config
from sales_insights.common.lake import Lake
from sales_insights.pipeline import gold_model as gm
from sales_insights.pipeline import inventory_model as im
from sales_insights.pipeline.inventory_silver import sales_lake_for

MOVEMENT_TYPE_NAMES = {
    "561": "Opening balance",
    "101": "Goods receipt",
    "313": "Transfer out",
    "315": "Transfer in",
    "601": "Goods issue",
    "602": "Goods issue reversal",
    "653": "Customer return",
    "701": "Stock count gain",
    "702": "Stock count loss",
}
COVER_WINDOW_DAYS = 28
LOW_COVER_DAYS = 7
QTY = "DECIMAL(18,3)"
MONEY = "DECIMAL(18,2)"


@dataclass
class InventoryGoldRun:
    rows: dict[str, int] = field(default_factory=dict)


def _blank_to_null(col: str) -> Column:
    return F.when(F.trim(F.col(col)) == "", None).otherwise(F.col(col))


def build_dim_location(locations: DataFrame) -> DataFrame:
    return locations.select(
        "location_id",
        "location_name",
        F.when(F.col("location_type") == "depot", F.lit("Depot"))
        .otherwise(F.lit("Plant store"))
        .alias("location_type"),
        "company_code",
        F.col("plant").alias("plant_code"),
        F.col("storage_location").alias("storage_location_code"),
        _blank_to_null("sales_office").alias("sales_office_code"),
        F.col("region").alias("district_code"),
        "province",
        _blank_to_null("supplying_location_id").alias("supplying_location_id"),
    )


def build_fact_stock_movements(movements: DataFrame) -> DataFrame:
    t, q = F.col("movement_type"), F.col("quantity").cast(QTY)
    name = F.coalesce(*[F.when(t == code, F.lit(label)) for code, label in MOVEMENT_TYPE_NAMES.items()])
    return movements.select(
        "material_document",
        "material_document_item",
        "posting_date",
        "movement_type",
        name.alias("movement_type_name"),
        "location_id",
        "product_id",
        q.alias("quantity"),
        F.when(t == "601", -q).otherwise(F.lit(0)).cast(QTY).alias("issued_quantity"),
        F.when(t == "101", q).otherwise(F.lit(0)).cast(QTY).alias("received_quantity"),
        _blank_to_null("sales_office").alias("sales_office_code"),
        _blank_to_null("reference_invoice_number").alias("reference_invoice_number"),
        _blank_to_null("reference_invoice_item").alias("reference_invoice_item"),
        _blank_to_null("counterpart_location_id").alias("counterpart_location_id"),
        F.col("_business_date").alias("delivered_date"),
    )


def build_fact_stock_daily(stock: DataFrame, fact_movements: DataFrame, products: DataFrame) -> DataFrame:
    """Closing stock plus the day's issues, the 28-day average issue rate, days of cover and stock value."""
    issued = (
        fact_movements.groupBy(F.col("posting_date").alias("snapshot_date"), "location_id", "product_id")
        .agg(F.sum("issued_quantity").alias("_issued"))
        .filter(F.col("_issued") != 0)
    )
    cost = products.select("product_id", F.col("standard_cost_amount").alias("_unit_cost"))
    day_no = F.datediff(F.col("snapshot_date"), F.lit("2000-01-01").cast("date"))
    window = Window.partitionBy("location_id", "product_id").orderBy("_day").rangeBetween(-(COVER_WINDOW_DAYS - 1), 0)
    s = (
        stock.join(issued, ["snapshot_date", "location_id", "product_id"], "left")
        .join(F.broadcast(cost), "product_id", "left")
        .withColumn("_issued", F.coalesce(F.col("_issued"), F.lit(0)).cast(QTY))
        .withColumn("_day", day_no)
        .withColumn("_avg", (F.sum("_issued").over(window) / F.lit(COVER_WINDOW_DAYS)).cast(QTY))
    )
    qty, avg = F.col("unrestricted_qty").cast(QTY), F.col("_avg")
    return s.select(
        "snapshot_date",
        "location_id",
        "product_id",
        qty.alias("stock_quantity"),
        F.col("in_transit_qty").cast(QTY).alias("in_transit_quantity"),
        (qty * F.coalesce(F.col("_unit_cost"), F.lit(0))).cast(MONEY).alias("stock_value_amount"),
        F.col("_issued").alias("issued_quantity"),
        avg.alias("avg_daily_issued_quantity_28d"),
        F.when(avg > 0, qty / avg).cast("DECIMAL(18,1)").alias("days_of_cover"),
        ((qty == 0) & (avg > 0)).alias("is_stock_out"),
        ((qty > 0) & (avg > 0) & (qty < avg * F.lit(LOW_COVER_DAYS))).alias("is_low_cover"),
    )


def build_fact_sales_stock_daily(fact_sales: DataFrame, stock_daily: DataFrame, dim_location: DataFrame) -> DataFrame:
    """Sales and depot stock per day x sales office x product (a drill-across table: both facts summed to the
    grain they share, then joined, so no sales line is counted twice). Only days both areas cover are kept."""
    keys = ["activity_date", "sales_office_code", "product_id"]
    depots = dim_location.filter(F.col("location_type") == "Depot").select("location_id", "sales_office_code")
    sales = fact_sales.groupBy(F.col("invoice_date").alias("activity_date"), "sales_office_code", "product_id").agg(
        F.sum("net_revenue_amount").cast(MONEY).alias("_revenue"),
        F.sum("quantity").cast(QTY).alias("_sold"),
        F.sum("sold_quantity").cast(QTY).alias("_invoiced"),
    )
    stock = stock_daily.join(F.broadcast(depots), "location_id").select(
        F.col("snapshot_date").alias("activity_date"),
        "sales_office_code",
        "product_id",
        F.col("issued_quantity").alias("_issued"),
        "stock_quantity",
        "in_transit_quantity",
        "avg_daily_issued_quantity_28d",
        F.col("is_stock_out").alias("_out"),
    )
    bounds = fact_sales.agg(F.min("invoice_date").alias("_s0"), F.max("invoice_date").alias("_s1")).crossJoin(
        stock_daily.agg(F.min("snapshot_date").alias("_t0"), F.max("snapshot_date").alias("_t1"))
    )
    both = (
        sales.join(stock, keys, "full_outer")
        .crossJoin(F.broadcast(bounds))
        .filter(F.col("activity_date").between(F.greatest("_s0", "_t0"), F.least("_s1", "_t1")))
        .join(F.broadcast(depots), "sales_office_code", "left")
    )
    return both.select(
        "activity_date",
        "sales_office_code",
        "location_id",
        "product_id",
        F.coalesce(F.col("_revenue"), F.lit(0)).cast(MONEY).alias("net_revenue_amount"),
        F.coalesce(F.col("_sold"), F.lit(0)).cast(QTY).alias("sales_quantity"),
        F.coalesce(F.col("_invoiced"), F.lit(0)).cast(QTY).alias("invoiced_quantity"),
        F.coalesce(F.col("_issued"), F.lit(0)).cast(QTY).alias("issued_quantity"),
        "stock_quantity",
        "in_transit_quantity",
        "avg_daily_issued_quantity_28d",
        F.coalesce(F.col("_out"), F.lit(False)).alias("is_stock_out"),
    )


def write(lake: Lake, df: DataFrame, table: str) -> None:
    lake.overwrite(gm.with_comments(df, table, im.TABLES), "gold", table)
    lake.describe("gold", table, im.TABLES[table]["description"], im.TABLES[table]["columns"])


def run_inventory_gold(
    spark: SparkSession, cfg: Config, lake: Lake | None = None, sales_lake: Lake | None = None
) -> InventoryGoldRun:
    lake = lake or Lake(spark, cfg)
    sales_lake = sales_lake or sales_lake_for(spark, cfg)
    run = InventoryGoldRun()
    write(lake, build_dim_location(lake.read("silver", "locations")), "dim_location")
    write(lake, build_fact_stock_movements(lake.read("silver", "movements")), "fact_stock_movements")
    daily = build_fact_stock_daily(
        lake.read("silver", "stock"), lake.read("gold", "fact_stock_movements"), sales_lake.read("gold", "dim_product")
    )
    write(lake, daily, "fact_stock_daily")
    both = build_fact_sales_stock_daily(
        sales_lake.read("gold", "fact_sales"), lake.read("gold", "fact_stock_daily"), lake.read("gold", "dim_location")
    )
    write(lake, both, "fact_sales_stock_daily")
    for table in im.TABLES:
        run.rows[table] = lake.read("gold", table).count()
    return run


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    argparse.ArgumentParser(description="Rebuild the inventory gold tables from silver.").parse_args(argv)
    cfg = load_config(area="inventory")
    run = run_inventory_gold(get_spark(cfg, app_name="inventory-gold"), cfg)
    for table, rows in run.rows.items():
        print(f"    gold.{table:<22} {rows:,} rows")


if __name__ == "__main__":
    main()
