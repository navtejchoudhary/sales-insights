"""Inventory silver: clean, typed stock movements and daily stock from the inventory area's bronze tables.

    uv run python -m sales_insights.pipeline.inventory_silver

Rebuilt in full on every run, like sales silver. Reuses the sales silver helpers (newest load of each file,
latest version per key, try_cast). Reads the product master from the sales catalog (gold.dim_product, the
shared product dimension); writes only to the inventory area.

Tables written (inventory area):
  silver.locations                     latest version per location
  silver.movements                     deduped, validated, typed; base unit taken from the product master
  silver.stock                         deduped, validated, typed closing stock
  silver.quarantine                    rows that failed validation (both files), with reasons and the raw row
  ops.manifest_totals, ops.manifest_dirt  what the source promised
  ops.dq_results                       silver checks appended
"""

from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_insights.common.config import Config
from sales_insights.common.lake import Lake
from sales_insights.pipeline import bronze as b
from sales_insights.pipeline.manifests import DIRT_COLUMNS, INVENTORY_TOTALS_COLUMNS, parse_inventory_manifest
from sales_insights.pipeline.silver import META, latest, latest_file_versions, source_columns, try_cast

MOVEMENT_KEY = ["material_document", "material_document_item"]
STOCK_KEY = ["snapshot_date", "location_id", "product_id"]
MOVEMENT_TYPES = ["561", "101", "313", "315", "601", "602", "653", "701", "702"]
INVALID_DIRT = ["unknown_location", "blank_movement_type", "negative_stock"]


@dataclass
class InventorySilverRun:
    run_id: str
    rows: dict[str, int] = field(default_factory=dict)
    duplicates_removed: int = 0
    quarantined: int = 0
    units_fixed: int = 0
    dq_failures: int = 0


# ---------------------------------------------------------------------------
# Rules (DataFrame in, DataFrame out)
# ---------------------------------------------------------------------------


def clean_locations(raw: DataFrame) -> DataFrame:
    loc = latest(raw, ["location_id"], ["last_updated_timestamp", "_load_ts"])
    return loc.withColumn("last_updated_timestamp", try_cast("last_updated_timestamp", "TIMESTAMP")).drop(
        *[c for c in META if c in loc.columns]
    )


def dedupe(df: DataFrame, key: list[str]) -> DataFrame:
    """Newest load of each file, then one row per key (exact copies, re-sent files)."""
    return latest(latest_file_versions(df), key, ["_load_ts", "_source_file"])


def _known(df: DataFrame, ids: DataFrame, col: str, flag: str) -> DataFrame:
    known = ids.select(F.col(col).alias(f"_{flag}_id")).distinct().withColumn(flag, F.lit(True))
    return df.join(F.broadcast(known), df[col] == F.col(f"_{flag}_id"), "left").drop(f"_{flag}_id")


def validate_movements(mv: DataFrame, locations: DataFrame, products: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (valid, rejected); rejected rows carry a ';'-separated reason list."""
    d = (
        _known(_known(mv, locations, "location_id", "_loc_ok"), products, "product_id", "_prod_ok")
        .withColumn("_date", try_cast("posting_date", "DATE"))
        .withColumn("_qty", try_cast("quantity", "DECIMAL(18,3)"))
    )
    missing_key = F.col("material_document").isNull() | F.col("material_document_item").isNull()
    reasons = F.concat_ws(
        ";",
        F.when(missing_key, F.lit("missing_key")),
        F.when(F.col("_date").isNull(), F.lit("invalid_posting_date")),
        F.when(F.col("_qty").isNull(), F.lit("invalid_quantity")),
        F.when(
            F.col("movement_type").isNull() | ~F.col("movement_type").isin(MOVEMENT_TYPES),
            F.lit("unknown_movement_type"),
        ),
        F.when(F.col("_loc_ok").isNull(), F.lit("unknown_location")),
        F.when(F.col("_prod_ok").isNull(), F.lit("unknown_product")),
    )
    d = d.withColumn("_reasons", reasons).drop("_date", "_qty", "_loc_ok", "_prod_ok")
    return d.filter(F.col("_reasons") == "").drop("_reasons"), d.filter(F.col("_reasons") != "")


def validate_stock(st: DataFrame, locations: DataFrame, products: DataFrame) -> tuple[DataFrame, DataFrame]:
    d = (
        _known(_known(st, locations, "location_id", "_loc_ok"), products, "product_id", "_prod_ok")
        .withColumn("_date", try_cast("snapshot_date", "DATE"))
        .withColumn("_qty", try_cast("unrestricted_qty", "DECIMAL(18,3)"))
        .withColumn("_transit", try_cast("in_transit_qty", "DECIMAL(18,3)"))
    )
    reasons = F.concat_ws(
        ";",
        F.when(F.col("_date").isNull(), F.lit("invalid_snapshot_date")),
        F.when(F.col("_qty").isNull() | F.col("_transit").isNull(), F.lit("invalid_quantity")),
        F.when((F.col("_qty") < 0) | (F.col("_transit") < 0), F.lit("negative_stock")),
        F.when(F.col("_loc_ok").isNull(), F.lit("unknown_location")),
        F.when(F.col("_prod_ok").isNull(), F.lit("unknown_product")),
    )
    d = d.withColumn("_reasons", reasons).drop("_date", "_qty", "_transit", "_loc_ok", "_prod_ok")
    return d.filter(F.col("_reasons") == "").drop("_reasons"), d.filter(F.col("_reasons") != "")


def normalise_units(mv: DataFrame, products: DataFrame) -> DataFrame:
    """The unit is the product's base unit from the product master ('CS', 'case' -> CA). Keeps the received
    text in source_unit so the fix stays visible."""
    units = products.select("product_id", F.col("base_unit").alias("_master_unit"))
    return (
        mv.join(F.broadcast(units), "product_id", "left")
        .withColumn("source_unit", F.col("base_unit"))
        .withColumn("base_unit", F.coalesce(F.col("_master_unit"), F.col("base_unit")))
        .drop("_master_unit")
    )


def type_movements(valid: DataFrame) -> DataFrame:
    return (
        valid.withColumn("posting_date", try_cast("posting_date", "DATE"))
        .withColumn("quantity", try_cast("quantity", "DECIMAL(18,3)"))
        .withColumn("last_updated_timestamp", try_cast("last_updated_timestamp", "TIMESTAMP"))
        .withColumn("_business_date", try_cast("_business_date", "DATE"))
    )


def type_stock(valid: DataFrame) -> DataFrame:
    return (
        valid.withColumn("snapshot_date", try_cast("snapshot_date", "DATE"))
        .withColumn("unrestricted_qty", try_cast("unrestricted_qty", "DECIMAL(18,3)"))
        .withColumn("in_transit_qty", try_cast("in_transit_qty", "DECIMAL(18,3)"))
        .withColumn("last_updated_timestamp", try_cast("last_updated_timestamp", "TIMESTAMP"))
        .withColumn("_business_date", try_cast("_business_date", "DATE"))
    )


def quarantine_frame(rejected: DataFrame, file_kind: str, key: list[str], run_id: str) -> DataFrame:
    """Rejected rows of either file in one shape: record_key is the manifest's key format (doc/item, date/loc/product)."""
    src = source_columns(rejected)
    return rejected.select(
        F.lit(file_kind).alias("file_kind"),
        F.concat_ws("/", *[F.col(c) for c in key]).alias("record_key"),
        F.col("product_id"),
        F.col("location_id"),
        F.col("posting_date" if file_kind == "movements" else "snapshot_date").alias("data_date"),
        F.col("_reasons").alias("reasons"),
        "_source_file",
        F.col("_business_date").alias("business_date"),
        "_load_ts",
        F.to_json(F.struct(*[F.col(f"`{c}`") for c in src])).alias("raw_row"),
        F.current_timestamp().alias("quarantined_ts"),
        F.lit(run_id).alias("run_id"),
    )


def manifest_frames(spark: SparkSession, manifests: DataFrame) -> tuple[DataFrame, DataFrame]:
    newest = latest(manifests, ["manifest_path"], ["_load_ts"]).select("manifest_path", "content").collect()
    totals, dirt = [], []
    for r in newest:
        t, d = parse_inventory_manifest(r["manifest_path"], r["content"])
        totals += t
        dirt += d
    totals_schema = (
        "manifest_path string, kind string, business_date string, file_kind string, data_date string, "
        "lines long, quantity double, in_transit_quantity double"
    )
    assert len(INVENTORY_TOTALS_COLUMNS) == 8
    dirt_schema = ", ".join(f"{c} string" for c in DIRT_COLUMNS)
    return spark.createDataFrame(totals, totals_schema), spark.createDataFrame(dirt, dirt_schema)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


def sales_lake_for(spark: SparkSession, cfg: Config) -> Lake:
    """The sales area's tables (shared dimensions), read only."""
    return Lake(spark, cfg.for_area("sales"))


def run_inventory_silver(
    spark: SparkSession, cfg: Config, lake: Lake | None = None, sales_lake: Lake | None = None
) -> InventorySilverRun:
    lake = lake or Lake(spark, cfg)
    sales_lake = sales_lake or sales_lake_for(spark, cfg)
    run = InventorySilverRun(run_id=f"inv-silver-{datetime.now(UTC):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}")
    now = datetime.now(UTC)
    dq: list[tuple] = []

    def check(name: str, target: str, expected, actual, ok: bool) -> None:
        dq.append(
            (run.run_id, now, "silver", name, target, None, None, str(expected), str(actual), "PASS" if ok else "FAIL")
        )

    products = sales_lake.read("gold", "dim_product").select("product_id", "base_unit")

    locations = clean_locations(lake.read("bronze", "masters_locations"))
    lake.overwrite(locations, "silver", "locations")
    locations = lake.read("silver", "locations")
    run.rows["locations"] = locations.count()
    distinct = locations.select("location_id").distinct().count()
    check("location_ids_unique", "silver.locations", run.rows["locations"], distinct, run.rows["locations"] == distinct)

    quarantined = []
    raw_total = 0
    for file_kind, key, validate, type_rows in (
        ("movements", MOVEMENT_KEY, validate_movements, type_movements),
        ("stock", STOCK_KEY, validate_stock, type_stock),
    ):
        raw = lake.read("bronze", file_kind)
        raw_total += raw.count()
        valid, rejected = validate(dedupe(raw, key), locations, products)
        if file_kind == "movements":
            valid = normalise_units(valid, products)
        lake.overwrite(type_rows(valid).drop("_run_id", "_rescued_data"), "silver", file_kind)
        quarantined.append(quarantine_frame(rejected, file_kind, key, run.run_id))
    quarantine = quarantined[0].unionByName(quarantined[1])
    lake.overwrite(quarantine, "silver", "quarantine")

    mv, st = lake.read("silver", "movements"), lake.read("silver", "stock")
    run.rows["movements"], run.rows["stock"] = mv.count(), st.count()
    run.quarantined = lake.read("silver", "quarantine").count()
    run.duplicates_removed = raw_total - run.rows["movements"] - run.rows["stock"] - run.quarantined
    run.units_fixed = mv.filter(F.col("source_unit") != F.col("base_unit")).count()

    for name, df, key in (("movements", mv, MOVEMENT_KEY), ("stock", st, STOCK_KEY)):
        dups = df.count() - df.select(*key).distinct().count()
        check(f"{name}_keys_unique", f"silver.{name}", 0, dups, dups == 0)
    unknown_units = mv.join(products, "product_id", "left_anti").count()
    check("movement_products_known", "silver.movements", 0, unknown_units, unknown_units == 0)

    if lake.exists("bronze", "manifests"):
        totals, dirt = manifest_frames(spark, lake.read("bronze", "manifests"))
        lake.overwrite(totals, "ops", "manifest_totals")
        lake.overwrite(dirt, "ops", "manifest_dirt")
        expected = {
            (r["business_date"], r["file"], r["key"])
            for r in lake.read("ops", "manifest_dirt").filter(F.col("dirt").isin(INVALID_DIRT)).collect()
        }
        actual = {
            (str(r["business_date"]), r["file_kind"], r["record_key"])
            for r in lake.read("silver", "quarantine").collect()
        }
        check("quarantine_matches_manifest", "silver.quarantine", len(expected), len(actual), expected == actual)

    lake.append(spark.createDataFrame(dq, b.DQ_SCHEMA), *b.DQ_RESULTS)
    run.dq_failures = sum(1 for r in dq if r[-1] == "FAIL")
    return run


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    argparse.ArgumentParser(description="Rebuild the inventory silver tables from bronze.").parse_args(argv)
    cfg = load_config(area="inventory")
    run = run_inventory_silver(get_spark(cfg, app_name="inventory-silver"), cfg)
    print(f"  run {run.run_id}")
    for name, rows in sorted(run.rows.items()):
        print(f"    silver.{name:<12} {rows:,} rows")
    print(
        f"  duplicates removed {run.duplicates_removed}, quarantined {run.quarantined}, units fixed {run.units_fixed}"
    )
    print(f"  data-quality failures: {run.dq_failures}" + ("  <-- check ops.dq_results" if run.dq_failures else ""))


if __name__ == "__main__":
    main()
