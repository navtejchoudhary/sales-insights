"""Silver: turn raw bronze rows into clean, typed, trustworthy tables.

    uv run python -m sales_insights.pipeline.silver

Rebuilt in full on every run (deterministic and idempotent; ~45k lines take seconds).
Switch to incremental MERGE only if volumes grow by orders of magnitude.

Tables written:
  silver.customers, silver.products     latest version per key, cleaned, gaps filled
  silver.<lookup>                       latest delivery of each lookup master
  silver.invoice_lines                  history + orders + changes: deduped, typed, enriched
  silver.quarantine                     rows that failed validation, with reasons and the raw row
  ops.manifest_totals, ops.manifest_dirt  what the source promised (for proving correctness)
  ops.dq_results                        silver checks appended

Every rule below is a small DataFrame -> DataFrame function, tested on its own.
"""

from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_insights.common.config import Config
from sales_insights.common.lake import Lake
from sales_insights.pipeline import bronze as b
from sales_insights.pipeline.manifests import DIRT_COLUMNS, TOTALS_COLUMNS, parse_manifest

LINE_KEY = ["invoice_number", "invoice_item"]
INVOICE_TYPES = ["ZAOR", "ZFOC", "ZARE", "S1", "ZACR"]
SALES_TYPES = ["ZAOR", "ZFOC"]
INVALID_DIRT = ["invalid_date", "negative_quantity_on_invoice"]
UNASSIGNED = "Unassigned"
META = ["_source_file", "_business_date", "_load_ts", "_run_id", "_rescued_data"]

MONEY_2 = [
    "revenue",
    "gross_revenue",
    "tax_amount",
    "profit",
    "net_sales",
    "sales_amount",
    "credit_amount",
    "netamount_lkr",
    "condition_amount",
    "condition_amount_ccy",
    "sales_order_condition_amount",
    "sales_order_condition_amount_ccy",
]
QTY_3 = ["quantity", "gross_weight", "net_weight", "cumulative_order_qty_sales_unit"]
RATE_6 = ["condition_rate"]
INTS = ["fiscal_year", "fiscal_period"]

LOOKUPS = [
    "regions",
    "cities",
    "companies",
    "plants",
    "storage_locations",
    "distribution_channels",
    "divisions",
    "customer_groups",
    "sales_offices",
    "sales_reps",
    "product_groups",
    "product_types",
]


@dataclass
class SilverRun:
    run_id: str
    rows: dict[str, int] = field(default_factory=dict)
    duplicates_removed: int = 0
    quarantined: int = 0
    unmapped_cities: int = 0
    dq_failures: int = 0


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def try_cast(col: str, sql_type: str) -> Column:
    """Cast that returns NULL instead of failing (Spark ANSI mode is on by default in Spark 4)."""
    return F.expr(f"try_cast(`{col}` AS {sql_type})")


def city_key(col: Column) -> Column:
    """Normalise a city spelling: COLOMBO - 10 / Colombo 10 / COLOMBO  1. -> COLOMBO 10 / COLOMBO 01."""
    c = F.upper(col)
    c = F.regexp_replace(c, r"\.", "")
    c = F.regexp_replace(c, r"\s*-\s*", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)
    return F.regexp_replace(c, r"\b(\d)\b", "0$1")


def latest(df: DataFrame, keys: list[str], order_desc: list[str]) -> DataFrame:
    """Keep one row per key: the first by the given columns, newest first."""
    w = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_desc])
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def latest_delivery(raw: DataFrame) -> DataFrame:
    """Lookup masters are full snapshots: keep only the newest delivery, drop load metadata."""
    newest = raw.agg(F.max("_load_ts")).collect()[0][0]
    return raw.filter(F.col("_load_ts") == newest).drop(*[c for c in META if c in raw.columns])


def source_columns(df: DataFrame) -> list[str]:
    return [c for c in df.columns if not c.startswith("_")]


# ---------------------------------------------------------------------------
# Masters
# ---------------------------------------------------------------------------


def clean_customers(raw: DataFrame, cities: DataFrame, regions: DataFrame) -> DataFrame:
    c = (
        raw.withColumn("customer_id", F.lpad(F.trim("customer_id"), 10, "0"))
        .withColumn("customer_name", F.trim("customer_name"))
        .withColumn("customer_full_name", F.trim("customer_full_name"))
    )
    c = latest(c, ["customer_id"], ["last_updated_timestamp", "_load_ts"])
    city_map = cities.select(
        city_key(F.col("city")).alias("_city_key"),
        F.col("city").alias("_canonical_city"),
        F.col("region").alias("_city_region"),
    )
    c = c.withColumn("_city_key", city_key(F.col("city"))).join(F.broadcast(city_map), "_city_key", "left")
    c = (
        c.withColumn("city_mapped", F.col("_canonical_city").isNotNull())
        .withColumn("city", F.coalesce(F.col("_canonical_city"), F.col("_city_key")))
        .withColumn("region", F.coalesce(F.col("region"), F.col("_city_region")))
        .withColumn("customer_group_name", F.coalesce(F.col("customer_group_name"), F.lit(UNASSIGNED)))
    )
    c = c.join(F.broadcast(regions.select("region", "district_name", "province")), "region", "left")
    return (
        c.withColumn("created_date", try_cast("created_date", "DATE"))
        .withColumn("last_updated_timestamp", try_cast("last_updated_timestamp", "TIMESTAMP"))
        .drop("_city_key", "_canonical_city", "_city_region", *[x for x in META if x in c.columns])
    )


def product_groups_seen_in_lines(lines: DataFrame) -> DataFrame:
    """Most common product group per product in invoice lines (fills gaps in the product master)."""
    counts = (
        lines.filter(F.col("product_group").isNotNull())
        .groupBy("product_id", "product_group", "product_group_name")
        .count()
    )
    return latest(counts, ["product_id"], ["count", "product_group"]).select(
        "product_id", F.col("product_group").alias("_seen_group"), F.col("product_group_name").alias("_seen_group_name")
    )


def clean_products(raw: DataFrame, seen_groups: DataFrame) -> DataFrame:
    p = latest(raw, ["product_id"], ["last_updated_timestamp", "_load_ts"])
    p = p.withColumn("product_description", F.upper(F.trim("product_description")))
    p = p.join(seen_groups, "product_id", "left")
    p = (
        p.withColumn("product_group", F.coalesce(F.col("product_group"), F.col("_seen_group")))
        .withColumn(
            "product_group_name", F.coalesce(F.col("product_group_name"), F.col("_seen_group_name"), F.lit(UNASSIGNED))
        )
        .drop("_seen_group", "_seen_group_name")
    )
    for col in ("list_price", "standard_cost"):
        p = p.withColumn(col, try_cast(col, "DECIMAL(18,2)"))
    for col in ("gross_weight", "net_weight"):
        p = p.withColumn(col, try_cast(col, "DECIMAL(18,3)"))
    return (
        p.withColumn("launch_date", try_cast("launch_date", "DATE"))
        .withColumn("last_updated_timestamp", try_cast("last_updated_timestamp", "TIMESTAMP"))
        .drop(*[x for x in META if x in p.columns])
    )


# ---------------------------------------------------------------------------
# Invoice lines
# ---------------------------------------------------------------------------


def unify(*frames: DataFrame) -> DataFrame:
    """History, orders and changes into one frame (missing columns become NULL)."""
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f, allowMissingColumns=True)
    return out


def latest_file_versions(lines: DataFrame) -> DataFrame:
    """A file the source re-sent with new content replaces its earlier load completely.

    Bronze keeps both loads (it is append-only); silver uses only the newest load of each file,
    so a row the source REMOVED from a corrected file disappears too.
    """
    w = Window.partitionBy("_source_file")
    return (
        lines.withColumn("_newest", F.max("_load_ts").over(w))
        .filter(F.col("_load_ts") == F.col("_newest"))
        .drop("_newest")
    )


def dedupe(lines: DataFrame) -> DataFrame:
    """One row per invoice line: newest load of each file, then one copy per key (duplicates, late copies)."""
    return latest(latest_file_versions(lines), LINE_KEY, ["_load_ts", "_source_file"])


def validate(lines: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (valid, rejected). Rejected rows carry a ';'-separated reason list."""
    d = (
        lines.withColumn("_date", try_cast("invoice_date", "DATE"))
        .withColumn("_qty", try_cast("quantity", "DECIMAL(18,3)"))
        .withColumn("_rev", try_cast("revenue", "DECIMAL(18,2)"))
    )
    missing_key = (
        F.col("invoice_number").isNull()
        | F.col("invoice_item").isNull()
        | F.col("customer_id").isNull()
        | F.col("product_id").isNull()
    )
    reasons = F.concat_ws(
        ";",
        F.when(missing_key, F.lit("missing_key")),
        F.when(F.col("_date").isNull(), F.lit("invalid_invoice_date")),
        F.when(F.col("_qty").isNull(), F.lit("invalid_quantity")),
        F.when(F.col("_rev").isNull(), F.lit("invalid_revenue")),
        F.when(
            F.col("invoice_type").isNull() | ~F.col("invoice_type").isin(INVOICE_TYPES), F.lit("unknown_invoice_type")
        ),
        F.when(F.col("invoice_type").isin(SALES_TYPES) & (F.col("_qty") < 0), F.lit("negative_quantity_on_invoice")),
    )
    d = d.withColumn("_reasons", reasons).drop("_date", "_qty", "_rev")
    valid = d.filter(F.col("_reasons") == "").drop("_reasons")
    rejected = d.filter(F.col("_reasons") != "")
    return valid, rejected


def quarantine_frame(rejected: DataFrame, run_id: str) -> DataFrame:
    src = source_columns(rejected)
    return rejected.select(
        "invoice_number",
        "invoice_item",
        F.col("_reasons").alias("reasons"),
        "_source_file",
        F.col("_business_date").alias("business_date"),
        "_load_ts",
        F.to_json(F.struct(*[F.col(f"`{c}`") for c in src])).alias("raw_row"),
        F.current_timestamp().alias("quarantined_ts"),
        F.lit(run_id).alias("run_id"),
    )


def type_lines(valid: DataFrame) -> DataFrame:
    t = valid.withColumn("invoice_date", try_cast("invoice_date", "DATE")).withColumn(
        "last_updated_timestamp", try_cast("last_updated_timestamp", "TIMESTAMP")
    )
    for cols, sql_type in (
        (MONEY_2, "DECIMAL(18,2)"),
        (QTY_3, "DECIMAL(18,3)"),
        (RATE_6, "DECIMAL(18,6)"),
        (INTS, "INT"),
    ):
        for col in cols:
            t = t.withColumn(col, try_cast(col, sql_type))
    return t.withColumn("_business_date", try_cast("_business_date", "DATE"))


def enrich(lines: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    """Fill blank codes from master data; take city/region from the customer master."""
    cust = customers.select(
        "customer_id",
        F.col("city").alias("_c_city"),
        F.col("region").alias("_c_region"),
        F.col("province"),
        F.col("sales_office").alias("_c_office"),
        F.col("customer_group").alias("_c_group"),
        F.col("customer_group_name").alias("_c_group_name"),
    )
    prod = products.select(
        "product_id",
        F.col("product_group").alias("_p_group"),
        F.col("product_group_name").alias("_p_group_name"),
        F.col("plant").alias("_p_plant"),
    )
    e = lines.withColumn("customer_id", F.lpad(F.trim("customer_id"), 10, "0"))
    e = e.join(F.broadcast(cust), "customer_id", "left").join(F.broadcast(prod), "product_id", "left")
    e = (
        e.withColumn("city", F.coalesce(F.col("_c_city"), city_key(F.col("city"))))
        .withColumn("region", F.coalesce(F.col("_c_region"), F.col("region")))
        .withColumn("sales_office", F.coalesce(F.col("sales_office"), F.col("_c_office")))
        .withColumn("customer_group", F.coalesce(F.col("customer_group"), F.col("_c_group")))
        .withColumn(
            "customer_group_name", F.coalesce(F.col("customer_group_name"), F.col("_c_group_name"), F.lit(UNASSIGNED))
        )
        .withColumn("product_group", F.coalesce(F.col("product_group"), F.col("_p_group")))
        .withColumn("product_group_name", F.coalesce(F.col("product_group_name"), F.col("_p_group_name")))
        .withColumn("plant", F.coalesce(F.col("plant"), F.col("_p_plant")))
    )
    return e.drop(
        "_c_city", "_c_region", "_c_office", "_c_group", "_c_group_name", "_p_group", "_p_group_name", "_p_plant"
    )


def flag_cancellations(lines: DataFrame) -> DataFrame:
    """Mark sales lines whose invoice was later cancelled (S1). Revenue still nets via the S1 rows."""
    cancels = (
        lines.filter(F.col("invoice_type") == "S1")
        .select(
            F.col("reference_invoice_number").alias("invoice_number"), F.col("invoice_number").alias("cancelled_by")
        )
        .dropDuplicates(["invoice_number"])
    )
    out = lines.join(cancels, "invoice_number", "left")
    return out.withColumn(
        "is_cancelled", F.col("invoice_type").isin(SALES_TYPES) & F.col("cancelled_by").isNotNull()
    ).withColumn("cancelled_by", F.when(F.col("is_cancelled"), F.col("cancelled_by")))


def add_arrival_delay(lines: DataFrame) -> DataFrame:
    """Days between invoice date and arrival in the feed (NULL for history)."""
    return lines.withColumn("arrival_delay_days", F.datediff(F.col("_business_date"), F.col("invoice_date")))


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


def manifest_frames(spark: SparkSession, manifests: DataFrame) -> tuple[DataFrame, DataFrame]:
    newest = latest(manifests, ["manifest_path"], ["_load_ts"]).select("manifest_path", "content").collect()
    totals, dirt = [], []
    for r in newest:
        t, d = parse_manifest(r["manifest_path"], r["content"])
        totals += t
        dirt += d
    totals_schema = (
        "manifest_path string, kind string, business_date string, file_kind string, invoice_date string, "
        "lines long, invoices long, revenue double, tax_amount double"
    )
    dirt_schema = ", ".join(f"{c} string" for c in DIRT_COLUMNS)
    assert len(TOTALS_COLUMNS) == 9
    return spark.createDataFrame(totals, totals_schema), spark.createDataFrame(dirt, dirt_schema)


def run_silver(spark: SparkSession, cfg: Config, lake: Lake | None = None) -> SilverRun:
    lake = lake or Lake(spark, cfg)
    run = SilverRun(run_id=f"silver-{datetime.now(UTC):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}")
    now = datetime.now(UTC)
    dq: list[tuple] = []

    def check(name: str, target: str, expected, actual, ok: bool) -> None:
        dq.append(
            (run.run_id, now, "silver", name, target, None, None, str(expected), str(actual), "PASS" if ok else "FAIL")
        )

    # Lookups
    for name in LOOKUPS:
        if lake.exists("bronze", f"masters_{name}"):
            df = latest_delivery(lake.read("bronze", f"masters_{name}"))
            lake.overwrite(df, "silver", name)
            run.rows[name] = df.count()
    cities, regions = lake.read("silver", "cities"), lake.read("silver", "regions")

    # Invoice lines: unify every bronze source that exists
    sources = [lake.read("bronze", t) for t in ("invoices_history", "orders", "changes") if lake.exists("bronze", t)]
    raw_lines = unify(*sources)

    # Customers and products
    customers = clean_customers(lake.read("bronze", "masters_customers"), cities, regions)
    lake.overwrite(customers, "silver", "customers")
    customers = lake.read("silver", "customers")
    run.unmapped_cities = customers.filter(~F.col("city_mapped")).count()
    run.rows["customers"] = customers.count()
    check(
        "customer_ids_unique",
        "silver.customers",
        run.rows["customers"],
        customers.select("customer_id").distinct().count(),
        run.rows["customers"] == customers.select("customer_id").distinct().count(),
    )
    check("customer_cities_mapped", "silver.customers", 0, run.unmapped_cities, run.unmapped_cities == 0)

    products = clean_products(lake.read("bronze", "masters_products"), product_groups_seen_in_lines(raw_lines))
    lake.overwrite(products, "silver", "products")
    products = lake.read("silver", "products")
    run.rows["products"] = products.count()
    unassigned = products.filter(F.col("product_group").isNull()).count()
    check("product_groups_filled", "silver.products", 0, unassigned, unassigned == 0)

    # Lines: dedupe -> validate -> type -> enrich -> flag
    total = raw_lines.count()
    deduped = dedupe(raw_lines)
    valid, rejected = validate(deduped)
    lines = add_arrival_delay(flag_cancellations(enrich(type_lines(valid), customers, products)))
    lake.overwrite(lines.drop("_run_id", "_rescued_data"), "silver", "invoice_lines")
    lines = lake.read("silver", "invoice_lines")
    run.rows["invoice_lines"] = lines.count()

    quarantine = quarantine_frame(rejected, run.run_id)
    lake.overwrite(quarantine, "silver", "quarantine")
    run.quarantined = lake.read("silver", "quarantine").count()
    run.duplicates_removed = total - run.rows["invoice_lines"] - run.quarantined

    # Manifests: what the source promised
    if lake.exists("bronze", "manifests"):
        totals, dirt = manifest_frames(spark, lake.read("bronze", "manifests"))
        lake.overwrite(totals, "ops", "manifest_totals")
        lake.overwrite(dirt, "ops", "manifest_dirt")
        expected = {
            (r["business_date"], r["key"])
            for r in lake.read("ops", "manifest_dirt")
            .filter((F.col("file") == "orders") & F.col("dirt").isin(INVALID_DIRT))
            .collect()
        }
        actual = {
            (str(r["business_date"]), f"{r['invoice_number']}/{r['invoice_item']}")
            for r in lake.read("silver", "quarantine").collect()
        }
        check("quarantine_matches_manifest", "silver.quarantine", len(expected), len(actual), expected == actual)

    # Required fields after filling
    for col in ("sales_office", "plant", "product_group", "customer_group_name", "invoice_date", "region"):
        nulls = lines.filter(F.col(col).isNull()).count()
        check(f"no_blank_{col}", "silver.invoice_lines", 0, nulls, nulls == 0)
    dup_keys = run.rows["invoice_lines"] - lines.select(*LINE_KEY).distinct().count()
    check("line_keys_unique", "silver.invoice_lines", 0, dup_keys, dup_keys == 0)

    lake.append(spark.createDataFrame(dq, b.DQ_SCHEMA), *b.DQ_RESULTS)
    run.dq_failures = sum(1 for r in dq if r[-1] == "FAIL")
    return run


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    argparse.ArgumentParser(description="Rebuild silver tables from bronze.").parse_args(argv)
    cfg = load_config()
    spark = get_spark(cfg, app_name="silver")
    run = run_silver(spark, cfg)
    print(f"  run {run.run_id}")
    for name, rows in sorted(run.rows.items()):
        print(f"    silver.{name:<24} {rows:,} rows")
    print(
        f"  duplicates removed {run.duplicates_removed}, quarantined {run.quarantined}, "
        f"unmapped cities {run.unmapped_cities}"
    )
    print(f"  data-quality failures: {run.dq_failures}" + ("  <-- check ops.dq_results" if run.dq_failures else ""))


if __name__ == "__main__":
    main()
