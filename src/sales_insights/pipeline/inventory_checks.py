"""Inventory checks: prove the stock tables add up, against the source and against sales.

    uv run python -m sales_insights.pipeline.inventory_checks      # exit code 1 if anything fails

1. Reconciliation with the source: per posting date, gold movements (lines, quantity) = the sum of what every
   delivery's manifest reported for that date; per snapshot date, gold stock (rows, quantity, in transit) = the
   manifests. Same idea as sales reconciliation; the manifests already leave out planted dirt.
2. Stock roll-forward: for every location x product x day, closing stock = yesterday's closing stock + that
   day's movements. The first snapshot day is the baseline. Days whose late lines may still arrive (the last
   MAX_LATE_DAYS) wait; a position whose movement was quarantined counts as explained, not failed.
3. Cross-area check with sales: every goods movement for a sale (601 issue, 602 reversal, 653 return) matches
   its invoice line in the sales gold (same date, product, quantity), and every sales line of a stocked product
   has its movement. Only dates older than SETTLE_DAYS are checked (late invoices may still arrive); lines
   quarantined on either side count as explained.

Results: ops.reconciliation, ops.stock_rollforward, ops.cross_checks (one row per date, appended every run)
and one summary row per check in ops.dq_results.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_insights.common.config import Config
from sales_insights.common.lake import Lake
from sales_insights.pipeline import bronze as b
from sales_insights.pipeline.inventory_silver import sales_lake_for

MAX_LATE_DAYS = 2  # a movement can reach the feed up to 2 days after its posting date
SETTLE_DAYS = 7  # a sales invoice can reach the feed up to 7 days after its date
SALES_DOCS = ["ZAOR", "ZFOC", "ZARE", "S1"]
SALES_MOVES = ["601", "602", "653"]
QTY_SLACK = 0.0005


@dataclass
class CheckResult:
    name: str
    groups: int = 0
    failed: int = 0
    explained: int = 0
    waiting: int = 0
    first_failures: list[dict] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.failed == 0


@dataclass
class InventoryChecks:
    run_id: str
    results: dict[str, CheckResult] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return all(r.ok for r in self.results.values())


def _within(e: Column, a: Column, tol: float, slack: float) -> Column:
    return F.abs(a - e) <= F.abs(e) * F.lit(tol) + F.lit(slack)


# ---------------------------------------------------------------------------
# 1. Reconciliation with the source
# ---------------------------------------------------------------------------


def expected_totals(manifest_totals: DataFrame) -> DataFrame:
    delivery = F.coalesce(F.col("business_date"), F.lit("history"))
    return manifest_totals.groupBy("file_kind", "data_date").agg(
        F.sum("lines").cast("long").alias("expected_lines"),
        F.sum("quantity").cast("double").alias("expected_quantity"),
        F.sum("in_transit_quantity").cast("double").alias("expected_in_transit"),
        F.concat_ws(", ", F.array_sort(F.collect_set(delivery))).alias("deliveries"),
    )


def actual_totals(movements: DataFrame, stock: DataFrame) -> DataFrame:
    mv = (
        movements.groupBy(F.col("posting_date").cast("string").alias("data_date"))
        .agg(F.count(F.lit(1)).alias("actual_lines"), F.sum("quantity").cast("double").alias("actual_quantity"))
        .withColumn("actual_in_transit", F.lit(0.0))
    )
    st = stock.groupBy(F.col("snapshot_date").cast("string").alias("data_date")).agg(
        F.count(F.lit(1)).alias("actual_lines"),
        F.sum("stock_quantity").cast("double").alias("actual_quantity"),
        F.sum("in_transit_quantity").cast("double").alias("actual_in_transit"),
    )
    return mv.withColumn("file_kind", F.lit("movements")).unionByName(st.withColumn("file_kind", F.lit("stock")))


def compare_totals(expected: DataFrame, actual: DataFrame, tolerance_pct: float) -> DataFrame:
    """Full outer join: a date missing on either side counts as 0 there, so it fails."""
    j = expected.join(actual, ["file_kind", "data_date"], "full_outer")
    for m in ("lines", "quantity", "in_transit"):
        j = j.withColumn(f"expected_{m}", F.coalesce(f"expected_{m}", F.lit(0))).withColumn(
            f"actual_{m}", F.coalesce(f"actual_{m}", F.lit(0))
        )
    tol = tolerance_pct / 100.0
    ok = (
        (F.col("expected_lines") == F.col("actual_lines"))
        & _within(F.col("expected_quantity"), F.col("actual_quantity"), tol, QTY_SLACK)
        & _within(F.col("expected_in_transit"), F.col("actual_in_transit"), tol, QTY_SLACK)
    )
    return j.withColumn("status", F.when(ok, F.lit("PASS")).otherwise(F.lit("FAIL")))


# ---------------------------------------------------------------------------
# 2. Stock roll-forward
# ---------------------------------------------------------------------------


def rollforward(stock: DataFrame, movements: DataFrame, quarantine: DataFrame, last_delivery: date) -> DataFrame:
    """One row per location x product x day: closing stock vs the previous closing stock + the movements since.

    "Previous" is the position's latest earlier snapshot, so a quarantined stock row or a missed delivery
    simply widens the interval instead of breaking the check.
    """
    pos = ["location_id", "product_id"]
    w = Window.partitionBy(*pos).orderBy("snapshot_date")
    snaps = (
        stock.select(*pos, "snapshot_date", "stock_quantity")
        .withColumn("_prev_date", F.lag("snapshot_date").over(w))
        .withColumn("_prev", F.lag("stock_quantity").over(w))
    )
    # Running total of movements per position over every snapshot and posting date (equi-joins only, so it
    # scales): movements since the previous snapshot = running total now - running total at the previous snapshot.
    events = (
        snaps.select(*pos, F.col("snapshot_date").alias("_d"), F.lit(0).cast("decimal(18,3)").alias("_q"))
        .unionByName(movements.select(*pos, F.col("posting_date").alias("_d"), F.col("quantity").alias("_q")))
        .groupBy(*pos, "_d")
        .agg(F.sum("_q").alias("_q"))
    )
    running = Window.partitionBy(*pos).orderBy("_d").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    cum = events.withColumn("_cum", F.sum("_q").over(running)).drop("_q")
    now_cum = cum.select(*pos, F.col("_d").alias("snapshot_date"), F.col("_cum").alias("_cum_now"))
    prev_cum = cum.select(*pos, F.col("_d").alias("_prev_date"), F.col("_cum").alias("_cum_prev"))
    since = (
        snaps.join(now_cum, [*pos, "snapshot_date"], "left")
        .join(prev_cum, [*pos, "_prev_date"], "left")
        .select(
            *pos,
            "snapshot_date",
            (F.coalesce("_cum_now", F.lit(0)) - F.coalesce("_cum_prev", F.lit(0))).alias("_moved_since"),
        )
    )
    q = quarantine.filter(F.col("file_kind") == "movements").select(
        "product_id", F.expr("try_cast(data_date AS DATE)").alias("_qdate")
    )
    first_day = stock.agg(F.min("snapshot_date")).collect()[0][0]
    settled = F.lit(last_delivery - timedelta(days=MAX_LATE_DAYS)).cast("date")
    r = snaps.filter(F.col("snapshot_date") > F.lit(first_day)).join(since, [*pos, "snapshot_date"], "left")
    explained = (
        r.join(q, "product_id")
        .filter(
            (F.col("_qdate") <= F.col("snapshot_date"))
            & (F.col("_prev_date").isNull() | (F.col("_qdate") > F.col("_prev_date")))
        )
        .select(*pos, "snapshot_date")
        .distinct()
        .withColumn("_explained", F.lit(True))
    )
    r = (
        r.join(explained, [*pos, "snapshot_date"], "left")
        .withColumn("expected_quantity", F.coalesce("_prev", F.lit(0)) + F.coalesce("_moved_since", F.lit(0)))
        .withColumn("difference", F.col("stock_quantity") - F.col("expected_quantity"))
    )
    status = (
        F.when(F.col("snapshot_date") > settled, F.lit("WAIT"))
        .when(F.abs(F.col("difference")) <= F.lit(QTY_SLACK), F.lit("PASS"))
        .when(F.col("_explained").isNotNull(), F.lit("EXPLAINED"))
        .otherwise(F.lit("FAIL"))
    )
    return r.select(*pos, "snapshot_date", "stock_quantity", "expected_quantity", "difference", status.alias("status"))


# ---------------------------------------------------------------------------
# 3. Cross-area check with sales
# ---------------------------------------------------------------------------


def cross_check_lines(
    fact_sales: DataFrame,
    dim_product: DataFrame,
    movements: DataFrame,
    sales_quarantine: DataFrame,
    inventory_quarantine: DataFrame,
    first: date,
    cutoff: date,
) -> DataFrame:
    """One row per invoice line of a stocked product (either side) dated first..cutoff, with a status."""
    stocked = dim_product.filter(F.col("product_type_code") != "ZSRV").select("product_id")
    sales = (
        fact_sales.filter(F.col("invoice_type").isin(SALES_DOCS))
        .join(F.broadcast(stocked), "product_id", "left_semi")
        .select(
            "invoice_number",
            "invoice_item",
            F.col("invoice_date").alias("_s_date"),
            F.col("product_id").alias("_s_product"),
            F.col("quantity").alias("_s_qty"),
        )
    )
    inv = movements.filter(F.col("movement_type").isin(SALES_MOVES)).select(
        F.col("reference_invoice_number").alias("invoice_number"),
        F.col("reference_invoice_item").alias("invoice_item"),
        F.col("posting_date").alias("_m_date"),
        F.col("product_id").alias("_m_product"),
        F.col("quantity").alias("_m_qty"),
    )
    lost_sales = sales_quarantine.select("invoice_number", "invoice_item").distinct().withColumn("_s_lost", F.lit(True))
    lost_moves = (
        inventory_quarantine.filter(F.col("file_kind") == "movements")
        .select(
            F.get_json_object("raw_row", "$.reference_invoice_number").alias("invoice_number"),
            F.get_json_object("raw_row", "$.reference_invoice_item").alias("invoice_item"),
        )
        .filter(F.col("invoice_number").isNotNull())
        .distinct()
        .withColumn("_m_lost", F.lit(True))
    )
    keys = ["invoice_number", "invoice_item"]
    j = (
        sales.join(inv, keys, "full_outer")
        .join(F.broadcast(lost_sales), keys, "left")
        .join(F.broadcast(lost_moves), keys, "left")
        .withColumn("check_date", F.coalesce("_s_date", "_m_date"))
        .filter(F.col("check_date").between(F.lit(first).cast("date"), F.lit(cutoff).cast("date")))
    )
    matched = (
        (F.col("_s_date") == F.col("_m_date"))
        & (F.col("_s_product") == F.col("_m_product"))
        & (F.abs(F.col("_m_qty") + F.col("_s_qty")) <= F.lit(QTY_SLACK))  # a sale issues stock: opposite signs
    )
    status = (
        F.when(F.col("_s_qty").isNotNull() & F.col("_m_qty").isNotNull() & matched, F.lit("PASS"))
        .when(F.col("_m_qty").isNull() & F.col("_m_lost").isNotNull(), F.lit("EXPLAINED"))
        .when(F.col("_s_qty").isNull() & F.col("_s_lost").isNotNull(), F.lit("EXPLAINED"))
        .when(F.col("_m_qty").isNull(), F.lit("FAIL_NO_MOVEMENT"))
        .when(F.col("_s_qty").isNull(), F.lit("FAIL_NO_SALE"))
        .otherwise(F.lit("FAIL_MISMATCH"))
    )
    return j.select(
        *keys,
        "check_date",
        F.coalesce("_s_product", "_m_product").alias("product_id"),
        F.col("_s_qty").alias("sales_quantity"),
        F.col("_m_qty").alias("movement_quantity"),
        status.alias("status"),
    )


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


def _summarise(lines: DataFrame, date_col: str) -> DataFrame:
    return lines.groupBy(F.col(date_col).alias("check_date")).agg(
        F.count(F.lit(1)).alias("checked"),
        F.sum(F.when(F.col("status") == "PASS", 1).otherwise(0)).alias("passed"),
        F.sum(F.when(F.col("status") == "EXPLAINED", 1).otherwise(0)).alias("explained"),
        F.sum(F.when(F.col("status") == "WAIT", 1).otherwise(0)).alias("waiting"),
        F.sum(F.when(F.col("status").startswith("FAIL"), 1).otherwise(0)).alias("failed"),
    )


def _result(name: str, summary: list, failures: list) -> CheckResult:
    return CheckResult(
        name,
        groups=len(summary),
        failed=sum(r["failed"] for r in summary),
        explained=sum(r["explained"] for r in summary),
        waiting=sum(r["waiting"] for r in summary),
        first_failures=failures,
    )


def run_inventory_checks(
    spark: SparkSession,
    cfg: Config,
    lake: Lake | None = None,
    sales_lake: Lake | None = None,
    first_cross_check_date: date | None = None,
) -> InventoryChecks:
    lake = lake or Lake(spark, cfg)
    sales_lake = sales_lake or sales_lake_for(spark, cfg)
    out = InventoryChecks(run_id=f"inv-checks-{datetime.now(UTC):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}")
    now = datetime.now(UTC)
    tol = float(cfg.business.get("reconciliation_tolerance_pct", 0.1))
    movements, stock = lake.read("gold", "fact_stock_movements"), lake.read("gold", "fact_stock_daily")
    quarantine = lake.read("silver", "quarantine")
    stamp = [F.lit(out.run_id).alias("run_id"), F.lit(now).cast("timestamp").alias("checked_ts")]

    # 1. Source totals
    rec = compare_totals(expected_totals(lake.read("ops", "manifest_totals")), actual_totals(movements, stock), tol)
    lake.append(rec.select(*stamp, *rec.columns), "ops", "reconciliation")
    rows = lake.read("ops", "reconciliation").filter(F.col("run_id") == out.run_id).collect()
    bad = [r.asDict() for r in rows if r["status"] == "FAIL"]
    out.results["reconciliation"] = CheckResult("reconciliation", len(rows), len(bad), first_failures=bad[:20])

    # 2. Roll-forward
    last_delivery = (
        movements.agg(F.max("delivered_date")).collect()[0][0] or stock.agg(F.max("snapshot_date")).collect()[0][0]
    )
    rf = _summarise(rollforward(stock, movements, quarantine, last_delivery), "snapshot_date")
    lake.append(rf.select(*stamp, *rf.columns), "ops", "stock_rollforward")
    summary = lake.read("ops", "stock_rollforward").filter(F.col("run_id") == out.run_id).collect()
    out.results["stock_rollforward"] = _result(
        "stock_rollforward", summary, [r.asDict() for r in summary if r["failed"]][:20]
    )

    # 3. Cross-area check with sales
    fact_sales = sales_lake.read("gold", "fact_sales")
    last_sale = fact_sales.agg(F.max("invoice_date")).collect()[0][0]
    last_move = movements.agg(F.max("posting_date")).collect()[0][0]
    cutoff = min(last_sale, last_move) - timedelta(days=SETTLE_DAYS)
    first = first_cross_check_date or max(
        fact_sales.agg(F.min("invoice_date")).collect()[0][0], movements.agg(F.min("posting_date")).collect()[0][0]
    )
    lines = cross_check_lines(
        fact_sales,
        sales_lake.read("gold", "dim_product"),
        movements,
        sales_lake.read("silver", "quarantine")
        if sales_lake.exists("silver", "quarantine")
        else spark.createDataFrame([], "invoice_number string, invoice_item string"),
        quarantine,
        first,
        cutoff,
    )
    xc = _summarise(lines, "check_date")
    lake.append(xc.select(*stamp, *xc.columns), "ops", "cross_checks")
    summary = lake.read("ops", "cross_checks").filter(F.col("run_id") == out.run_id).collect()
    failures = [r.asDict() for r in lines.filter(F.col("status").startswith("FAIL")).limit(20).collect()]
    out.results["cross_check_sales"] = _result("cross_check_sales", summary, failures)

    dq = [
        (out.run_id, now, "gold", name, "gold.fact_stock_daily" if name != "cross_check_sales" else "sales vs stock",
         None, None, f"{r.groups} groups", f"{r.failed} failed, {r.explained} explained, {r.waiting} waiting",
         "PASS" if r.ok else "FAIL")
        for name, r in out.results.items()
    ]  # fmt: skip
    lake.append(spark.createDataFrame(dq, b.DQ_SCHEMA), *b.DQ_RESULTS)
    return out


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    argparse.ArgumentParser(description="Check inventory gold against the source and against sales.").parse_args(argv)
    cfg = load_config(area="inventory")
    out = run_inventory_checks(get_spark(cfg, app_name="inventory-checks"), cfg)
    print(f"  run {out.run_id}")
    for r in out.results.values():
        print(f"  {r.name:<20} {r.groups} groups, failed {r.failed}, explained {r.explained}, waiting {r.waiting}")
        for f in r.first_failures[:5]:
            print(f"    FAIL {f}")
    if not out.ok:
        print("  INVENTORY CHECKS FAILED: do not publish; see ops.dq_results.")
        sys.exit(1)
    print("  inventory checks PASSED.")


if __name__ == "__main__":
    main()
