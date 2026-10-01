"""Reconciliation: prove gold adds up to what the source system says it sent.

    uv run python -m sales_insights.pipeline.reconcile      # exit code 1 if anything fails

For every (delivery, file kind, invoice date) the manifests promise a line count, an invoice count and
revenue. Gold must match each within the tolerance in config (reconciliation_tolerance_pct, 0.1%).
The manifests' totals already leave out the duplicate copies and invalid rows they injected, so a
correct pipeline matches them EXACTLY; the tolerance only absorbs rounding.

Results:
  ops.reconciliation   one row per compared group, every run (appended, so history is kept)
  ops.dq_results       one PASS/FAIL row per delivery (layer "gold", check "reconciliation")
"""

from __future__ import annotations

import argparse
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_insights.common.config import Config
from sales_insights.common.lake import Lake
from sales_insights.pipeline import bronze as b

HISTORY = "history"
KEYS = ["business_date", "file_kind", "invoice_date"]
MEASURES = ["lines", "invoices", "revenue"]


@dataclass
class ReconRun:
    run_id: str
    groups: int = 0
    failed_groups: int = 0
    failures: list[dict] = field(default_factory=list)


def expected_totals(manifest_totals: DataFrame) -> DataFrame:
    """What the manifests promised (history has no business date: labelled 'history')."""
    return manifest_totals.select(
        F.coalesce(F.col("business_date"), F.lit(HISTORY)).alias("business_date"),
        "file_kind",
        "invoice_date",
        F.col("lines").cast("long").alias("expected_lines"),
        F.col("invoices").cast("long").alias("expected_invoices"),
        F.col("revenue").cast("double").alias("expected_revenue"),
    )


def actual_totals(fact: DataFrame) -> DataFrame:
    """What gold holds, grouped the same way as the manifests."""
    return fact.groupBy(
        F.coalesce(F.col("loaded_business_date").cast("string"), F.lit(HISTORY)).alias("business_date"),
        F.col("source_kind").alias("file_kind"),
        F.col("invoice_date").cast("string").alias("invoice_date"),
    ).agg(
        F.count(F.lit(1)).alias("actual_lines"),
        F.countDistinct("invoice_number").alias("actual_invoices"),
        F.sum("net_revenue_amount").cast("double").alias("actual_revenue"),
    )


def compare(expected: DataFrame, actual: DataFrame, tolerance_pct: float) -> DataFrame:
    """Full outer join: a group missing on either side counts as 0 there, so it fails."""
    j = expected.join(actual, KEYS, "full_outer")
    for m in MEASURES:
        j = j.withColumn(f"expected_{m}", F.coalesce(f"expected_{m}", F.lit(0))).withColumn(
            f"actual_{m}", F.coalesce(f"actual_{m}", F.lit(0))
        )
    tol = F.lit(tolerance_pct / 100.0)

    def within(m: str, slack: float) -> Column:
        e, a = F.col(f"expected_{m}"), F.col(f"actual_{m}")
        return F.abs(a - e) <= F.abs(e) * tol + F.lit(slack)

    j = j.withColumn("revenue_diff", F.round(F.col("actual_revenue") - F.col("expected_revenue"), 2))
    j = j.withColumn(
        "revenue_diff_pct",
        F.when(F.col("expected_revenue") != 0, F.round(F.col("revenue_diff") / F.abs("expected_revenue") * 100, 4)),
    )
    ok = within("lines", 0) & within("invoices", 0) & within("revenue", 0.005)  # half a cent for float rounding
    return j.withColumn("status", F.when(ok, F.lit("PASS")).otherwise(F.lit("FAIL")))


def run_reconcile(spark: SparkSession, cfg: Config, lake: Lake | None = None) -> ReconRun:
    lake = lake or Lake(spark, cfg)
    run = ReconRun(run_id=f"reconcile-{datetime.now(UTC):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}")
    now = datetime.now(UTC)
    tol = float(cfg.business.get("reconciliation_tolerance_pct", 0.1))

    result = compare(
        expected_totals(lake.read("ops", "manifest_totals")), actual_totals(lake.read("gold", "fact_sales")), tol
    )
    result = result.select(
        F.lit(run.run_id).alias("run_id"), F.lit(now).cast("timestamp").alias("checked_ts"), *result.columns
    )
    lake.append(result, "ops", "reconciliation")
    rows = lake.read("ops", "reconciliation").filter(F.col("run_id") == run.run_id).collect()

    run.groups = len(rows)
    run.failures = [r.asDict() for r in rows if r["status"] == "FAIL"]
    run.failed_groups = len(run.failures)

    by_delivery: dict[str, list] = {}
    for r in rows:
        by_delivery.setdefault(r["business_date"], []).append(r)
    dq = []
    for delivery, group in sorted(by_delivery.items()):
        expected = round(sum(r["expected_revenue"] for r in group), 2)
        actual = round(sum(r["actual_revenue"] for r in group), 2)
        status = "FAIL" if any(r["status"] == "FAIL" for r in group) else "PASS"
        business_date = None if delivery == HISTORY else delivery
        dq.append(
            (
                run.run_id,
                now,
                "gold",
                "reconciliation",
                "gold.fact_sales",
                business_date,
                None,
                str(expected),
                str(actual),
                status,
            )
        )
    if dq:
        lake.append(spark.createDataFrame(dq, b.DQ_SCHEMA), *b.DQ_RESULTS)
    return run


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    argparse.ArgumentParser(description="Check gold against the manifests' control totals.").parse_args(argv)
    cfg = load_config()
    run = run_reconcile(get_spark(cfg, app_name="reconcile"), cfg)
    print(f"  run {run.run_id}")
    print(f"  compared {run.groups} groups (delivery x file x invoice date), failed {run.failed_groups}")
    for f in run.failures[:20]:
        print(
            f"    FAIL {f['business_date']} {f['file_kind']} {f['invoice_date']}: "
            f"lines {f['actual_lines']}/{f['expected_lines']}, invoices {f['actual_invoices']}/{f['expected_invoices']}, "
            f"revenue {f['actual_revenue']:,.2f}/{f['expected_revenue']:,.2f}"
        )
    if run.failed_groups:
        print("  RECONCILIATION FAILED: gold does not match the source. Do not publish; see ops.reconciliation.")
        sys.exit(1)
    print("  reconciliation PASSED: gold matches every manifest.")


if __name__ == "__main__":
    main()
