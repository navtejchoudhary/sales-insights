"""Reconciliation for SAP billing data (client area): prove nothing was lost or invented, source to gold.

    uv run python -m sales_insights.pipeline.sap_reconcile       # exit code 1 if anything fails

Three links, each one checked separately so a failure says WHERE a row went missing:

  1. source -> bronze   (sap_silver.py, check copy_complete_vs_source_counts)
     every row the source says it holds was copied: lines and net amount per company, month and category.
  2. source -> silver   (here, check source_to_silver)
     per company, billing month and category: the source's lines minus the rows filtered for a stated
     reason (ops.sap_row_fates) = the lines in silver, and the same for the net amount as the source stores
     it (unsigned, document currency). A join that drops or doubles a line fails here.
  3. silver -> gold     (here, check silver_to_gold)
     per invoice date and billing type: gold's lines and net revenue (signed, company currency) = silver's.

Lines must match exactly; amounts within half a cent per group (rounding only).

Results:
  ops.sap_reconciliation   one row per group and check, every run (appended, so history is kept)
  ops.dq_results           one PASS/FAIL row per check and run (layer "gold")
"""

from __future__ import annotations

import argparse
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_insights.common.config import Config
from sales_insights.common.lake import Lake
from sales_insights.pipeline import bronze as b
from sales_insights.pipeline.sap_silver import BUCKET, KEPT

AMOUNT_SLACK = 0.005  # half a cent: rounding only
GROUP_COLUMNS = [*BUCKET, "invoice_date", "invoice_type"]


@dataclass
class SapReconRun:
    run_id: str
    groups: dict[str, int] = field(default_factory=dict)
    failed: dict[str, int] = field(default_factory=dict)
    failures: list[dict] = field(default_factory=list)

    @property
    def failed_groups(self) -> int:
        return sum(self.failed.values())


def _status(j: DataFrame) -> DataFrame:
    for c in ("expected_lines", "actual_lines", "expected_amount", "actual_amount"):
        j = j.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
    ok = (F.col("expected_lines") == F.col("actual_lines")) & (
        F.abs(F.col("expected_amount") - F.col("actual_amount")) <= F.lit(AMOUNT_SLACK)
    )
    return j.withColumn("status", F.when(ok, F.lit("PASS")).otherwise(F.lit("FAIL")))


def source_to_silver(source_counts: DataFrame, fates: DataFrame, lines: DataFrame) -> DataFrame:
    """Expected in silver = the source's totals minus every row filtered for a stated reason."""
    filtered = (
        fates.filter(F.col("fate") != KEPT)
        .groupBy(*BUCKET)
        .agg(F.sum("line_count").alias("_f_lines"), F.sum("netamount_sum").alias("_f_amount"))
    )
    expected = (
        source_counts.groupBy(*BUCKET)
        .agg(F.sum("line_count").alias("_s_lines"), F.sum("netamount_sum").alias("_s_amount"))
        .join(filtered, BUCKET, "left")
        .select(
            *BUCKET,
            (F.col("_s_lines") - F.coalesce(F.col("_f_lines"), F.lit(0))).alias("expected_lines"),
            (F.col("_s_amount") - F.coalesce(F.col("_f_amount"), F.lit(0))).alias("expected_amount"),
        )
    )
    actual = lines.groupBy(
        "company_code", F.date_format("invoice_date", "yyyy-MM").alias("billing_month"), "sd_document_category"
    ).agg(F.count(F.lit(1)).alias("actual_lines"), F.sum("document_net_amount").alias("actual_amount"))
    return _status(expected.join(actual, BUCKET, "full_outer")).withColumn("check_name", F.lit("source_to_silver"))


def silver_to_gold(lines: DataFrame, fact: DataFrame) -> DataFrame:
    """Gold holds exactly silver's lines and revenue, per invoice date and billing type."""

    def totals(df: DataFrame, revenue: str, prefix: str) -> DataFrame:
        return df.groupBy("invoice_date", "invoice_type").agg(
            F.count(F.lit(1)).alias(f"{prefix}_lines"), F.sum(revenue).alias(f"{prefix}_amount")
        )

    j = totals(lines, "revenue", "expected").join(
        totals(fact, "net_revenue_amount", "actual"), ["invoice_date", "invoice_type"], "full_outer"
    )
    return _status(j).withColumn("check_name", F.lit("silver_to_gold"))


def run_sap_reconcile(spark: SparkSession, cfg: Config, lake: Lake | None = None) -> SapReconRun:
    lake = lake or Lake(spark, cfg)
    run = SapReconRun(run_id=f"sap-reconcile-{datetime.now(UTC):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}")
    now = datetime.now(UTC)
    lines = lake.read("silver", "invoice_lines")
    results = [
        source_to_silver(lake.read("ops", "source_counts"), lake.read("ops", "sap_row_fates"), lines),
        silver_to_gold(lines, lake.read("gold", "fact_sales")),
    ]
    out = results[0].unionByName(results[1], allowMissingColumns=True)
    out = out.select(
        F.lit(run.run_id).alias("run_id"),
        F.lit(now).cast("timestamp").alias("checked_ts"),
        "check_name",
        *[F.col(c).cast("string").alias(c) for c in GROUP_COLUMNS],
        F.col("expected_lines").cast("long"),
        F.col("actual_lines").cast("long"),
        F.col("expected_amount").cast("DECIMAL(28,2)"),
        F.col("actual_amount").cast("DECIMAL(28,2)"),
        "status",
    )
    lake.append(out, "ops", "sap_reconciliation")
    rows = lake.read("ops", "sap_reconciliation").filter(F.col("run_id") == run.run_id).collect()

    dq = []
    for name in ("source_to_silver", "silver_to_gold"):
        mine = [r for r in rows if r["check_name"] == name]
        bad = [r.asDict() for r in mine if r["status"] == "FAIL"]
        run.groups[name], run.failed[name] = len(mine), len(bad)
        run.failures += bad
        expected = sum(r["expected_lines"] for r in mine)
        actual = sum(r["actual_lines"] for r in mine)
        status = "FAIL" if bad or not mine else "PASS"
        dq.append((run.run_id, now, "gold", name, "gold.fact_sales", None, None, f"{expected} lines",
                   f"{actual} lines, {len(bad)} group(s) differ", status))  # fmt: skip
        if not mine:
            run.failed[name] = 1  # nothing to compare is a failure, never a silent pass
    lake.append(spark.createDataFrame(dq, b.DQ_SCHEMA), *b.DQ_RESULTS)
    return run


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    parser = argparse.ArgumentParser(description="Check SAP billing data source -> silver -> gold.")
    parser.add_argument("--area", default="client", help="business area with SAP-shaped data (default client)")
    cfg = load_config(area=parser.parse_args(argv).area)
    run = run_sap_reconcile(get_spark(cfg, app_name="sap-reconcile"), cfg)
    print(f"  run {run.run_id}")
    for name, groups in run.groups.items():
        note = "nothing to compare" if not groups else f"{groups - run.failed[name]} of {groups} groups match"
        print(f"    {name:<18} {note}")
    for f in run.failures[:20]:
        where = {k: f[k] for k in GROUP_COLUMNS if f.get(k)}
        print(
            f"    FAIL {f['check_name']} {where}: lines {f['actual_lines']}/{f['expected_lines']}, "
            f"amount {f['actual_amount']}/{f['expected_amount']}"
        )
    if run.failed_groups:
        print("  RECONCILIATION FAILED: do not publish; see ops.sap_reconciliation.")
        sys.exit(1)
    print("  reconciliation PASSED: every source row is in gold or filtered for a stated reason.")


if __name__ == "__main__":
    main()
