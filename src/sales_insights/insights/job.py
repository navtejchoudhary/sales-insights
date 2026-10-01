"""Insights job: what changed, who drove it, what looks wrong, and the weekly deck.

    uv run python -m sales_insights.insights.job            # insights -> gold.insights, deck -> output/
    uv run python -m sales_insights.insights.job --no-deck  # insights only

Runs after gold. Steps:
  1. like-for-like comparisons for the latest data date (insights/periods.py)
  2. headline KPIs, top movers by province / product group / product / customer group / channel
  3. alerts: possible stock-outs, unusual days
  4. narrative: template sentences; on Databricks optionally rewritten by ai_query (numbers checked)
  5. gold.insights (appended, one run_id per run) and the weekly PowerPoint deck
"""

from __future__ import annotations

import argparse
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sales_insights.common.config import Config
from sales_insights.common.lake import Lake
from sales_insights.insights import analysis as an
from sales_insights.insights import data as dt
from sales_insights.insights import narrative as nr
from sales_insights.insights import periods as pr

MOVER_COMPARISONS = ["month_vs_last_year", "mtd_vs_last_year"]
STOCK_OUT_WINDOW_DAYS = 60  # 28-day baseline before the last sale + up to a month without sales
ANOMALY_WINDOW_DAYS = 63  # 8 weeks of history + the last 7 days

INSIGHTS_DESCRIPTION = (
    "Automatic sales insights, one row per finding per run: headline KPIs, top movers, possible stock-outs "
    "and unusual days. Every number is computed from gold.fact_sales with like-for-like periods."
)
INSIGHTS_COLUMNS = {
    "run_id": "Insights run that produced the row.",
    "generated_ts": "When the run happened (UTC).",
    "as_of_date": "Latest invoice date in the data the run used.",
    "kind": "headline, mover, stock_out or anomaly.",
    "comparison": "Which like-for-like comparison, e.g. mtd_vs_last_year (empty for alerts).",
    "dimension": "What the mover is grouped by: province, product_group, product, customer_group, channel.",
    "item": "The province, product, etc. the finding is about.",
    "metric": "Measure compared, e.g. net_revenue.",
    "current": "Value in the current period.",
    "previous": "Value in the comparison period.",
    "change": "current - previous.",
    "change_pct": "Change as a percentage of the previous value.",
    "severity": "1 = act now, 2 = notable, 3 = context.",
    "text": "The finding as one plain-English sentence.",
}


def collect(lake: Lake) -> tuple[date, dict, list[an.Insight], object]:
    fact = dt.enriched_fact(lake).cache()
    as_of = dt.data_until(fact)
    insights: list[an.Insight] = []
    totals = {}
    comps = {c.key: c for c in pr.standard(as_of)}
    for c in comps.values():
        cur, prev = dt.period_totals(fact, c)
        totals[c.key] = (cur, prev)
        insights += an.headline(c, cur, prev)
    for key in MOVER_COMPARISONS:
        for dim in dt.DIMENSIONS:
            insights += an.movers(comps[key], dim, dt.period_by(fact, comps[key], dim))
    start = as_of - timedelta(days=STOCK_OUT_WINDOW_DAYS - 1)
    days = [start + timedelta(days=i) for i in range(STOCK_OUT_WINDOW_DAYS)]
    insights += an.stock_outs(dt.daily_units_by_product_province(fact, start, as_of), days)
    insights += an.anomalies(dt.daily_revenue(fact, as_of - timedelta(days=ANOMALY_WINDOW_DAYS - 1), as_of))
    return as_of, {"comparisons": comps, "totals": totals}, an.rank(insights), fact


def write(spark: SparkSession, lake: Lake, as_of: date, insights: list[an.Insight]) -> str:
    run_id = f"insights-{datetime.now(UTC):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}"
    now = datetime.now(UTC)
    rows = [(run_id, now, as_of, *i.as_row().values()) for i in insights]
    schema = (
        "run_id string, generated_ts timestamp, as_of_date date, kind string, comparison string, dimension string, "
        "item string, metric string, current double, previous double, change double, change_pct double, "
        "severity int, text string"
    )
    df = spark.createDataFrame(rows, schema)
    for col, text in INSIGHTS_COLUMNS.items():
        df = df.withMetadata(col, {"comment": text})
    lake.append(df, "gold", "insights")
    lake.describe("gold", "insights", INSIGHTS_DESCRIPTION, INSIGHTS_COLUMNS)
    return run_id


def reconciliation_status(lake: Lake) -> str:
    if not lake.exists("ops", "reconciliation"):
        return "Reconciliation has not run yet."
    rec = lake.read("ops", "reconciliation")
    last = rec.agg(F.max("run_id")).first()[0]
    r = rec.filter(F.col("run_id") == last).agg(
        F.count("*").alias("n"), F.sum(F.when(F.col("status") == "FAIL", 1)).alias("f")
    )
    n, failed = r.first()
    return (
        f"Reconciliation: {n - (failed or 0)} of {n} checks against the source system's own totals passed."
        if n
        else "Reconciliation found nothing to compare."
    )


def deck_data(fact, as_of: date, ctx: dict, insights: list[an.Insight], summary: list[str], lake: Lake):
    from sales_insights.deck.deck import DeckData, KpiCard

    comps, totals = ctx["comparisons"], ctx["totals"]
    mtd = comps["mtd_vs_last_year"]
    cur, prev = totals["mtd_vs_last_year"]
    cards = []
    for m in ("net_revenue", "units", "invoice_count", "gross_margin_pct"):
        c, p = cur.get(m), prev.get(m)
        change = None if c is None or p is None else c - p
        cards.append(
            KpiCard(
                an.MEASURES[m][0],
                an.fmt_value(m, c),
                f"{an.fmt_change(m, change, an.pct_change(c, p))} vs {mtd.previous.label()}",
                None if change is None else change >= 0,
            )
        )
    last_m = pr.last_complete_month(as_of)
    trend = [(m.strftime("%b %y"), v / 1e6) for m, v in dt.monthly_revenue(fact, pr.shift_months(last_m, -12), last_m)]
    month = comps["month_vs_last_year"]
    movers = {
        "By province": [(i, (c - p) / 1e6) for i, c, p in dt.period_by(fact, month, "province")],
        "By product group": [(i, (c - p) / 1e6) for i, c, p in dt.period_by(fact, month, "product_group")],
    }
    alerts = [i.text for i in insights if i.kind in ("stock_out", "anomaly")]
    about = [
        f"Data up to {as_of:%d %b %Y}. About 3% of invoices reach us up to 7 days late, so the last few days can still grow slightly.",
        "Every comparison uses periods of the same length: month to date is compared with the same days of the earlier period, never with a whole month.",
        "Net revenue = invoices minus returns, cancellations and price corrections, excluding VAT (LKR). Invoices = paid, non-cancelled invoices.",
        "Gross margin % = (net revenue - standard cost) / net revenue. Draft definition, to be confirmed.",
        "Possible stock-out = a product that sold on at least 80% of the 28 days before its last sale in a province, then nothing for 3+ days.",
        reconciliation_status(lake),
        "Definitions: docs/kpi_definitions.md. Every number in this deck is computed by the pipeline; the AI only words the summary.",
    ]
    return DeckData(
        title="Weekly sales insights",
        subtitle=f"Data up to {as_of:%A %d %B %Y}",
        cards=cards,
        cards_caption=f"Month to date ({mtd.current.label()}) compared with the same days last year ({mtd.previous.label()})",
        key_points=summary[:3],
        trend=trend,
        trend_caption=f"Net revenue per month, LKR millions, last 13 complete months (to {last_m:%B %Y})",
        movers=movers,
        movers_caption=f"{month.current.label()} vs {month.previous.label()}: change in net revenue, LKR millions",
        alerts=alerts,
        summary=summary,
        about=about,
    )


def run(spark: SparkSession, cfg: Config, lake: Lake | None = None, deck_dir: Path | None = None) -> dict:
    lake = lake or Lake(spark, cfg)
    as_of, ctx, insights, fact = collect(lake)
    facts = nr.summary(insights)
    model = None if lake.by_path else cfg.business.get("narrative_model")
    summary = nr.ai_rewrite(spark, facts, model)
    run_id = write(spark, lake, as_of, insights)
    deck = None
    if deck_dir is not None:
        from sales_insights.deck.deck import build_deck

        deck = build_deck(deck_data(fact, as_of, ctx, insights, summary, lake), deck_dir / f"weekly_deck_{as_of}.pptx")
    fact.unpersist()
    return {"run_id": run_id, "as_of": as_of, "insights": insights, "summary": summary, "deck": deck}


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    parser = argparse.ArgumentParser(description="Compute sales insights and the weekly deck.")
    parser.add_argument("--no-deck", action="store_true", help="skip the PowerPoint deck")
    args = parser.parse_args(argv)
    cfg = load_config()
    out = run(
        get_spark(cfg, app_name="insights"), cfg, deck_dir=None if args.no_deck else Path(cfg.path(cfg.output_path))
    )
    ins = out["insights"]
    kinds = {k: sum(1 for i in ins if i.kind == k) for k in ("headline", "mover", "stock_out", "anomaly")}
    print(f"  run {out['run_id']}  data up to {out['as_of']}")
    print(f"  {len(ins)} insights: " + ", ".join(f"{v} {k}" for k, v in kinds.items()))
    print("  summary:")
    for s in out["summary"]:
        print(f"    - {s}")
    if out["deck"]:
        print(f"  deck: {out['deck']}")


if __name__ == "__main__":
    main()
