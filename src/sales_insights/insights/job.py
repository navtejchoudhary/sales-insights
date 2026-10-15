"""Insights job: what changed, who drove it, what looks wrong, and the weekly deck.

    uv run python -m sales_insights.insights.job            # insights -> gold.insights, deck -> output/
    uv run python -m sales_insights.insights.job --no-deck  # insights only
    uv run python -m sales_insights.insights.job --area inventory   # stock insights -> inventory gold.insights

Runs after gold. Steps:
  1. like-for-like comparisons for the latest data date (insights/periods.py)
  2. headline KPIs, top movers by province / product group / product / customer group / channel
  3. alerts: revenue drop (last 7 days vs the same 7 days last year), possible stock-outs, unusual days
  4. forecast: same month last year x fiscal-year-to-date growth, to the end of the fiscal year (insights/forecast.py)
  5. narrative: template sentences; on Databricks optionally rewritten by ai_query (numbers checked)
  6. gold.insights (appended, one run_id per run), gold.forecast (rebuilt) and the weekly PowerPoint deck
The Databricks SQL alerts (bundle/resources/alerts.yml) read gold.insights and email when an alert is found.

Inventory area (`run_stock`, the inventory pipeline's insights step): confirmed stock-outs, low cover and
overstock (insights/stock.py) -> gold.insights of the inventory catalog, same columns. The weekly deck shows
the latest of them on a "Stock" slide when a stock lake is given.
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
from sales_insights.insights import forecast as fc
from sales_insights.insights import narrative as nr
from sales_insights.insights import periods as pr
from sales_insights.insights import stock as st

MOVER_COMPARISONS = ["month_vs_last_year", "mtd_vs_last_year"]
STOCK_OUT_WINDOW_DAYS = 60  # 28-day baseline before the last sale + up to a month without sales
ANOMALY_WINDOW_DAYS = 63  # 8 weeks of history + the last 7 days

INSIGHTS_DESCRIPTION = (
    "Automatic sales insights, one row per finding per run: headline KPIs, top movers, alerts (revenue drop, "
    "possible stock-outs, unusual days) and the revenue forecast. Every number is computed from gold.fact_sales "
    "with like-for-like periods."
)
INSIGHTS_COLUMNS = {
    "run_id": "Insights run that produced the row.",
    "generated_ts": "When the run happened (UTC).",
    "as_of_date": "Latest invoice date in the data the run used.",
    "kind": "headline, mover, revenue_drop, stock_out, anomaly or forecast.",
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

FORECAST_DESCRIPTION = (
    "Net revenue forecast per month, from the current month to the end of the fiscal year (March), rebuilt every "
    "morning. Method: same month last year x the fiscal-year-to-date growth on last year; the low-high range is "
    "that growth -/+ one standard deviation of the month-by-month growth. Money in LKR excluding VAT."
)
FORECAST_COLUMNS = {
    "run_id": "Insights run that produced the forecast.",
    "generated_ts": "When the forecast was made (UTC).",
    "as_of_date": "Latest invoice date in the data the forecast used.",
    "month_start_date": "First day of the forecast month.",
    "forecast_net_revenue": "Forecast net revenue for the whole month, LKR.",
    "low_net_revenue": "Low end of the likely range, LKR.",
    "high_net_revenue": "High end of the likely range, LKR.",
    "last_year_net_revenue": "Actual net revenue of the same month last year, LKR (the base of the forecast).",
    "actual_to_date_net_revenue": "Net revenue already invoiced in this month (current month only; empty for future months).",
    "growth_pct": "Growth used for every month: fiscal year to date vs the same months last year, in percent.",
    "growth_basis": "Which months the growth compares, e.g. Apr-Sep 2026 vs Apr-Sep 2025.",
    "method": "How the forecast is made, in words.",
}
STOCK_INSIGHTS_DESCRIPTION = (
    "Automatic stock insights, one row per finding per run: confirmed stock-outs (zero stock of a product the "
    "location issues), low cover (under 7 days) and overstock (over 120 days of cover, ranked by the value above "
    "that level). Computed from gold.fact_stock_daily on the latest stock date."
)
STOCK_INSIGHTS_COLUMNS = {
    **INSIGHTS_COLUMNS,
    "as_of_date": "Latest stock date in the data the run used.",
    "kind": "stock_out_confirmed, low_cover, overstock or overstock_total.",
    "dimension": "location_product for findings about one stock position; empty for totals.",
    "item": "Product and location, e.g. MC600 WEEDICIDE 1LTR (CASE) | Colombo Depot.",
    "metric": "What is measured: stock, days_of_cover or excess_stock_value.",
}
STOCK_SLIDE_LINES = 6
OUTLOOK_HISTORY_MONTHS = 6  # complete months of actuals shown before the forecast on the deck slide


def collect(lake: Lake, fy_start_month: int = 4) -> tuple[date, dict, list[an.Insight], object]:
    fact = dt.enriched_fact(lake)
    if lake.by_path:  # local speed-up only: serverless compute rejects cache() / unpersist()
        fact = fact.cache()
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
    week_ly = pr.last_7_days_last_year(as_of)
    insights += an.revenue_drop(week_ly, *dt.period_totals(fact, week_ly))
    insights += an.stock_outs(dt.daily_units_by_product_province(fact, start, as_of), days)
    insights += an.anomalies(dt.daily_revenue(fact, as_of - timedelta(days=ANOMALY_WINDOW_DAYS - 1), as_of))
    monthly = dict(dt.monthly_revenue(fact, pr.shift_months(as_of.replace(day=1), -24), as_of.replace(day=1)))
    forecast = fc.build(monthly, as_of, fy_start_month)
    insights += fc.insights(forecast)
    ctx = {"comparisons": comps, "totals": totals, "forecast": forecast, "monthly": monthly}
    return as_of, ctx, an.rank(insights), fact


def write(
    spark: SparkSession,
    lake: Lake,
    as_of: date,
    insights: list[an.Insight],
    description: str = INSIGHTS_DESCRIPTION,
    columns: dict[str, str] = INSIGHTS_COLUMNS,
    prefix: str = "insights",
) -> str:
    run_id = f"{prefix}-{datetime.now(UTC):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}"
    now = datetime.now(UTC)
    rows = [(run_id, now, as_of, *i.as_row().values()) for i in insights]
    schema = (
        "run_id string, generated_ts timestamp, as_of_date date, kind string, comparison string, dimension string, "
        "item string, metric string, current double, previous double, change double, change_pct double, "
        "severity int, text string"
    )
    df = spark.createDataFrame(rows, schema)
    for col, text in columns.items():
        df = df.withMetadata(col, {"comment": text})
    lake.append(df, "gold", "insights")
    lake.describe("gold", "insights", description, columns)
    return run_id


def run_stock(spark: SparkSession, cfg: Config, lake: Lake | None = None, sales_lake: Lake | None = None) -> dict:
    """Inventory area: stock findings on the latest stock date -> gold.insights of the inventory catalog."""
    from sales_insights.insights import stock_data as sd

    lake = lake or Lake(spark, cfg)
    sales_lake = sales_lake or Lake(spark, cfg.for_area("sales"))
    as_of, positions = sd.latest_positions(lake, sales_lake)
    if as_of is None:
        return {"run_id": None, "as_of": None, "insights": []}
    insights = st.stock_insights(positions, as_of)
    run_id = write(spark, lake, as_of, insights, STOCK_INSIGHTS_DESCRIPTION, STOCK_INSIGHTS_COLUMNS, "stock")
    return {"run_id": run_id, "as_of": as_of, "insights": insights}


def latest_stock_findings(stock_lake: Lake | None) -> tuple[date | None, list[str]]:
    """The newest stock insights run (for the deck): alerts first, then low cover, then the overstock total."""
    if stock_lake is None or not stock_lake.exists("gold", "insights"):
        return None, []
    ins = stock_lake.read("gold", "insights")
    latest = ins.agg(F.max_by("run_id", "generated_ts")).first()[0]
    rows = ins.filter(F.col("run_id") == latest).select("as_of_date", "kind", "severity", "text").collect()
    order = {k: i for i, k in enumerate(st.STOCK_KINDS)}
    picked = sorted((r for r in rows if r["kind"] != "overstock"), key=lambda r: (order.get(r["kind"], 9), r["text"]))
    return (rows[0]["as_of_date"] if rows else None), [r["text"] for r in picked]


def write_forecast(spark: SparkSession, lake: Lake, run_id: str, as_of: date, forecast: fc.Forecast | None) -> int:
    """Rebuild gold.forecast (only the latest forecast is kept; earlier ones live in the table history)."""
    now = datetime.now(UTC)
    rows = [
        (run_id, now, as_of, m.month, m.forecast, m.low, m.high, m.last_year, m.actual_to_date,
         forecast.growth_pct, forecast.basis, fc.METHOD)
        for m in (forecast.months if forecast else [])
    ]  # fmt: skip
    schema = (
        "run_id string, generated_ts timestamp, as_of_date date, month_start_date date, forecast_net_revenue double, "
        "low_net_revenue double, high_net_revenue double, last_year_net_revenue double, "
        "actual_to_date_net_revenue double, growth_pct double, growth_basis string, method string"
    )
    df = spark.createDataFrame(rows, schema)
    for col, text in FORECAST_COLUMNS.items():
        df = df.withMetadata(col, {"comment": text})
    lake.overwrite(df, "gold", "forecast")
    lake.describe("gold", "forecast", FORECAST_DESCRIPTION, FORECAST_COLUMNS)
    return len(rows)


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


def deck_data(
    fact,
    as_of: date,
    ctx: dict,
    insights: list[an.Insight],
    summary: list[str],
    lake: Lake,
    stock_as_of: date | None = None,
    stock_lines: list[str] | None = None,
):
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
    alerts = [i.text for i in insights if i.kind in nr.ALERT_KINDS]
    stock_lines = stock_lines or []
    forecast, monthly = ctx["forecast"], ctx["monthly"]
    history = [pr.shift_months(last_m, -k) for k in range(OUTLOOK_HISTORY_MONTHS - 1, -1, -1)]
    outlook = [(m.strftime("%b %y"), monthly.get(m, 0.0) / 1e6, None) for m in history]
    outlook += [(f.month.strftime("%b %y"), None, f.forecast / 1e6) for f in (forecast.months if forecast else [])]
    outlook_notes = (
        [f"{f.label}: about {an.fmt_lkr(f.forecast)}{fc.range_text(f.low, f.high)}" for f in forecast.months[:3]]
        + [f"Method: same month last year x growth of {forecast.growth_pct:+.1f}% ({forecast.basis}). "
           "It assumes this year's growth continues; weather, prices or a competitor can change that."]
        if forecast else []
    )  # fmt: skip
    about = [
        f"Data up to {as_of:%d %b %Y}. About 3% of invoices reach us up to 7 days late, so the last few days can still grow slightly.",
        "Every comparison uses periods of the same length: month to date is compared with the same days of the earlier period, never with a whole month.",
        "Net revenue = invoices minus returns, cancellations and price corrections, excluding VAT (LKR). Invoices = paid, non-cancelled invoices.",
        "Gross margin % = (net revenue - standard cost) / net revenue.",
        "Possible stock-out = a product that sold on at least 80% of the 28 days before its last sale in a province, then nothing for 3+ days.",
        f"Revenue drop alert = net revenue of the last 7 days {an.REVENUE_DROP_PCT:.0f}%+ below the same 7 days last year.",
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
        outlook=outlook,
        outlook_caption=f"Net revenue per month, LKR millions: last {OUTLOOK_HISTORY_MONTHS} complete months (actual) "
        "and the forecast to the end of the fiscal year",
        outlook_notes=outlook_notes,
        stock=stock_lines[:STOCK_SLIDE_LINES],
        stock_caption=(f"Stock data up to {stock_as_of:%d %b %Y}: depots and stores with no stock, under "
                       f"{st.LOW_COVER_DAYS} days of cover, and stock above {st.OVERSTOCK_DAYS} days of cover"
                       if stock_as_of else ""),
    )  # fmt: skip


def run(
    spark: SparkSession,
    cfg: Config,
    lake: Lake | None = None,
    deck_dir: Path | None = None,
    stock_lake: Lake | None = None,
) -> dict:
    """Sales insights, forecast and (optionally) the deck. `stock_lake` (the inventory area) adds a Stock slide."""
    lake = lake or Lake(spark, cfg)
    as_of, ctx, insights, fact = collect(lake, int(cfg.business.get("fiscal_year_start_month", 4)))
    facts = nr.summary(insights)
    model = None if lake.by_path else cfg.business.get("narrative_model")
    summary = nr.ai_rewrite(spark, facts, model)
    run_id = write(spark, lake, as_of, insights)
    forecast_months = write_forecast(spark, lake, run_id, as_of, ctx["forecast"])
    deck = None
    if deck_dir is not None:
        from sales_insights.deck.deck import build_deck

        stock_as_of, stock_lines = latest_stock_findings(stock_lake)
        data = deck_data(fact, as_of, ctx, insights, summary, lake, stock_as_of, stock_lines)
        deck = build_deck(data, deck_dir / f"weekly_deck_{as_of}.pptx")
    if lake.by_path:
        fact.unpersist()
    return {"run_id": run_id, "as_of": as_of, "insights": insights, "summary": summary, "deck": deck,
            "forecast": ctx["forecast"], "forecast_months": forecast_months}  # fmt: skip


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    parser = argparse.ArgumentParser(description="Compute sales insights and the weekly deck.")
    parser.add_argument("--no-deck", action="store_true", help="skip the PowerPoint deck")
    parser.add_argument("--area", default="sales", choices=["sales", "inventory"], help="business area")
    args = parser.parse_args(argv)
    cfg = load_config(area=args.area)
    spark = get_spark(cfg, app_name="insights")
    if cfg.area == "inventory":
        out = run_stock(spark, cfg)
        print(f"  run {out['run_id']}  stock up to {out['as_of']}  {len(out['insights'])} findings")
        for i in out["insights"]:
            print(f"    - {i.text}")
        return
    stock_lake = Lake(spark, cfg.for_area("inventory")) if "inventory" in cfg.area_names() else None
    out = run(spark, cfg, deck_dir=None if args.no_deck else Path(cfg.path(cfg.output_path)), stock_lake=stock_lake)
    ins = out["insights"]
    kinds = {k: sum(1 for i in ins if i.kind == k)
             for k in ("headline", "mover", "revenue_drop", "stock_out", "anomaly", "forecast")}  # fmt: skip
    print(f"  run {out['run_id']}  data up to {out['as_of']}")
    print(f"  {len(ins)} insights: " + ", ".join(f"{v} {k}" for k, v in kinds.items()))
    print("  summary:")
    for s in out["summary"]:
        print(f"    - {s}")
    f = out["forecast"]
    if f:
        print(f"  forecast: {out['forecast_months']} months, growth {f.growth_pct:+.1f}% ({f.basis}) -> gold.forecast")
    if out["deck"]:
        print(f"  deck: {out['deck']}")


if __name__ == "__main__":
    main()
