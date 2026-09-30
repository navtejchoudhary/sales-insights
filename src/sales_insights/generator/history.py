"""History generator: 18 months of invoice lines in the reference SAP O2C extract format.

Output (like a source-system backfill):

    staging/history/invoices_YYYYMM.csv     one file per calendar month
    staging/history/history_manifest.json   written LAST: rows, SHA-256, control totals per day

Columns: the reference extract's 74 columns in order, plus `reference_invoice_number`
(the invoice a return, cancellation or price correction reverses). See docs/invoice_data.md.

All sales and their fates come from generator/engine.py, so the history and the daily
drops always agree: a return dated in October appears in October's daily file, never here.
History contains no data-quality dirt; dirt goes into the daily drops.

Run:
    uv run python -m sales_insights.generator.history
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from sales_insights.generator import engine as e
from sales_insights.generator import masters as m
from sales_insights.generator import stories

# Re-exported for callers and tests
REFERENCE_COLUMNS = e.REFERENCE_COLUMNS
ADDED_COLUMNS = e.ADDED_COLUMNS
COLUMNS = e.COLUMNS
history_window = e.history_window
control_totals = e.control_totals


def build_history(seed: int, as_of: date, start: date | None = None) -> pd.DataFrame:
    """All invoice lines dated in the history window (or from `start`, for fast tests)."""
    tables, _ = m.build_masters(seed, as_of, dirt=False)
    mi = e.MasterIndex(tables)
    window_start, end = e.history_window(as_of)

    sales: list[e.Sale] = []
    d = start or window_start
    while d <= end:
        sales.extend(e.sales_for_day(mi, seed, d, window_start))
        d += timedelta(days=1)

    lines = [s.line for s in sales]
    for number, group in e.group_by_invoice(sales).items():
        fate = e.invoice_fate(mi, seed, number, group, live_from=as_of)
        lines.extend(c for c in fate.changes if c["invoice_date"] <= end.isoformat())
        if fate.cancel_date and fate.cancel_date <= end:
            for s in group:  # the original is now cancelled, and was updated at cancellation time
                s.line["billing_status"] = "C"
                s.line["last_updated_timestamp"] = fate.cancel_stamp

    df = pd.DataFrame(lines, columns=COLUMNS)
    return df.sort_values(["invoice_date", "invoice_number", "invoice_item"], kind="stable").reset_index(drop=True)


def write_history(df: pd.DataFrame, out_dir: Path, seed: int, as_of: date) -> Path:
    target = out_dir / "history"
    target.mkdir(parents=True, exist_ok=True)
    files = {}
    for month, part in df.groupby(df["invoice_date"].str[:7].str.replace("-", "")):
        path = target / f"invoices_{month}.csv"
        part.to_csv(path, index=False, lineterminator="\n")
        files[path.name] = {"rows": int(len(part)), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    manifest = {
        "kind": "history",
        "source_system": "SAP S/4HANA (simulated)",
        "as_of": as_of.isoformat(),
        "seed": seed,
        "date_from": df["invoice_date"].min(),
        "date_to": df["invoice_date"].max(),
        "currency": m.CURRENCY,
        "columns_added_to_reference": ADDED_COLUMNS,
        "files": files,
        "control_totals": control_totals(df),
        "dirt": [],
    }
    (target / "history_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return target


def story_summary(df: pd.DataFrame, as_of: date) -> list[str]:
    """Plain-English check that each planted story is visible in the numbers."""
    tables, _ = m.build_masters(0, as_of, dirt=False)  # lookups only (seed-independent)
    province = dict(tables["regions"][["region", "province"]].values)
    x = df.assign(
        rev=df["revenue"].astype(float),
        qty=df["quantity"].astype(float),
        date=pd.to_datetime(df["invoice_date"]),
        province=df["region"].map(province),
    )
    sales = x[x["invoice_type"] == "ZAOR"]
    out = []

    monthly = x.groupby(x["date"].dt.to_period("M"))["rev"].sum()
    peak = monthly[[p.month in stories.MAHA_PEAK_MONTHS for p in monthly.index]].mean()
    off = monthly[[p.month in (6, 7, 8) for p in monthly.index]].mean()
    out.append(f"1 Seasonality: Maha-season months average {peak / off - 1:+.0%} revenue vs Jun-Aug")

    prod_rev = sales.groupby("product_id")["rev"].sum().sort_values(ascending=False)
    top = prod_rev.head(max(1, round(len(prod_rev) * 0.2))).sum() / prod_rev.sum()
    out.append(f"2 80/20: top 20% of products = {top:.0%} of sales revenue")

    cust_count = sales.groupby("province")["customer_id"].nunique()
    per_cust = sales.groupby("province")["rev"].sum() / cust_count
    weak = per_cust[stories.WEAK_PROVINCE]
    others = per_cust.drop(stories.WEAK_PROVINCE).mean()
    out.append(
        f"3 Weak province: {stories.WEAK_PROVINCE} revenue per customer {weak / others - 1:+.0%} vs other provinces"
    )

    hero = sales[sales["product_id"] == stories.HERO_PRODUCT_ID]
    unit = hero["rev"] / hero["qty"]
    before = unit[hero["date"] < pd.Timestamp(stories.PRICE_RISE_FROM)].mean()
    after = unit[hero["date"] >= pd.Timestamp(stories.PRICE_RISE_FROM)].mean()
    out.append(
        f"4 Price rise: {stories.HERO_PRODUCT_ID} average net unit price {after / before - 1:+.1%} from {stories.PRICE_RISE_FROM}"
    )

    new = sales[sales["product_id"] == stories.NEW_PRODUCT_ID]
    first = new["date"].min().date() if len(new) else None
    by_month = new.groupby(new["date"].dt.to_period("M"))["qty"].sum()
    out.append(
        f"5 New product: {stories.NEW_PRODUCT_ID} first sale {first}; monthly units {by_month.astype(int).tolist()}"
    )

    ret = x[x["invoice_type"] == "ZARE"]
    sold_p = sales[sales["product_id"] == stories.RETURNS_SPIKE_PRODUCT_ID]
    ret_p = ret[ret["product_id"] == stories.RETURNS_SPIKE_PRODUCT_ID]
    cut = pd.Timestamp(stories.RETURNS_SPIKE_FROM)
    r_before = len(ret_p[ret_p["date"] < cut]) / max(1, len(sold_p[sold_p["date"] < cut]))
    r_after = len(ret_p[ret_p["date"] >= cut]) / max(1, len(sold_p[sold_p["date"] >= cut]))
    out.append(
        f"6 Returns spike: {stories.RETURNS_SPIKE_PRODUCT_ID} return rate {r_before:.1%} before, {r_after:.1%} from {stories.RETURNS_SPIKE_FROM}"
    )
    out.append(f"7 Stockout: live story, starts {stories.STOCKOUT_FROM} (daily drops, not history)")
    return out


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config

    cfg = load_config()
    parser = argparse.ArgumentParser(description="Generate 18 months of invoice history.")
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=date.fromisoformat(cfg.business["simulation_start"]),
        help="Simulation start (YYYY-MM-DD). History ends the day before.",
    )
    parser.add_argument("--seed", type=int, default=cfg.business["random_seed"])
    parser.add_argument("--out", type=Path, default=Path(cfg.path(cfg.staging_path)))
    args = parser.parse_args(argv)

    df = build_history(args.seed, args.as_of)
    target = write_history(df, args.out, args.seed, args.as_of)
    totals = control_totals(df)
    print(
        f"  {totals['lines']:,} invoice lines, {totals['invoices']:,} documents, "
        f"{df['invoice_date'].min()} to {df['invoice_date'].max()}"
    )
    print(f"  revenue LKR {totals['revenue']:,.2f}   by type {totals['by_invoice_type']}")
    for line in story_summary(df, args.as_of):
        print("  " + line)
    print(f"History written to {target}")


if __name__ == "__main__":
    main()
