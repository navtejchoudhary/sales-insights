"""History generator: 18 months of invoice lines in the reference SAP O2C extract format.

Output (like a source-system backfill):

    staging/history/invoices_YYYYMM.csv     one file per calendar month
    staging/history/history_manifest.json   written LAST: rows, SHA-256, control totals per day

Every line follows the reference extract's 74 columns, in the same order, plus one added
column `reference_invoice_number` (SAP reference document) so returns and cancellations
point at the invoice they reverse.

Document types (sign conventions copied from the reference):
    ZAOR  standard invoice          quantity +, revenue +
    ZFOC  free-of-charge delivery   quantity +, revenue = -tax (company bears the VAT)
    ZARE  return credit memo        quantity -, revenue -, credit_amount +, sales_amount 0
    S1    invoice cancellation      quantity -, revenue -, net_sales 0, sales_amount 0

The planted stories (generator/stories.py) are visible in the numbers.
History is clean of *data-quality* dirt; dirt goes into the daily drops.

Run:
    uv run python -m sales_insights.generator.history --as-of 2026-10-01
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from sales_insights.generator import masters as m
from sales_insights.generator import stories

# The reference extract's columns, in order, plus our one addition at the end
REFERENCE_COLUMNS = [
    "invoice_number",
    "invoice_item",
    "invoice_date",
    "customer_id",
    "customer_name",
    "customer_full_name",
    "customer_group",
    "customer_group_name",
    "country",
    "city",
    "region",
    "product_id",
    "product_group",
    "product_group_name",
    "product_type",
    "product_type_name",
    "product_description",
    "distribution_channel",
    "distribution_channel_name",
    "division",
    "division_name",
    "sales_office",
    "sales_office_name",
    "plant",
    "plant_name",
    "company_code",
    "company_name",
    "sales_org",
    "sales_org_name",
    "invoice_type",
    "invoice_category",
    "order_type",
    "debit_credit_indicator",
    "revenue",
    "gross_revenue",
    "tax_amount",
    "quantity",
    "profit",
    "net_sales",
    "sales_amount",
    "credit_amount",
    "sd_document_category",
    "document_currency",
    "company_currency",
    "billing_status",
    "process_status",
    "last_updated_timestamp",
    "gross_weight",
    "net_weight",
    "weight_unit",
    "batch_number",
    "storage_location",
    "payer_customer_id",
    "fiscal_year",
    "fiscal_period",
    "billing_quantity_unit",
    "base_unit",
    "fiscal_month",
    "netamount_lkr",
    "return_item_processing_type",
    "condition_amount",
    "condition_amount_ccy",
    "sales_order_condition_amount",
    "sales_order_condition_amount_ccy",
    "condition_rate",
    "condition_types",
    "condition_category",
    "condition_application",
    "condition_used",
    "relevant_for_sales",
    "pricing_credit_debit",
    "pricing_credit_debit_item",
    "calculated_sales_unit",
    "cumulative_order_qty_sales_unit",
]
ADDED_COLUMNS = ["reference_invoice_number"]
COLUMNS = REFERENCE_COLUMNS + ADDED_COLUMNS

VAT_RATE = 0.18  # Sri Lanka VAT from Jan 2024; export sales (channel 20) are zero-rated
CONDITION_TYPES = "EK02, VPRS, ZAPR, ZNB1, ZNBT, ZPR0, ZSP1, ZSTR, ZVAT, ZVT1"
MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# Demand model --------------------------------------------------------------
BASE_INVOICES_PER_DAY = 40.0
ANNUAL_GROWTH = 0.06
WEEKDAY_FACTOR = [1.0, 1.0, 1.05, 1.0, 1.1, 0.75, 0.25]  # Mon..Sun
HOLIDAYS = {(4, 13): 0.2, (4, 14): 0.2, (12, 25): 0.3, (1, 1): 0.4}  # New Year, Christmas

# customer_group -> (order frequency weight, mean lines per invoice, mean quantity, discount %)
GROUP_BEHAVIOUR = {
    "20": (3.0, 2.6, 60.0, 12.0),  # Distributors
    "21": (1.6, 1.9, 15.0, 8.0),  # Direct Dealers
    "03": (1.0, 1.3, 4.0, 2.0),  # Domestic
    "24": (1.4, 1.6, 30.0, 10.0),  # Plantations
    "22": (0.6, 1.2, 8.0, 5.0),  # Institution
    "37": (0.5, 1.4, 25.0, 15.0),  # Government
}
MODERN_TRADE_DISCOUNT = 5.0

# Product popularity rank (1 = best seller). Unlisted crop products follow in master order.
POPULARITY_ORDER = [
    stories.HERO_PRODUCT_ID,
    "PRO02.5GR",
    "HGL1LTR",
    "000000000020006092",
    "000000000020006095",
    stories.NEW_PRODUCT_ID,  # ranked as a mid-level seller so its ramp-up is visible
    "HEX004LT",
    "KAR400ML",
    stories.RETURNS_SPIKE_PRODUCT_ID,
    "HGL4LTR",
    "KAR1L",
    "MAN500GR",
]
NEW_PRODUCT_RAMP_DAYS = 120  # new launches take ~4 months to reach full demand
NEW_PRODUCT_CEILING = 0.6  # the story product then settles at 60% of its rank's demand
PRICE_RISE_DEMAND_FACTOR = 0.93  # fewer weedicide units after the price rise

BASE_RETURN_RATE = 0.012
SPIKE_RETURN_RATE = 0.15
CANCEL_RATE = 0.008
FREE_OF_CHARGE_RATE = 0.005
BUILDING_INVOICES_PER_DAY = 1.2


def fiscal(d: date) -> tuple[int, int, str]:
    """Fiscal year (labelled by start year), period (April = 1) and month abbreviation."""
    fy = d.year if d.month >= 4 else d.year - 1
    return fy, (d.month - 4) % 12 + 1, MONTH_ABBR[d.month - 1]


def season_factor(d: date) -> float:
    if d.month in stories.MAHA_PEAK_MONTHS:
        return 1.35
    if d.month in stories.YALA_PEAK_MONTHS:
        return 1.2
    if d.month in (12, 1):
        return 0.8
    return 0.9


def day_factor(d: date, start: date) -> float:
    growth = (1 + ANNUAL_GROWTH) ** ((d - start).days / 365.0)
    return season_factor(d) * WEEKDAY_FACTOR[d.weekday()] * HOLIDAYS.get((d.month, d.day), 1.0) * growth


def list_price_on(product: pd.Series, d: date) -> float:
    """Masters hold today's price; before the price rise the group was 8% cheaper."""
    price = float(product["list_price"])
    if product["product_group"] == stories.PRICE_RISE_PRODUCT_GROUP and d < stories.PRICE_RISE_FROM:
        price /= 1 + stories.PRICE_RISE_PCT / 100
    return price


def _money(x: float) -> str:
    return f"{x:.2f}"


def _qty(x: float) -> str:
    return f"{x:.3f}"


def _stamp(rng: np.random.Generator, d: date, max_days: int = 2) -> str:
    t = datetime.combine(d, datetime.min.time()) + timedelta(
        days=int(rng.integers(0, max_days + 1)), seconds=int(rng.integers(6 * 3600, 22 * 3600))
    )
    return t.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(rng.integers(0, 1000)):03d}"


# ---------------------------------------------------------------------------
# Master preparation
# ---------------------------------------------------------------------------


class MasterIndex:
    """Clean masters, joined and pre-computed for fast lookups while generating."""

    def __init__(self, tables: dict[str, pd.DataFrame]):
        lk = {name: tables[name] for name in tables}
        regions = lk["regions"].set_index("region")
        self.channel_name = dict(lk["distribution_channels"].values)
        self.office_name = dict(lk["sales_offices"][["sales_office", "sales_office_name"]].values)
        self.plant_name = dict(lk["plants"][["plant", "plant_name"]].values)
        self.company_name = dict(lk["companies"][["company_code", "company_name"]].values)

        c = lk["customers"].copy()
        c["province"] = c["region"].map(regions["province"])
        c["created"] = pd.to_datetime(c["created_date"]).dt.date
        freq = c["customer_group"].map(lambda g: GROUP_BEHAVIOUR[g][0]).astype(float)
        freq = freq.where(c["province"] != stories.WEAK_PROVINCE, freq * stories.WEAK_PROVINCE_FACTOR)
        self.customers = c.reset_index(drop=True)
        self.customer_weight = freq.to_numpy()
        self.customer_created = np.array(self.customers["created"].tolist())

        p = lk["products"].copy()
        p["launch"] = pd.to_datetime(p["launch_date"]).dt.date
        self.products = p.set_index("product_id", drop=False)
        crop = p[(p["company_code"] == "2010") & (p["product_type"] != "ZROH")]
        order = POPULARITY_ORDER + [pid for pid in crop["product_id"] if pid not in POPULARITY_ORDER]
        rank = {pid: i + 1 for i, pid in enumerate(order)}
        self.crop_ids = np.array(crop["product_id"].tolist())
        self.crop_base_weight = np.array([1.0 / rank[pid] ** stories.POPULARITY_SKEW for pid in self.crop_ids])
        self.crop_launch = np.array([self.products.at[pid, "launch"] for pid in self.crop_ids])
        self.crop_group = np.array([self.products.at[pid, "product_group"] for pid in self.crop_ids])
        self.raw_ids = p.loc[p["product_type"] == "ZROH", "product_id"].tolist()
        self.building_ids = p.loc[p["company_code"] == "4010", "product_id"].tolist()

    def crop_weights(self, d: date) -> np.ndarray:
        w = self.crop_base_weight.copy()
        age = np.array([(d - launch).days for launch in self.crop_launch])
        w[age < 0] = 0.0
        # Recently launched products ramp up over ~5 months; the story product stays smaller
        recent = age < 600  # launched inside (or just before) the history window
        w[recent] *= np.clip(age[recent] / NEW_PRODUCT_RAMP_DAYS, 0.05, 1.0)
        w[self.crop_ids == stories.NEW_PRODUCT_ID] *= NEW_PRODUCT_CEILING
        if d >= stories.PRICE_RISE_FROM:
            w[self.crop_group == stories.PRICE_RISE_PRODUCT_GROUP] *= PRICE_RISE_DEMAND_FACTOR
        return w / w.sum()


# ---------------------------------------------------------------------------
# Line builder
# ---------------------------------------------------------------------------


def _line(
    mi: MasterIndex,
    rng: np.random.Generator,
    *,
    invoice_number: str,
    item: int,
    d: date,
    cust: pd.Series,
    prod: pd.Series,
    qty: float,
    unit_net: float,
    unit_list: float,
    doc: str,
    reference: str = "",
    stamp: str | None = None,
) -> dict:
    export = cust["distribution_channel"] == "20"
    cost = float(prod["standard_cost"])
    fy, period, mon = fiscal(d)
    gross_list = qty * unit_list

    if doc == "ZAOR":
        revenue = round(qty * unit_net, 2)
        tax = 0.0 if export else round(revenue * VAT_RATE, 2)
        net_sales = sales_amount = revenue
        credit = 0.0
        cat, sdcat, dci, pcd, ret = "L", "M", "DI", "D", ""
    elif doc == "ZFOC":
        tax = round(qty * cost * VAT_RATE, 2)
        revenue = net_sales = sales_amount = -tax
        credit = 0.0
        cat, sdcat, dci, pcd, ret = "L", "M", "DI", "D", ""
    elif doc == "ZARE":
        revenue = -round(abs(qty) * unit_net, 2)
        tax = 0.0 if export else round(abs(revenue) * VAT_RATE, 2)
        net_sales, sales_amount, credit = revenue, 0.0, abs(revenue)
        cat, sdcat, dci, pcd, ret = "L", "O", "CI", "C", "X"
    elif doc == "S1":
        revenue = -round(abs(qty) * unit_net, 2)
        tax = 0.0 if export else round(abs(revenue) * VAT_RATE, 2)
        net_sales = sales_amount = credit = 0.0
        cat, sdcat, dci, pcd, ret = "L", "N", "", "", "X"
    else:
        raise ValueError(doc)

    signed_qty = qty if doc in ("ZAOR", "ZFOC") else -abs(qty)
    profit = round(revenue - signed_qty * cost, 2) if doc != "ZFOC" else round(-(qty * cost) - tax, 2)
    weight = abs(qty) * float(prod["net_weight"])
    unit = prod["sales_unit"]
    has_batch = prod["product_type"] in ("ZFRT", "ZTRD") and rng.random() > 0.18
    storage = "0010" if prod["company_code"] == "4010" else ("0090" if rng.random() < 0.55 else "0030")
    company = prod["company_code"]
    cond = _money(gross_list * (1 + (0 if export else VAT_RATE)))

    return {
        "invoice_number": invoice_number,
        "invoice_item": f"{item * 10:06d}",
        "invoice_date": d.isoformat(),
        "customer_id": cust["customer_id"],
        "customer_name": cust["customer_name"],
        "customer_full_name": cust["customer_full_name"],
        "customer_group": cust["customer_group"],
        "customer_group_name": cust["customer_group_name"],
        "country": cust["country"],
        "city": cust["city"],
        "region": cust["region"],
        "product_id": prod["product_id"],
        "product_group": prod["product_group"],
        "product_group_name": prod["product_group_name"],
        "product_type": prod["product_type"],
        "product_type_name": prod["product_type_name"],
        "product_description": prod["product_description"],
        "distribution_channel": cust["distribution_channel"],
        "distribution_channel_name": mi.channel_name[cust["distribution_channel"]],
        "division": prod["division"],
        "division_name": prod["division_name"],
        "sales_office": cust["sales_office"],
        "sales_office_name": mi.office_name[cust["sales_office"]],
        "plant": prod["plant"],
        "plant_name": mi.plant_name[prod["plant"]],
        "company_code": company,
        "company_name": mi.company_name[company],
        "sales_org": company,
        "sales_org_name": mi.company_name[company],
        "invoice_type": doc,
        "invoice_category": cat,
        "order_type": "O",
        "debit_credit_indicator": dci,
        "revenue": _money(revenue),
        "gross_revenue": "0.00",
        "tax_amount": _money(tax),
        "quantity": _qty(signed_qty),
        "profit": _money(profit),
        "net_sales": _money(net_sales),
        "sales_amount": _money(sales_amount),
        "credit_amount": _money(credit),
        "sd_document_category": sdcat,
        "document_currency": m.CURRENCY,
        "company_currency": m.CURRENCY,
        "billing_status": "A",
        "process_status": "C",
        "last_updated_timestamp": stamp or _stamp(rng, d),
        "gross_weight": _qty(weight * 1.08),
        "net_weight": _qty(weight),
        "weight_unit": "KGM",
        "batch_number": f"{int(rng.integers(100000, 9999999)):07d}" if has_batch else "",
        "storage_location": storage,
        "payer_customer_id": cust["payer_customer_id"],
        "fiscal_year": str(fy),
        "fiscal_period": str(period),
        "billing_quantity_unit": unit,
        "base_unit": prod["base_unit"],
        "fiscal_month": mon,
        "netamount_lkr": _money(revenue),
        "return_item_processing_type": ret,
        "condition_amount": cond,
        "condition_amount_ccy": cond,
        "sales_order_condition_amount": cond,
        "sales_order_condition_amount_ccy": cond,
        "condition_rate": f"{unit_list:.6f}",
        "condition_types": CONDITION_TYPES,
        "condition_category": "Q",
        "condition_application": "V",
        "condition_used": "1",
        "relevant_for_sales": "X",
        "pricing_credit_debit": pcd,
        "pricing_credit_debit_item": dci,
        "calculated_sales_unit": unit,
        "cumulative_order_qty_sales_unit": _qty(abs(qty)),
        "reference_invoice_number": reference,
    }


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------


def history_window(as_of: date) -> tuple[date, date]:
    """History runs from the first day of the window to yesterday."""
    return as_of - timedelta(days=m.HISTORY_DAYS), as_of - timedelta(days=1)


def build_history(seed: int, as_of: date, start: date | None = None) -> pd.DataFrame:
    tables, _ = m.build_masters(seed, as_of, dirt=False)
    mi = MasterIndex(tables)
    window_start, end = history_window(as_of)
    start = start or window_start
    rng = np.random.default_rng(seed + 100)

    lines: list[dict] = []
    sales_meta: list[tuple] = []  # (line index, customer row, product id, qty, unit_net, unit_list, date)
    invoice_seq = {"ZAOR": 9201100000, "S1": 9101100000, "ZARE": 9301100000}

    def next_number(doc: str) -> str:
        invoice_seq[doc] += 1
        return str(invoice_seq[doc])

    customers = mi.customers
    d = start
    while d <= end:
        f = day_factor(d, window_start)
        active = mi.customer_created <= d
        cw = np.where(active, mi.customer_weight, 0.0)
        cw = cw / cw.sum()
        pw = mi.crop_weights(d)

        # Crop-protection invoices (company 2010)
        for _ in range(int(rng.poisson(BASE_INVOICES_PER_DAY * f))):
            ci = int(rng.choice(len(customers), p=cw))
            cust = customers.iloc[ci]
            freq, mean_lines, mean_qty, disc = GROUP_BEHAVIOUR[cust["customer_group"]]
            if cust["distribution_channel"] == "30":
                disc = MODERN_TRADE_DISCOUNT
            n_lines = min(1 + int(rng.poisson(mean_lines - 1)), 8, int((pw > 0).sum()))
            picks = list(rng.choice(mi.crop_ids, size=n_lines, replace=False, p=pw))
            if cust["customer_group"] == "20" and rng.random() < 0.03:
                picks.append(mi.raw_ids[int(rng.integers(len(mi.raw_ids)))])
            number = next_number("ZAOR")
            for item, pid in enumerate(picks, start=1):
                prod = mi.products.loc[pid]
                unit_list = list_price_on(prod, d)
                q = mean_qty / (3.0 if prod["sales_unit"] == "CA" else 1.0)
                qty = float(max(1, round(rng.lognormal(math.log(q), 0.6))))
                doc = "ZFOC" if rng.random() < FREE_OF_CHARGE_RATE else "ZAOR"
                unit_net = unit_list * (1 - (disc + rng.normal(0, 1.0)) / 100)
                lines.append(
                    _line(
                        mi,
                        rng,
                        invoice_number=number,
                        item=item,
                        d=d,
                        cust=cust,
                        prod=prod,
                        qty=qty,
                        unit_net=unit_net,
                        unit_list=unit_list,
                        doc=doc,
                    )
                )
                if doc == "ZAOR":
                    sales_meta.append((len(lines) - 1, ci, pid, qty, unit_net, unit_list, d))

        # Building-solutions invoices (company 4010), rare and large
        for _ in range(int(rng.poisson(BUILDING_INVOICES_PER_DAY * WEEKDAY_FACTOR[d.weekday()]))):
            ci = int(rng.choice(len(customers), p=cw))
            cust = customers.iloc[ci]
            number = next_number("ZAOR")
            n = 1 + int(rng.random() < 0.3)
            for item, pid in enumerate(rng.choice(mi.building_ids, size=n, replace=False), start=1):
                prod = mi.products.loc[pid]
                unit_list = float(prod["list_price"])
                qty = float(max(1, round(rng.lognormal(0.3, 0.5)))) if prod["product_type"] != "ZSRV" else 1.0
                unit_net = unit_list * (1 - rng.uniform(0, 8) / 100)
                lines.append(
                    _line(
                        mi,
                        rng,
                        invoice_number=number,
                        item=item,
                        d=d,
                        cust=cust,
                        prod=prod,
                        qty=qty,
                        unit_net=unit_net,
                        unit_list=unit_list,
                        doc="ZAOR",
                    )
                )
                sales_meta.append((len(lines) - 1, ci, pid, qty, unit_net, unit_list, d))
        d += timedelta(days=1)

    # Returns: some sold lines come back as credit memos 3-30 days later (spike on one product)
    returned: set[str] = set()
    for idx, ci, pid, qty, unit_net, unit_list, sold in sales_meta:
        spike = pid == stories.RETURNS_SPIKE_PRODUCT_ID and sold >= stories.RETURNS_SPIKE_FROM
        if rng.random() >= (SPIKE_RETURN_RATE if spike else BASE_RETURN_RATE):
            continue
        ret_date = sold + timedelta(days=int(rng.integers(3, 31)))
        if ret_date > end:
            continue  # arrives in a future daily drop
        ret_qty = float(max(1, math.ceil(qty * rng.uniform(0.2, 1.0))))
        returned.add(lines[idx]["invoice_number"])
        lines.append(
            _line(
                mi,
                rng,
                invoice_number=next_number("ZARE"),
                item=1,
                d=ret_date,
                cust=customers.iloc[ci],
                prod=mi.products.loc[pid],
                qty=ret_qty,
                unit_net=unit_net,
                unit_list=unit_list,
                doc="ZARE",
                reference=lines[idx]["invoice_number"],
            )
        )

    # Cancellations: a few whole invoices are cancelled 0-5 days later.
    # Only invoices with no returns and no free-of-charge lines, so a cancellation reverses everything.
    has_free = {ln["invoice_number"] for ln in lines if ln["invoice_type"] == "ZFOC"}
    by_invoice: dict[str, list[tuple]] = {}
    for meta in sales_meta:
        by_invoice.setdefault(lines[meta[0]]["invoice_number"], []).append(meta)
    for number in sorted(by_invoice):
        if rng.random() >= CANCEL_RATE or number in returned or number in has_free:
            continue
        metas = by_invoice[number]
        cancel_date = metas[0][6] + timedelta(days=int(rng.integers(0, 6)))
        if cancel_date > end:
            continue
        stamp = _stamp(rng, cancel_date, 0)
        cancel_number = next_number("S1")
        for item, (idx, ci, pid, qty, unit_net, unit_list, _sold) in enumerate(metas, start=1):
            lines[idx]["billing_status"] = "C"  # original is now cancelled...
            lines[idx]["last_updated_timestamp"] = stamp  # ...and was updated at cancellation time
            lines.append(
                _line(
                    mi,
                    rng,
                    invoice_number=cancel_number,
                    item=item,
                    d=cancel_date,
                    cust=customers.iloc[ci],
                    prod=mi.products.loc[pid],
                    qty=qty,
                    unit_net=unit_net,
                    unit_list=unit_list,
                    doc="S1",
                    reference=number,
                    stamp=stamp,
                )
            )

    df = pd.DataFrame(lines, columns=COLUMNS)
    return df.sort_values(["invoice_date", "invoice_number", "invoice_item"], kind="stable").reset_index(drop=True)


# ---------------------------------------------------------------------------
# IO
# ---------------------------------------------------------------------------


def control_totals(df: pd.DataFrame) -> dict:
    num = df[["revenue", "tax_amount", "quantity"]].astype(float)
    by_day = (
        df.assign(revenue=num["revenue"], tax_amount=num["tax_amount"])
        .groupby("invoice_date")
        .agg(
            lines=("invoice_number", "size"),
            invoices=("invoice_number", "nunique"),
            revenue=("revenue", "sum"),
            tax_amount=("tax_amount", "sum"),
        )
    )
    return {
        "lines": int(len(df)),
        "invoices": int(df["invoice_number"].nunique()),
        "revenue": round(float(num["revenue"].sum()), 2),
        "tax_amount": round(float(num["tax_amount"].sum()), 2),
        "by_invoice_type": {k: int(v) for k, v in df["invoice_type"].value_counts().sort_index().items()},
        "by_day": {
            day: {
                "lines": int(r.lines),
                "invoices": int(r.invoices),
                "revenue": round(float(r.revenue), 2),
                "tax_amount": round(float(r.tax_amount), 2),
            }
            for day, r in by_day.iterrows()
        },
    }


def write_history(df: pd.DataFrame, out_dir: Path, seed: int, as_of: date) -> Path:
    target = out_dir / "history"
    target.mkdir(parents=True, exist_ok=True)
    files = {}
    for month, part in df.groupby(df["invoice_date"].str[:7].str.replace("-", "")):
        path = target / f"invoices_{month}.csv"
        part.to_csv(path, index=False, lineterminator="\n")
        files[path.name] = {"rows": int(len(part)), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    start, end = df["invoice_date"].min(), df["invoice_date"].max()
    manifest = {
        "kind": "history",
        "source_system": "SAP S/4HANA (simulated)",
        "as_of": as_of.isoformat(),
        "seed": seed,
        "date_from": start,
        "date_to": end,
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
        default=date.today(),
        help="Reference date (YYYY-MM-DD). History ends the day before.",
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
