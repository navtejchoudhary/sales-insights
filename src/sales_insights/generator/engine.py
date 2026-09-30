"""Shared invoice engine used by the history backfill AND the daily simulator.

Why a shared engine: any single day must be reproducible on its own ("same seed + same
date = identical files"), and history and daily files must agree about every invoice.
So every random choice is seeded from what it is about:

    sales on day D            rng([seed, 1, D])
    fate of invoice N         rng([seed, 2, N])   -> returned? cancelled? price-corrected? late?
    fate of line (N, item)    rng([seed, 3, N, item])

Invoice numbers are derived, not counted, so no global counter is needed (10 digits):

    ZAOR / ZFOC invoice    9 + yymmdd + nnn         e.g. 9261023004
    ZARE return memo       7 + <invoice digits 2-10>
    S1   cancellation      8 + <invoice digits 2-10>
    ZACR price correction  6 + <invoice digits 2-10>
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from sales_insights.generator import masters as m
from sales_insights.generator import stories

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
PRICE_CORRECTION_RATE = 0.004
LATE_ARRIVAL_RATE = 0.03  # live days only: invoice reaches the feed 1-7 days after its date
STOCKOUT_SUBSTITUTE_ID = "MC6004LT"  # during the stockout some buyers switch to the 4-litre pack
STOCKOUT_SUBSTITUTE_SHARE = 0.4
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
    amount: float = 0.0,
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
    elif doc == "ZACR":  # price correction: money only, no goods move
        revenue = -round(amount, 2)
        tax = 0.0 if export else round(abs(revenue) * VAT_RATE, 2)
        net_sales, sales_amount, credit = revenue, 0.0, abs(revenue)
        cat, sdcat, dci, pcd, ret = "A", "O", "CI", "C", "X"
        qty = 0.0
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
# Per-day sales and per-invoice fates (the reproducibility core)
# ---------------------------------------------------------------------------


@dataclass
class Sale:
    """One sold line plus what is needed to reverse or correct it later."""

    line: dict
    ci: int
    pid: str
    qty: float
    unit_net: float
    unit_list: float
    d: date
    free: bool = False


@dataclass
class Fate:
    """What happens to an invoice after it is raised. Derived only from seed + invoice number."""

    delay_days: int = 0  # days between invoice date and arrival in the feed (live days only)
    changes: list[dict] | None = None  # ZARE / S1 / ZACR lines, each with its own invoice_date
    cancel_date: date | None = None
    cancel_stamp: str = ""


def history_window(as_of: date) -> tuple[date, date]:
    """History runs from the first day of the window to the day before as_of."""
    return as_of - timedelta(days=m.HISTORY_DAYS), as_of - timedelta(days=1)


def sales_for_day(mi: MasterIndex, seed: int, d: date, window_start: date) -> list[Sale]:
    """All invoice lines raised on day d. Same inputs -> same output, whatever else was generated."""
    rng = np.random.default_rng([seed, 1, d.toordinal()])
    out: list[Sale] = []
    customers = mi.customers
    active = mi.customer_created <= d
    cw = np.where(active, mi.customer_weight, 0.0)
    cw = cw / cw.sum()
    pw = mi.crop_weights(d)
    stockout = d >= stories.STOCKOUT_FROM
    seq = 0

    def number() -> str:
        nonlocal seq
        seq += 1
        return f"9{d:%y%m%d}{seq:03d}"

    # Crop-protection invoices (company 2010)
    for _ in range(int(rng.poisson(BASE_INVOICES_PER_DAY * day_factor(d, window_start)))):
        ci = int(rng.choice(len(customers), p=cw))
        cust = customers.iloc[ci]
        _freq, mean_lines, mean_qty, disc = GROUP_BEHAVIOUR[cust["customer_group"]]
        if cust["distribution_channel"] == "30":
            disc = MODERN_TRADE_DISCOUNT
        n_lines = min(1 + int(rng.poisson(mean_lines - 1)), 8, int((pw > 0).sum()))
        picks = [str(p) for p in rng.choice(mi.crop_ids, size=n_lines, replace=False, p=pw)]
        if cust["customer_group"] == "20" and rng.random() < 0.03:
            picks.append(mi.raw_ids[int(rng.integers(len(mi.raw_ids)))])
        qty_draws = [float(rng.lognormal(0, 0.6)) for _ in picks]  # drawn up front: stable stream

        # Story 7: the hero product is out of stock in one province; some buyers switch pack size
        if stockout and cust["province"] == stories.STOCKOUT_PROVINCE and stories.HERO_PRODUCT_ID in picks:
            i = picks.index(stories.HERO_PRODUCT_ID)
            if rng.random() < STOCKOUT_SUBSTITUTE_SHARE and STOCKOUT_SUBSTITUTE_ID not in picks:
                picks[i] = STOCKOUT_SUBSTITUTE_ID
                qty_draws[i] /= 3.0
            else:
                picks.pop(i)
                qty_draws.pop(i)
        if not picks:
            continue

        inv = number()
        for item, (pid, draw) in enumerate(zip(picks, qty_draws, strict=True), start=1):
            prod = mi.products.loc[pid]
            unit_list = list_price_on(prod, d)
            q = mean_qty / (3.0 if prod["sales_unit"] == "CA" else 1.0)
            qty = float(max(1, round(q * draw)))
            free = rng.random() < FREE_OF_CHARGE_RATE
            unit_net = unit_list * (1 - (disc + rng.normal(0, 1.0)) / 100)
            line = _line(
                mi,
                rng,
                invoice_number=inv,
                item=item,
                d=d,
                cust=cust,
                prod=prod,
                qty=qty,
                unit_net=unit_net,
                unit_list=unit_list,
                doc="ZFOC" if free else "ZAOR",
            )
            out.append(Sale(line, ci, pid, qty, unit_net, unit_list, d, free))

    # Building-solutions invoices (company 4010), rare and large
    for _ in range(int(rng.poisson(BUILDING_INVOICES_PER_DAY * WEEKDAY_FACTOR[d.weekday()]))):
        ci = int(rng.choice(len(customers), p=cw))
        cust = customers.iloc[ci]
        inv = number()
        n = 1 + int(rng.random() < 0.3)
        for item, pid in enumerate(rng.choice(mi.building_ids, size=n, replace=False), start=1):
            prod = mi.products.loc[str(pid)]
            unit_list = float(prod["list_price"])
            qty = float(max(1, round(rng.lognormal(0.3, 0.5)))) if prod["product_type"] != "ZSRV" else 1.0
            unit_net = unit_list * (1 - rng.uniform(0, 8) / 100)
            line = _line(
                mi,
                rng,
                invoice_number=inv,
                item=item,
                d=d,
                cust=cust,
                prod=prod,
                qty=qty,
                unit_net=unit_net,
                unit_list=unit_list,
                doc="ZAOR",
            )
            out.append(Sale(line, ci, str(pid), qty, unit_net, unit_list, d))
    return out


def group_by_invoice(sales: list[Sale]) -> dict[str, list[Sale]]:
    grouped: dict[str, list[Sale]] = {}
    for s in sales:
        grouped.setdefault(s.line["invoice_number"], []).append(s)
    return grouped


def invoice_fate(mi: MasterIndex, seed: int, number: str, lines: list[Sale], live_from: date) -> Fate:
    """Returns, cancellation, price correction and late arrival for one invoice.

    Every random number is drawn up front in a fixed order, so the fate never depends on
    which branch runs. Invoices dated before `live_from` (history) never arrive late.
    """
    rng = np.random.default_rng([seed, 2, int(number)])
    return_offset = int(rng.integers(3, 31))
    cancel_u, cancel_offset = rng.random(), int(rng.integers(0, 6))
    corr_u, corr_offset, corr_pick, corr_pct = rng.random(), int(rng.integers(2, 15)), rng.random(), rng.uniform(3, 10)
    late_u, late_days = rng.random(), int(rng.integers(1, 8))

    d = lines[0].d
    delay = late_days if (d >= live_from and late_u < LATE_ARRIVAL_RATE) else 0
    arrives = d + timedelta(days=delay)
    fate = Fate(delay_days=delay, changes=[])
    tail = number[1:]

    def when(offset: int) -> date:  # a change can never reach the feed before its invoice
        return max(d + timedelta(days=offset), arrives)

    # Returns: each sold line independently; one return memo per invoice
    returned = []
    for s in lines:
        if s.free:
            continue
        lr = np.random.default_rng([seed, 3, int(number), int(s.line["invoice_item"])])
        spike = s.pid == stories.RETURNS_SPIKE_PRODUCT_ID and d >= stories.RETURNS_SPIKE_FROM
        u, share = lr.random(), lr.uniform(0.2, 1.0)
        if u < (SPIKE_RETURN_RATE if spike else BASE_RETURN_RATE):
            returned.append((s, float(max(1, math.ceil(s.qty * share)))))
    if returned:
        rd = when(return_offset)
        for item, (s, rq) in enumerate(returned, start=1):
            fate.changes.append(
                _line(
                    mi,
                    rng,
                    invoice_number=f"7{tail}",
                    item=item,
                    d=rd,
                    cust=mi.customers.iloc[s.ci],
                    prod=mi.products.loc[s.pid],
                    qty=rq,
                    unit_net=s.unit_net,
                    unit_list=s.unit_list,
                    doc="ZARE",
                    reference=number,
                )
            )

    # Cancellation: whole invoice, only if nothing was returned and no free-of-charge line
    if cancel_u < CANCEL_RATE and not returned and not any(s.free for s in lines):
        fate.cancel_date = when(cancel_offset)
        fate.cancel_stamp = _stamp(rng, fate.cancel_date, 0)
        for item, s in enumerate(lines, start=1):
            fate.changes.append(
                _line(
                    mi,
                    rng,
                    invoice_number=f"8{tail}",
                    item=item,
                    d=fate.cancel_date,
                    cust=mi.customers.iloc[s.ci],
                    prod=mi.products.loc[s.pid],
                    qty=s.qty,
                    unit_net=s.unit_net,
                    unit_list=s.unit_list,
                    doc="S1",
                    reference=number,
                    stamp=fate.cancel_stamp,
                )
            )
        return fate

    # Price correction: a credit for a pricing error on one line
    paid = [s for s in lines if not s.free]
    if paid and corr_u < PRICE_CORRECTION_RATE:
        s = paid[int(corr_pick * len(paid))]
        amount = abs(float(s.line["revenue"])) * corr_pct / 100
        fate.changes.append(
            _line(
                mi,
                rng,
                invoice_number=f"6{tail}",
                item=1,
                d=when(corr_offset),
                cust=mi.customers.iloc[s.ci],
                prod=mi.products.loc[s.pid],
                qty=0.0,
                unit_net=s.unit_net,
                unit_list=s.unit_list,
                doc="ZACR",
                reference=number,
                amount=amount,
            )
        )
    return fate


def control_totals(df: pd.DataFrame, by: str = "invoice_date") -> dict:
    """Line count, document count, revenue and tax: overall, by invoice type and by day."""
    if df.empty:
        return {"lines": 0, "invoices": 0, "revenue": 0.0, "tax_amount": 0.0, "by_invoice_type": {}, f"by_{by}": {}}
    num = df.assign(revenue=df["revenue"].astype(float), tax_amount=df["tax_amount"].astype(float))
    grouped = num.groupby(by).agg(
        lines=("invoice_number", "size"),
        invoices=("invoice_number", "nunique"),
        revenue=("revenue", "sum"),
        tax_amount=("tax_amount", "sum"),
    )
    return {
        "lines": int(len(df)),
        "invoices": int(df["invoice_number"].nunique()),
        "revenue": round(float(num["revenue"].sum()), 2),
        "tax_amount": round(float(num["tax_amount"].sum()), 2),
        "by_invoice_type": {k: int(v) for k, v in df["invoice_type"].value_counts().sort_index().items()},
        f"by_{by}": {
            k: {
                "lines": int(r.lines),
                "invoices": int(r.invoices),
                "revenue": round(float(r.revenue), 2),
                "tax_amount": round(float(r.tax_amount), 2),
            }
            for k, r in grouped.iterrows()
        },
    }
