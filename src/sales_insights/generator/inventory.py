"""Inventory (stock) generator for the inventory area: an SAP MM-style extract that agrees with sales.

Plan: docs/multiple_gold_layers.md (section 4). Output (the inventory area's staging folder):

    masters/locations.csv + masters_manifest.json           17 stock locations (written once)
    history/movements_YYYYMM.csv, stock_YYYYMM.csv          18 months, one file per month and kind
    history/history_manifest.json                           written LAST
    business_date=YYYY-MM-DD/movements_YYYYMMDD.csv         stock movements delivered that day
    business_date=YYYY-MM-DD/stock_YYYYMMDD.csv             closing stock of that day
    business_date=YYYY-MM-DD/manifest_YYYYMMDD.json         written LAST: rows, SHA-256, control totals, dirt

Where stock is held
    plant stores   2010-0090 Distribution Hub (crop products), 2010-0010 Main Store (raw materials),
                   2010-0032 Returns Store (customer returns), 4020-0010 Main Store (building solutions)
    depots         one per sales office (storage locations 8001-8011 of plant 2010), supplied by the hub

How stock moves (SAP movement types)
    561  opening balance on the first history day      313 / 315  hub -> depot transfer (out / in, in transit between)
    101  goods receipt at a plant store                 601  goods issue for an invoice (ZAOR, ZFOC) at the
    602  cancellation (S1) puts the goods back at            customer's sales-office depot
         the plant store                                653  customer return (ZARE) into the returns store
    701 / 702  month-end stock count difference (gain / loss); price corrections (ZACR) move no goods

Consistency with sales: every 601 / 602 / 653 comes from a sales line the engine generates, on that line's
invoice date, and carries its invoice_number + invoice_item. Services (ZSRV) hold no stock.

Reproducibility (same rule as the sales generator): no random stream is shared across days. Sales come from
generator/engine.py; receipts, lead times and counts use generators seeded by date ([seed, 21|22|23, day]); late
delivery of a movement line is seeded by its document and item ([seed, 24, document, item]); daily dirt by
[seed, 29, day]. Stock on a day depends on every earlier day, so building day D replays from the first history
day: deterministic, so any day is byte-identical whatever else was generated. The sales files are not touched.

Run:
    uv run python -m sales_insights.generator.inventory --history                       # masters + 18 months
    uv run python -m sales_insights.generator.inventory --from 2026-10-01 --to 2026-10-13
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from sales_insights.generator import engine as e
from sales_insights.generator import masters as m
from sales_insights.generator import stories

SOURCE_SYSTEM = "SAP S/4HANA MM (simulated)"

HUB = "2010-0090"
RAW_STORE = "2010-0010"
RETURNS_STORE = "2010-0032"
BUILDING_STORE = "4020-0010"
FIRST_DEPOT_SLOC = 8001

MOVEMENT_TYPES = {  # code -> (text, debit/credit indicator: S = into stock, H = out of stock)
    "561": ("Initial entry of stock balances", "S"),
    "101": ("Goods receipt", "S"),
    "313": ("Stock transfer: removal (in transit)", "H"),
    "315": ("Stock transfer: placement from transit", "S"),
    "601": ("Goods issue for delivery", "H"),
    "602": ("Reversal of goods issue", "S"),
    "653": ("Customer return to unrestricted stock", "S"),
    "701": ("Physical inventory: gain", "S"),
    "702": ("Physical inventory: loss", "H"),
}

LOCATION_COLUMNS = [
    "location_id",
    "location_name",
    "location_type",
    "company_code",
    "plant",
    "storage_location",
    "sales_office",
    "region",
    "province",
    "supplying_location_id",
    "last_updated_timestamp",
]
MOVEMENT_COLUMNS = [
    "material_document",
    "material_document_item",
    "posting_date",
    "movement_type",
    "movement_type_text",
    "debit_credit_indicator",
    "company_code",
    "plant",
    "storage_location",
    "location_id",
    "product_id",
    "quantity",
    "base_unit",
    "sales_office",
    "reference_invoice_number",
    "reference_invoice_item",
    "counterpart_location_id",
    "last_updated_timestamp",
]
STOCK_COLUMNS = [
    "snapshot_date",
    "company_code",
    "plant",
    "storage_location",
    "location_id",
    "product_id",
    "unrestricted_qty",
    "in_transit_qty",
    "base_unit",
    "last_updated_timestamp",
]

# Replenishment policy (min/max on average daily issues of the last DEMAND_WINDOW_DAYS days)
DEMAND_WINDOW_DAYS = 28
DEPOT_REORDER_DAYS, DEPOT_TARGET_DAYS = 7, 21
STORE_REORDER_DAYS, STORE_TARGET_DAYS = 14, 42
TRANSFER_LEAD_DAYS = (1, 3)  # inclusive range
RECEIPT_LEAD_DAYS = (4, 8)
COUNT_DIFF_SHARE = 0.05  # share of stock positions with a difference at each month-end count

# Daily-feed dirt (history is clean, as for sales). Every injected row is listed in the manifest.
LATE_MOVEMENT_RATE = 0.02
MAX_LATE_DAYS = 2
DUPLICATE_RATE = 0.003
UNIT_VARIANT_RATE = 0.02
UNKNOWN_LOCATION_RATE = 0.001
BLANK_MOVEMENT_TYPE_RATE = 0.001
NEGATIVE_STOCK_RATE = 0.001
UNKNOWN_LOCATION_ID = "2010-9999"
UNIT_VARIANTS = {
    "CA": ["CS", "case"],
    "BO": ["BTL", "bottle"],
    "BG": ["BAG", "bag"],
    "EA": ["PC", "ea"],
    "KGM": ["KG", "kg"],
    "LTR": ["L", "ltr"],
}


def _qty(x: float) -> str:
    return f"{x:.3f}"


def _last_day_of_month(d: date) -> bool:
    return (d + timedelta(days=1)).day == 1


# ---------------------------------------------------------------------------
# Locations master
# ---------------------------------------------------------------------------


def build_locations(as_of: date) -> pd.DataFrame:
    """Plant storage locations (from the sales masters' fixed lists) plus one depot per sales office."""
    plants = {p: (name, region, company) for p, name, region, company in m.PLANTS}
    province = {code: prov for code, _district, prov, _share in m.DISTRICTS}
    stamp = f"{e.history_window(as_of)[0].isoformat()} 00:00:00.000"
    rows = []
    for plant, sloc, name in m.STORAGE_LOCATIONS:
        plant_name, region, company = plants[plant]
        rows.append(
            {
                "location_id": f"{plant}-{sloc}",
                "location_name": f"{plant_name} - {name}",
                "location_type": "plant_store",
                "company_code": company,
                "plant": plant,
                "storage_location": sloc,
                "sales_office": "",
                "region": region,
                "province": province[region],
                "supplying_location_id": "",
                "last_updated_timestamp": stamp,
            }
        )
    for i, (office, office_name, region, prov) in enumerate(m.SALES_OFFICES):
        sloc = str(FIRST_DEPOT_SLOC + i)
        rows.append(
            {
                "location_id": f"2010-{sloc}",
                "location_name": f"{office_name} Depot",
                "location_type": "depot",
                "company_code": "2010",
                "plant": "2010",
                "storage_location": sloc,
                "sales_office": office,
                "region": region,
                "province": prov,
                "supplying_location_id": HUB,
                "last_updated_timestamp": stamp,
            }
        )
    return pd.DataFrame(rows, columns=LOCATION_COLUMNS)


def write_locations(locations: pd.DataFrame, out_dir: Path, seed: int, as_of: date) -> Path:
    target = out_dir / "masters"
    target.mkdir(parents=True, exist_ok=True)
    path = target / "locations.csv"
    locations.to_csv(path, index=False, lineterminator="\n")
    manifest = {
        "kind": "masters",
        "area": "inventory",
        "source_system": SOURCE_SYSTEM,
        "as_of": as_of.isoformat(),
        "seed": seed,
        "files": {path.name: {"rows": len(locations), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}},
        "dirt": [],
    }
    (target / "masters_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return target


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------


@dataclass
class _Pending:
    arrive: date
    location: str
    product_id: str
    qty: int
    source: str  # HUB for a transfer (stock in transit), "" for a receipt from outside


@dataclass
class InventoryDrop:
    business_date: date
    movements: pd.DataFrame
    stock: pd.DataFrame
    dirt: list[dict] = field(default_factory=list)
    control_totals: dict = field(default_factory=dict)


class InventorySimulator:
    """Replays stock day by day from the first history day; keeps every day's movements and closing stock."""

    def __init__(self, seed: int, as_of: date):
        self.seed, self.as_of = seed, as_of
        tables, _ = m.build_masters(seed, as_of, dirt=False)
        self.mi = e.MasterIndex(tables)
        self.window_start, _ = e.history_window(as_of)
        self.locations = build_locations(as_of)
        self.loc = {
            r.location_id: {"company_code": r.company_code, "plant": r.plant, "storage_location": r.storage_location}
            for r in self.locations.itertuples(index=False)
        }
        depots = self.locations[self.locations["location_type"] == "depot"]
        self.depot_of_office = dict(zip(depots["sales_office"], depots["location_id"], strict=True))
        self.depots = sorted(depots["location_id"])
        self.story_depots = sorted(depots.loc[depots["province"] == stories.STOCKOUT_PROVINCE, "location_id"])
        self.last_transfer_day = stories.STOCKOUT_FROM - timedelta(days=stories.STOCKOUT_LAST_TRANSFER_DAYS)

        p = tables["products"]
        self.unit = dict(zip(p["product_id"], p["base_unit"], strict=True))
        self.launch = {pid: date.fromisoformat(x) for pid, x in zip(p["product_id"], p["launch_date"], strict=True)}
        self.home: dict[str, str | None] = {}
        for pid, ptype, company in zip(p["product_id"], p["product_type"], p["company_code"], strict=True):
            if ptype == "ZSRV":
                self.home[pid] = None  # services: nothing to store
            elif company == "4010":
                self.home[pid] = BUILDING_STORE
            elif ptype == "ZROH":
                self.home[pid] = RAW_STORE
            else:
                self.home[pid] = "depot"
        self.crop = sorted(pid for pid, h in self.home.items() if h == "depot")
        self.raw = sorted(pid for pid, h in self.home.items() if h == RAW_STORE)
        self.building = sorted(pid for pid, h in self.home.items() if h == BUILDING_STORE)
        # every stock position that gets a row in the daily stock file (once its product has launched)
        self.positions = sorted(
            [(d, pid) for d in self.depots for pid in self.crop]
            + [(HUB, pid) for pid in self.crop]
            + [(RETURNS_STORE, pid) for pid in self.crop + self.raw]
            + [(RAW_STORE, pid) for pid in self.raw]
            + [(BUILDING_STORE, pid) for pid in self.building]
        )

        self._sales: dict[date, list[e.Sale]] = {}
        self._changes: dict[date, list[dict]] = defaultdict(list)
        self.on_hand: dict[tuple[str, str], int] = defaultdict(int)
        self.pending: list[_Pending] = []
        self.demand: dict[tuple[str, str], deque] = {}
        self.movements: dict[date, list[dict]] = {}
        self.snapshots: dict[date, list[dict]] = {}
        self._next = self.window_start

    # -- sales the engine generates -------------------------------------------------

    def sales_on(self, d: date) -> list[e.Sale]:
        if d not in self._sales:
            self._sales[d] = e.sales_for_day(self.mi, self.seed, d, self.window_start)
        return self._sales[d]

    def issue_location(self, line: dict) -> str | None:
        home = self.home[line["product_id"]]
        return self.depot_of_office[line["sales_office"]] if home == "depot" else home

    def issues_on(self, d: date) -> dict[tuple[str, str], int]:
        """Goods issued on day d per (location, product), from the invoice lines raised that day."""
        out: dict[tuple[str, str], int] = defaultdict(int)
        for s in self.sales_on(d):
            loc = self.issue_location(s.line)
            if loc:
                out[(loc, s.pid)] += round(abs(float(s.line["quantity"])))
        return out

    # -- helpers ---------------------------------------------------------------------

    def _avg(self, key: tuple[str, str]) -> float:
        q = self.demand.get(key)
        return sum(q) / len(q) if q else 0.0

    def _peak(self, key: tuple[str, str]) -> float:
        """Largest day of the demand window: safety stock against one big distributor order."""
        q = self.demand.get(key)
        return max(q) if q else 0.0

    def _network_avg(self, pid: str) -> float:
        return sum(self._avg((d, pid)) for d in self.depots)

    def _in_transit(self, loc: str, pid: str) -> int:
        return sum(p.qty for p in self.pending if p.source and p.location == loc and p.product_id == pid)

    def _on_order(self, loc: str, pid: str) -> int:
        return sum(p.qty for p in self.pending if not p.source and p.location == loc and p.product_id == pid)

    def _frozen(self, loc: str, pid: str, d: date) -> bool:
        """Story I1: from the last transfer on, the story depots get nothing more of the hero product."""
        return pid == stories.HERO_PRODUCT_ID and loc in self.story_depots and d >= self.last_transfer_day

    # -- one day ---------------------------------------------------------------------

    def _step(self, d: date) -> None:
        rng = np.random.default_rng([self.seed, 21, d.toordinal()])
        stamps = np.random.default_rng([self.seed, 23, d.toordinal()])
        mv: list[dict] = []
        seq = 0

        def doc() -> str:
            nonlocal seq
            seq += 1
            if seq > 999:
                raise AssertionError(f"more than 999 material documents on {d}")
            return f"4{d:%y%m%d}{seq:03d}"

        def post(document, item, mtype, loc, pid, qty, office="", ref=("", ""), counterpart=""):
            row = self.loc[loc]
            text, dc = MOVEMENT_TYPES[mtype]
            stamp = f"{d.isoformat()} {int(stamps.integers(6, 22)):02d}:{int(stamps.integers(0, 60)):02d}:"
            stamp += f"{int(stamps.integers(0, 60)):02d}.{int(stamps.integers(0, 1000)):03d}"
            mv.append(
                {
                    "material_document": document,
                    "material_document_item": f"{item:04d}",
                    "posting_date": d.isoformat(),
                    "movement_type": mtype,
                    "movement_type_text": text,
                    "debit_credit_indicator": dc,
                    "company_code": row["company_code"],
                    "plant": row["plant"],
                    "storage_location": row["storage_location"],
                    "location_id": loc,
                    "product_id": pid,
                    "quantity": _qty(qty),
                    "base_unit": self.unit[pid],
                    "sales_office": office,
                    "reference_invoice_number": ref[0],
                    "reference_invoice_item": ref[1],
                    "counterpart_location_id": counterpart,
                    "last_updated_timestamp": stamp,
                }
            )
            self.on_hand[(loc, pid)] += qty

        def receive(loc: str, pid: str, qty: int) -> None:
            post(doc(), 1, "101", loc, pid, qty)

        def transfer_now(depot: str, pid: str, qty: int) -> None:
            if self.on_hand[(HUB, pid)] < qty:  # the hub never goes negative: emergency receipt first
                receive(
                    HUB, pid, qty - self.on_hand[(HUB, pid)] + math.ceil(STORE_REORDER_DAYS * self._network_avg(pid))
                )
            post(doc(), 1, "313", HUB, pid, -qty, counterpart=depot)
            post(doc(), 1, "315", depot, pid, qty, counterpart=HUB)

        # 0. Opening balances on the first history day (sized on the first four weeks of issues)
        if d == self.window_start:
            first = [self.issues_on(d + timedelta(days=i)) for i in range(DEMAND_WINDOW_DAYS)]
            est = {key: sum(day.get(key, 0) for day in first) / DEMAND_WINDOW_DAYS for key in self.positions}
            for loc, pid in self.positions:
                if self.launch[pid] <= d and loc != RETURNS_STORE and loc != HUB:
                    self.demand[(loc, pid)] = deque([est[(loc, pid)]] * DEMAND_WINDOW_DAYS, maxlen=DEMAND_WINDOW_DAYS)
            opening = doc()
            item = 0
            for loc, pid in self.positions:
                if self.launch[pid] > d or loc == RETURNS_STORE:
                    continue
                days = DEPOT_TARGET_DAYS if loc in self.depots else STORE_TARGET_DAYS
                rate = self._network_avg(pid) if loc == HUB else est[(loc, pid)]
                qty = math.ceil(days * rate)
                if qty > 0:
                    item += 1
                    post(opening, item, "561", loc, pid, qty)

        # 1. Arrivals: transfers placed into depots, receipts at plant stores
        for p in sorted((p for p in self.pending if p.arrive == d), key=lambda p: (p.location, p.product_id)):
            if p.source:
                post(doc(), 1, "315", p.location, p.product_id, p.qty, counterpart=p.source)
            else:
                receive(p.location, p.product_id, p.qty)
        self.pending = [p for p in self.pending if p.arrive != d]
        if d == stories.OVERSTOCK_RECEIPT_DATE:  # story I3: one oversized purchase
            key = (BUILDING_STORE, stories.OVERSTOCK_PRODUCT_ID)
            receive(*key, math.ceil(stories.OVERSTOCK_DAYS_OF_COVER * max(self._avg(key), 0.2)))

        # 2. Story I1: the last transfer of the hero product to the story depots, sized to last exactly
        #    until the day before the stock-out (the issues are known: sales are generated from the seed)
        if d == self.last_transfer_day:
            for depot in self.story_depots:
                key = (depot, stories.HERO_PRODUCT_ID)
                need = 0
                for i in range(stories.STOCKOUT_LAST_TRANSFER_DAYS):
                    need += self.issues_on(d + timedelta(days=i)).get(key, 0)
                qty = need - self.on_hand[key] - self._in_transit(*key)
                if qty > 0:
                    transfer_now(depot, stories.HERO_PRODUCT_ID, qty)

        # 3. Sales: invoice lines issue goods; cancellations and returns raised today bring goods back
        for number, lines in e.group_by_invoice(self.sales_on(d)).items():
            for c in e.invoice_fate(self.mi, self.seed, number, lines, live_from=self.as_of).changes:
                self._changes[date.fromisoformat(c["invoice_date"])].append(c)
        issues = self.issues_on(d)
        for (loc, pid), qty in sorted(issues.items()):
            short = qty - self.on_hand[(loc, pid)]
            if short > 0:  # stock never goes negative: emergency replenishment before the issue
                if loc in self.depots:
                    transfer_now(loc, pid, short + math.ceil(DEPOT_REORDER_DAYS * self._avg((loc, pid))))
                else:
                    receive(loc, pid, short + math.ceil(STORE_REORDER_DAYS * self._avg((loc, pid))))
        for lines in e.group_by_invoice(self.sales_on(d)).values():
            document, item = None, 0
            for s in lines:
                loc = self.issue_location(s.line)
                if not loc:
                    continue
                document = document or doc()
                item += 1
                qty = round(abs(float(s.line["quantity"])))
                ref = (s.line["invoice_number"], s.line["invoice_item"])
                post(document, item, "601", loc, s.pid, -qty, s.line["sales_office"], ref)
        for number in sorted({c["invoice_number"] for c in self._changes.get(d, [])}):
            document, item = None, 0
            for c in sorted(
                (c for c in self._changes[d] if c["invoice_number"] == number), key=lambda c: c["invoice_item"]
            ):
                home = self.home[c["product_id"]]
                if c["invoice_type"] not in ("ZARE", "S1") or home is None:
                    continue  # price corrections move no goods; services hold no stock
                document = document or doc()
                item += 1
                qty = round(abs(float(c["quantity"])))
                ref = (c["invoice_number"], c["invoice_item"])
                if c["invoice_type"] == "ZARE":
                    loc = BUILDING_STORE if home == BUILDING_STORE else RETURNS_STORE
                    post(document, item, "653", loc, c["product_id"], qty, c["sales_office"], ref)
                else:
                    loc = HUB if home == "depot" else home
                    post(document, item, "602", loc, c["product_id"], qty, c["sales_office"], ref)
        self._changes.pop(d, None)

        # 4. Demand memory (issues per day, last DEMAND_WINDOW_DAYS days)
        for key in self.positions:
            if key[0] in (HUB, RETURNS_STORE) or self.launch[key[1]] > d:
                continue
            self.demand.setdefault(key, deque(maxlen=DEMAND_WINDOW_DAYS)).append(issues.get(key, 0))

        # 5. End-of-day review: depots order from the hub; plant stores order from outside
        for depot in self.depots:
            for pid in self.crop:
                if self.launch[pid] > d or self._frozen(depot, pid, d):
                    continue
                avg, peak = self._avg((depot, pid)), self._peak((depot, pid))
                position = self.on_hand[(depot, pid)] + self._in_transit(depot, pid)
                if avg <= 0 or position >= DEPOT_REORDER_DAYS * avg + peak:
                    continue
                qty = min(math.ceil(DEPOT_TARGET_DAYS * avg + peak - position), self.on_hand[(HUB, pid)])
                if qty > 0:
                    lead = int(rng.integers(TRANSFER_LEAD_DAYS[0], TRANSFER_LEAD_DAYS[1] + 1))
                    post(doc(), 1, "313", HUB, pid, -qty, counterpart=depot)
                    self.pending.append(_Pending(d + timedelta(days=lead), depot, pid, qty, HUB))
        for loc, pid in (
            [(HUB, x) for x in self.crop]
            + [(RAW_STORE, x) for x in self.raw]
            + [(BUILDING_STORE, x) for x in self.building]
        ):
            if self.launch[pid] > d:
                continue
            if loc == HUB:  # the hub's demand is every depot's; its safety stock covers each depot's peak day
                avg, peak = self._network_avg(pid), sum(self._peak((x, pid)) for x in self.depots)
            else:
                avg, peak = self._avg((loc, pid)), self._peak((loc, pid))
            position = self.on_hand[(loc, pid)] + self._on_order(loc, pid)
            if avg > 0 and position < STORE_REORDER_DAYS * avg + peak:
                lead = int(rng.integers(RECEIPT_LEAD_DAYS[0], RECEIPT_LEAD_DAYS[1] + 1))
                qty = math.ceil(STORE_TARGET_DAYS * avg + peak - position)
                self.pending.append(_Pending(d + timedelta(days=lead), loc, pid, qty, ""))

        # 6. Month-end stock count: a few positions differ slightly from the books
        if _last_day_of_month(d):
            counts = np.random.default_rng([self.seed, 22, d.toordinal()])
            document, item = None, 0
            for loc, pid in self.positions:
                u, pct = counts.random(), counts.uniform(-0.02, 0.01)  # drawn for every position: stable stream
                held = self.on_hand[(loc, pid)]
                if loc == RETURNS_STORE or self._frozen(loc, pid, d) or held <= 0 or u >= COUNT_DIFF_SHARE:
                    continue
                diff = round(held * pct) or (1 if pct > 0 else -1)
                diff = max(diff, -held)
                document = document or doc()
                item += 1
                post(document, item, "701" if diff > 0 else "702", loc, pid, diff)

        # 7. Closing stock
        snap = []
        for loc, pid in self.positions:
            if self.launch[pid] > d:
                continue
            row = self.loc[loc]
            held = self.on_hand[(loc, pid)]
            if held < 0:
                raise AssertionError(f"negative stock {loc} {pid} on {d}")
            snap.append(
                {
                    "snapshot_date": d.isoformat(),
                    "company_code": row["company_code"],
                    "plant": row["plant"],
                    "storage_location": row["storage_location"],
                    "location_id": loc,
                    "product_id": pid,
                    "unrestricted_qty": _qty(held),
                    "in_transit_qty": _qty(self._in_transit(loc, pid)),
                    "base_unit": self.unit[pid],
                    "last_updated_timestamp": f"{d.isoformat()} 23:59:59.000",
                }
            )
        self.movements[d] = mv
        self.snapshots[d] = snap

    def run_until(self, end: date) -> None:
        while self._next <= end:
            self._step(self._next)
            self._next += timedelta(days=1)

    # -- outputs ---------------------------------------------------------------------

    def build_history(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """All movements and closing stock of the history window (the 18 months before as_of)."""
        end = self.as_of - timedelta(days=1)
        self.run_until(end)
        days = [d for d in sorted(self.movements) if d <= end]
        movements = pd.DataFrame([r for d in days for r in self.movements[d]], columns=MOVEMENT_COLUMNS)
        stock = pd.DataFrame([r for d in days for r in self.snapshots[d]], columns=STOCK_COLUMNS)
        return movements, stock

    def delay(self, row: dict) -> int:
        """Days between a movement's posting and its delivery (live days only; seeded by document + item)."""
        if date.fromisoformat(row["posting_date"]) < self.as_of:
            return 0
        rng = np.random.default_rng([self.seed, 24, int(row["material_document"]), int(row["material_document_item"])])
        late, days = rng.random(), int(rng.integers(1, MAX_LATE_DAYS + 1))
        return days if late < LATE_MOVEMENT_RATE else 0

    def build_day(self, d: date) -> InventoryDrop:
        """The delivery of business day d: movements arriving that day, closing stock of d, with dirt."""
        if d < self.as_of:
            raise ValueError(f"{d} is before the simulation start {self.as_of}; it belongs to the history")
        self.run_until(d)
        rows = []
        for back in range(MAX_LATE_DAYS, -1, -1):
            p = d - timedelta(days=back)
            if p >= self.as_of:
                rows += [r for r in self.movements[p] if p + timedelta(days=self.delay(r)) == d]
        movements = pd.DataFrame(rows, columns=MOVEMENT_COLUMNS)
        stock = pd.DataFrame(self.snapshots[d], columns=STOCK_COLUMNS)
        return _dirty(InventoryDrop(d, movements, stock), np.random.default_rng([self.seed, 29, d.toordinal()]))


# ---------------------------------------------------------------------------
# Dirt and control totals
# ---------------------------------------------------------------------------


def _mkeys(df: pd.DataFrame, idx) -> list[str]:
    return [f"{a}/{b}" for a, b in df.loc[idx, ["material_document", "material_document_item"]].itertuples(index=False)]


def _skeys(df: pd.DataFrame, idx) -> list[str]:
    return [
        f"{a}/{b}/{c}"
        for a, b, c in df.loc[idx, ["snapshot_date", "location_id", "product_id"]].itertuples(index=False)
    ]


def _sample(rng: np.random.Generator, idx: pd.Index, rate: float, exclude: set | None = None) -> list:
    pool = [i for i in idx if not exclude or i not in exclude]
    k = int(rng.binomial(len(pool), rate)) if pool else 0
    return sorted(rng.choice(pool, size=k, replace=False).tolist()) if k else []


def movement_totals(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"lines": 0, "quantity": 0.0, "by_movement_type": {}, "by_posting_date": {}}
    q = df.assign(q=df["quantity"].astype(float))
    by_date = q.groupby("posting_date").agg(lines=("q", "size"), quantity=("q", "sum"))
    return {
        "lines": len(df),
        "quantity": round(float(q["q"].sum()), 3),
        "by_movement_type": {k: int(v) for k, v in df["movement_type"].value_counts().sort_index().items()},
        "by_posting_date": {
            k: {"lines": int(r.lines), "quantity": round(float(r.quantity), 3)} for k, r in by_date.iterrows()
        },
    }


def stock_totals(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"rows": 0, "unrestricted_qty": 0.0, "in_transit_qty": 0.0, "by_snapshot_date": {}}
    q = df.assign(u=df["unrestricted_qty"].astype(float), t=df["in_transit_qty"].astype(float))
    by_date = q.groupby("snapshot_date").agg(rows=("u", "size"), u=("u", "sum"), t=("t", "sum"))
    return {
        "rows": len(df),
        "unrestricted_qty": round(float(q["u"].sum()), 3),
        "in_transit_qty": round(float(q["t"].sum()), 3),
        "by_snapshot_date": {
            k: {"rows": int(r.rows), "unrestricted_qty": round(float(r.u), 3), "in_transit_qty": round(float(r.t), 3)}
            for k, r in by_date.iterrows()
        },
    }


def _dirty(drop: InventoryDrop, rng: np.random.Generator) -> InventoryDrop:
    """Planted dirt for one daily delivery. Control totals are the TRUE totals: without the invalid rows and copies."""
    mv, st = drop.movements.copy(), drop.stock.copy()
    log: list[dict] = []
    late = [i for i in mv.index if mv.at[i, "posting_date"] < drop.business_date.isoformat()]
    log.append({"file": "movements", "dirt": "late_movement", "count": len(late), "keys": _mkeys(mv, late),
                "expect": "load normally; reconcile per posting date across deliveries"})  # fmt: skip

    unknown = _sample(rng, mv.index, UNKNOWN_LOCATION_RATE)
    blank = _sample(rng, mv.index, BLANK_MOVEMENT_TYPE_RATE, set(unknown))
    for i in unknown:
        mv.loc[i, ["location_id", "storage_location"]] = [UNKNOWN_LOCATION_ID, UNKNOWN_LOCATION_ID[-4:]]
    for i in blank:
        mv.loc[i, ["movement_type", "movement_type_text"]] = ""
    log.append({"file": "movements", "dirt": "unknown_location", "count": len(unknown), "keys": _mkeys(mv, unknown),
                "expect": "quarantine: location_id not in the locations master"})  # fmt: skip
    log.append({"file": "movements", "dirt": "blank_movement_type", "count": len(blank), "keys": _mkeys(mv, blank),
                "expect": "quarantine: movement_type missing"})  # fmt: skip
    negative = _sample(rng, st.index, NEGATIVE_STOCK_RATE)
    for i in negative:
        st.at[i, "unrestricted_qty"] = _qty(-abs(float(st.at[i, "unrestricted_qty"])) - 1)
    log.append({"file": "stock", "dirt": "negative_stock", "count": len(negative), "keys": _skeys(st, negative),
                "expect": "quarantine: negative unrestricted stock is not allowed"})  # fmt: skip

    totals = {
        "movements": movement_totals(drop.movements.drop(index=unknown + blank)),
        "stock": stock_totals(drop.stock.drop(index=negative)),
        "note": "True totals: invalid rows and duplicate copies listed under dirt are excluded.",
    }

    variants = _sample(rng, mv.index, UNIT_VARIANT_RATE, set(unknown + blank))
    for i in variants:
        options = UNIT_VARIANTS.get(mv.at[i, "base_unit"], [mv.at[i, "base_unit"].lower()])
        mv.at[i, "base_unit"] = options[int(rng.integers(len(options)))]
    log.append({"file": "movements", "dirt": "unit_text_variant", "count": len(variants), "keys": _mkeys(mv, variants),
                "expect": "replace with the product's base unit from the product master"})  # fmt: skip
    copies = _sample(rng, mv.index, DUPLICATE_RATE)
    log.append({"file": "movements", "dirt": "duplicate_line", "count": len(copies), "keys": _mkeys(mv, copies),
                "expect": "dedupe on material_document + material_document_item"})  # fmt: skip
    if copies:
        mv = pd.concat([mv, mv.loc[copies]]).sort_index(kind="stable").reset_index(drop=True)
    return InventoryDrop(drop.business_date, mv.reset_index(drop=True), st, log, totals)


# ---------------------------------------------------------------------------
# Writing files (manifest last)
# ---------------------------------------------------------------------------


def _write_csv(df: pd.DataFrame, path: Path) -> dict:
    df.to_csv(path, index=False, lineterminator="\n")
    return {"rows": len(df), "columns": int(df.shape[1]), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def write_history(movements: pd.DataFrame, stock: pd.DataFrame, out_dir: Path, seed: int, as_of: date) -> Path:
    target = out_dir / "history"
    target.mkdir(parents=True, exist_ok=True)
    files = {}
    for kind, df, col in (("movements", movements, "posting_date"), ("stock", stock, "snapshot_date")):
        for month, part in df.groupby(df[col].str[:7].str.replace("-", "")):
            files[f"{kind}_{month}.csv"] = _write_csv(part, target / f"{kind}_{month}.csv")
    manifest = {
        "kind": "history",
        "area": "inventory",
        "source_system": SOURCE_SYSTEM,
        "as_of": as_of.isoformat(),
        "seed": seed,
        "date_from": stock["snapshot_date"].min(),
        "date_to": stock["snapshot_date"].max(),
        "files": files,
        "control_totals": {"movements": movement_totals(movements), "stock": stock_totals(stock)},
        "dirt": [],
    }
    (target / "history_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return target


def folder_name(d: date) -> str:
    return f"business_date={d.isoformat()}"


def write_day(drop: InventoryDrop, out_dir: Path, seed: int, as_of: date) -> Path:
    target = out_dir / folder_name(drop.business_date)
    target.mkdir(parents=True, exist_ok=True)
    stamp = drop.business_date.strftime("%Y%m%d")
    files = {
        f"movements_{stamp}.csv": _write_csv(drop.movements, target / f"movements_{stamp}.csv"),
        f"stock_{stamp}.csv": _write_csv(drop.stock, target / f"stock_{stamp}.csv"),
    }
    manifest = {
        "kind": "daily",
        "area": "inventory",
        "business_date": drop.business_date.isoformat(),
        "source_system": SOURCE_SYSTEM,
        "simulation_start": as_of.isoformat(),
        "seed": seed,
        "files": files,
        "control_totals": drop.control_totals,
        "dirt": drop.dirt,
    }
    # Manifest LAST: its presence means the day's delivery is complete
    (target / f"manifest_{stamp}.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return target


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config

    cfg = load_config(area="inventory")
    start = date.fromisoformat(cfg.business["simulation_start"])
    parser = argparse.ArgumentParser(description="Generate the inventory area's source files (stock, movements).")
    parser.add_argument("--history", action="store_true", help="write the locations master and the 18-month history")
    parser.add_argument("--date", type=date.fromisoformat, help="one business date (YYYY-MM-DD)")
    parser.add_argument("--from", dest="date_from", type=date.fromisoformat, help="first date of a range")
    parser.add_argument("--to", dest="date_to", type=date.fromisoformat, help="last date of a range")
    parser.add_argument("--seed", type=int, default=cfg.business["random_seed"])
    parser.add_argument("--out", type=Path, default=Path(cfg.path(cfg.staging_path)))
    args = parser.parse_args(argv)

    if args.date:
        days = [args.date]
    elif args.date_from and args.date_to:
        days = [args.date_from + timedelta(days=i) for i in range((args.date_to - args.date_from).days + 1)]
    elif args.history:
        days = []
    else:
        parser.error("give --history, --date, or --from and --to")

    sim = InventorySimulator(args.seed, start)
    if args.history:
        write_locations(sim.locations, args.out, args.seed, start)
        movements, stock = sim.build_history()
        write_history(movements, stock, args.out, args.seed, start)
        print(f"  locations {len(sim.locations)}, history movements {len(movements):,}, stock rows {len(stock):,}")
    for d in days:
        drop = sim.build_day(d)
        write_day(drop, args.out, args.seed, start)
        dirt = ", ".join(f"{x['dirt']} {x['count']}" for x in drop.dirt if x["count"])
        print(f"  {d}  movements {len(drop.movements):>4}  stock rows {len(drop.stock):>4}  dirt: {dirt or 'none'}")
    print(f"Written to {args.out}")


if __name__ == "__main__":
    main()
