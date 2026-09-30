"""Daily simulator: one folder per business day, like a source system's nightly extract.

    staging/business_date=YYYY-MM-DD/
        orders_YYYYMMDD.csv       new invoice lines (ZAOR, ZFOC) that ARRIVED that day,
                                  including late invoices dated up to 7 days earlier
        changes_YYYYMMDD.csv      returns (ZARE), cancellations (S1) and price corrections
                                  (ZACR) against earlier invoices (history or live)
        manifest_YYYYMMDD.json    written LAST: rows, SHA-256, control totals, dirt log

Control totals are the TRUE business totals: they exclude injected duplicate copies and
invalid rows (both listed under "dirt"). Reconciliation checks gold against them exactly.

Same seed + same simulation start + same date = byte-identical files, in any run order.

Run:
    uv run python -m sales_insights.generator.simulate --date 2026-10-01
    uv run python -m sales_insights.generator.simulate --from 2026-10-01 --to 2026-10-29
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from sales_insights.generator import dirt as dirt_mod
from sales_insights.generator import engine as e
from sales_insights.generator import masters as m
from sales_insights.generator import stories

LOOKBACK_DAYS = 45  # returns up to 30 days + late arrival up to 7 days + margin
MAX_LATE_DAYS = 7


@dataclass
class DayDrop:
    business_date: date
    orders: pd.DataFrame
    changes: pd.DataFrame
    dirt: list[dict] = field(default_factory=list)
    control_totals: dict = field(default_factory=dict)
    added_columns: list[str] = field(default_factory=list)


def folder_name(d: date) -> str:
    return f"business_date={d.isoformat()}"


class Simulator:
    """Holds the masters and a per-day sales cache, so simulating many days stays fast."""

    def __init__(self, seed: int, as_of: date):
        self.seed, self.as_of = seed, as_of
        tables, _ = m.build_masters(seed, as_of, dirt=False)
        self.mi = e.MasterIndex(tables)
        self.window_start, _ = e.history_window(as_of)
        reps = tables["sales_reps"]
        self.reps_by_office = {o: sorted(g["sales_rep_id"]) for o, g in reps.groupby("sales_office")}
        self._sales: dict[date, dict[str, list[e.Sale]]] = {}
        self._fates: dict[str, e.Fate] = {}

    def invoices_on(self, d: date) -> dict[str, list[e.Sale]]:
        if d not in self._sales:
            self._sales[d] = e.group_by_invoice(e.sales_for_day(self.mi, self.seed, d, self.window_start))
        return self._sales[d]

    def fate(self, number: str, lines: list[e.Sale]) -> e.Fate:
        if number not in self._fates:
            self._fates[number] = e.invoice_fate(self.mi, self.seed, number, lines, live_from=self.as_of)
        return self._fates[number]

    def _rep(self, customer_id: str, office: str) -> str:
        reps = self.reps_by_office.get(office, [])
        return reps[int(customer_id) % len(reps)] if reps else ""

    def build_day(self, d: date) -> DayDrop:
        if d < self.as_of:
            raise ValueError(f"{d} is before the simulation start {self.as_of}; it belongs to the history")

        # 1. Orders that ARRIVE today: raised today, or raised up to 7 days ago and delayed
        orders = []
        for back in range(MAX_LATE_DAYS, -1, -1):
            p = d - timedelta(days=back)
            if p < self.as_of:
                continue
            for number, lines in self.invoices_on(p).items():
                if p + timedelta(days=self.fate(number, lines).delay_days) == d:
                    orders.extend(s.line for s in lines)

        # 2. Changes dated today, against any invoice from the last 45 days (history included)
        changes = []
        for back in range(LOOKBACK_DAYS, -1, -1):
            p = d - timedelta(days=back)
            for number, lines in self.invoices_on(p).items():
                changes.extend(c for c in self.fate(number, lines).changes if c["invoice_date"] == d.isoformat())

        orders_df = pd.DataFrame(orders, columns=e.COLUMNS).reset_index(drop=True)
        changes_df = pd.DataFrame(changes, columns=e.COLUMNS)
        changes_df = changes_df.sort_values(["invoice_number", "invoice_item"], kind="stable").reset_index(drop=True)

        # 3. Dirt (own random stream per day). Control totals come from the clean rows first.
        rng = np.random.default_rng([self.seed, 9, d.toordinal()])
        log: list[dict] = []
        orders_df, invalid_log, invalid_keys = dirt_mod.invalid_values(orders_df, rng)
        clean_orders = pd.DataFrame(orders, columns=e.COLUMNS)
        keys = clean_orders["invoice_number"] + "/" + clean_orders["invoice_item"]
        totals = {
            "orders": e.control_totals(clean_orders[~keys.isin(invalid_keys)]),
            "changes": e.control_totals(changes_df),
            "note": "True business totals: duplicate copies and invalid rows listed under dirt are excluded.",
        }
        orders_df, null_log = dirt_mod.null_fields(orders_df, rng)
        orders_df, city_log = dirt_mod.city_variants(orders_df, rng)
        orders_df, dup_log = dirt_mod.duplicates(orders_df, rng)
        changes_df, change_dup_log = dirt_mod.duplicates(changes_df, rng, rate=0.01)
        for entry in invalid_log + null_log + city_log + dup_log:
            log.append({"file": "orders", **entry})
        for entry in change_dup_log:
            log.append({"file": "changes", **entry})

        # 4. Schema evolution: a new column appears from a fixed date
        added = []
        if d >= stories.SCHEMA_CHANGE_FROM:
            for df in (orders_df, changes_df):
                df[stories.NEW_COLUMN] = [
                    self._rep(c, o) for c, o in zip(df["customer_id"], df["sales_office"], strict=True)
                ]
            added = [stories.NEW_COLUMN]

        return DayDrop(d, orders_df, changes_df, log, totals, added)


def write_day(drop: DayDrop, out_dir: Path, seed: int, as_of: date) -> Path:
    target = out_dir / folder_name(drop.business_date)
    target.mkdir(parents=True, exist_ok=True)
    stamp = drop.business_date.strftime("%Y%m%d")
    files = {}
    for kind, df in (("orders", drop.orders), ("changes", drop.changes)):
        path = target / f"{kind}_{stamp}.csv"
        df.to_csv(path, index=False, lineterminator="\n")
        files[path.name] = {
            "rows": int(len(df)),
            "columns": int(df.shape[1]),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    manifest = {
        "kind": "daily",
        "business_date": drop.business_date.isoformat(),
        "source_system": "SAP S/4HANA (simulated)",
        "simulation_start": as_of.isoformat(),
        "seed": seed,
        "currency": m.CURRENCY,
        "columns_added_to_reference": e.ADDED_COLUMNS + drop.added_columns,
        "files": files,
        "control_totals": drop.control_totals,
        "dirt": drop.dirt,
    }
    # Manifest LAST: its presence means the day's delivery is complete
    (target / f"manifest_{stamp}.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return target


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config

    cfg = load_config()
    start = date.fromisoformat(cfg.business["simulation_start"])
    parser = argparse.ArgumentParser(description="Simulate daily source-system drops.")
    parser.add_argument("--date", type=date.fromisoformat, help="One business date (YYYY-MM-DD)")
    parser.add_argument("--from", dest="date_from", type=date.fromisoformat, help="First date of a range")
    parser.add_argument("--to", dest="date_to", type=date.fromisoformat, help="Last date of a range")
    parser.add_argument("--seed", type=int, default=cfg.business["random_seed"])
    parser.add_argument("--out", type=Path, default=Path(cfg.path(cfg.staging_path)))
    args = parser.parse_args(argv)

    if args.date:
        days = [args.date]
    elif args.date_from and args.date_to:
        days = [args.date_from + timedelta(days=i) for i in range((args.date_to - args.date_from).days + 1)]
    else:
        parser.error("give --date, or --from and --to")

    sim = Simulator(args.seed, start)
    for d in days:
        drop = sim.build_day(d)
        write_day(drop, args.out, args.seed, start)
        dirt_counts = ", ".join(f"{x['dirt']} {x['count']}" for x in drop.dirt if x["count"])
        by_date = drop.control_totals["orders"].get("by_invoice_date", {})
        late = sum(v["lines"] for k, v in by_date.items() if k < d.isoformat())
        print(
            f"  {d}  orders {len(drop.orders):>4}  changes {len(drop.changes):>3}  late lines {late:>3}"
            f"  {'+' + ','.join(drop.added_columns) if drop.added_columns else ''}"
        )
        print(f"              dirt: {dirt_counts}")
    print(f"Written to {args.out}")


if __name__ == "__main__":
    main()
