"""Insight logic in plain Python: what changed, who moved it, what looks wrong.

Spark (insights/data.py) reduces gold to small aggregates; everything here works on those small results,
so every rule is unit-tested without Spark and behaves the same on a Mac and on Databricks.

Every number in an insight comes from this code. The AI (on Databricks) only rewrites the sentences.
"""

from __future__ import annotations

import statistics
from dataclasses import asdict, dataclass
from datetime import date

from sales_insights.insights.periods import Comparison

MEASURES = {  # name -> (label, kind)
    "net_revenue": ("Net revenue", "money"),
    "units": ("Units (cases)", "number"),
    "invoice_count": ("Invoices", "number"),
    "active_customers": ("Active customers", "number"),
    "gross_margin_pct": ("Gross margin %", "pct"),
    "returns_rate_pct": ("Returns rate %", "pct"),
}
DIMENSION_LABELS = {
    "province": "province",
    "product_group": "product group",
    "product": "product",
    "customer_group": "customer group",
    "channel": "channel",
}


@dataclass(frozen=True)
class Insight:
    kind: str  # headline | mover | stock_out | anomaly
    comparison: str  # Comparison.key, or "" for alerts
    dimension: str  # "" for headline
    item: str
    metric: str
    current: float | None
    previous: float | None
    change: float | None
    change_pct: float | None
    severity: int  # 1 = act now, 2 = notable, 3 = context
    text: str

    def as_row(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def fmt_lkr(v: float | None) -> str:
    if v is None:
        return "n/a"
    a = abs(v)
    sign = "-" if v < 0 else ""
    if a >= 1e6:
        return f"{sign}LKR {a / 1e6:,.1f}M"
    if a >= 1e3:
        return f"{sign}LKR {a / 1e3:,.0f}K"
    return f"{sign}LKR {a:,.0f}"


def fmt_value(metric: str, v: float | None) -> str:
    kind = MEASURES.get(metric, ("", "number"))[1]
    if v is None:
        return "n/a"
    if kind == "money":
        return fmt_lkr(v)
    if kind == "pct":
        return f"{v:.1f}%"
    return f"{v:,.0f}"


def fmt_change(metric: str, change: float | None, change_pct: float | None) -> str:
    """'up 12.3%' / 'down 0.4 points' (percent measures change in points, not percent of percent)."""
    if change is None:
        return "no comparison available"
    if MEASURES.get(metric, ("", ""))[1] == "pct":
        word = "up" if change >= 0 else "down"
        return f"{word} {abs(change):.1f} points"
    if change_pct is None:
        return f"{'up' if change >= 0 else 'down'} {fmt_value(metric, abs(change))}"
    return f"{'up' if change_pct >= 0 else 'down'} {abs(change_pct):.1f}%"


def pct_change(cur: float | None, prev: float | None) -> float | None:
    if cur is None or prev in (None, 0):
        return None
    return (cur - prev) / abs(prev) * 100


# ---------------------------------------------------------------------------
# Headline: how are we doing?
# ---------------------------------------------------------------------------


def headline(comp: Comparison, current: dict, previous: dict) -> list[Insight]:
    """One insight per measure. `current`/`previous` map measure name -> value."""
    out = []
    for m in MEASURES:
        cur, prev = current.get(m), previous.get(m)
        change = None if cur is None or prev is None else cur - prev
        pct = pct_change(cur, prev)
        label = MEASURES[m][0]
        text = (
            f"{label} {comp.current.label()}: {fmt_value(m, cur)}, {fmt_change(m, change, pct)} "
            f"on {comp.previous.label()} ({fmt_value(m, prev)})."
        )
        big = pct is not None and abs(pct) >= 10 and m == "net_revenue"
        out.append(Insight("headline", comp.key, "", "", m, cur, prev, change, pct, 2 if big else 3, text))
    return out


# ---------------------------------------------------------------------------
# Movers: who drove the change?
# ---------------------------------------------------------------------------


def movers(comp: Comparison, dimension: str, rows: list[tuple[str, float, float]], n: int = 3) -> list[Insight]:
    """Top n risers and fallers by change in net revenue. rows = (item, current, previous).

    share_of_change tells how much of the total movement one item explains; items that moved by less
    than 1% of the total revenue are ignored as noise.
    """
    rows = [(item or "Unknown", cur or 0.0, prev or 0.0) for item, cur, prev in rows]
    total_cur = sum(r[1] for r in rows)
    total_change = total_cur - sum(r[2] for r in rows)
    floor = 0.01 * max(abs(total_cur), 1.0)
    changed = [(item, cur, prev, cur - prev) for item, cur, prev in rows if abs(cur - prev) >= floor]
    ups = sorted((r for r in changed if r[3] > 0), key=lambda r: -r[3])[:n]
    downs = sorted((r for r in changed if r[3] < 0), key=lambda r: r[3])[:n]
    label = DIMENSION_LABELS.get(dimension, dimension)
    out = []
    for item, cur, prev, ch in ups + downs:
        pct = pct_change(cur, prev)
        share = ch / abs(total_change) * 100 if total_change else None
        direction = "rose" if ch > 0 else "fell"
        pct_txt = f" ({pct:+.1f}%)" if pct is not None else f" (no sales in {comp.previous.label()})"
        if share is not None and abs(share) > 100:
            share_txt = "; more than the whole net change, as others moved the other way"
        elif share is not None and abs(share) >= 15:
            share_txt = f"; {abs(share):.0f}% of the total change"
        else:
            share_txt = ""
        text = (
            f"{item} ({label}) {direction} by {fmt_lkr(abs(ch))}{pct_txt}, {comp.current.label()} vs "
            f"{comp.previous.label()}{share_txt}."
        )
        severity = 2 if (share is not None and abs(share) >= 30) or (pct is not None and abs(pct) >= 25) else 3
        out.append(Insight("mover", comp.key, dimension, item, "net_revenue", cur, prev, ch, pct, severity, text))
    return out


# ---------------------------------------------------------------------------
# Alerts: what looks wrong right now?
# ---------------------------------------------------------------------------


def stock_outs(
    daily_units: dict[tuple[str, str], dict[date, float]],
    days: list[date],
    quiet_days: int = 3,
    min_active_share: float = 0.8,
) -> list[Insight]:
    """Products that normally sell almost every day in a province but have sold nothing for `quiet_days`.

    daily_units[(product, province)] = {day: units invoiced}; days = every day in the window, oldest first.
    A product selling on 80%+ of days has well under a 1% chance of three empty days by luck.
    """
    if len(days) <= quiet_days:
        return []
    history, recent = days[:-quiet_days], days[-quiet_days:]
    out = []
    for (product, province), by_day in sorted(daily_units.items()):
        active = sum(1 for d in history if by_day.get(d, 0) > 0)
        if active / len(history) < min_active_share or any(by_day.get(d, 0) > 0 for d in recent):
            continue
        avg = sum(by_day.get(d, 0) for d in history) / len(history)
        text = (
            f"Possible stock-out: {product} has had no sales in {province} for {quiet_days} days "
            f"(since {recent[0]:%d %b}); it sold on {active} of the previous {len(history)} days, "
            f"about {avg:,.0f} cases a day."
        )
        out.append(
            Insight(
                "stock_out", "", "product_province", f"{product} | {province}", "units", 0.0, avg, -avg, -100.0, 1, text
            )
        )
    return out


def anomalies(daily_revenue: dict[date, float], recent_days: int = 7, z_limit: float = 3.0) -> list[Insight]:
    """Recent days whose net revenue is far from the same weekday's usual level (z-score vs earlier weeks)."""
    days = sorted(daily_revenue)
    if len(days) <= recent_days:
        return []
    recent, history = days[-recent_days:], days[:-recent_days]
    out = []
    for d in recent:
        same = [daily_revenue[h] for h in history if h.weekday() == d.weekday()]
        if len(same) < 4:
            continue
        mean, sd = statistics.fmean(same), statistics.pstdev(same)
        if sd == 0:
            continue
        z = (daily_revenue[d] - mean) / sd
        if abs(z) >= z_limit:
            word = "above" if z > 0 else "below"
            text = (
                f"Unusual day: {d:%a %d %b} net revenue {fmt_lkr(daily_revenue[d])} is {abs(z):.1f} standard deviations "
                f"{word} a typical {d:%A} ({fmt_lkr(mean)})."
            )
            out.append(Insight("anomaly", "", "day", d.isoformat(), "net_revenue", daily_revenue[d], mean,
                               daily_revenue[d] - mean, pct_change(daily_revenue[d], mean), 2, text))  # fmt: skip
    return out


def rank(insights: list[Insight]) -> list[Insight]:
    """Most important first: alerts, then big moves, then context."""
    order = {"stock_out": 0, "anomaly": 1, "headline": 2, "mover": 3}
    return sorted(insights, key=lambda i: (i.severity, order.get(i.kind, 9), -(abs(i.change or 0))))
