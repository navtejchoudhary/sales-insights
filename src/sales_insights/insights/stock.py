"""Stock insight rules in plain Python: what is empty, what is running low, what is sitting idle.

Spark (insights/stock_data.py) reduces the inventory gold to one row per stock position on the latest stock
date; everything here works on those rows, so every rule is unit-tested without Spark.

    Confirmed stock-out   zero stock of a product the location issued in the last 28 days      severity 1
    Low cover             less than LOW_COVER_DAYS of stock at the 28-day issue rate (depots)   severity 2
    Overstock             more than OVERSTOCK_DAYS of cover; the OVERSTOCK_TOP positions with   severity 3
                          the most stock value above that level, plus one total

The sales job's "possible stock-out" (insights/analysis.py) guesses from sales alone; a confirmed
stock-out is the stock data saying the shelf is empty, with the supplying hub's stock next to it.
Overstock is ranked by the VALUE above 120 days of cover: about 50 slow sellers sit above 120 days, and a
list of all of them would hide the few that tie up real money.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sales_insights.insights.analysis import Insight, fmt_lkr

LOW_COVER_DAYS = 7  # same as is_low_cover in gold.fact_stock_daily
OVERSTOCK_DAYS = 120
OVERSTOCK_TOP = 5
LOW_COVER_TOP = 5
STOCK_KINDS = ("stock_out_confirmed", "low_cover", "overstock", "overstock_total")
ALERT_KINDS = ("stock_out_confirmed",)


@dataclass(frozen=True)
class Position:
    """One location x product on the latest stock date (only the ones a rule may pick)."""

    location: str
    location_type: str  # Depot | Plant store
    province: str
    product: str
    stock: float
    avg_daily: float  # 28-day average issues per day
    unit_cost: float
    is_stock_out: bool
    is_low_cover: bool
    out_since: date | None = None  # first day at zero (stock-outs only)
    avg_before: float | None = None  # average daily issues on the day it ran out
    supplier: str | None = None  # the location that replenishes it (the Galle hub for depots)
    supplier_stock: float | None = None

    @property
    def days_of_cover(self) -> float | None:
        return self.stock / self.avg_daily if self.avg_daily > 0 else None

    @property
    def item(self) -> str:
        return f"{self.product} | {self.location}"


def _qty(v: float) -> str:
    return f"{v:,.0f}" if v >= 10 else f"{v:,.1f}".removesuffix(".0")


def confirmed_stock_outs(positions: list[Position], as_of: date) -> list[Insight]:
    out = []
    for p in sorted(positions, key=lambda p: (p.location, p.product)):
        if not p.is_stock_out:
            continue
        rate = p.avg_before if p.avg_before is not None else p.avg_daily
        since = p.out_since or as_of
        days = (as_of - since).days + 1
        text = (
            f"Confirmed stock-out: {p.location} has had no {p.product} since {since:%d %b} "
            f"({days} day{'s' if days != 1 else ''}); it normally issues about {_qty(rate)} a day."
        )
        if p.supplier and p.supplier_stock:
            text += f" {p.supplier} still holds {_qty(p.supplier_stock)}."
        out.append(Insight("stock_out_confirmed", "", "location_product", p.item, "stock", 0.0, rate, -rate,
                           -100.0, 1, text))  # fmt: skip
    return out


def low_cover(positions: list[Position], top: int = LOW_COVER_TOP) -> list[Insight]:
    """Depots with stock for less than LOW_COVER_DAYS days; the shortest cover first."""
    low = [p for p in positions if p.is_low_cover and p.location_type == "Depot" and p.days_of_cover is not None]
    out = []
    for p in sorted(low, key=lambda p: p.days_of_cover)[:top]:
        text = (
            f"Low cover: {p.location} has {_qty(p.stock)} {p.product} left, about {p.days_of_cover:.1f} days "
            f"at its usual {_qty(p.avg_daily)} a day."
        )
        out.append(Insight("low_cover", "", "location_product", p.item, "days_of_cover", p.days_of_cover,
                           float(LOW_COVER_DAYS), p.days_of_cover - LOW_COVER_DAYS, None, 2, text))  # fmt: skip
    return out


def excess_value(p: Position, days: int = OVERSTOCK_DAYS) -> float:
    """Value of the stock above `days` of cover, at standard cost."""
    return max(p.stock - days * p.avg_daily, 0.0) * p.unit_cost


def overstock(positions: list[Position], days: int = OVERSTOCK_DAYS, top: int = OVERSTOCK_TOP) -> list[Insight]:
    over = [p for p in positions if p.avg_daily > 0 and p.days_of_cover is not None and p.days_of_cover > days]
    if not over:
        return []
    over.sort(key=lambda p: -excess_value(p, days))
    total = sum(excess_value(p, days) for p in over)
    biggest = ", ".join(f"{p.product} at {p.location} ({fmt_lkr(excess_value(p, days))})" for p in over[:3])
    out = [
        Insight("overstock_total", "", "", "", "excess_stock_value", total, None, None, None, 3,
                f"Overstock: {len(over)} stock positions hold more than {days} days of cover, {fmt_lkr(total)} of "
                f"stock above that level. Largest: {biggest}.")
    ]  # fmt: skip
    for p in over[:top]:
        text = (
            f"Overstock: {p.location} holds {_qty(p.stock)} {p.product}, {p.days_of_cover:,.0f} days of cover at "
            f"{_qty(p.avg_daily)} a day; {fmt_lkr(excess_value(p, days))} of it is above {days} days."
        )
        out.append(Insight("overstock", "", "location_product", p.item, "days_of_cover", p.days_of_cover,
                           float(days), p.days_of_cover - days, None, 3, text))  # fmt: skip
    return out


def stock_insights(positions: list[Position], as_of: date) -> list[Insight]:
    """Every stock finding, most important first."""
    return confirmed_stock_outs(positions, as_of) + low_cover(positions) + overstock(positions)
