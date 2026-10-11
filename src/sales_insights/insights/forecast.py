"""Seasonal revenue forecast (plain Python): same month last year x this year's like-for-like growth.

Why this method: crop-protection sales follow the cultivation seasons (peaks before Maha and Yala, a dip in
December), so the best guide to next November is last November. With only 18 months of history, a model
such as ai_forecast cannot learn that pattern and draws a near-straight line (tested on Day 15).

    growth    = net revenue of the fiscal year so far (complete months only)
                / net revenue of the same months last year
    forecast  = same month last year x growth
    range     = same month last year x (growth -/+ one standard deviation of the month-by-month growth)

Months: from the current (incomplete) month to the end of the fiscal year, at least 3 months ahead.
If fewer than 3 months of the fiscal year are complete (April to June), growth uses the last 6 complete
months instead, so it never rests on one or two months.

Every number in a forecast sentence comes from here; the AI never forecasts.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import date

from sales_insights.insights.analysis import Insight, fmt_lkr
from sales_insights.insights.periods import last_complete_month, shift_months

MIN_FY_MONTHS = 3  # fewer complete fiscal-year months than this: use the last FALLBACK_MONTHS instead
FALLBACK_MONTHS = 6
MIN_HORIZON = 3
METHOD = "same month last year x fiscal-year-to-date growth"


@dataclass(frozen=True)
class ForecastMonth:
    month: date  # first day of the month
    forecast: float
    low: float
    high: float
    last_year: float
    actual_to_date: float | None  # sales already in the data for the current, incomplete month

    @property
    def label(self) -> str:
        return self.month.strftime("%b %Y")


@dataclass(frozen=True)
class Forecast:
    growth: float  # e.g. 1.239 = +23.9%
    spread: float  # one standard deviation of the month-by-month growth factors
    basis: str  # e.g. "Apr-Sep 2026 vs Apr-Sep 2025"
    months: list[ForecastMonth]

    @property
    def growth_pct(self) -> float:
        return (self.growth - 1) * 100

    @property
    def total(self) -> float:
        return sum(m.forecast for m in self.months)

    @property
    def total_low(self) -> float:
        return sum(m.low for m in self.months)

    @property
    def total_high(self) -> float:
        return sum(m.high for m in self.months)


def fiscal_year_start(d: date, start_month: int) -> date:
    return date(d.year if d.month >= start_month else d.year - 1, start_month, 1)


def _months(first: date, last: date) -> list[date]:
    out, m = [], first
    while m <= last:
        out.append(m)
        m = shift_months(m, 1)
    return out


def _span(months: list[date]) -> str:
    a, b = months[0], months[-1]
    return f"{a:%b}-{b:%b %Y}" if a.year == b.year else f"{a:%b %Y}-{b:%b %Y}"


def build(monthly: dict[date, float], as_of: date, fy_start_month: int = 4) -> Forecast | None:
    """monthly = {first day of month: net revenue}, including the current incomplete month if any.

    Returns None when there is not enough history (no complete month with a year-earlier month to compare).
    """
    last_full = last_complete_month(as_of)
    fy_start = fiscal_year_start(last_full, fy_start_month)
    basis_months = _months(fy_start, last_full) if fy_start <= last_full else []
    if len(basis_months) < MIN_FY_MONTHS:
        basis_months = _months(shift_months(last_full, -(FALLBACK_MONTHS - 1)), last_full)
    pairs = [(m, monthly[m], monthly[shift_months(m, -12)]) for m in basis_months
             if m in monthly and shift_months(m, -12) in monthly and monthly[shift_months(m, -12)] > 0]  # fmt: skip
    if not pairs:
        return None
    growth = sum(c for _, c, _ in pairs) / sum(p for _, _, p in pairs)
    ratios = [c / p for _, c, p in pairs]
    spread = statistics.pstdev(ratios) if len(ratios) > 1 else 0.0
    used = [m for m, _, _ in pairs]
    basis = f"{_span(used)} vs {_span([shift_months(m, -12) for m in used])}"

    first = shift_months(last_full, 1)
    fy_end = shift_months(fiscal_year_start(first, fy_start_month), 11)
    last = max(fy_end, shift_months(first, MIN_HORIZON - 1))
    rows = []
    for m in _months(first, last):
        ly = monthly.get(shift_months(m, -12))
        if ly is None:
            continue
        actual = monthly.get(m) if m <= as_of else None
        rows.append(ForecastMonth(m, ly * growth, ly * max(growth - spread, 0.0), ly * (growth + spread), ly, actual))
    return Forecast(growth, spread, basis, rows)


def range_text(low: float, high: float) -> str:
    return "" if fmt_lkr(low) == fmt_lkr(high) else f" (likely {fmt_lkr(low)} to {fmt_lkr(high)})"


def insights(fc: Forecast | None) -> list[Insight]:
    """Two context sentences: the current month, and the total to the end of the fiscal year."""
    if fc is None or not fc.months:
        return []
    out = []
    cur = fc.months[0]
    so_far = f" {fmt_lkr(cur.actual_to_date)} is already invoiced." if cur.actual_to_date else ""
    out.append(
        Insight(
            "forecast", "", "month", cur.month.isoformat(), "net_revenue", cur.forecast, cur.last_year,
            cur.forecast - cur.last_year, fc.growth_pct,
            3,
            f"Forecast: {cur.month:%B %Y} on track for about {fmt_lkr(cur.forecast)}{range_text(cur.low, cur.high)}, "
            f"based on {shift_months(cur.month, -12):%B %Y} ({fmt_lkr(cur.last_year)}) and growth of "
            f"{fc.growth_pct:+.1f}% ({fc.basis}).{so_far}",
        )
    )  # fmt: skip
    if len(fc.months) > 1:
        a, b = fc.months[0], fc.months[-1]
        ly = sum(m.last_year for m in fc.months)
        out.append(
            Insight(
                "forecast", "", "period", f"{a.month.isoformat()}..{b.month.isoformat()}", "net_revenue", fc.total, ly,
                fc.total - ly, fc.growth_pct,
                3,
                f"Forecast {a.label} to {b.label}: about {fmt_lkr(fc.total)} in total"
                f"{range_text(fc.total_low, fc.total_high)}, against {fmt_lkr(ly)} in the same months last year.",
            )
        )  # fmt: skip
    return out
