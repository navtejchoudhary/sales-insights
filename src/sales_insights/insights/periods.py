"""Like-for-like comparison periods (plain Python).

The trap this avoids: on 5 October, "October" holds 5 days of sales. Comparing it with all of September
says sales fell 85%; nothing actually happened. Every comparison here pairs windows of the SAME length
and the same position in the calendar:

    month to date        1-5 Oct 2026   vs  1-5 Sep 2026     (same days, previous month)
    month to date, LY    1-5 Oct 2026   vs  1-5 Oct 2025     (same days, last year)
    last 7 days          29 Sep-5 Oct   vs  22-28 Sep
    last complete month  Sep 2026       vs  Sep 2025         (and vs Aug 2026)
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class Window:
    start: date
    end: date

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1

    def label(self) -> str:
        if self.start.day == 1 and self.end == month_end(self.start):
            return self.start.strftime("%b %Y")
        if self.start.month == self.end.month and self.start.year == self.end.year:
            return f"{self.start.day}-{self.end.day} {self.start:%b %Y}"
        return f"{self.start:%d %b} - {self.end:%d %b %Y}"


@dataclass(frozen=True)
class Comparison:
    key: str  # stable id, e.g. "mtd_vs_prev_month"
    name: str  # plain English, e.g. "month to date vs the same days last month"
    current: Window
    previous: Window


def month_end(d: date) -> date:
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def shift_months(d: date, months: int) -> date:
    """Same day in another month, clamped to that month's length (31 Mar - 1 month = 28/29 Feb)."""
    y, m = divmod(d.month - 1 + months, 12)
    year, month = d.year + y, m + 1
    return date(year, month, min(d.day, calendar.monthrange(year, month)[1]))


def month_to_date(as_of: date) -> Comparison:
    start = as_of.replace(day=1)
    return Comparison(
        "mtd_vs_prev_month",
        "month to date vs the same days last month",
        Window(start, as_of),
        Window(shift_months(start, -1), shift_months(as_of, -1)),
    )


def month_to_date_last_year(as_of: date) -> Comparison:
    start = as_of.replace(day=1)
    return Comparison(
        "mtd_vs_last_year",
        "month to date vs the same days last year",
        Window(start, as_of),
        Window(shift_months(start, -12), shift_months(as_of, -12)),
    )


def last_7_days(as_of: date) -> Comparison:
    cur = Window(as_of - timedelta(days=6), as_of)
    return Comparison(
        "last_7_days", "last 7 days vs the 7 days before", cur, Window(cur.start - timedelta(7), cur.end - timedelta(7))
    )


def last_complete_month(as_of: date) -> date:
    """First day of the latest month that is fully covered by the data."""
    first = as_of.replace(day=1)
    return first if as_of == month_end(as_of) else shift_months(first, -1)


def complete_month_vs_last_year(as_of: date) -> Comparison:
    m = last_complete_month(as_of)
    return Comparison(
        "month_vs_last_year",
        "last complete month vs the same month last year",
        Window(m, month_end(m)),
        Window(shift_months(m, -12), month_end(shift_months(m, -12))),
    )


def complete_month_vs_prev_month(as_of: date) -> Comparison:
    m = last_complete_month(as_of)
    p = shift_months(m, -1)
    return Comparison(
        "month_vs_prev_month",
        "last complete month vs the month before",
        Window(m, month_end(m)),
        Window(p, month_end(p)),
    )


def standard(as_of: date) -> list[Comparison]:
    return [
        month_to_date(as_of),
        month_to_date_last_year(as_of),
        last_7_days(as_of),
        complete_month_vs_last_year(as_of),
        complete_month_vs_prev_month(as_of),
    ]
