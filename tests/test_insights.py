"""Insights tests: periods, analysis rules and narrative in plain Python; the whole job on the tiny gold world."""

from datetime import date, timedelta

from test_kpis import build_tiny_gold

from sales_insights.insights import analysis as an
from sales_insights.insights import forecast as fc
from sales_insights.insights import narrative as nr
from sales_insights.insights import periods as pr

AS_OF = date(2026, 10, 5)


# ---------------------------------------------------------------------------
# Periods: always like for like
# ---------------------------------------------------------------------------


def test_month_to_date_compares_the_same_days():
    c = pr.month_to_date(AS_OF)
    assert (c.current.start, c.current.end) == (date(2026, 10, 1), date(2026, 10, 5))
    assert (c.previous.start, c.previous.end) == (date(2026, 9, 1), date(2026, 9, 5))
    assert c.current.days == c.previous.days == 5
    ly = pr.month_to_date_last_year(AS_OF)
    assert (ly.previous.start, ly.previous.end) == (date(2025, 10, 1), date(2025, 10, 5))
    assert c.current.label() == "1-5 Oct 2026"


def test_month_lengths_are_clamped():
    assert pr.shift_months(date(2026, 3, 31), -1) == date(2026, 2, 28)
    c = pr.month_to_date(date(2026, 10, 31))
    assert c.previous.end == date(2026, 9, 30)


def test_last_complete_month():
    assert pr.last_complete_month(AS_OF) == date(2026, 9, 1)
    assert pr.last_complete_month(date(2026, 10, 31)) == date(2026, 10, 1)
    c = pr.complete_month_vs_last_year(AS_OF)
    assert (c.current.label(), c.previous.label()) == ("Sep 2026", "Sep 2025")
    w = pr.last_7_days(AS_OF)
    assert (w.current.start, w.previous.start, w.previous.end) == (
        date(2026, 9, 29),
        date(2026, 9, 22),
        date(2026, 9, 28),
    )
    assert {c.key for c in pr.standard(AS_OF)} == {
        "mtd_vs_prev_month", "mtd_vs_last_year", "last_7_days", "month_vs_last_year", "month_vs_prev_month",
    }  # fmt: skip


# ---------------------------------------------------------------------------
# Analysis rules
# ---------------------------------------------------------------------------


def test_formatting():
    assert an.fmt_lkr(27_042_568.63) == "LKR 27.0M"
    assert an.fmt_lkr(-950_000) == "-LKR 950K"
    assert an.fmt_change("net_revenue", 10, 12.345) == "up 12.3%"
    assert an.fmt_change("gross_margin_pct", -0.43, -2.0) == "down 0.4 points"  # percentages move in points


def test_headline_sentences():
    comp = pr.month_to_date_last_year(AS_OF)
    cur = {"net_revenue": 1_100_000.0, "gross_margin_pct": 21.0}
    prev = {"net_revenue": 1_000_000.0, "gross_margin_pct": 20.0}
    out = {i.metric: i for i in an.headline(comp, cur, prev)}
    assert out["net_revenue"].text == "Net revenue 1-5 Oct 2026: LKR 1.1M, up 10.0% on 1-5 Oct 2025 (LKR 1.0M)."
    assert out["net_revenue"].severity == 2  # a 10%+ revenue swing is notable
    assert out["gross_margin_pct"].change == 1.0
    assert out["units"].current is None and "no comparison" in out["units"].text


def test_movers_rank_explain_and_ignore_noise():
    comp = pr.complete_month_vs_last_year(AS_OF)
    rows = [("Northern", 600.0, 400.0), ("Southern", 100.0, 300.0), ("Western", 500.0, 499.0), ("Uva", 50.0, 0.0)]
    out = an.movers(comp, "province", rows)
    assert [i.item for i in out] == ["Northern", "Uva", "Southern"]  # Western's +1 is noise
    north = out[0]
    assert north.change == 200 and north.change_pct == 50.0
    assert "Northern (province) rose by LKR 200 (+50.0%)" in north.text
    assert "more than the whole net change" in north.text  # +200 of a net +51: the others fell
    assert "(no sales in Sep 2025)" in out[1].text  # nothing in the comparison period


def _days(n, end=date(2026, 10, 25)):
    return [end - timedelta(days=n - 1 - i) for i in range(n)]


def test_stock_out_detects_a_regular_seller_that_stopped():
    days = _days(60)
    hero = dict.fromkeys(days[:-3], 10.0)  # sold every day, then nothing for 3 days
    sometimes = dict.fromkeys(days[:-3:2], 5.0)  # sells every other day: silence is normal
    still_selling = dict.fromkeys(days, 3.0)
    data = {("MC600 WEEDICIDE 1LTR", "Western"): hero, ("SPRAYER", "Uva"): sometimes, ("HGL", "Central"): still_selling}
    out = an.stock_outs(data, days)
    assert [i.item for i in out] == ["MC600 WEEDICIDE 1LTR | Western"]
    assert out[0].severity == 1
    assert "no sales in Western for 3 days (last sale 22 Oct); before that it sold on 28 of 28 days" in out[0].text


def test_a_long_stock_out_keeps_being_reported():
    days = _days(60)
    hero = dict.fromkeys(days[:-12], 10.0)  # silent for 12 days: the baseline is the 28 days before the last sale
    out = an.stock_outs({("MC600 WEEDICIDE 1LTR", "Western"): hero}, days)
    assert len(out) == 1 and "for 12 days" in out[0].text


def test_no_alert_without_enough_history():
    days = _days(20)
    assert an.stock_outs({("NEW", "Western"): dict.fromkeys(days[:-3], 1.0)}, days) == []


def test_anomaly_flags_an_unusual_day_only():
    days = _days(63, end=date(2026, 10, 25))
    revenue = {d: 1_000_000.0 + (d.toordinal() % 3) * 10_000 for d in days}
    spike = days[-2]
    revenue[spike] = 3_000_000.0
    out = an.anomalies(revenue)
    assert [i.item for i in out] == [spike.isoformat()]
    assert "above a typical" in out[0].text


# ---------------------------------------------------------------------------
# Narrative
# ---------------------------------------------------------------------------


def _insight(kind, text, severity=3, metric="net_revenue", change=1.0, comparison="", dimension=""):
    return an.Insight(kind, comparison, dimension, "", metric, 1.0, 1.0, change, 1.0, severity, text)


def test_summary_order_alerts_latest_period_then_movers():
    month, mtd = "month_vs_last_year", "mtd_vs_last_year"
    ins = [
        _insight("mover", "Province up.", change=50, comparison=month, dimension="province"),
        _insight("mover", "Province down.", change=-20, comparison=month, dimension="province"),
        _insight("mover", "Product up.", change=80, comparison=month, dimension="product"),
        _insight("mover", "Channel up.", change=500, comparison=month, dimension="channel"),  # too few values: skipped
        _insight("headline", "Month headline.", severity=2, comparison=month),
        _insight("headline", "MTD headline.", comparison=mtd),
        _insight("headline", "MTD units.", metric="units", comparison=mtd),
        _insight("stock_out", "Stock-out.", severity=1),
    ]
    assert nr.summary(ins) == [
        "Stock-out.",
        "MTD headline.",
        "Month headline.",
        "Product up.",
        "Province up.",
        "Province down.",
    ]


def test_summary_puts_this_months_forecast_after_the_headlines():
    ins = [
        _insight("forecast", "Period forecast.", dimension="period"),
        _insight("forecast", "Month forecast.", dimension="month"),
        _insight("headline", "MTD headline.", comparison="mtd_vs_last_year"),
        _insight("revenue_drop", "Revenue drop.", severity=1),
    ]
    assert nr.summary(ins) == ["Revenue drop.", "MTD headline.", "Month forecast."]


# ---------------------------------------------------------------------------
# Revenue-drop alert
# ---------------------------------------------------------------------------


def test_revenue_drop_compares_the_same_weekdays_last_year():
    c = pr.last_7_days_last_year(AS_OF)
    assert (c.current.start, c.current.end) == (date(2026, 9, 29), date(2026, 10, 5))
    assert (c.previous.start, c.previous.end) == (date(2025, 9, 30), date(2025, 10, 6))
    assert c.current.start.weekday() == c.previous.start.weekday()


def test_revenue_drop_fires_only_at_the_limit():
    c = pr.last_7_days_last_year(AS_OF)
    assert an.revenue_drop(c, {"net_revenue": 91.0}, {"net_revenue": 100.0}) == []  # -9%: no alert
    [i] = an.revenue_drop(c, {"net_revenue": 90.0}, {"net_revenue": 100.0})  # -10%: alert
    assert (i.kind, i.severity, round(i.change_pct, 1)) == ("revenue_drop", 1, -10.0)
    assert i.text.startswith("Revenue drop: net revenue 29 Sep - 05 Oct 2026 is LKR 90, down 10.0%")
    assert an.revenue_drop(c, {"net_revenue": 50.0}, {"net_revenue": None}) == []  # nothing to compare with
    assert an.rank([_insight("stock_out", "s", severity=1), i])[0] is i  # the biggest alert leads


# ---------------------------------------------------------------------------
# Forecast
# ---------------------------------------------------------------------------


def _months_from(first: date, values: list[float]) -> dict:
    return {pr.shift_months(first, k): v for k, v in enumerate(values)}


def test_forecast_is_last_year_times_fiscal_year_growth():
    # FY2025 (Apr 2025-Mar 2026) then Apr-Sep 2026 at exactly +20%, and 10 days of October 2026
    last_year = [100, 120, 140, 160, 180, 200, 210, 190, 90, 110, 130, 150]
    monthly = _months_from(date(2025, 4, 1), last_year)
    monthly.update(_months_from(date(2026, 4, 1), [v * 1.2 for v in last_year[:6]]))
    monthly[date(2026, 10, 1)] = 70.0
    f = fc.build(monthly, date(2026, 10, 10))
    assert round(f.growth, 3) == 1.2 and f.spread == 0  # same growth every month: no spread
    assert f.basis == "Apr-Sep 2026 vs Apr-Sep 2025"
    assert [m.label for m in f.months] == ["Oct 2026", "Nov 2026", "Dec 2026", "Jan 2027", "Feb 2027", "Mar 2027"]
    oct_ = f.months[0]
    assert (round(oct_.forecast, 6), oct_.last_year, oct_.actual_to_date) == (252.0, 210, 70.0)
    assert round(f.months[2].forecast, 6) == 108.0  # December dip kept: 90 x 1.2
    assert f.months[1].actual_to_date is None
    assert round(f.total, 6) == round(sum(last_year[6:]) * 1.2, 6)


def test_forecast_range_widens_with_uneven_growth():
    last_year = [100.0] * 12
    monthly = _months_from(date(2025, 4, 1), last_year)
    monthly.update(_months_from(date(2026, 4, 1), [110, 130, 110, 130, 110, 130]))  # +10% / +30%
    f = fc.build(monthly, date(2026, 10, 10))
    assert round(f.growth, 3) == 1.2 and round(f.spread, 3) == 0.1
    m = f.months[0]
    assert (round(m.low, 6), round(m.forecast, 6), round(m.high, 6)) == (110.0, 120.0, 130.0)
    [month, period] = fc.insights(f)
    assert month.text.startswith("Forecast: October 2026 on track for about LKR 120 (likely LKR 110 to LKR 130)")
    assert "+20.0% (Apr-Sep 2026 vs Apr-Sep 2025)" in month.text
    assert period.text.startswith("Forecast Oct 2026 to Mar 2027: about LKR 720 in total")


def test_forecast_early_in_the_fiscal_year_uses_the_last_six_months():
    monthly = _months_from(date(2025, 1, 1), [100.0] * 17)  # Jan 2025 - May 2026, flat
    f = fc.build(monthly, date(2026, 5, 20))  # only April 2026 is complete in FY2026
    assert f.basis == "Jan-Apr 2026 vs Jan-Apr 2025"  # Nov-Dec 2025 have no year-earlier month in the data
    assert len(f.months) == 11 and f.months[-1].label == "Mar 2027"


def test_no_forecast_without_last_year():
    assert fc.build({date(2026, 9, 1): 500.0, date(2026, 10, 1): 100.0}, date(2026, 10, 10)) is None
    assert fc.insights(None) == []


class _FakeSpark:
    def __init__(self, reply):
        self.reply = reply

    def sql(self, query, args=None):
        reply = self.reply

        class R:
            def first(self):
                return {"t": reply}

        return R()


def test_ai_rewrite_keeps_our_numbers_or_falls_back():
    facts = ["Net revenue 1-5 Oct 2026: LKR 27.0M, up 4.1% on 1-5 Oct 2025."]
    assert nr.ai_rewrite(None, facts, None) == facts  # local: no model
    good = _FakeSpark("- Revenue for 1-5 Oct 2026 reached LKR 27.0M, up 4.1% on last year.")
    assert nr.ai_rewrite(good, facts, "m") == ["Revenue for 1-5 Oct 2026 reached LKR 27.0M, up 4.1% on last year."]
    invented = _FakeSpark("- Revenue reached LKR 31.5M, up 9% on last year.")
    assert nr.ai_rewrite(invented, facts, "m") == facts  # a new number: rejected


# ---------------------------------------------------------------------------
# The whole job (Spark)
# ---------------------------------------------------------------------------


def test_job_runs_end_to_end(spark, cfg, tmp_path):
    from sales_insights.insights import job

    lake = build_tiny_gold(spark, cfg, tmp_path / "lake")
    out = job.run(spark, cfg, lake=lake, deck_dir=tmp_path / "output")
    assert out["as_of"] == date(2026, 10, 10)
    saved = lake.read("gold", "insights")
    assert saved.count() == len(out["insights"]) > 0
    assert saved.filter("kind = 'headline'").count() == 5 * len(an.MEASURES)  # 5 comparisons x 6 measures
    oct_vs_ly = [i for i in out["insights"] if i.kind == "headline" and i.comparison == "mtd_vs_last_year"]
    rev = next(i for i in oct_vs_ly if i.metric == "net_revenue")
    assert (rev.current, rev.previous) == (1332.0, 1000.0)  # 1-10 Oct 2026 vs 1-10 Oct 2025 (hand-checked world)
    assert out["deck"].exists() and out["deck"].name == "weekly_deck_2026-10-10.pptx"
    assert saved.schema["text"].metadata["comment"].startswith("The finding")
    # 4-10 Oct 2026 (232) vs 5-11 Oct 2025 (1000): far below last year -> the revenue-drop alert fires
    [drop] = [i for i in out["insights"] if i.kind == "revenue_drop"]
    assert (drop.current, drop.previous) == (232.0, 1000.0)
    # no month of last year to compare with in the tiny world: no forecast, an empty gold.forecast table
    assert out["forecast"] is None and lake.read("gold", "forecast").count() == 0
    assert "forecast_net_revenue" in lake.read("gold", "forecast").columns
