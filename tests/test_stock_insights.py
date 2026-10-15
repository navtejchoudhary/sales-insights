"""Stock insight rules (insights/stock.py): plain Python, no Spark.

Positions as on 29 Oct 2026 in the generated data, rounded: the Colombo depot ran out of MC6001LT at the end
of 22 Oct while the Galle hub still holds stock; a slow seller sits on 500+ days of cover.
"""

from datetime import date

from sales_insights.insights import stock as st
from sales_insights.insights.analysis import rank

AS_OF = date(2026, 10, 29)


def pos(**kw) -> st.Position:
    base = {"location": "Colombo Depot", "location_type": "Depot", "province": "Western", "product": "MC600 1L",
            "stock": 100.0, "avg_daily": 10.0, "unit_cost": 1000.0, "is_stock_out": False, "is_low_cover": False}  # fmt: skip
    return st.Position(**{**base, **kw})


def test_confirmed_stock_out_names_the_day_the_rate_and_the_hub():
    p = pos(stock=0.0, avg_daily=56.1, is_stock_out=True, out_since=date(2026, 10, 22), avg_before=74.0,
            supplier="Galle Industrial - Distribution Hub", supplier_stock=12894.0)  # fmt: skip
    (i,) = st.confirmed_stock_outs([p, pos()], AS_OF)
    assert (i.kind, i.severity, i.item) == ("stock_out_confirmed", 1, "MC600 1L | Colombo Depot")
    assert "since 22 Oct (8 days)" in i.text and "about 74 a day" in i.text
    assert "Galle Industrial - Distribution Hub still holds 12,894." in i.text


def test_stock_out_without_history_or_supplier_still_reads_well():
    (i,) = st.confirmed_stock_outs(
        [pos(stock=0.0, avg_daily=0.5, is_stock_out=True, location_type="Plant store")], AS_OF
    )
    assert "since 29 Oct (1 day)" in i.text and "about 0.5 a day." in i.text and "holds" not in i.text


def test_low_cover_only_for_depots_shortest_first():
    found = st.low_cover([
        pos(location="Galle Depot", stock=1.0, avg_daily=2.5, is_low_cover=True),
        pos(location="Kandy Depot", stock=10.0, avg_daily=2.0, is_low_cover=True),
        pos(location="Hub", location_type="Plant store", stock=1.0, avg_daily=1.0, is_low_cover=True),
        pos(location="Matara Depot", stock=500.0, avg_daily=2.0),
    ])  # fmt: skip
    assert [i.item for i in found] == ["MC600 1L | Galle Depot", "MC600 1L | Kandy Depot"]
    assert "about 0.4 days at its usual 2.5 a day" in found[0].text and found[0].severity == 2


def test_overstock_is_ranked_by_value_above_120_days_with_a_total():
    cheap_but_slow = pos(location="Galle Depot", product="CHEAP", stock=1000.0, avg_daily=1.0, unit_cost=10.0)
    dear = pos(location="Negombo Depot", product="KAR4L", stock=626.0, avg_daily=1.2, unit_cost=7000.0)
    normal = pos(location="Kandy Depot", stock=100.0, avg_daily=10.0)  # 10 days of cover
    found = st.overstock([cheap_but_slow, dear, normal], top=1)
    total, top = found
    assert total.kind == "overstock_total" and "2 stock positions hold more than 120 days" in total.text
    assert st.excess_value(dear) == (626.0 - 120 * 1.2) * 7000.0  # LKR 3.4M above 120 days
    assert top.item == "KAR4L | Negombo Depot" and "522 days of cover" in top.text
    assert "LKR 3.4M of it is above 120 days" in top.text


def test_positions_without_issues_are_never_overstock():
    """The hub issues nothing to customers (it supplies depots): no cover, so never called overstock."""
    assert st.overstock([pos(location="Hub", stock=12894.0, avg_daily=0.0)]) == []


def test_stock_findings_rank_alerts_first():
    found = st.stock_insights(
        [pos(stock=0.0, is_stock_out=True), pos(stock=5.0, avg_daily=2.0, is_low_cover=True),
         pos(stock=5000.0, avg_daily=1.0)], AS_OF,
    )  # fmt: skip
    assert [i.kind for i in rank(found)][:2] == ["stock_out_confirmed", "low_cover"]
    assert set(st.ALERT_KINDS) <= set(st.STOCK_KINDS)
