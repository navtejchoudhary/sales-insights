"""Deck tests: the PowerPoint is built from plain data, so no Spark is needed."""

from pptx import Presentation
from pptx.enum.chart import XL_CHART_TYPE

from sales_insights.deck.deck import DeckData, KpiCard, build_deck


def sample(alerts=True) -> DeckData:
    return DeckData(
        title="Weekly sales insights",
        subtitle="Data up to Sunday 25 October 2026",
        cards=[
            KpiCard("Net revenue", "LKR 160.2M", "up 4.1% vs 1-25 Oct 2025", True),
            KpiCard("Units (cases)", "78,410", "down 2.0% vs 1-25 Oct 2025", False),
            KpiCard("Invoices", "1,402", "up 1.2% vs 1-25 Oct 2025", True),
            KpiCard("Gross margin %", "21.4%", "up 0.6 points vs 1-25 Oct 2025", True),
        ],
        cards_caption="Month to date (1-25 Oct 2026) compared with the same days last year (1-25 Oct 2025)",
        key_points=["Possible stock-out: MC600 WEEDICIDE 1LTR in Western.", "Revenue up 4.1%.", "Northern rose most."],
        trend=[(f"M{i}", 100 + i * 5.0) for i in range(13)],
        trend_caption="Net revenue per month, LKR millions",
        movers={
            "By province": [("Northern", 12.5), ("Southern", -4.2), ("Western", 3.1)],
            "By product group": [("WEEDICIDE", 6.0), ("INSECTICIDE", -1.5)],
        },
        movers_caption="Sep 2026 vs Sep 2025: change in net revenue, LKR millions",
        alerts=["Possible stock-out: MC600 WEEDICIDE 1LTR has had no sales in Western for 3 days."] if alerts else [],
        summary=["Point one.", "Point two."],
        about=["Data up to 25 Oct 2026."],
    )


def _texts(slide):
    return " ".join(sh.text_frame.text for sh in slide.shapes if sh.has_text_frame)


def test_deck_has_seven_slides_with_native_charts(tmp_path):
    path = build_deck(sample(), tmp_path / "deck.pptx")
    prs = Presentation(path)
    assert len(prs.slides) == 7
    s = list(prs.slides)
    assert "Weekly sales insights" in _texts(s[0])
    assert "LKR 160.2M" in _texts(s[1]) and "What to know" in _texts(s[1])
    charts = [sh.chart for sh in s[2].shapes if sh.has_chart]
    assert len(charts) == 1 and charts[0].chart_type == XL_CHART_TYPE.LINE_MARKERS
    assert len(list(charts[0].plots[0].categories)) == 13
    bars = [sh.chart for sh in s[3].shapes if sh.has_chart]
    assert len(bars) == 2 and all(c.chart_type == XL_CHART_TYPE.BAR_CLUSTERED for c in bars)
    assert "MC600 WEEDICIDE 1LTR" in _texts(s[4])
    assert "Point two." in _texts(s[5])


def test_deck_without_alerts_says_so(tmp_path):
    prs = Presentation(build_deck(sample(alerts=False), tmp_path / "deck.pptx"))
    assert "No alerts this week" in _texts(list(prs.slides)[4])
