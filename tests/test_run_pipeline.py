"""One-command pipeline tests: source bootstrap without Spark, then a 3-day run through every step."""

from datetime import date

import pytest

from sales_insights.common.lake import Lake
from sales_insights.generator import history as h
from sales_insights.generator import masters as m
from sales_insights.pipeline import run_pipeline as rp

AS_OF = date(2026, 10, 1)
SEED = 20261001


def test_day_range():
    assert rp.day_range(date(2026, 10, 30), date(2026, 11, 2)) == [
        date(2026, 10, 30), date(2026, 10, 31), date(2026, 11, 1), date(2026, 11, 2),
    ]  # fmt: skip
    with pytest.raises(ValueError):
        rp.day_range(date(2026, 10, 2), date(2026, 10, 1))


@pytest.fixture
def source(tmp_path):
    """Master data + a short history already in staging, so the test does not build 18 months."""
    staging, landing = tmp_path / "staging", tmp_path / "landing"
    tables, log = m.build_masters(SEED, AS_OF)
    m.write_masters(tables, log, staging, SEED, AS_OF)
    h.write_history(h.build_history(SEED, AS_OF, start=date(2026, 9, 25)), staging, SEED, AS_OF)
    return staging, landing


def test_ensure_source_generates_missing_days_and_delivers_once(cfg, source):
    staging, landing = source
    done = rp.ensure_source(cfg, staging, landing, date(2026, 10, 3))
    assert done == ["generated 3 daily drop(s)", "initial delivery of master data and history"]
    assert (staging / "business_date=2026-10-03" / "manifest_20261003.json").exists()
    assert (landing / "masters").exists() and not (landing / "business_date=2026-10-01").exists()
    assert rp.ensure_source(cfg, staging, landing, date(2026, 10, 3)) == []  # second call: nothing to do


def test_three_days_through_every_step(spark, cfg, source, tmp_path):
    staging, landing = source
    lines = []
    results = rp.run_range(
        spark, cfg, [date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3)],
        staging=staging, landing=landing, lake=Lake(spark, cfg, root=tmp_path / "lake"), downstream="last",
        deck_dir=tmp_path / "output", strict_stories=False, write_answer_key=False, echo=lines.append,
    )  # fmt: skip
    assert [r.ok for r in results] == [True, True, True]
    assert [len(r.steps) for r in results] == [2, 2, 8]  # drip + bronze daily, everything after the last day
    final = {st.step: st.detail for st in results[-1].steps}
    assert final["reconcile"].endswith("checks passed")
    assert final["kpis"] == "4 KPI views match the metric view"
    assert "deck weekly_deck_2026-10-03.pptx" in final["insights"]
    report = rp.write_report(results, tmp_path / "report.csv")
    assert len(report.read_text().splitlines()) == 4
    assert any("bronze" in line for line in lines)
