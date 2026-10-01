"""Genie preparation tests.

Part 1 (no Spark): benchmarks and agent configuration are complete and consistent.
Part 2 (Spark): every ground-truth query runs on the tiny gold world from test_kpis.py.
The story checks themselves are proved on the real data by `python -m sales_insights.semantic.answer_key`.
"""

from datetime import date
from decimal import Decimal

import yaml
from test_kpis import build_tiny_gold

from sales_insights.semantic import answer_key as ak
from sales_insights.semantic import metric_views as mvs


def _space():
    return yaml.safe_load((ak.GENIE / "space.yaml").read_text())


def test_benchmarks_are_complete():
    qs = ak.load()
    assert 25 <= len(qs) <= 30
    assert len({q.id for q in qs}) == len(qs)
    assert {q.category for q in qs} <= ak.CATEGORIES
    tables = {t: f"t_{t}" for t in mvs.GOLD_TABLES}
    for q in qs:
        assert "${" not in ak.render(q, tables), q.id
        if q.check:
            compile(q.check, q.id, "eval")


def test_every_planted_story_is_tested():
    stories = {q.story.split()[0] for q in ak.load() if q.category == "story"}
    assert stories == {"1", "2", "3", "4", "5", "6", "7"}


def test_evaluate_pass_fail_pending():
    q = ak.Question("X", "story", "q?", "SELECT 1", check="row['province'] == 'Southern'")
    assert ak.evaluate(q, [{"province": "Southern"}], date(2026, 10, 5)) == "PASS"
    assert ak.evaluate(q, [{"province": "Western"}], date(2026, 10, 5)) == "FAIL"
    assert ak.evaluate(q, [], date(2026, 10, 5)) == "FAIL"  # no rows: the check cannot hold
    live = ak.Question("Y", "story", "q?", "SELECT 1", check="True", needs_data_until=date(2026, 10, 25))
    assert ak.evaluate(live, [{}], date(2026, 10, 5)) == "PENDING"
    assert ak.evaluate(live, [{}], date(2026, 10, 29)) == "PASS"
    assert ak.evaluate(ak.Question("Z", "basic", "q?", "SELECT 1"), [{}], None) == ""


def test_answers_are_readable():
    text = ak.as_text([{"province": "Southern", "revenue": Decimal("1794371.5")}])
    assert text == "province=Southern, revenue=1,794,371.50"
    assert ak.as_text([{"n": i} for i in range(12)]).endswith("(12 rows)")


def test_space_configuration_follows_genie_best_practice():
    space = _space()
    assert len(space["data"]) <= 5  # Databricks: five or fewer data assets
    assert len(space["instructions"]) < 2000  # few, focused text instructions
    assert len(space["sample_questions"]) == 5
    ids = {q.id for q in ak.load()}
    examples = {e["from_benchmark"] for e in space["example_queries"]}
    assert examples <= ids
    assert set(space["benchmarks_exclude"]) == examples  # never score Genie on questions it was taught


def test_every_query_runs_on_gold(spark, cfg, tmp_path):
    lake = build_tiny_gold(spark, cfg, tmp_path / "lake")
    rows = ak.build(spark, lake)
    assert len(rows) == len(ak.load())
    by_id = {r["id"]: r for r in rows}
    assert by_id["B30"]["story_check"] == "PENDING"  # tiny world ends 10 Oct; the stock-out starts 23 Oct
    assert by_id["B27"]["expected_answer"].startswith("month=2026-09-01")  # last COMPLETE month
    path = ak.write_csv(rows, tmp_path / "answer_key.csv")
    assert path.read_text().splitlines()[0].startswith("id,category,question,expected_answer")
