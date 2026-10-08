"""Genie preparation tests.

Part 1 (no Spark): benchmarks and agent configuration are complete and consistent.
Part 2 (Spark): every ground-truth query runs on the tiny gold world from test_kpis.py.
The story checks themselves are proved on the real data by `python -m sales_insights.semantic.answer_key`.
"""

import re
from datetime import date
from decimal import Decimal

import yaml
from test_kpis import build_tiny_gold

from sales_insights.common.config import load_config
from sales_insights.pipeline import gold_model as gm
from sales_insights.semantic import answer_key as ak
from sales_insights.semantic import genie_setup as gs
from sales_insights.semantic import genie_sync as sync
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
        assert len(q.paraphrases) == 2 and q.question not in q.paraphrases, q.id  # reworded, not copied


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
    assert len(space["data"]) <= 6  # few, focused data assets (dim_channel is needed by the trusted queries)
    assert len(space["instructions"]) < 2800  # few, focused text instructions (each line fixes a measured error)
    assert len(space["sample_questions"]) == 5
    ids = {q.id for q in ak.load()}
    examples = {e["from_benchmark"] for e in space["example_queries"]}
    assert examples <= ids
    assert set(space["benchmarks_exclude"]) == examples  # never score Genie on questions it was taught
    for table, cols in space["hidden_columns"].items():
        assert set(cols) < set(gm.columns(table)), table  # real columns, and never all of them


def test_trusted_queries_are_well_formed():
    tqs = gs.trusted_queries()
    assert len(tqs) >= 5 and len({q.name for q in tqs}) == len(tqs)
    tables = gs.table_names(load_config("dev"))
    for q in tqs:
        declared = {p["name"] for p in q.parameters}
        used = set(re.findall(r"(?<![:\w]):(\w+)", q.sql))
        assert used == declared, q.id  # every :parameter declared, every declared one used
        assert all(p["type"] in gs.PARAM_TYPES and len(p["comment"]) > 15 for p in q.parameters), q.id
        assert "${" not in gs.render(q.sql, tables) and "sales_dev.gold.fact_sales" in gs.render(q.sql, tables)


def test_benchmark_set_scores_every_wording_of_untaught_questions():
    qs, taught = ak.load(), set(_space()["benchmarks_exclude"])
    scored = ak.scored(qs, taught)
    assert len(scored) == 3 * (len(qs) - len(taught))  # question + 2 rewordings each
    assert not {qid for _, _, qid in scored} & taught
    assert ("B01", qs[0].question, "B01") in scored and ("B01-b", qs[0].paraphrases[1], "B01") in scored


def test_setup_sheet_uses_real_table_names(tmp_path):
    md_path, csv_path = gs.write(load_config("dev"), tmp_path, as_of=date(2026, 10, 26))
    md = md_path.read_text()
    assert "`sales_dev.gold.sales_metrics`" in md and "${" not in md
    assert md.count("### T0") == len(gs.trusted_queries())
    rows = csv_path.read_text().splitlines()
    assert rows[0] == "benchmark_id,question,ground_truth_sql" and len(rows) == 1 + 75


def test_demo_week_questions_are_not_scored_before_their_data_exists():
    qs, taught = ak.load(), set(_space()["benchmarks_exclude"])
    tables = gs.table_names(load_config("dev"))
    early = gs.benchmarks(qs, taught, tables, as_of=date(2026, 10, 8))
    later = gs.benchmarks(qs, taught, tables, as_of=date(2026, 10, 26))
    assert len(later) == 75 and len(early) == 72  # B30 (stock-out from 23 Oct) and its 2 rewordings wait
    assert not any(r["benchmark_id"].startswith("B30") for r in early)


def test_sync_replaces_only_the_benchmarks():
    space = {
        "version": 2,
        "data_sources": {"tables": [{"identifier": "sales_dev.gold.fact_sales"}]},
        "instructions": {"text_instructions": [{"content": ["keep me"]}]},
        "benchmarks": {"questions": [{"id": "old", "question": ["old?"], "answer": []}]},
    }
    rows = [
        {"benchmark_id": "B01", "question": "Q1?", "ground_truth_sql": "SELECT 1"},
        {"benchmark_id": "B02", "question": "Q2?", "ground_truth_sql": "SELECT 2"},
    ]
    new = sync.replace_benchmarks(space, rows)
    qs = new["benchmarks"]["questions"]
    assert sorted(q["question"][0] for q in qs) == ["Q1?", "Q2?"]
    assert {q["answer"][0]["content"][0] for q in qs} == {"SELECT 1", "SELECT 2"}
    assert [q["id"] for q in qs] == sorted(q["id"] for q in qs)
    assert new["instructions"] == space["instructions"] and new["data_sources"] == space["data_sources"]
    assert space["benchmarks"]["questions"][0]["id"] == "old"  # the input is not modified


def test_every_query_runs_on_gold(spark, cfg, tmp_path):
    lake = build_tiny_gold(spark, cfg, tmp_path / "lake")
    rows = ak.build(spark, lake)
    assert len(rows) == len(ak.load())
    by_id = {r["id"]: r for r in rows}
    assert by_id["B30"]["story_check"] == "PENDING"  # tiny world ends 10 Oct; the stock-out starts 23 Oct
    assert by_id["B27"]["expected_answer"].startswith("month=2026-09-01")  # last COMPLETE month
    path = ak.write_csv(rows, tmp_path / "answer_key.csv")
    assert path.read_text().splitlines()[0].startswith("id,category,question,expected_answer")


def test_trusted_queries_give_the_hand_calculated_answers(spark, cfg, tmp_path):
    """Tiny world from test_kpis.py: data until 10 Oct 2026, so last month = Sep 2026, this month = 1-10 Oct."""
    lake = build_tiny_gold(spark, cfg, tmp_path / "lake")
    tables = mvs.table_names(lake)
    tq = {q.name: q for q in gs.trusted_queries()}

    def run(name, **args):
        q = tq[name]
        return [r.asDict() for r in spark.sql(gs.render(q.sql, tables), args={**q.example_args(), **args}).collect()]

    for q in tq.values():
        assert run(q.name), q.id  # every query runs with its example parameters

    sep = run("kpis_for_period", period="last_complete_month")[0]
    assert (sep["period_start"], sep["data_until"]) == (date(2026, 9, 1), date(2026, 9, 30))
    assert (sep["net_revenue"], sep["invoices"], float(sep["gross_margin_pct"])) == (Decimal("500.00"), 1, 40.0)
    mtd = run("kpis_for_period", period="month_to_date")[0]
    assert (mtd["net_revenue"], mtd["active_customers"], mtd["data_until"]) == (
        Decimal("1332.00"),
        2,
        date(2026, 10, 10),
    )
    assert (float(mtd["gross_margin_pct"]), float(mtd["returns_rate_pct"])) == (35.4, 5.9)

    vs_ly = run("compare_period", period="month_to_date", compare_to="last_year")[0]
    assert (vs_ly["previous_start"], vs_ly["previous_end"]) == (date(2025, 10, 1), date(2025, 10, 10))
    assert (vs_ly["net_revenue"], vs_ly["previous_net_revenue"]) == (Decimal("1332.00"), Decimal("1000.00"))
    assert (float(vs_ly["net_revenue_change_pct"]), vs_ly["invoices"], vs_ly["previous_invoices"]) == (33.2, 2, 1)
    vs_aug = run("compare_period", period="last_complete_month", compare_to="previous_period")[0]
    assert vs_aug["previous_start"] == date(2026, 8, 1) and vs_aug["previous_net_revenue"] is None  # no August sales

    top = run("top_by_dimension", dimension="province", fiscal_year=2026, top_n=5)
    assert [(r["item"], r["net_revenue"], float(r["share_of_total_pct"])) for r in top] == [
        ("Western", Decimal("1550.00"), 84.6),
        ("Southern", Decimal("282.00"), 15.4),
    ]
    per_cust = run("revenue_per_active_customer", group_by="province", fiscal_year=2026)
    assert [(r["grp"], r["active_customers"]) for r in per_cust] == [("Western", 1), ("Southern", 1)]

    returns = run("returns_rate_by_product", since_date=date(2026, 7, 1), top_n=5)
    assert (returns[0]["product"], float(returns[0]["returns_rate_pct"])) == ("WEED KILLER 1L", 4.76)

    trend = run("monthly_trend", filter_by="product_group", filter_value="weed", fiscal_year=2026)
    assert [(r["month"], r["net_revenue"], float(r["price_per_case"])) for r in trend] == [
        (date(2026, 9, 1), Decimal("500.00"), 100.0),
        (date(2026, 10, 1), Decimal("1250.00"), 100.0),
    ]
