"""Inventory semantic layer: metric views with window measures, KPI SQL twins, Genie agents (Stock, Sales & Stock).

Part 1 (no Spark): the window-measure compiler, the YAML and SQL files, the benchmarks and agent configuration.
Part 2 (Spark): a tiny stock world where every month-end value is worked out by hand below; the metric view
must give the last day's stock (never a sum over days) and agree with the KPI SQL.
The real-data checks (every KPI agrees, every benchmark query runs) are in test_inventory_pipeline.py.

The tiny world (product P, standard cost 10; L1 = Colombo Depot, Western; L2 = Galle hub, Southern):
  date        L1 stock  L1 issued  L1 avg 28d   L2 stock
  2026-09-29        10          2          1.0         100
  2026-09-30         4          6          1.0          90
  2026-10-01         0          4          2.0          80     <- L1 stock-out
September stock on hand = 4 + 90 = 94 (30 Sep), not 10+4+100+90; October = 0 + 80 = 80; issued Sep = 8.
"""

import re
from datetime import date
from decimal import Decimal

import pytest
import yaml

from sales_insights.common.config import load_config
from sales_insights.pipeline import gold_model as gm
from sales_insights.pipeline import inventory_model as im
from sales_insights.semantic import answer_key as ak
from sales_insights.semantic import genie_setup as gs
from sales_insights.semantic import kpis as k
from sales_insights.semantic import metric_views as mvs

INV = ak.SPACES["inventory"]
BOTH = ak.SPACES["sales_and_stock"]
SHARED = {t: gm.TABLES[t] for t in mvs.GOLD_TABLES}
ALL_TABLES = {**SHARED, **im.TABLES}


def _space(agent):
    return yaml.safe_load((agent.folder / "space.yaml").read_text())


# ---------------------------------------------------------------------------
# Part 1: the window-measure compiler (no Spark)
# ---------------------------------------------------------------------------


def test_only_on_last_day_wraps_every_aggregate():
    wrap = mvs.only_on_last_day
    assert wrap("SUM(s.q)", "s.d", "L.d") == "SUM(CASE WHEN s.d = L.d THEN s.q END)"
    assert wrap("COUNT_IF(s.flag)", "s.d", "L.d") == "COUNT_IF(CASE WHEN s.d = L.d THEN s.flag END)"
    assert wrap("COUNT(DISTINCT s.id)", "s.d", "L.d") == "COUNT(DISTINCT CASE WHEN s.d = L.d THEN s.id END)"
    assert wrap("COUNT(*)", "s.d", "L.d") == "COUNT(CASE WHEN s.d = L.d THEN 1 END)"
    assert wrap("SUM(COALESCE(s.q, 0)) / SUM(s.a)", "s.d", "L.d") == (
        "SUM(CASE WHEN s.d = L.d THEN COALESCE(s.q, 0) END) / SUM(CASE WHEN s.d = L.d THEN s.a END)"
    )
    assert wrap("SUM(CASE WHEN s.t = ')' THEN s.q END)", "s.d", "L.d").endswith("THEN s.q END END)")  # quotes


def test_only_current_last_windows_are_compiled_locally():
    def view(window):
        spec = {
            "fields": [{"name": "d", "expr": "source.d"}],
            "measures": [{"name": "m", "expr": "SUM(x)", "window": [window]}],
        }
        return mvs.MetricView("v", "", spec)

    assert view({"order": "d", "range": "current", "semiadditive": "last"}).windows == {
        "m": {"order": "d", "range": "current", "semiadditive": "last"}
    }
    for unsupported in (
        {"order": "d", "range": "trailing 7 day", "semiadditive": "last"},
        {"order": "d", "range": "current", "semiadditive": "first"},
        {"order": "d", "range": "current", "semiadditive": "last", "offset": "-1 month"},
    ):
        with pytest.raises(NotImplementedError):
            _ = view(unsupported).windows
    with pytest.raises(ValueError, match="not a field"):
        _ = view({"order": "nope", "range": "current", "semiadditive": "last"}).windows


def test_window_query_takes_the_last_day_of_each_group():
    mv = mvs.load("inventory_metrics")
    tables = {t: f"t_{t}" for t in ALL_TABLES}
    sql = mvs.compile_query(mv, tables, ["month", "province"], ["stock_on_hand", "issued_quantity", "days_of_cover"])
    assert "LEFT JOIN (" in sql and "MAX(source.snapshot_date) AS `snapshot_date`" in sql
    assert "date_dim.month_start_date <=> __last.`__k0` AND location_dim.province <=> __last.`__k1`" in sql
    assert "SUM(CASE WHEN source.snapshot_date = __last.`snapshot_date` THEN source.stock_quantity END)" in sql
    assert "SUM(source.issued_quantity)" in sql  # a flow: summed over every day, not wrapped
    assert "MEASURE" not in sql
    no_fields = mvs.compile_query(mv, tables, [], ["stock_on_hand"])
    assert "CROSS JOIN (" in no_fields and "GROUP BY" not in no_fields.split(") AS __last")[-1]
    flows_only = mvs.compile_query(mv, tables, ["month"], ["issued_quantity"])
    assert "__last" not in flows_only  # no window measure asked: the query stays a plain GROUP BY


def test_sales_metric_view_compiles_exactly_as_before():
    """No window measures in sales_metrics: the compiled SQL keeps its old shape (sales unchanged)."""
    mv = mvs.load()
    sql = mvs.compile_query(mv, {t: f"t_{t}" for t in mvs.GOLD_TABLES}, ["month"], ["net_revenue", "gross_margin_pct"])
    assert mv.windows == {} and "__last" not in sql
    assert sql.startswith(
        "SELECT date_dim.month_start_date AS `month`, SUM(source.net_revenue_amount) AS `net_revenue`"
    )
    assert sql.endswith("LEFT JOIN t_dim_date AS date_dim ON source.invoice_date = date_dim.date\nGROUP BY 1")


class _Lake:
    def __init__(self, area, prefix):
        self.cfg = type("Cfg", (), {"area": area})()
        self.prefix = prefix

    def sql_name(self, layer, name):
        return f"{self.prefix}.{layer}.{name}"


def test_table_names_mix_the_area_tables_with_the_shared_ones():
    sales, inventory = _Lake("sales", "sales_dev"), _Lake("inventory", "inventory_dev")
    assert mvs.table_names(sales) == {t: f"sales_dev.gold.{t}" for t in mvs.GOLD_TABLES}  # as before
    names = mvs.table_names(inventory, sales)
    assert names["dim_product"] == "sales_dev.gold.dim_product" and names["fact_sales"] == "sales_dev.gold.fact_sales"
    assert names["fact_stock_daily"] == "inventory_dev.gold.fact_stock_daily"
    assert names["fact_sales_stock_daily"] == "inventory_dev.gold.fact_sales_stock_daily"
    with pytest.raises(ValueError, match="pass sales_lake"):
        k.area_tables(inventory)


# ---------------------------------------------------------------------------
# Part 1: metric views, KPI SQL, agreements (no Spark)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["inventory_metrics", "sales_and_stock"])
def test_inventory_metric_views_only_use_real_gold_columns(name):
    mv = mvs.load(name)
    assert mv.spec["version"] == 1.1
    alias_table = {"source": mv.spec["source"].strip("${}")}
    for j in mv.spec["joins"]:
        alias_table[j["name"]] = j["source"].strip("${}")
    exprs = list(mv.fields.values()) + list(mv.measures.values()) + [j["on"] for j in mv.spec["joins"]]
    for expr in exprs:
        for alias, col in re.findall(r"\b(\w+)\.(\w+)\b", expr):
            assert alias in alias_table, f"unknown alias {alias} in {expr}"
            assert col in gm.columns(alias_table[alias], ALL_TABLES), f"{alias}.{col} is not a gold column"
    for m in mv.spec["measures"]:
        assert m.get("display_name") and m.get("comment"), m["name"]
    for measure in mv.measures:
        mvs.expand(mv, measure, windows=True)  # every MEASURE() resolves, every window compiles


def test_stock_measures_are_levels_and_flows_add_up():
    mv = mvs.load("inventory_metrics")
    assert set(mv.windows) == {
        "stock_on_hand", "stock_value", "in_transit", "avg_daily_issued_28d", "stock_out_positions",
        "low_cover_positions", "active_positions",
    }  # fmt: skip
    assert "issued_quantity" not in mv.windows and "stock_out_days" not in mv.windows
    both = mvs.load("sales_and_stock")
    assert set(both.windows) == {"depot_stock", "depot_in_transit", "avg_daily_issued_28d", "stock_out_positions"}
    assert not {"net_revenue", "units_sold", "units_invoiced"} & set(both.windows)


def test_inventory_kpi_sql_files_render_and_match_the_metric_views():
    area = k.AREAS["inventory"]
    assert k.names(area.folder) == ["kpi_sales_and_stock_monthly", "kpi_stock_daily", "kpi_stock_month_end"]
    assert k.names() == k.names(k.AREAS["sales"].folder)  # sales KPI list unchanged
    tables = {t: f"t_{t}" for t in ALL_TABLES}
    for name in k.names(area.folder):
        assert "${" not in k.render(name, tables, area.folder)
    assert {a.kpi for a in area.agreements} == set(k.names(area.folder))
    for a in area.agreements:
        mv = mvs.load(a.metric_view)
        assert a.metric_view in area.metric_views
        assert set(a.keys.values()) <= set(mv.fields) and set(a.measures) <= set(mv.measures), a.kpi


# ---------------------------------------------------------------------------
# Part 1: Genie agents (no Spark)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("agent", "prefix", "count"), [(INV, "INV", 25), (BOTH, "SS", 15)])
def test_inventory_benchmarks_are_complete(agent, prefix, count):
    qs = ak.load(agent.benchmarks)
    assert len(qs) == count and len({q.id for q in qs}) == count
    assert all(q.id.startswith(prefix) for q in qs)
    assert {q.category for q in qs} <= ak.CATEGORIES
    tables = {t: f"t_{t}" for t in ALL_TABLES}
    for q in qs:
        assert "${" not in ak.render(q, tables), q.id
        if q.check:
            compile(q.check, q.id, "eval")
        assert len(q.paraphrases) == 2 and q.question not in q.paraphrases, q.id


def test_every_inventory_story_is_tested():
    stories = {q.story.split()[0] for q in ak.load(INV.benchmarks) if q.category == "story"}
    assert stories == {"I1", "I2", "I3", "I4"}
    combined = " ".join(q.story for q in ak.load(BOTH.benchmarks) if q.category == "story")
    assert {"I1", "I2", "I3", "I4", "7"} <= set(combined.split())
    demo = next(q for q in ak.load(BOTH.benchmarks) if q.id == "SS01")
    assert "Why did sales of MC6001LT stop in Western" in demo.question and demo.check


def test_checks_can_use_names_inside_generator_expressions():
    q = ak.Question("X", "story", "q?", "SELECT 1", check="all(r['d'] >= date(2026, 10, 23) for r in rows)")
    assert ak.evaluate(q, [{"d": date(2026, 10, 24)}], date(2026, 10, 30)) == "PASS"
    assert ak.evaluate(q, [{"d": date(2026, 10, 22)}], date(2026, 10, 30)) == "FAIL"


@pytest.mark.parametrize("agent", [ak.SPACES["sales"], INV, BOTH])
def test_checks_never_multiply_by_a_float(agent):
    """Spark returns DECIMAL for ROUND(...) and sums: Decimal * 1.3 raises TypeError, so the check would always
    FAIL on Databricks and on the Mac (it passed on floats). Scale with whole numbers instead (10 * x > 13 * y)."""
    for q in ak.load(agent.benchmarks):
        assert not re.search(r"\d\.\d", q.check or ""), q.id


def test_growth_story_checks_work_on_decimals():
    qs = {q.id: q for q in [*ak.load(INV.benchmarks), *ak.load(BOTH.benchmarks)]}
    after = date(2026, 10, 30)
    assert (
        ak.evaluate(qs["INV16"], [{"avg_daily_before": Decimal("6.0"), "avg_daily_since": Decimal("13.9")}], after)
        == "PASS"
    )
    assert (
        ak.evaluate(qs["INV16"], [{"avg_daily_before": Decimal("6.0"), "avg_daily_since": Decimal("7.0")}], after)
        == "FAIL"
    )
    both = {"avg_daily_units_before": Decimal("6.0"), "avg_daily_units_since": Decimal("13.9")}
    assert ak.evaluate(qs["SS03"], [both], after) == "PASS"


@pytest.mark.parametrize("agent", [INV, BOTH])
def test_inventory_agents_follow_genie_best_practice(agent):
    space = _space(agent)
    assert len(space["data"]) <= 6
    assert len(space["instructions"]) < 2800
    assert len(space["sample_questions"]) == 5
    ids = {q.id for q in ak.load(agent.benchmarks)}
    examples = {e["from_benchmark"] for e in space["example_queries"]}
    assert examples <= ids and set(space["benchmarks_exclude"]) == examples
    for table, cols in space["hidden_columns"].items():
        assert set(cols) < set(gm.columns(table, ALL_TABLES)), table
    assert "never" in space["instructions"].lower()  # stock is never summed over days


def test_the_demo_question_is_scored_not_taught():
    assert "SS01" not in _space(BOTH)["benchmarks_exclude"]


@pytest.mark.parametrize(("agent", "scored"), [(INV, 3 * (25 - 5)), (BOTH, 3 * (15 - 3))])
def test_inventory_setup_sheets_use_real_table_names(tmp_path, agent, scored):
    md_path, csv_path = gs.write(load_config("dev"), tmp_path, as_of=date(2026, 10, 30), agent=agent)
    assert md_path.name == f"genie_setup_{agent.name}.md" and csv_path.name == f"genie_benchmarks_{agent.name}.csv"
    md = md_path.read_text()
    assert "${" not in md and "`sales_dev.gold.dim_product`" in md
    assert "`inventory_dev.gold." in md and "None for this agent yet." in md
    assert len(csv_path.read_text().splitlines()) == 1 + scored


def test_live_story_questions_wait_for_their_data():
    cfg = load_config("dev")
    early = gs.sheet(cfg, date(2026, 10, 14), INV)[1]
    assert len(early) == 3 * (25 - 5) - 6  # INV14 (22 Oct) and INV16 (25 Oct) and their rewordings wait
    assert not {r["benchmark_id"][:5] for r in early} & {"INV14", "INV16"}
    assert len(gs.sheet(cfg, date(2026, 10, 14), BOTH)[1]) == 3 * (15 - 3) - 9  # SS01, SS03, SS11 wait


def test_sales_setup_sheet_names_are_unchanged(tmp_path):
    md_path, csv_path = gs.write(load_config("dev"), tmp_path, as_of=date(2026, 10, 26))
    assert (md_path.name, csv_path.name) == ("genie_setup.md", "genie_benchmarks.csv")
    assert gs.table_names(load_config("dev"))["fact_sales"] == "sales_dev.gold.fact_sales"


# ---------------------------------------------------------------------------
# Part 2: the tiny stock world (Spark)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def tiny(spark, cfg):
    from sales_insights.pipeline import gold as g

    D = date.fromisoformat
    stock = [
        # snapshot_date, location, stock, value, issued, avg 28d, cover, stock-out, low cover
        ("2026-09-29", "L1", "10", "100.00", "2", "1.000", "10.0", False, False),
        ("2026-09-30", "L1", "4", "40.00", "6", "1.000", "4.0", False, True),
        ("2026-10-01", "L1", "0", "0.00", "4", "2.000", None, True, False),
        ("2026-09-29", "L2", "100", "1000.00", "0", "0.000", None, False, False),
        ("2026-09-30", "L2", "90", "900.00", "0", "0.000", None, False, False),
        ("2026-10-01", "L2", "80", "800.00", "0", "0.000", None, False, False),
    ]
    rows = [
        (D(d), loc, "P", Decimal(q), Decimal(0), Decimal(v), Decimal(i), Decimal(a), None if c is None else Decimal(c), so, lc)
        for d, loc, q, v, i, a, c, so, lc in stock
    ]  # fmt: skip
    spark.createDataFrame(
        rows,
        "snapshot_date date, location_id string, product_id string, stock_quantity decimal(18,3), "
        "in_transit_quantity decimal(18,3), stock_value_amount decimal(18,2), issued_quantity decimal(18,3), "
        "avg_daily_issued_quantity_28d decimal(18,3), days_of_cover decimal(18,1), is_stock_out boolean, "
        "is_low_cover boolean",
    ).createOrReplaceTempView("tiny_stock")
    spark.createDataFrame(
        [("L1", "Colombo Depot", "Depot", "C001", "Western"), ("L2", "Galle hub", "Plant store", None, "Southern")],
        "location_id string, location_name string, location_type string, sales_office_code string, province string",
    ).createOrReplaceTempView("tiny_location")
    spark.createDataFrame(
        [("P", "WEED KILLER 1L", "WEEDICIDE", "ZFRT")],
        "product_id string, product_description string, product_group_name string, product_type_name string",
    ).createOrReplaceTempView("tiny_product")
    g.build_dim_date(
        spark, date(2026, 4, 1), date(2027, 3, 31), cfg.business, as_of=date(2026, 10, 1)
    ).createOrReplaceTempView("tiny_date")
    return {"fact_stock_daily": "tiny_stock", "dim_location": "tiny_location", "dim_product": "tiny_product",
            "dim_date": "tiny_date"}  # fmt: skip


def _by(df, key):
    return {r[key]: r for r in df.collect()}


def test_stock_by_month_is_the_last_day_not_a_sum(spark, tiny):
    mv = mvs.load("inventory_metrics")
    measures = [
        "stock_on_hand",
        "stock_value",
        "issued_quantity",
        "stock_out_positions",
        "stock_out_days",
        "days_of_cover",
    ]
    months = _by(mvs.query(spark, None, ["month"], measures, mv, tiny), "month")
    sep, oct_ = months[date(2026, 9, 1)], months[date(2026, 10, 1)]
    assert (sep["stock_on_hand"], sep["stock_value"], sep["issued_quantity"]) == (94, Decimal("940.00"), 8)
    assert float(sep["days_of_cover"]) == 94.0  # 94 / (1 + 0)
    assert (sep["stock_out_positions"], sep["stock_out_days"]) == (0, 0)
    assert (oct_["stock_on_hand"], oct_["stock_out_positions"], oct_["stock_out_days"]) == (80, 1, 1)
    latest = mvs.query(spark, None, [], ["stock_on_hand"], mv, tiny).collect()[0]
    assert latest["stock_on_hand"] == 80  # no date asked: the latest day


def test_last_day_is_per_group(spark, tiny):
    mv = mvs.load("inventory_metrics")
    by_loc = _by(
        mvs.query(spark, None, ["location", "month"], ["stock_on_hand"], mv, tiny).filter("month = DATE'2026-09-01'"),
        "location",
    )
    assert (by_loc["Colombo Depot"]["stock_on_hand"], by_loc["Galle hub"]["stock_on_hand"]) == (4, 90)
    daily = _by(mvs.query(spark, None, ["snapshot_date"], ["stock_on_hand"], mv, tiny), "snapshot_date")
    assert [daily[date(2026, 9, d)]["stock_on_hand"] for d in (29, 30)] == [110, 94]


def test_month_end_kpi_sql_agrees_with_the_metric_view(spark, tiny):
    area = k.AREAS["inventory"]
    for a in (x for x in area.agreements if x.metric_view == "inventory_metrics"):
        kpi_df = spark.sql(k.render(a.kpi, tiny, area.folder))
        mv_df = mvs.query(spark, None, list(a.keys.values()), a.measures, mvs.load(a.metric_view), tiny)
        assert kpi_df.count() > 0 and k.compare(kpi_df, mv_df, a.keys, a.measures).count() == 0, a.kpi
