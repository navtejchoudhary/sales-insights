"""Gold + reconciliation tests.

Part 1: the model file and each builder on tiny tables.
Part 2: masters + history + 13, 14, 17 and 22 Oct through bronze -> silver -> gold -> reconcile,
        proved against the manifests per invoice date; then a tampered fact must FAIL reconciliation.
Corrections arriving in a later delivery (same invoice line, newer version) must still reconcile.
"""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from sales_insights.common.lake import Lake
from sales_insights.drip import drip
from sales_insights.generator import history as h
from sales_insights.generator import masters as m
from sales_insights.generator import simulate as s
from sales_insights.pipeline import bronze as b
from sales_insights.pipeline import gold as g
from sales_insights.pipeline import gold_model as gm
from sales_insights.pipeline import reconcile as rc
from sales_insights.pipeline import silver as sv

AS_OF = date(2026, 10, 1)
SEED = 20261001
DAYS = (13, 14, 17, 22)
ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Part 1a: the model (no Spark)
# ---------------------------------------------------------------------------


def test_every_table_and_column_has_a_description():
    for table, spec in gm.TABLES.items():
        assert len(spec["description"]) > 30, table
        for col, text in spec["columns"].items():
            assert len(text) > 10, f"{table}.{col}"


def test_naming_conventions():
    fact = gm.columns("fact_sales")
    money = [c for c in fact if c.endswith("_amount")]
    assert "net_revenue_amount" in money and "tax_amount" in money
    assert all(c.endswith(("_date", "_ts")) or "date" not in c for c in fact)  # dates end _date


def test_data_dictionary_doc_is_up_to_date():
    doc = (ROOT / "docs" / "gold_model.md").read_text()
    assert doc == gm.data_dictionary(), "run: uv run python -m sales_insights.pipeline.gold_model > docs/gold_model.md"


def test_fiscal_bounds_cover_whole_fiscal_years():
    assert g.fiscal_bounds(date(2025, 4, 1), date(2026, 9, 30), 4) == (date(2025, 4, 1), date(2027, 3, 31))
    assert g.fiscal_bounds(date(2025, 3, 31), date(2026, 4, 1), 4) == (date(2024, 4, 1), date(2027, 3, 31))


# ---------------------------------------------------------------------------
# Part 1b: builders on tiny tables (Spark)
# ---------------------------------------------------------------------------

LINE_SCHEMA = (
    "invoice_number string, invoice_item string, invoice_type string, invoice_date date, fiscal_year int, "
    "fiscal_period int, customer_id string, product_id string, region string, sales_office string, "
    "distribution_channel string, plant string, company_code string, quantity decimal(18,3), "
    "revenue decimal(18,2), gross_revenue decimal(18,2), tax_amount decimal(18,2), is_cancelled boolean, "
    "cancelled_by string, reference_invoice_number string, _source_file string, _business_date date, "
    "arrival_delay_days int"
)


def _line(number, kind, qty, rev, cancelled=False, ref=None, source="business_date=2026-10-13/orders_20261013.csv"):
    q, r = Decimal(qty), Decimal(rev)
    return (
        number, "10", kind, date(2026, 10, 13), 2026, 7, "0001001303", "MAN500GR", "05", "C001", "10", "2010",
        "2010", q, r, r, Decimal("0.00"), cancelled, "8261013001" if cancelled else None, ref, source,
        date(2026, 10, 13), 0,
    )  # fmt: skip


def test_fact_sales_splits_revenue_by_document_type(spark):
    changes = "business_date=2026-10-13/changes_20261013.csv"
    lines = spark.createDataFrame(
        [
            _line("9261013001", "ZAOR", "10", "1000.00", cancelled=True),
            _line("9261013002", "ZFOC", "2", "-36.00"),
            _line("7261013009", "ZARE", "-1", "-100.00", ref="9261013009", source=changes),
            _line("8261013001", "S1", "-10", "-1000.00", ref="9261013001", source=changes),
            _line("6261013008", "ZACR", "0", "-50.00", ref="9261013008", source=changes),
        ],
        LINE_SCHEMA,
    )
    products = spark.createDataFrame([("MAN500GR", Decimal("60.00"))], "product_id string, standard_cost decimal(18,2)")
    fact = g.build_fact_sales(lines, products)
    assert sorted(fact.columns) == sorted(gm.columns("fact_sales"))
    t = fact.agg(*[F.sum(c).alias(c) for c in fact.columns if c.endswith(("_amount", "_quantity"))]).collect()[0]
    assert t["net_revenue_amount"] == Decimal("-186.00")
    assert t["invoiced_revenue_amount"] == Decimal("964.00")
    assert t["returns_amount"] == Decimal("-100.00")
    assert t["cancellations_amount"] == Decimal("-1000.00")
    assert t["price_corrections_amount"] == Decimal("-50.00")
    parts = ["invoiced_revenue_amount", "returns_amount", "cancellations_amount", "price_corrections_amount"]
    assert sum(t[p] for p in parts) == t["net_revenue_amount"]  # the four parts always add up to net revenue
    assert t["cost_amount"] == Decimal("60.00")  # (10 + 2 - 1 - 10 + 0) x 60
    assert (t["sold_quantity"], t["returned_quantity"]) == (Decimal("12.000"), Decimal("-1.000"))
    rows = {r["invoice_number"]: r for r in fact.collect()}
    assert rows["9261013002"]["document_type"] == "Free of charge" and rows["9261013002"]["is_free_of_charge"]
    assert rows["9261013001"]["cancelled_by_invoice_number"] == "8261013001"
    assert rows["7261013009"]["source_kind"] == "changes" and rows["9261013001"]["source_kind"] == "orders"
    assert rows["9261013001"]["sales_rep_id"] is None  # column absent before 22 Oct: still in gold, empty


def test_relative_periods_are_like_for_like():
    p = g.relative_periods(date(2026, 10, 7), 4)  # a Wednesday, a week into October, FY2026
    assert p["latest_day"] == (date(2026, 10, 7), date(2026, 10, 7))
    assert p["month_to_date"] == (date(2026, 10, 1), date(2026, 10, 7))
    assert p["same_days_last_month"] == (date(2026, 9, 1), date(2026, 9, 7))
    assert p["same_days_last_year"] == (date(2025, 10, 1), date(2025, 10, 7))
    assert p["last_7_days"] == (date(2026, 10, 1), date(2026, 10, 7))
    assert p["previous_7_days"] == (date(2026, 9, 24), date(2026, 9, 30))
    assert p["last_complete_month"] == (date(2026, 9, 1), date(2026, 9, 30))  # never the part-month October
    assert p["month_before_last_complete_month"] == (date(2026, 8, 1), date(2026, 8, 31))
    assert p["last_complete_month_last_year"] == (date(2025, 9, 1), date(2025, 9, 30))
    assert p["fiscal_year_to_date"] == (date(2026, 4, 1), date(2026, 10, 7))
    assert p["same_period_last_fiscal_year"] == (date(2025, 4, 1), date(2025, 10, 7))
    assert p["current_fiscal_year"] == (date(2026, 4, 1), date(2027, 3, 31))
    assert p["previous_fiscal_year"] == (date(2025, 4, 1), date(2026, 3, 31))
    # on the last day of a month, that month is complete; in March the fiscal year is about to end
    assert g.relative_periods(date(2026, 9, 30), 4)["last_complete_month"] == (date(2026, 9, 1), date(2026, 9, 30))
    end = g.relative_periods(date(2027, 3, 31), 4)
    assert end["fiscal_year_to_date"] == (date(2026, 4, 1), date(2027, 3, 31))
    assert end["same_period_last_fiscal_year"] == (date(2025, 4, 1), date(2026, 3, 31))
    assert set(p) == {
        c.removeprefix("is_") for c in gm.columns("dim_date") if c.startswith("is_") and c != "is_weekend"
    }


def test_dim_date_fiscal_calendar_and_seasons(spark, cfg):
    days = g.build_dim_date(spark, date(2026, 3, 29), date(2026, 4, 2), cfg.business, as_of=date(2026, 4, 1))
    d = {r["date"]: r for r in days.collect()}
    assert (d[date(2026, 4, 1)]["is_latest_day"], d[date(2026, 4, 2)]["is_latest_day"]) == (True, False)
    assert d[date(2026, 3, 31)]["is_last_complete_month"] and not d[date(2026, 4, 1)]["is_last_complete_month"]
    assert d[date(2026, 4, 1)]["is_fiscal_year_to_date"] and d[date(2026, 3, 31)]["is_previous_fiscal_year"]
    assert len(d) == 5
    sun, mar31, apr1 = d[date(2026, 3, 29)], d[date(2026, 3, 31)], d[date(2026, 4, 1)]
    assert (sun["day_name"], sun["day_of_week"], sun["is_weekend"]) == ("Sunday", 7, True)
    assert sun["week_start_date"] == date(2026, 3, 23)  # weeks start on Monday
    assert (mar31["fiscal_year"], mar31["fiscal_period"], mar31["fiscal_quarter"]) == (2025, 12, 4)
    assert (mar31["fiscal_year_label"], mar31["cultivation_season"]) == ("FY2025", "Maha")
    assert (apr1["fiscal_year"], apr1["fiscal_period"], apr1["fiscal_quarter"]) == (2026, 1, 1)
    assert (apr1["month_name"], apr1["quarter"], apr1["cultivation_season"]) == ("April", 2, "Inter-season")
    assert apr1["month_start_date"] == date(2026, 4, 1)


def test_with_comments_rejects_an_undocumented_column(spark):
    df = spark.createDataFrame(
        [("05", "Colombo", "Western", "LK", "x")], "a string, b string, c string, d string, e string"
    )
    df = df.toDF("district_code", "district_name", "province", "country", "surprise")
    with pytest.raises(ValueError, match="surprise"):
        gm.with_comments(df, "dim_region")


def test_compare_passes_exact_and_fails_differences(spark):
    expected = spark.createDataFrame(
        [
            ("2026-10-13", 10, 4, 1000.0),  # exact
            ("2026-10-12", 10, 4, 1000.0),  # revenue off by 1%
            ("2026-10-10", 2, 2, -100.0),  # missing in gold
            ("2026-09-30", 10, 4, 100000.0),  # 0.05% off: inside the 0.1% tolerance
        ],
        "invoice_date string, expected_lines long, expected_invoices long, expected_revenue double",
    )
    actual = spark.createDataFrame(
        [
            ("2026-10-13", 10, 4, 1000.0),
            ("2026-10-12", 10, 4, 1010.0),
            ("2026-09-30", 10, 4, 100050.0),
            ("2026-10-11", 1, 1, 5.0),  # in gold but never sent
        ],
        "invoice_date string, actual_lines long, actual_invoices long, actual_revenue double",
    )
    out = {r["invoice_date"]: r["status"] for r in rc.compare(expected, actual, 0.1).collect()}
    assert out == {
        "2026-10-13": "PASS",
        "2026-10-12": "FAIL",
        "2026-10-10": "FAIL",
        "2026-09-30": "PASS",
        "2026-10-11": "FAIL",
    }


def _totals(spark, rows):
    return spark.createDataFrame(
        rows,
        "manifest_path string, kind string, business_date string, file_kind string, invoice_date string, "
        "lines long, invoices long, revenue double, tax_amount double",
    )


def _bronze_lines(spark, rows):
    """(invoice_number, invoice_item, invoice_date, quantity, revenue, source file, load time) as bronze holds them."""
    return spark.createDataFrame(
        rows,
        "invoice_number string, invoice_item string, invoice_date string, quantity string, revenue string, "
        "_source_file string, _load_ts string",
    )


def test_correction_in_a_later_delivery_reconciles(spark):
    """Quantity 1 -> 3 on a 3 Oct line, sent again in the 15 Oct delivery (same invoice + item).

    Silver keeps the newest version; the source reports the correction as a net change for 3 Oct
    (0 extra lines, 0 extra invoices, revenue difference 0), so 3 Oct still adds up. The old
    per-delivery check would have failed the 3 Oct delivery (1 of its 2 lines moved to 15 Oct).
    """
    d3, d15 = "business_date=2026-10-03/orders_20261003.csv", "business_date=2026-10-15/orders_20261015.csv"
    bronze = _bronze_lines(
        spark,
        [
            ("9261003015", "000010", "2026-10-03", "1.000", "100.00", d3, "2026-10-03 06:30:00"),
            ("9261003015", "000020", "2026-10-03", "2.000", "50.00", d3, "2026-10-03 06:30:00"),
            ("9261015001", "000010", "2026-10-15", "4.000", "80.00", d15, "2026-10-15 06:30:00"),
            ("9261003015", "000010", "2026-10-03", "3.000", "100.00", d15, "2026-10-15 06:30:00"),  # correction
        ],
    )
    silver = sv.dedupe(bronze)
    assert silver.count() == 3
    fixed = silver.filter((F.col("invoice_number") == "9261003015") & (F.col("invoice_item") == "000010")).first()
    assert (fixed["quantity"], fixed["_source_file"]) == ("3.000", d15)  # newest version kept
    fact = silver.withColumn("net_revenue_amount", F.col("revenue").cast("decimal(18,2)"))

    manifests = _totals(
        spark,
        [
            ("m03", "daily", "2026-10-03", "orders", "2026-10-03", 2, 1, 150.0, 0.0),
            ("m15", "daily", "2026-10-15", "orders", "2026-10-15", 1, 1, 80.0, 0.0),
            ("m15", "daily", "2026-10-15", "orders", "2026-10-03", 0, 0, 0.0, 0.0),  # the correction: net change
        ],
    )
    result = rc.compare(rc.expected_totals(manifests), rc.actual_totals(fact), 0.1)
    out = {r["invoice_date"]: r for r in result.collect()}
    assert {d: r["status"] for d, r in out.items()} == {"2026-10-03": "PASS", "2026-10-15": "PASS"}
    assert out["2026-10-03"]["deliveries"] == "2026-10-03, 2026-10-15"

    # A real problem still fails: the corrected line lost in gold
    lost = fact.filter(~((F.col("invoice_number") == "9261003015") & (F.col("invoice_item") == "000010")))
    bad = rc.compare(rc.expected_totals(manifests), rc.actual_totals(lost), 0.1).filter("status = 'FAIL'")
    assert [r["invoice_date"] for r in bad.collect()] == ["2026-10-03"]


def test_correction_that_changes_revenue_reconciles(spark):
    """Price fixed from 100.00 to 120.00 in a later delivery: the source reports +20.00 for 3 Oct."""
    d3, d15 = "business_date=2026-10-03/orders_20261003.csv", "business_date=2026-10-15/orders_20261015.csv"
    bronze = _bronze_lines(
        spark,
        [
            ("9261003015", "000010", "2026-10-03", "1.000", "100.00", d3, "2026-10-03 06:30:00"),
            ("9261003015", "000010", "2026-10-03", "1.000", "120.00", d15, "2026-10-15 06:30:00"),
        ],
    )
    fact = sv.dedupe(bronze).withColumn("net_revenue_amount", F.col("revenue").cast("decimal(18,2)"))
    manifests = _totals(
        spark,
        [
            ("m03", "daily", "2026-10-03", "orders", "2026-10-03", 1, 1, 100.0, 0.0),
            ("m15", "daily", "2026-10-15", "orders", "2026-10-03", 0, 0, 20.0, 0.0),
        ],
    )
    [r] = rc.compare(rc.expected_totals(manifests), rc.actual_totals(fact), 0.1).collect()
    assert (r["status"], r["expected_revenue"], r["actual_revenue"]) == ("PASS", 120.0, 120.0)


# ---------------------------------------------------------------------------
# Part 2: the whole chain, proved against the manifests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def world(spark, cfg, tmp_path_factory):
    root = tmp_path_factory.mktemp("gold")
    staging, landing = root / "staging", root / "landing"
    lake = Lake(spark, cfg, root=root / "lake")

    tables, log = m.build_masters(SEED, AS_OF)
    m.write_masters(tables, log, staging, SEED, AS_OF)
    history = h.build_history(SEED, AS_OF, start=date(2026, 9, 25))
    h.write_history(history, staging, SEED, AS_OF)
    sim = s.Simulator(SEED, AS_OF)
    for i in DAYS:
        s.write_day(sim.build_day(date(2026, 10, i)), staging, SEED, AS_OF)
    drip.deliver_initial(staging, landing)
    for i in DAYS:
        drip.execute(drip.plan(date(2026, 10, i), staging), staging, landing)

    b.run_bronze(spark, cfg, landing=landing, lake=lake)
    sv.run_silver(spark, cfg, lake=lake)
    gold = g.run_gold(spark, cfg, lake=lake)
    recon = rc.run_reconcile(spark, cfg, lake=lake)
    manifests = [
        json.loads((staging / s.folder_name(date(2026, 10, i)) / f"manifest_202610{i}.json").read_text()) for i in DAYS
    ]
    return {"lake": lake, "gold": gold, "recon": recon, "history": history, "manifests": manifests}


def test_reconciliation_passes_for_every_invoice_date(world):
    r = world["recon"]
    assert r.failed_groups == 0, r.failures[:3]
    rec = world["lake"].read("ops", "reconciliation")
    fact_dates = world["lake"].read("gold", "fact_sales").select("invoice_date").distinct().count()
    assert r.groups == rec.count() == fact_dates  # one check per invoice date
    sources = {x["deliveries"] for x in rec.collect()}
    assert "history" in sources and any("2026-10-22" in x for x in sources)  # backfill and daily deliveries
    dq = world["lake"].read("ops", "dq_results").filter(F.col("check_name") == "reconciliation")
    assert [x["status"] for x in dq.collect()] == ["PASS"]  # one summary row per run


def test_fact_total_equals_all_manifests(world):
    daily = sum(mf["control_totals"][k]["revenue"] for mf in world["manifests"] for k in ("orders", "changes"))
    history = world["history"]["revenue"].astype(float).sum()
    fact = world["lake"].read("gold", "fact_sales")
    total = float(fact.agg(F.sum("net_revenue_amount")).collect()[0][0])
    assert total == pytest.approx(daily + history, abs=0.01)
    silver = world["lake"].read("silver", "invoice_lines")
    assert fact.count() == silver.count() == fact.select("invoice_number", "invoice_item").distinct().count()


def test_every_fact_key_finds_its_dimension(world):
    lake = world["lake"]
    fact = lake.read("gold", "fact_sales")
    for fk, dim, pk in (
        ("customer_id", "dim_customer", "customer_id"),
        ("product_id", "dim_product", "product_id"),
        ("district_code", "dim_region", "district_code"),
        ("distribution_channel_code", "dim_channel", "distribution_channel_code"),
        ("invoice_date", "dim_date", "date"),
    ):
        keys = lake.read("gold", dim).select(F.col(pk).alias("_pk"))  # renamed: fact and dim share names
        orphans = fact.join(keys, fact[fk] == keys["_pk"], "left_anti").count()
        assert orphans == 0, f"{fk} -> {dim}"


def test_dims_are_unique_and_complete(world):
    lake, rows = world["lake"], world["gold"].rows
    assert rows["dim_customer"] == 600 and rows["dim_product"] == 40
    assert rows["dim_region"] == 25 and rows["dim_channel"] == 3
    dd = lake.read("gold", "dim_date")
    first, last = dd.agg(F.min("date"), F.max("date")).collect()[0]
    assert (first, last) == (date(2026, 4, 1), date(2027, 3, 31))  # whole fiscal year 2026
    assert rows["dim_date"] == 365
    for table, key in (("dim_customer", "customer_id"), ("dim_product", "product_id"), ("dim_date", "date")):
        df = lake.read("gold", table)
        assert df.count() == df.select(key).distinct().count(), table


def test_column_comments_are_stored_with_the_table(world):
    for table in gm.TABLES:
        schema = world["lake"].read("gold", table).schema
        for f in schema.fields:
            assert f.metadata.get("comment") == gm.TABLES[table]["columns"][f.name], f"{table}.{f.name}"


def test_measures_and_types(world):
    fact = world["lake"].read("gold", "fact_sales")
    types = dict(fact.dtypes)
    assert all(types[c] == "decimal(18,2)" for c in fact.columns if c.endswith("_amount"))
    assert types["quantity"] == "decimal(18,3)" and types["invoice_date"] == "date"
    parts = F.col("invoiced_revenue_amount") + F.col("returns_amount") + F.col("cancellations_amount")
    parts = parts + F.col("price_corrections_amount")
    assert fact.filter(parts != F.col("net_revenue_amount")).count() == 0
    assert fact.filter(F.col("document_type").isNull() | F.col("source_kind").isNull()).count() == 0


def test_sales_rep_appears_from_22_october(world):
    fact = world["lake"].read("gold", "fact_sales")
    before = fact.filter(F.col("loaded_business_date") < date(2026, 10, 22))
    on_22 = fact.filter((F.col("loaded_business_date") == date(2026, 10, 22)) & (F.col("source_kind") == "orders"))
    assert before.filter(F.col("sales_rep_id").isNotNull()).count() == 0
    assert on_22.filter(F.col("sales_rep_id").isNotNull()).count() > 0


def test_tampered_gold_fails_reconciliation(world, spark):
    """Drop one daily line: exactly its invoice date must FAIL."""
    lake = world["lake"]
    fact = lake.read("gold", "fact_sales")
    victim = fact.filter(F.col("source_kind") == "orders").orderBy("invoice_number", "invoice_item").first()
    tampered = fact.filter(
        ~((F.col("invoice_number") == victim["invoice_number"]) & (F.col("invoice_item") == victim["invoice_item"]))
    )
    out = rc.compare(rc.expected_totals(lake.read("ops", "manifest_totals")), rc.actual_totals(tampered), 0.1)
    failed = [r["invoice_date"] for r in out.filter("status = 'FAIL'").collect()]
    assert failed == [str(victim["invoice_date"])]
