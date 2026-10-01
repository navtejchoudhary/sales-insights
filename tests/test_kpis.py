"""KPI SQL + metric view tests.

Part 1 (no Spark): the metric view YAML and KPI SQL files are well-formed and only use real gold columns.
Part 2 (Spark): a tiny hand-made gold world where every KPI value is worked out by hand below,
then KPI SQL and the metric view must agree on every number.

The tiny world (LKR, P1 = crop chemical ZFRT cost 60, P2 = add-on ZTRD seedling trays cost 40):
  2025-10-05  A1  invoice       C1 Western   P1  10 cases  1,000
  2026-09-10  B1  invoice       C1 Western   P1   5 cases    500
  2026-10-03  C1  invoice       C1 Western   P1  10 cases  1,000  + line 20: P2 1 case 100
  2026-10-04  D1  invoice       C2 Southern  P1   4 cases    400
  2026-10-06  R1  return of D1  C2           P1  -1 case    -100
  2026-10-07  E1  invoice       C2           P1   2 cases    200  (later cancelled)
  2026-10-08  S1  cancels E1    C2           P1  -2 cases   -200
  2026-10-09  K1  price corr.   C1           P1   0          -50
  2026-10-10  F1  free of chg.  C2           P2   1 case     -18  (minus the VAT the company bears)
October 2026: net revenue 1,332 = invoiced 1,682 - returns 100 - cancellations 200 - price corrections 50
"""

import re
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from sales_insights.common.lake import Lake
from sales_insights.pipeline import gold as g
from sales_insights.pipeline import gold_model as gm
from sales_insights.semantic import kpis as k
from sales_insights.semantic import metric_views as mvs

OCT, SEP = date(2026, 10, 1), date(2026, 9, 1)


# ---------------------------------------------------------------------------
# Part 1: definitions (no Spark)
# ---------------------------------------------------------------------------


def test_metric_view_is_complete_and_documented():
    mv = mvs.load()
    assert mv.spec["version"] == 1.1
    assert len(mv.fields) >= 15 and len(mv.measures) >= 12
    for m in mv.spec["measures"]:
        assert m.get("display_name"), m["name"]
    for name in mv.measures:
        mvs.expand(mv, name)  # every MEASURE() reference resolves, no cycles


def test_metric_view_only_uses_real_gold_columns():
    mv = mvs.load()
    alias_table = {"source": "fact_sales"}
    for j in mv.spec["joins"]:
        alias_table[j["name"]] = j["source"].strip("${}")
    exprs = list(mv.fields.values()) + list(mv.measures.values()) + [j["on"] for j in mv.spec["joins"]]
    for expr in exprs:
        for alias, col in re.findall(r"\b(\w+)\.(\w+)\b", expr):
            assert alias in alias_table, f"unknown alias {alias} in {expr}"
            assert col in gm.columns(alias_table[alias]), f"{alias}.{col} is not a gold column"


def test_measure_expansion():
    mv = mvs.load()
    sql = mvs.expand(mv, "gross_margin_pct")
    assert "MEASURE" not in sql and "SUM(source.margin_amount)" in sql
    with pytest.raises(KeyError):
        mvs.expand(mv, "no_such_measure")
    looped = mvs.MetricView(
        "x", "", {"measures": [{"name": "a", "expr": "MEASURE(b)"}, {"name": "b", "expr": "MEASURE(a)"}]}
    )
    with pytest.raises(ValueError, match="cycle"):
        mvs.expand(looped, "a")


def test_kpi_sql_files_render():
    tables = {t: f"t_{t}" for t in mvs.GOLD_TABLES}
    assert k.names() == [
        "kpi_attach_rate_monthly",
        "kpi_product_monthly",
        "kpi_sales_by_province_monthly",
        "kpi_sales_daily",
        "kpi_sales_monthly",
    ]
    for name in k.names():
        sql = k.render(name, tables)
        assert "${" not in sql and "t_fact_sales" in sql
    assert {a.kpi for a in k.AGREEMENTS} <= set(k.names())
    mv = mvs.load()
    for a in k.AGREEMENTS:
        assert set(a.keys.values()) <= set(mv.fields) and set(a.measures) <= set(mv.measures), a.kpi


# ---------------------------------------------------------------------------
# Part 2: numbers (Spark)
# ---------------------------------------------------------------------------

LINE_SCHEMA = (
    "invoice_number string, invoice_item string, invoice_type string, invoice_date date, fiscal_year int, "
    "fiscal_period int, customer_id string, product_id string, region string, sales_office string, "
    "distribution_channel string, plant string, company_code string, quantity decimal(18,3), "
    "revenue decimal(18,2), gross_revenue decimal(18,2), tax_amount decimal(18,2), is_cancelled boolean, "
    "cancelled_by string, reference_invoice_number string, _source_file string, _business_date date, "
    "arrival_delay_days int"
)
CUSTOMERS = {"0000000001": ("05", "Colombo", "Western"), "0000000002": ("31", "Galle", "Southern")}


def _line(number, kind, day, cust, prod, qty, rev, item="10", cancelled_by=None, ref=None):
    c = "000000000" + cust
    source = "history/invoices_x.csv" if day < OCT else f"business_date={day}/orders_x.csv"
    return (
        number, item, kind, day, 2026, 7, c, prod, CUSTOMERS[c][0], "C001", "10", "2010", "2010",
        Decimal(qty), Decimal(rev), Decimal(rev), Decimal("0"), cancelled_by is not None, cancelled_by, ref,
        source, None if day < OCT else day, 0,
    )  # fmt: skip


@pytest.fixture(scope="module")
def lake(spark, cfg, tmp_path_factory):
    return build_tiny_gold(spark, cfg, tmp_path_factory.mktemp("kpis") / "lake")


def build_tiny_gold(spark, cfg, root):
    """The tiny world from the module docstring, as gold tables (also used by test_genie.py)."""
    lake = Lake(spark, cfg, root=root)
    d = date
    lines = [
        _line("9251005001", "ZAOR", d(2025, 10, 5), "1", "P1", "10", "1000.00"),
        _line("9260910001", "ZAOR", d(2026, 9, 10), "1", "P1", "5", "500.00"),
        _line("9261003001", "ZAOR", d(2026, 10, 3), "1", "P1", "10", "1000.00"),
        _line("9261003001", "ZAOR", d(2026, 10, 3), "1", "P2", "1", "100.00", item="20"),
        _line("9261004001", "ZAOR", d(2026, 10, 4), "2", "P1", "4", "400.00"),
        _line("7261004001", "ZARE", d(2026, 10, 6), "2", "P1", "-1", "-100.00", ref="9261004001"),
        _line("9261007001", "ZAOR", d(2026, 10, 7), "2", "P1", "2", "200.00", cancelled_by="8261007001"),
        _line("8261007001", "S1", d(2026, 10, 8), "2", "P1", "-2", "-200.00", ref="9261007001"),
        _line("6261003001", "ZACR", d(2026, 10, 9), "1", "P1", "0", "-50.00", ref="9261003001"),
        _line("9261010001", "ZFOC", d(2026, 10, 10), "2", "P2", "1", "-18.00"),
    ]
    products = spark.createDataFrame(
        [
            ("P1", "WEED KILLER 1L", "2CHE06", "WEEDICIDE", "ZFRT", Decimal("60.00")),
            ("P2", "SEEDLING TRAY", "2HAF09", "SEEDLING TRAYS", "ZTRD", Decimal("40.00")),
        ],
        "product_id string, product_description string, product_group string, product_group_name string, "
        "product_type string, standard_cost decimal(18,2)",
    )
    fact = g.build_fact_sales(spark.createDataFrame(lines, LINE_SCHEMA), products)
    lake.overwrite(gm.with_comments(fact, "fact_sales"), "gold", "fact_sales")

    def dim(table, rows):
        cols = gm.columns(table)
        data = [tuple(r.get(c) for c in cols) for r in rows]
        return spark.createDataFrame(data, ", ".join(f"`{c}` string" for c in cols))

    lake.overwrite(
        dim(
            "dim_customer",
            [
                {"customer_id": cid, "customer_name": f"Customer {cid[-1]}", "district_code": dc, "district_name": dn,
                 "province": prov, "customer_group_name": "Domestic", "city": dn.upper(), "sales_office_name": "Colombo"}
                for cid, (dc, dn, prov) in CUSTOMERS.items()
            ],
        ),
        "gold",
        "dim_customer",
    )  # fmt: skip
    lake.overwrite(
        dim(
            "dim_product",
            [
                {"product_id": r["product_id"], "product_description": r["product_description"],
                 "product_group_code": r["product_group"], "product_group_name": r["product_group_name"],
                 "product_type_code": r["product_type"], "product_type_name": r["product_type"]}
                for r in products.collect()
            ],
        ),
        "gold",
        "dim_product",
    )  # fmt: skip
    lake.overwrite(
        dim("dim_channel", [{"distribution_channel_code": "10", "distribution_channel_name": "General Trade"}]),
        "gold",
        "dim_channel",
    )
    lake.overwrite(dim("dim_region", [{"district_code": "05"}]), "gold", "dim_region")
    lake.overwrite(g.build_dim_date(spark, date(2025, 4, 1), date(2027, 3, 31), cfg.business), "gold", "dim_date")
    return lake


def _row(df, **where):
    for col, value in where.items():
        df = df.filter(F.col(col) == value)
    rows = df.collect()
    assert len(rows) == 1, where
    return rows[0]


def test_monthly_kpis_match_the_hand_calculation(spark, lake):
    r = _row(k.kpi(spark, lake, "kpi_sales_monthly"), month=OCT)
    assert r["net_revenue"] == Decimal("1332.00")
    assert r["invoiced_revenue"] == Decimal("1682.00")
    assert (r["returns_value"], r["cancellations_value"], r["price_corrections_value"]) == (100, 200, 50)
    assert r["units"] == Decimal("15.000")
    assert (r["invoice_count"], r["active_customers"]) == (2, 2)  # E1 cancelled; F1 is free of charge
    assert r["average_invoice_value"] == Decimal("666.00")
    assert r["gross_margin"] == Decimal("472.00")  # cost: 13 x 60 + 2 x 40 = 860
    assert float(r["gross_margin_pct"]) == 35.44
    assert float(r["returns_rate_pct"]) == 5.95  # 100 / 1,682
    assert float(r["mom_growth_pct"]) == 166.4  # vs Sep 500
    assert float(r["yoy_growth_pct"]) == 33.2  # vs Oct 2025 1,000
    assert r["fiscal_year"] == "FY2026" and r["fiscal_period"] == 7
    sep = _row(k.kpi(spark, lake, "kpi_sales_monthly"), month=SEP)
    assert sep["net_revenue"] == Decimal("500.00") and sep["yoy_growth_pct"] is None  # no Sep 2025 sales


def test_province_and_product_kpis(spark, lake):
    prov = k.kpi(spark, lake, "kpi_sales_by_province_monthly").filter(F.col("month") == OCT)
    got = {r["province"]: (r["net_revenue"], r["revenue_per_active_customer"]) for r in prov.collect()}
    assert got == {
        "Western": (Decimal("1050.00"), Decimal("1050.00")),
        "Southern": (Decimal("282.00"), Decimal("282.00")),
    }
    p1 = _row(k.kpi(spark, lake, "kpi_product_monthly"), month=OCT, product_id="P1")
    assert (p1["net_revenue"], p1["invoiced_revenue"], p1["units"]) == (Decimal("1250.00"), 1600, Decimal("13.000"))
    assert float(p1["returns_rate_pct"]) == 6.25 and float(p1["net_price_per_unit"]) == 96.15


def test_daily_kpis(spark, lake):
    r = _row(k.kpi(spark, lake, "kpi_sales_daily"), invoice_date=date(2026, 10, 6))
    assert r["net_revenue"] == Decimal("-100.00") and r["returns_value"] == Decimal("100.00")


def test_attach_rate_draft(spark, lake):
    a = {r["month"]: r for r in k.kpi(spark, lake, "kpi_attach_rate_monthly").collect()}
    assert (a[OCT]["chemical_invoices"], a[OCT]["chemical_invoices_with_add_on"]) == (2, 1)
    assert float(a[OCT]["attach_rate_pct"]) == 50.0
    assert float(a[SEP]["attach_rate_pct"]) == 0.0


def test_metric_view_gives_the_same_numbers(spark, lake):
    oct_row = _row(
        mvs.query(spark, lake, ["month"], ["net_revenue", "gross_margin_pct", "average_invoice_value"]), month=OCT
    )
    assert oct_row["net_revenue"] == Decimal("1332.00") and float(oct_row["average_invoice_value"]) == 666.0
    by_season = {
        r["cultivation_season"]: r["net_revenue"]
        for r in mvs.query(spark, lake, ["cultivation_season"], ["net_revenue"]).collect()
    }
    assert by_season == {"Maha": Decimal("2832.00")}  # Oct 2025, Sep 2026 and Oct 2026 all fall in Maha
    for name, rows, bad in k.check_against_metric_view(spark, lake):
        assert rows > 0 and bad == 0, name


def test_compare_catches_a_wrong_formula(spark, lake):
    """If someone edits a KPI formula and not the metric view, the check must notice."""
    wrong = k.kpi(spark, lake, "kpi_sales_monthly").withColumn("units", F.col("units") + 1)
    mv = mvs.query(spark, lake, ["month"], ["units"])
    assert k.compare(wrong, mv, {"month": "month"}, ["units"]).count() == 3  # Oct 2025, Sep 2026, Oct 2026


def test_publish_locally_creates_temp_views(spark, cfg, lake):
    done = k.publish(spark, cfg, lake)
    assert done == k.names()
    assert spark.sql("SELECT SUM(net_revenue) AS r FROM kpi_sales_monthly").collect()[0]["r"] == Decimal("2832.00")
