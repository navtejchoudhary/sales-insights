"""Silver tests with Spark + Delta.

Part 1: every cleaning rule on a tiny hand-made table (one test per dirt type).
Part 2: the whole layer on a generated world, proved against the manifests:
  masters + history (25-30 Sep) + 13, 14 and 17 Oct; then 17 Oct's orders are re-sent
  with one extra duplicate line. Silver must catch exactly the rows the manifests list.
"""

import json
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sales_insights.common.lake import Lake
from sales_insights.drip import drip
from sales_insights.generator import history as h
from sales_insights.generator import masters as m
from sales_insights.generator import simulate as s
from sales_insights.pipeline import bronze as b
from sales_insights.pipeline import silver as sv

AS_OF = date(2026, 10, 1)
SEED = 20261001
DAYS = (13, 14, 17)


def frame(spark, rows: list[dict]) -> DataFrame:
    """Small all-text table, like bronze (every column STRING)."""
    cols = list(rows[0])
    schema = ", ".join(f"`{c}` string" for c in cols)
    return spark.createDataFrame([tuple(r.get(c) for c in cols) for r in rows], schema)


# ---------------------------------------------------------------------------
# Part 1: rules on tiny tables
# ---------------------------------------------------------------------------


def test_city_key_maps_every_variant_style(spark):
    variants = ["COLOMBO - 10.", "Colombo 10", "colombo  10", "COLOMBO 10", " COLOMBO-10 "]
    df = frame(spark, [{"city": v} for v in [*variants, "COLOMBO  1", "Kandy.", "nuwara eliya"]])
    out = [r[0] for r in df.select(sv.city_key(F.col("city"))).collect()]
    assert out == ["COLOMBO 10"] * 5 + ["COLOMBO 01", "KANDY", "NUWARA ELIYA"]


@pytest.fixture(scope="module")
def geo(spark):
    cities = frame(spark, [{"city": "COLOMBO 10", "region": "05"}, {"city": "KANDY", "region": "11"}])
    regions = frame(
        spark,
        [
            {"region": "05", "district_name": "Colombo", "province": "Western"},
            {"region": "11", "district_name": "Kandy", "province": "Central"},
        ],
    )
    return cities, regions


def _customer(**kw):
    row = {
        "customer_id": "0001001303",
        "customer_name": "Harvest Agro",
        "customer_full_name": "Harvest Agro Pvt Ltd",
        "city": "COLOMBO 10",
        "region": "05",
        "sales_office": "C001",
        "customer_group": "03",
        "customer_group_name": "Domestic",
        "created_date": "2024-07-06",
        "last_updated_timestamp": "2026-01-25 00:34:06.082",
        "_load_ts": "2026-10-01 10:00:00",
    }
    return {**row, **kw}


def test_clean_customers_fixes_master_dirt(spark, geo):
    raw = frame(
        spark,
        [
            # id_without_leading_zeros + trailing_spaces + city_text_variant, newest version
            _customer(customer_id="1001303", customer_name="Harvest Agro  ", city="colombo - 10."),
            # duplicate_older_version of the same customer: must lose
            _customer(customer_name="OLD NAME", last_updated_timestamp="2025-01-01 00:00:00.000"),
            # blank_region (filled from the city) + blank_customer_group
            _customer(
                customer_id="0000050316", city="Kandy", region=None, customer_group=None, customer_group_name=None
            ),
            # a city nobody knows: kept, but flagged
            _customer(customer_id="0000099999", city="Atlantis"),
        ],
    )
    out = {r["customer_id"]: r for r in sv.clean_customers(raw, *geo).collect()}
    assert sorted(out) == ["0000050316", "0000099999", "0001001303"]
    c = out["0001001303"]
    assert (c["customer_name"], c["city"], c["province"], c["city_mapped"]) == (
        "Harvest Agro",
        "COLOMBO 10",
        "Western",
        True,
    )
    k = out["0000050316"]
    assert (k["city"], k["region"], k["province"], k["customer_group_name"]) == ("KANDY", "11", "Central", "Unassigned")
    assert out["0000099999"]["city_mapped"] is False
    assert isinstance(c["created_date"], date)


def _product(**kw):
    row = {
        "product_id": "MAN500GR",
        "product_description": "MANCOZEB 500GR",
        "product_group": "2CHE03",
        "product_group_name": "FUNGICIDE",
        "plant": "2010",
        "list_price": "950.00",
        "standard_cost": "600.50",
        "gross_weight": "10.500",
        "net_weight": "10.000",
        "launch_date": "2020-01-01",
        "last_updated_timestamp": "2026-06-01 10:00:00.000",
        "_load_ts": "2026-10-01 10:00:00",
    }
    return {**row, **kw}


def test_clean_products_fills_group_from_lines_and_fixes_case(spark):
    raw = frame(
        spark,
        [
            _product(product_group=None, product_group_name=None),  # blank_product_group
            _product(product_id="KAR4L", product_description="Karate 4l", product_group="2CHE01"),  # case variant
        ],
    )
    lines = frame(
        spark,
        [{"product_id": "MAN500GR", "product_group": "2CHE03", "product_group_name": "FUNGICIDE"}] * 3
        + [{"product_id": "MAN500GR", "product_group": "2CHE99", "product_group_name": "WRONG"}],
    )
    out = {r["product_id"]: r for r in sv.clean_products(raw, sv.product_groups_seen_in_lines(lines)).collect()}
    assert (out["MAN500GR"]["product_group"], out["MAN500GR"]["product_group_name"]) == ("2CHE03", "FUNGICIDE")
    assert out["KAR4L"]["product_description"] == "KARATE 4L"
    assert out["KAR4L"]["list_price"] == Decimal("950.00")


def _line(number="9261013001", item="10", **kw):
    row = {
        "invoice_number": number,
        "invoice_item": item,
        "invoice_type": "ZAOR",
        "invoice_date": "2026-10-13",
        "customer_id": "0001001303",
        "product_id": "MAN500GR",
        "quantity": "5.000",
        "revenue": "4750.00",
        "_source_file": "business_date=2026-10-13/orders_20261013.csv",
        "_business_date": "2026-10-13",
        "_load_ts": "2026-10-13 10:00:00",
    }
    return {**row, **kw}


def test_dedupe_removes_duplicate_lines(spark):
    raw = frame(spark, [_line(), _line(), _line(item="20")])  # duplicate_line
    assert sv.dedupe(raw).count() == 2


def test_resent_file_replaces_its_earlier_load(spark):
    first, second = "2026-10-17 10:00:00", "2026-10-18 10:00:00"
    f = "business_date=2026-10-17/orders_20261017.csv"
    raw = frame(
        spark,
        [
            _line(item="10", revenue="100.00", _source_file=f, _load_ts=first),
            _line(item="20", revenue="200.00", _source_file=f, _load_ts=first),  # source removed this later
            _line(item="10", revenue="111.00", _source_file=f, _load_ts=second),  # corrected
        ],
    )
    out = [(r["invoice_item"], r["revenue"]) for r in sv.dedupe(raw).collect()]
    assert out == [("10", "111.00")]


def test_validate_quarantines_each_bad_value_with_a_reason(spark):
    raw = frame(
        spark,
        [
            _line(item="10"),  # fine
            _line(item="20", invoice_date="13/10/2026"),  # invalid_date: dd/mm/yyyy
            _line(item="30", invoice_date="2026-13-13"),  # invalid_date: month 13
            _line(item="40", invoice_date="0000-00-00"),  # invalid_date: zeros
            _line(item="50", quantity="-5.000"),  # negative_quantity_on_invoice
            _line(item="60", invoice_type="ZARE", quantity="-5.000", revenue="-4750.00"),  # a return: fine
            _line(item="70", invoice_type="XXXX"),
            _line(item="80", customer_id=None),
            _line(item="90", revenue="abc"),
        ],
    )
    valid, rejected = sv.validate(raw)
    assert sorted(r["invoice_item"] for r in valid.collect()) == ["10", "60"]
    reasons = {r["invoice_item"]: r["_reasons"] for r in rejected.collect()}
    assert reasons == {
        "20": "invalid_invoice_date",
        "30": "invalid_invoice_date",
        "40": "invalid_invoice_date",
        "50": "negative_quantity_on_invoice",
        "70": "unknown_invoice_type",
        "80": "missing_key",
        "90": "invalid_revenue",
    }
    q = sv.quarantine_frame(rejected, "run-1").filter(F.col("invoice_item") == "20").collect()[0]
    assert json.loads(q["raw_row"])["invoice_date"] == "13/10/2026"  # the original value is kept
    assert q["business_date"] == "2026-10-13"


def test_type_lines_uses_decimals_and_dates(spark):
    cols = sv.MONEY_2 + sv.QTY_3 + sv.RATE_6 + sv.INTS
    row = _line(last_updated_timestamp="2026-10-13 10:00:00.000", **dict.fromkeys(cols, "1"))
    t = sv.type_lines(frame(spark, [row]))
    types = dict(t.dtypes)
    assert types["revenue"] == "decimal(18,2)" and types["quantity"] == "decimal(18,3)"
    assert types["condition_rate"] == "decimal(18,6)" and types["fiscal_year"] == "int"
    assert types["invoice_date"] == "date" and types["_business_date"] == "date"


def test_enrich_fills_blanks_from_master_data(spark):
    lines = frame(
        spark,
        [
            {
                "customer_id": "1001303",  # without leading zeros
                "product_id": "MAN500GR",
                "city": "colombo - 10.",  # city_text_variant
                "region": "05",
                "sales_office": None,  # blank_sales_office
                "customer_group": None,  # blank_customer_group
                "customer_group_name": None,
                "product_group": None,  # blank_product_group
                "product_group_name": None,
                "plant": None,  # blank_plant
            }
        ],
    )
    customers = frame(
        spark,
        [
            {
                "customer_id": "0001001303",
                "city": "COLOMBO 10",
                "region": "05",
                "province": "Western",
                "sales_office": "C001",
                "customer_group": "03",
                "customer_group_name": "Domestic",
            }
        ],
    )
    products = frame(
        spark,
        [{"product_id": "MAN500GR", "product_group": "2CHE03", "product_group_name": "FUNGICIDE", "plant": "2010"}],
    )
    r = sv.enrich(lines, customers, products).collect()[0]
    assert (r["customer_id"], r["city"], r["province"], r["sales_office"]) == (
        "0001001303",
        "COLOMBO 10",
        "Western",
        "C001",
    )
    assert (r["customer_group"], r["customer_group_name"]) == ("03", "Domestic")
    assert (r["product_group"], r["product_group_name"], r["plant"]) == ("2CHE03", "FUNGICIDE", "2010")


def test_flag_cancellations_marks_the_original_invoice(spark):
    lines = frame(
        spark,
        [
            {"invoice_number": "9261013001", "invoice_type": "ZAOR", "reference_invoice_number": None},
            {"invoice_number": "8261013001", "invoice_type": "S1", "reference_invoice_number": "9261013001"},
            {"invoice_number": "9261013002", "invoice_type": "ZAOR", "reference_invoice_number": None},
        ],
    )
    out = {r["invoice_number"]: r for r in sv.flag_cancellations(lines).collect()}
    assert out["9261013001"]["is_cancelled"] is True and out["9261013001"]["cancelled_by"] == "8261013001"
    assert out["8261013001"]["is_cancelled"] is False and out["8261013001"]["cancelled_by"] is None
    assert out["9261013002"]["is_cancelled"] is False


# ---------------------------------------------------------------------------
# Part 2: the whole layer, proved against the manifests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def world(spark, cfg, tmp_path_factory):
    root = tmp_path_factory.mktemp("silver")
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

    # The source re-sends 17 Oct's orders with one more duplicate line
    edited = landing / s.folder_name(date(2026, 10, 17)) / "orders_20261017.csv"
    text = edited.read_text().splitlines()
    edited.write_text("\n".join([*text, text[1]]) + "\n")
    b.run_bronze(spark, cfg, landing=landing, lake=lake)

    manifests = {
        i: json.loads((staging / s.folder_name(date(2026, 10, i)) / f"manifest_202610{i}.json").read_text())
        for i in DAYS
    }
    run = sv.run_silver(spark, cfg, lake=lake)
    superseded = manifests[17]["files"]["orders_20261017.csv"]["rows"]  # the first load of 17 Oct's orders
    return {"run": run, "lake": lake, "manifests": manifests, "history": history, "superseded": superseded}


def _dirt(world, kind: str, file: str = "orders") -> list[tuple[str, str]]:
    return [
        (mf["business_date"], key)
        for mf in world["manifests"].values()
        for d in mf["dirt"]
        if d["dirt"] == kind and d["file"] == file
        for key in d["keys"]
    ]


def test_silver_has_no_data_quality_failures(world):
    assert world["run"].dq_failures == 0
    dq = world["lake"].read("ops", "dq_results").filter(F.col("layer") == "silver")
    assert dq.filter(F.col("status") == "FAIL").count() == 0
    assert dq.count() >= 10


def test_quarantine_holds_exactly_the_invalid_rows_from_the_manifests(world):
    expected = set(_dirt(world, "invalid_date") + _dirt(world, "negative_quantity_on_invoice"))
    assert len(expected) > 0  # 13 and 14 Oct carry invalid rows, so this is not vacuous
    q = world["lake"].read("silver", "quarantine").collect()
    assert {(r["business_date"], f"{r['invoice_number']}/{r['invoice_item']}") for r in q} == expected


def test_duplicates_and_the_superseded_load_are_removed(world):
    dups = sum(d["count"] for mf in world["manifests"].values() for d in mf["dirt"] if d["dirt"] == "duplicate_line")
    assert world["run"].duplicates_removed == dups + 1 + world["superseded"]
    lines = world["lake"].read("silver", "invoice_lines")
    assert lines.count() == lines.select(*sv.LINE_KEY).distinct().count()


def test_daily_revenue_matches_manifest_control_totals(world):
    expected = sum(
        mf["control_totals"][kind]["revenue"] for mf in world["manifests"].values() for kind in ("orders", "changes")
    )
    lines = world["lake"].read("silver", "invoice_lines").filter(F.col("_business_date").isNotNull())
    actual = lines.agg(F.sum("revenue")).collect()[0][0]
    assert float(actual) == pytest.approx(expected, abs=0.01)
    expected_lines = sum(
        mf["control_totals"][kind]["lines"] for mf in world["manifests"].values() for kind in ("orders", "changes")
    )
    assert lines.count() == expected_lines


def test_history_matches_its_manifest(world):
    lines = world["lake"].read("silver", "invoice_lines").filter(F.col("_business_date").isNull())
    assert lines.count() == len(world["history"])
    actual = float(lines.agg(F.sum("revenue")).collect()[0][0])
    assert actual == pytest.approx(world["history"]["revenue"].astype(float).sum(), abs=0.01)


def test_manifest_totals_table_matches_silver(world):
    totals = world["lake"].read("ops", "manifest_totals")
    assert {r[0] for r in totals.select("file_kind").distinct().collect()} == {"history", "orders", "changes"}
    promised = totals.filter(F.col("kind") == "daily").agg(F.sum("lines")).collect()[0][0]
    lines = world["lake"].read("silver", "invoice_lines").filter(F.col("_business_date").isNotNull())
    assert lines.count() == promised


def test_every_city_is_canonical(world):
    lake = world["lake"]
    canonical = {r["city"] for r in lake.read("silver", "cities").collect()}
    variants = {k for _, k in _dirt(world, "city_text_variant")}
    lines = lake.read("silver", "invoice_lines")
    assert {r["city"] for r in lines.select("city").distinct().collect()} <= canonical
    fixed = lines.filter(F.concat_ws("/", "invoice_number", "invoice_item").isin(list(variants)))
    assert fixed.count() > 0 and fixed.filter(~F.col("city").isin(list(canonical))).count() == 0
    assert world["run"].unmapped_cities == 0


def test_blank_fields_in_orders_are_filled(world):
    lines = world["lake"].read("silver", "invoice_lines")
    for col in ("sales_office", "plant", "product_group", "customer_group_name"):
        keys = [k for _, k in _dirt(world, f"blank_{col.replace('_name', '')}")]
        if keys:
            hit = lines.filter(F.concat_ws("/", "invoice_number", "invoice_item").isin(keys))
            assert hit.filter(F.col(col).isNull()).count() == 0, col


def test_masters_are_one_clean_row_per_key(world):
    lake = world["lake"]
    customers = lake.read("silver", "customers")
    assert customers.count() == 600
    assert customers.filter(F.length("customer_id") != 10).count() == 0
    assert customers.filter(F.col("region").isNull() | F.col("province").isNull()).count() == 0
    products = lake.read("silver", "products")
    assert products.count() == 40 and products.filter(F.col("product_group").isNull()).count() == 0


def test_types_cancellations_and_arrival_delay(world):
    lines = world["lake"].read("silver", "invoice_lines")
    types = dict(lines.dtypes)
    assert types["revenue"] == "decimal(18,2)" and types["invoice_date"] == "date"
    cancelled = lines.filter(F.col("is_cancelled"))
    assert cancelled.filter(~F.col("cancelled_by").startswith("8")).count() == 0
    daily = lines.filter(F.col("_business_date").isNotNull())
    assert daily.filter(F.col("arrival_delay_days") < 0).count() == 0
    assert daily.filter(F.col("arrival_delay_days") > 0).count() > 0  # late arrivals exist
    assert lines.filter(F.col("_business_date").isNull() & F.col("arrival_delay_days").isNotNull()).count() == 0
    assert "_run_id" not in lines.columns and "_rescued_data" not in lines.columns


def test_rerun_gives_the_same_tables(world, spark, cfg):
    lake = world["lake"]
    before = lake.read("silver", "invoice_lines").count()
    again = sv.run_silver(spark, cfg, lake=lake)
    assert again.rows["invoice_lines"] == before and again.dq_failures == 0
