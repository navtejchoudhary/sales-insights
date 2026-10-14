"""Inventory pipeline tests: silver rules, gold builders, the three checks, and a real run end to end.

Part 1: the model and manifest parsing (no Spark).
Part 2: each rule on tiny tables (Spark).
Part 3: sales (short history + 1-12 Oct) and inventory (stock from 25 Sep + 1-12 Oct) through
        bronze -> silver -> gold -> checks, proved against the manifests and against sales; then a
        removed movement must make every check fail.
"""

import json
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from sales_insights.common.lake import Lake
from sales_insights.drip import drip
from sales_insights.generator import history as h
from sales_insights.generator import inventory as inv
from sales_insights.generator import masters as m
from sales_insights.generator import simulate as s
from sales_insights.pipeline import bronze as b
from sales_insights.pipeline import gold as g
from sales_insights.pipeline import inventory_checks as ic
from sales_insights.pipeline import inventory_gold as ig
from sales_insights.pipeline import inventory_model as im
from sales_insights.pipeline import inventory_silver as isv
from sales_insights.pipeline import silver as sv
from sales_insights.pipeline.manifests import parse_inventory_manifest

AS_OF = date(2026, 10, 1)
SEED = 20261001
HISTORY_FROM = "2026-09-25"
DAYS = [AS_OF + timedelta(days=i) for i in range(12)]  # 1-12 Oct, every day (the checks need whole days)
ROOT = Path(__file__).resolve().parents[1]
D = date.fromisoformat


# ---------------------------------------------------------------------------
# Part 1: model and manifests (no Spark)
# ---------------------------------------------------------------------------


def test_every_inventory_table_and_column_has_a_description():
    for table, spec in im.TABLES.items():
        assert len(spec["description"]) > 30, table
        for col, text in spec["columns"].items():
            assert len(text) > 10, f"{table}.{col}"
            assert col.endswith(("_date", "_ts")) or "date" not in col, col  # dates end _date


def test_inventory_data_dictionary_doc_is_up_to_date():
    doc = (ROOT / "docs" / "inventory_gold_model.md").read_text()
    assert doc == im.data_dictionary(), (
        "run: uv run python -m sales_insights.pipeline.inventory_model > docs/inventory_gold_model.md"
    )


def test_inventory_manifest_totals_and_dirt():
    content = json.dumps(
        {
            "kind": "daily",
            "business_date": "2026-10-13",
            "control_totals": {
                "movements": {"by_posting_date": {"2026-10-12": {"lines": 2, "quantity": -5.0}}},
                "stock": {
                    "by_snapshot_date": {"2026-10-13": {"rows": 3, "unrestricted_qty": 40.0, "in_transit_qty": 7.0}}
                },
            },
            "dirt": [{"file": "stock", "dirt": "negative_stock", "count": 1, "keys": ["2026-10-13/2010-8001/X"]}],
        }
    )
    totals, dirt = parse_inventory_manifest("business_date=2026-10-13/manifest_20261013.json", content)
    assert [t[3:] for t in totals] == [("movements", "2026-10-12", 2, -5.0, 0.0), ("stock", "2026-10-13", 3, 40.0, 7.0)]
    assert [d[3:] for d in dirt] == [("stock", "negative_stock", "2026-10-13/2010-8001/X")]


# ---------------------------------------------------------------------------
# Part 2: rules on tiny tables (Spark)
# ---------------------------------------------------------------------------


def frame(spark, rows: list[dict]):
    cols = list(rows[0])
    return spark.createDataFrame(
        [tuple(r.get(c) for c in cols) for r in rows], ", ".join(f"`{c}` string" for c in cols)
    )


def _mv(doc="4261013001", item="0001", **kw):
    row = {
        "material_document": doc, "material_document_item": item, "posting_date": "2026-10-13", "movement_type": "601",
        "location_id": "2010-8001", "product_id": "MC6001LT", "quantity": "-5.000", "base_unit": "CA",
        "sales_office": "C001", "reference_invoice_number": "9261013001", "reference_invoice_item": "000010",
        "counterpart_location_id": None, "last_updated_timestamp": "2026-10-13 10:00:00.000",
        "_source_file": "business_date=2026-10-13/movements_20261013.csv", "_business_date": "2026-10-13",
        "_load_ts": "2026-10-13 07:00:00",
    }  # fmt: skip
    row.update(kw)
    return row


@pytest.fixture(scope="module")
def masters(spark):
    locations = frame(spark, [{"location_id": "2010-8001"}, {"location_id": "2010-0090"}])
    products = frame(
        spark, [{"product_id": "MC6001LT", "base_unit": "CA"}, {"product_id": "HGL400ML", "base_unit": "BO"}]
    )
    return locations, products


def test_validate_movements_quarantines_each_bad_row_with_a_reason(spark, masters):
    rows = frame(
        spark,
        [
            _mv("4261013001"),
            _mv("4261013002", location_id="2010-9999"),
            _mv("4261013003", movement_type=None),
            _mv("4261013004", product_id="NOPE"),
            _mv("4261013005", posting_date="13/10/2026"),
        ],
    )
    valid, rejected = isv.validate_movements(rows, *masters)
    assert [r["material_document"] for r in valid.collect()] == ["4261013001"]
    reasons = {r["material_document"]: r["_reasons"] for r in rejected.collect()}
    assert reasons == {
        "4261013002": "unknown_location",
        "4261013003": "unknown_movement_type",
        "4261013004": "unknown_product",
        "4261013005": "invalid_posting_date",
    }


def test_validate_stock_quarantines_negative_stock(spark, masters):
    base = {
        "snapshot_date": "2026-10-13",
        "location_id": "2010-8001",
        "product_id": "MC6001LT",
        "in_transit_qty": "0.000",
    }
    rows = frame(
        spark,
        [{**base, "unrestricted_qty": "10.000"}, {**base, "product_id": "HGL400ML", "unrestricted_qty": "-3.000"}],
    )
    valid, rejected = isv.validate_stock(rows, *masters)
    assert valid.count() == 1
    assert [r["_reasons"] for r in rejected.collect()] == ["negative_stock"]


def test_units_come_from_the_product_master(spark, masters):
    rows = frame(spark, [_mv(base_unit="CS"), _mv("4261013002", base_unit="case"), _mv("4261013003")])
    fixed = isv.normalise_units(rows, masters[1]).collect()
    assert {r["base_unit"] for r in fixed} == {"CA"}
    assert sorted(r["source_unit"] for r in fixed) == ["CA", "CS", "case"]


def test_dedupe_removes_exact_copies(spark):
    rows = frame(spark, [_mv(), _mv(), _mv("4261013002")])
    assert isv.dedupe(rows, isv.MOVEMENT_KEY).count() == 2


def _stock_rows(spark, rows):
    return spark.createDataFrame(
        [(loc, pid, D(day), Decimal(q)) for loc, pid, day, q in rows],
        "location_id string, product_id string, snapshot_date date, stock_quantity decimal(18,3)",
    )


def _move_rows(spark, rows):
    return spark.createDataFrame(
        [(loc, pid, D(day), Decimal(q)) for loc, pid, day, q in rows],
        "location_id string, product_id string, posting_date date, quantity decimal(18,3)",
    )


def _quarantine(spark, rows=()):
    return spark.createDataFrame(list(rows), "file_kind string, product_id string, data_date string")


STOCK = [("L", "P", f"2026-10-0{i}", q) for i, q in [(1, "10"), (2, "12"), (3, "10"), (4, "10"), (5, "10")]]
MOVES = [("L", "P", "2026-10-02", "5"), ("L", "P", "2026-10-02", "-3"), ("L", "P", "2026-10-03", "-2")]


def _rollforward(spark, moves=MOVES, quarantine=()):
    out = ic.rollforward(
        _stock_rows(spark, STOCK), _move_rows(spark, moves), _quarantine(spark, quarantine), D("2026-10-05")
    )
    return {str(r["snapshot_date"]): r["status"] for r in out.collect()}


def test_rollforward_passes_when_stock_adds_up(spark):
    assert _rollforward(spark) == {
        "2026-10-02": "PASS",
        "2026-10-03": "PASS",
        "2026-10-04": "WAIT",
        "2026-10-05": "WAIT",
    }


def test_rollforward_fails_on_a_missing_movement_unless_it_was_quarantined(spark):
    assert _rollforward(spark, MOVES[:2])["2026-10-03"] == "FAIL"
    assert _rollforward(spark, MOVES[:2], [("movements", "P", "2026-10-03")])["2026-10-03"] == "EXPLAINED"


def test_rollforward_bridges_a_missing_stock_row(spark):
    stock = [r for r in STOCK if r[2] != "2026-10-02"]  # e.g. a quarantined negative-stock row
    out = ic.rollforward(_stock_rows(spark, stock), _move_rows(spark, MOVES), _quarantine(spark), D("2026-10-05"))
    assert {str(r["snapshot_date"]): r["status"] for r in out.collect()}["2026-10-03"] == "PASS"


def _cross(spark, sales, moves, lost_sales=(), lost_moves=()):
    fact = spark.createDataFrame(
        [(n, "000010", t, D("2026-10-02"), p, Decimal(q)) for n, t, p, q in sales],
        "invoice_number string, invoice_item string, invoice_type string, invoice_date date, product_id string, "
        "quantity decimal(18,3)",
    )
    products = spark.createDataFrame([("P", "ZFRT"), ("SRV", "ZSRV")], "product_id string, product_type_code string")
    mv = spark.createDataFrame(
        [(t, n, "000010", D("2026-10-02"), p, Decimal(q)) for t, n, p, q in moves],
        "movement_type string, reference_invoice_number string, reference_invoice_item string, posting_date date, "
        "product_id string, quantity decimal(18,3)",
    )
    sq = spark.createDataFrame([(n, "000010") for n in lost_sales], "invoice_number string, invoice_item string")
    iq = spark.createDataFrame(
        [
            ("movements", json.dumps({"reference_invoice_number": n, "reference_invoice_item": "000010"}))
            for n in lost_moves
        ],
        "file_kind string, raw_row string",
    )
    out = ic.cross_check_lines(fact, products, mv, sq, iq, D("2026-10-01"), D("2026-10-05"))
    return {r["invoice_number"]: r["status"] for r in out.collect()}


SALES = [
    ("9001", "ZAOR", "P", "5"),
    ("7001", "ZARE", "P", "-2"),
    ("8001", "S1", "P", "-1"),
    ("9002", "ZAOR", "SRV", "1"),
]
SALE_MOVES = [("601", "9001", "P", "-5"), ("653", "7001", "P", "2"), ("602", "8001", "P", "1")]


def test_cross_check_matches_every_sales_line_and_ignores_services(spark):
    assert _cross(spark, SALES, SALE_MOVES) == {"9001": "PASS", "7001": "PASS", "8001": "PASS"}


def test_cross_check_finds_missing_and_wrong_movements(spark):
    assert _cross(spark, SALES, SALE_MOVES[1:])["9001"] == "FAIL_NO_MOVEMENT"
    assert _cross(spark, SALES, [("601", "9001", "P", "-4"), *SALE_MOVES[1:]])["9001"] == "FAIL_MISMATCH"
    assert _cross(spark, SALES[1:], SALE_MOVES)["9001"] == "FAIL_NO_SALE"


def test_cross_check_explains_lines_quarantined_on_either_side(spark):
    assert _cross(spark, SALES, SALE_MOVES[1:], lost_moves=["9001"])["9001"] == "EXPLAINED"
    assert _cross(spark, SALES[1:], SALE_MOVES, lost_sales=["9001"])["9001"] == "EXPLAINED"


def test_compare_totals(spark):
    exp = spark.createDataFrame(
        [("movements", "2026-10-02", 3, -5.0, 0.0, "history"), ("stock", "2026-10-02", 2, 20.0, 4.0, "history")],
        "file_kind string, data_date string, expected_lines long, expected_quantity double, "
        "expected_in_transit double, deliveries string",
    )
    act = spark.createDataFrame(
        [("2026-10-02", 3, -5.0, 0.0, "movements"), ("2026-10-02", 2, 21.0, 4.0, "stock")],
        "data_date string, actual_lines long, actual_quantity double, actual_in_transit double, file_kind string",
    )
    out = {r["file_kind"]: r["status"] for r in ic.compare_totals(exp, act, 0.1).collect()}
    assert out == {"movements": "PASS", "stock": "FAIL"}


def test_stock_daily_cover_and_stock_out(spark):
    stock = spark.createDataFrame(
        [(D(f"2026-10-{i:02d}"), "L", "P", Decimal(q), Decimal(0)) for i, q in [(1, "28"), (2, "0")]],
        "snapshot_date date, location_id string, product_id string, unrestricted_qty decimal(18,3), in_transit_qty decimal(18,3)",
    )
    issues = spark.createDataFrame(
        [(D("2026-10-01"), "L", "P", Decimal(28))],
        "posting_date date, location_id string, product_id string, issued_quantity decimal(18,3)",
    )
    products = spark.createDataFrame(
        [("P", Decimal("100.00"))], "product_id string, standard_cost_amount decimal(18,2)"
    )
    rows = {str(r["snapshot_date"]): r for r in ig.build_fact_stock_daily(stock, issues, products).collect()}
    assert rows["2026-10-01"]["avg_daily_issued_quantity_28d"] == Decimal("1.000")
    assert rows["2026-10-01"]["days_of_cover"] == Decimal("28.0")
    assert rows["2026-10-01"]["stock_value_amount"] == Decimal("2800.00")
    assert rows["2026-10-02"]["is_stock_out"] is True
    assert rows["2026-10-01"]["is_stock_out"] is False


def test_sales_and_stock_side_by_side(spark):
    """Day 0 has only sales and day 3 only stock: both are cut (only days both areas cover). Building product B
    sells without depot stock; P at depot L1 has both. Nothing is double counted."""
    sales = spark.createDataFrame(
        [
            (D("2026-09-30"), "C001", "P", "9", "900.00", "9"),
            (D("2026-10-01"), "C001", "P", "5", "500.00", "5"),
            (D("2026-10-01"), "C001", "P", "-1", "-100.00", "0"),  # a return of an older invoice
            (D("2026-10-02"), "C001", "B", "2", "300.00", "2"),
        ],
        "invoice_date date, sales_office_code string, product_id string, quantity string, net_revenue_amount string, "
        "sold_quantity string",
    ).selectExpr("invoice_date", "sales_office_code", "product_id", "CAST(quantity AS DECIMAL(18,3)) AS quantity",
                 "CAST(net_revenue_amount AS DECIMAL(18,2)) AS net_revenue_amount",
                 "CAST(sold_quantity AS DECIMAL(18,3)) AS sold_quantity")  # fmt: skip
    stock = spark.createDataFrame(
        [(D(f"2026-10-0{i}"), "L1", "P", Decimal(q), Decimal(i), Decimal(0), Decimal("1.000"), q == "0")
         for i, q in [(1, "6"), (2, "0"), (3, "4")]]
        + [(D("2026-10-01"), "HUB", "P", Decimal(50), Decimal(0), Decimal(0), Decimal(0), False)],
        "snapshot_date date, location_id string, product_id string, stock_quantity decimal(18,3), "
        "issued_quantity decimal(18,3), in_transit_quantity decimal(18,3), avg_daily_issued_quantity_28d decimal(18,3), "
        "is_stock_out boolean",
    )  # fmt: skip
    locations = spark.createDataFrame(
        [("L1", "Depot", "C001"), ("HUB", "Plant store", None)], "location_id string, location_type string, sales_office_code string"
    )  # fmt: skip
    out = ig.build_fact_sales_stock_daily(sales, stock, locations)
    assert out.columns == list(im.TABLES["fact_sales_stock_daily"]["columns"])
    rows = {(str(r["activity_date"]), r["product_id"]): r for r in out.collect()}
    assert set(rows) == {("2026-10-01", "P"), ("2026-10-02", "P"), ("2026-10-02", "B")}  # 30 Sep and 3 Oct cut
    p1 = rows[("2026-10-01", "P")]
    assert (p1["net_revenue_amount"], p1["sales_quantity"], p1["invoiced_quantity"]) == (400, 4, 5)
    assert (p1["stock_quantity"], p1["issued_quantity"], p1["location_id"], p1["is_stock_out"]) == (6, 1, "L1", False)
    p2 = rows[("2026-10-02", "P")]
    assert (p2["net_revenue_amount"], p2["stock_quantity"], p2["is_stock_out"]) == (0, 0, True)  # no sales, stock-out
    b2 = rows[("2026-10-02", "B")]
    assert (b2["net_revenue_amount"], b2["stock_quantity"], b2["location_id"], b2["is_stock_out"]) == (
        300,
        None,
        "L1",
        False,
    )


# ---------------------------------------------------------------------------
# Part 3: a real run, sales and inventory side by side
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def world(spark, cfg, tmp_path_factory):
    root = tmp_path_factory.mktemp("inventory")
    inv_cfg = cfg.for_area("inventory")

    # Sales: masters + history from 25 Sep + every day 1-12 Oct, through gold
    s_staging, s_landing = root / "sales" / "staging", root / "sales" / "landing"
    sales_lake = Lake(spark, cfg, root=root / "sales" / "lake")
    tables, log = m.build_masters(SEED, AS_OF)
    m.write_masters(tables, log, s_staging, SEED, AS_OF)
    h.write_history(h.build_history(SEED, AS_OF, start=D(HISTORY_FROM)), s_staging, SEED, AS_OF)
    sim = s.Simulator(SEED, AS_OF)
    for d in DAYS:
        s.write_day(sim.build_day(d), s_staging, SEED, AS_OF)
    drip.deliver_initial(s_staging, s_landing)
    for d in DAYS:
        drip.execute(drip.plan(d, s_staging), s_staging, s_landing)
    b.run_bronze(spark, cfg, landing=s_landing, lake=sales_lake)
    sv.run_silver(spark, cfg, lake=sales_lake)
    g.run_gold(spark, cfg, lake=sales_lake)

    # Inventory: locations + stock history from 25 Sep + every day 1-12 Oct
    i_staging, i_landing = root / "inventory" / "staging", root / "inventory" / "landing"
    lake = Lake(spark, inv_cfg, root=root / "inventory" / "lake")
    isim = inv.InventorySimulator(SEED, AS_OF)
    inv.write_locations(isim.locations, i_staging, SEED, AS_OF)
    mv, st = isim.build_history()
    inv.write_history(
        mv[mv["posting_date"] >= HISTORY_FROM], st[st["snapshot_date"] >= HISTORY_FROM], i_staging, SEED, AS_OF
    )
    manifests = {}
    for d in DAYS:
        target = inv.write_day(isim.build_day(d), i_staging, SEED, AS_OF)
        manifests[d] = json.loads((target / f"manifest_{d:%Y%m%d}.json").read_text())
    drip.deliver_initial(i_staging, i_landing)
    for d in DAYS:
        drip.execute(drip.plan(d, i_staging), i_staging, i_landing)
    bronze = b.run_bronze(spark, inv_cfg, landing=i_landing, lake=lake)
    silver = isv.run_inventory_silver(spark, inv_cfg, lake=lake, sales_lake=sales_lake)
    gold = ig.run_inventory_gold(spark, inv_cfg, lake=lake, sales_lake=sales_lake)
    checks = ic.run_inventory_checks(spark, inv_cfg, lake=lake, sales_lake=sales_lake, first_cross_check_date=AS_OF)
    return {
        "lake": lake, "sales_lake": sales_lake, "cfg": inv_cfg, "manifests": manifests, "sim": isim,
        "bronze": bronze, "silver": silver, "gold": gold, "checks": checks,
    }  # fmt: skip


def _dirt(world, kind):
    return [
        (d.isoformat(), k)
        for d, mf in world["manifests"].items()
        for x in mf["dirt"]
        if x["dirt"] == kind
        for k in x["keys"]
    ]


def test_bronze_loads_the_inventory_files_into_the_inventory_lake(world):
    assert world["bronze"].dq_failures == 0
    assert set(world["bronze"].rows_by_target) >= {"movements", "stock", "masters_locations", "manifests"}


def test_silver_has_no_data_quality_failures(world):
    assert world["silver"].dq_failures == 0
    failed = world["lake"].read("ops", "dq_results").filter((F.col("layer") == "silver") & (F.col("status") == "FAIL"))
    assert failed.count() == 0


def test_quarantine_holds_exactly_the_planted_invalid_rows(world):
    expected = {(d, k) for kind in isv.INVALID_DIRT for d, k in _dirt(world, kind)}
    actual = {(str(r["business_date"]), r["record_key"]) for r in world["lake"].read("silver", "quarantine").collect()}
    assert expected and actual == expected


def test_units_and_duplicates_are_fixed(world):
    mv = world["lake"].read("silver", "movements")
    assert world["silver"].units_fixed == len(_dirt(world, "unit_text_variant"))
    assert world["silver"].duplicates_removed == len(_dirt(world, "duplicate_line"))
    keys = mv.select(F.concat_ws("/", "material_document", "material_document_item").alias("k"))
    assert keys.count() == keys.distinct().count()


def test_every_inventory_check_passes(world):
    r = world["checks"].results
    assert r["reconciliation"].ok, r["reconciliation"].first_failures[:3]
    assert r["stock_rollforward"].ok, r["stock_rollforward"].first_failures[:3]
    assert r["cross_check_sales"].ok, r["cross_check_sales"].first_failures[:3]
    assert r["cross_check_sales"].groups >= 5  # 1-5 Oct are settled (12 Oct minus 7 days)
    assert r["stock_rollforward"].waiting > 0  # the last 2 days wait for late lines


def test_gold_tables_match_the_source(world):
    lake = world["lake"]
    daily = lake.read("gold", "fact_stock_daily")
    last = DAYS[-1].isoformat()
    expected_rows = world["manifests"][DAYS[-1]]["control_totals"]["stock"]["rows"]
    assert daily.filter(F.col("snapshot_date") == last).count() == expected_rows
    assert lake.read("gold", "dim_location").count() == 17
    assert daily.filter(F.col("stock_quantity") < 0).count() == 0


def test_sales_and_stock_table_adds_up_to_sales(world):
    both = world["lake"].read("gold", "fact_sales_stock_daily")
    first, last = both.agg(F.min("activity_date"), F.max("activity_date")).collect()[0]
    sales = world["sales_lake"].read("gold", "fact_sales").filter(F.col("invoice_date").between(first, last))
    assert (
        both.agg(F.sum("net_revenue_amount")).collect()[0][0] == sales.agg(F.sum("net_revenue_amount")).collect()[0][0]
    )
    keys = ["activity_date", "sales_office_code", "product_id"]
    assert both.count() == both.select(*keys).distinct().count()  # grain
    assert both.filter(F.col("location_id").isNotNull()).select("location_id").distinct().count() <= 11  # depots only


def test_inventory_kpis_match_the_metric_views(world, spark):
    from sales_insights.semantic import kpis as k

    results = k.check_against_metric_view(spark, world["lake"], world["sales_lake"])
    assert [name for name, _, _ in results] == [a.kpi for a in k.INVENTORY_AGREEMENTS]
    for name, rows, bad in results:
        assert rows > 0 and bad == 0, name


def test_every_stock_benchmark_query_runs(world, spark):
    """Story checks need the full history (proved by the answer_key step on real data); here every query runs."""
    from sales_insights.semantic import answer_key as ak

    for agent in (ak.SPACES["inventory"], ak.SPACES["sales_and_stock"]):
        rows = ak.build(spark, world["lake"], agent=agent, sales_lake=world["sales_lake"])
        assert len(rows) == len(ak.load(agent.benchmarks)), agent.name
        assert rows[0]["data_until"] not in ("", "None"), agent.name


def test_a_lost_movement_fails_the_checks(world, spark):
    """Remove one goods issue from gold: reconciliation, roll-forward and the sales cross-check must all FAIL."""
    lake, cfg = world["lake"], world["cfg"]
    fact = lake.read("gold", "fact_stock_movements")
    victim = (
        fact.filter((F.col("movement_type") == "601") & (F.col("posting_date") == "2026-10-02")).limit(1).collect()[0]
    )
    tampered = fact.filter(
        ~(
            (F.col("material_document") == victim["material_document"])
            & (F.col("material_document_item") == victim["material_document_item"])
        )
    ).localCheckpoint()
    lake.overwrite(tampered, "gold", "fact_stock_movements")
    try:
        out = ic.run_inventory_checks(
            spark, cfg, lake=lake, sales_lake=world["sales_lake"], first_cross_check_date=AS_OF
        )
        assert not out.results["reconciliation"].ok
        assert not out.results["stock_rollforward"].ok
        assert not out.results["cross_check_sales"].ok
    finally:
        ig.run_inventory_gold(spark, cfg, lake=lake, sales_lake=world["sales_lake"])  # restore
