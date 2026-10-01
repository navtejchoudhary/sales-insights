"""Bronze tests with Spark + Delta, following a realistic delivery story.

Scenario (one bronze run after each step):
  run1  initial delivery (masters + a short history), 13, 14 and 17 Oct (13/14 carry invalid rows)
  run2  nothing new                                  -> loads nothing
  run3  18 Oct missed, 19 Oct brings 18 + 19 + Monday redelivery of 18's orders
  run4  18's orders file delivered yet again (identical) -> loads nothing
  run5  20, 21 and 22 Oct (22 Oct adds the sales_rep_id column)
  run6  23 Oct folder half-delivered (no manifest)    -> waits
  run7  23 Oct delivery completes                     -> loads it
  run8  the source edits 17 Oct's orders after sending -> reloaded and flagged
"""

import json
import shutil
from datetime import date, timedelta

import pytest
from pyspark.sql import functions as F

from sales_insights.common.lake import Lake
from sales_insights.drip import drip
from sales_insights.generator import history as h
from sales_insights.generator import masters as m
from sales_insights.generator import simulate as s
from sales_insights.pipeline import bronze as b

AS_OF = date(2026, 10, 1)
SEED = 20261001


@pytest.fixture(scope="module")
def world(spark, cfg, tmp_path_factory):
    root = tmp_path_factory.mktemp("bronze")
    staging, landing = root / "staging", root / "landing"
    lake = Lake(spark, cfg, root=root / "lake")

    tables, log = m.build_masters(SEED, AS_OF)
    m.write_masters(tables, log, staging, SEED, AS_OF)
    h.write_history(h.build_history(SEED, AS_OF, start=date(2026, 9, 25)), staging, SEED, AS_OF)
    sim = s.Simulator(SEED, AS_OF)
    for i in (13, 14, *range(17, 24)):
        s.write_day(sim.build_day(date(2026, 10, i)), staging, SEED, AS_OF)

    def deliver(day):
        drip.execute(drip.plan(day, staging), staging, landing)

    def run():
        return b.run_bronze(spark, cfg, landing=landing, lake=lake)

    runs = {}
    drip.deliver_initial(staging, landing)
    for i in (13, 14, 17):
        deliver(date(2026, 10, i))
    runs[1] = run()
    runs[2] = run()
    deliver(date(2026, 10, 18))
    deliver(date(2026, 10, 19))
    runs[3] = run()
    shutil.copyfile(
        staging / s.folder_name(date(2026, 10, 18)) / "orders_20261018.csv",
        landing / s.folder_name(date(2026, 10, 18)) / "orders_20261018.csv",
    )
    runs[4] = run()
    for i in (20, 21, 22):
        deliver(date(2026, 10, i))
    runs[5] = run()

    day23 = s.folder_name(date(2026, 10, 23))
    (landing / day23).mkdir()
    shutil.copyfile(staging / day23 / "orders_20261023.csv", landing / day23 / "orders_20261023.csv")
    runs[6] = run()
    for f in ("changes_20261023.csv", "manifest_20261023.json"):
        shutil.copyfile(staging / day23 / f, landing / day23 / f)
    runs[7] = run()

    edited = landing / s.folder_name(date(2026, 10, 17)) / "orders_20261017.csv"
    lines = edited.read_text().splitlines()
    edited.write_text("\n".join([*lines, lines[1]]) + "\n")
    runs[8] = run()

    return {"runs": runs, "lake": lake, "staging": staging, "landing": landing}


def _orders(world):
    return world["lake"].read("bronze", "orders")


def _manifest(world, day):
    folder = world["staging"] / s.folder_name(day)
    return json.loads((folder / f"manifest_{day:%Y%m%d}.json").read_text())


def test_run1_loads_everything_delivered(world):
    r = world["runs"][1]
    assert r.rows_by_target["masters_customers"] == 612
    expected = sum(_manifest(world, date(2026, 10, i))["files"][f"orders_202610{i}.csv"]["rows"] for i in (13, 14, 17))
    assert r.rows_by_target["orders"] == expected
    assert r.rows_by_target["invoices_history"] > 0
    assert r.dq_failures == 0


def test_run2_is_a_no_op(world):
    r = world["runs"][2]
    assert r.loaded == [] and r.reloaded_changed == []
    assert r.skipped_already_loaded == len(world["runs"][1].loaded)


def test_missing_day_arrives_with_next_day_and_loads_once(world):
    r = world["runs"][3]
    assert "business_date=2026-10-18/orders_20261018.csv" in r.loaded
    assert "business_date=2026-10-19/orders_20261019.csv" in r.loaded
    rows_18 = _orders(world).filter(F.col("_business_date") == "2026-10-18").count()
    assert rows_18 == _manifest(world, date(2026, 10, 18))["files"]["orders_20261018.csv"]["rows"]


def test_identical_redelivery_is_skipped(world):
    r = world["runs"][4]
    assert r.loaded == [] and r.reloaded_changed == []


def test_new_column_added_not_failed(world):
    df = _orders(world)
    assert "sales_rep_id" in df.columns
    before = df.filter(F.col("_business_date") < "2026-10-22")
    after = df.filter(F.col("_business_date") == "2026-10-22")
    assert before.filter(F.col("sales_rep_id").isNotNull()).count() == 0
    assert after.filter(F.col("sales_rep_id").isNotNull()).count() > 0


def test_incomplete_folder_waits_then_loads(world):
    assert world["runs"][6].incomplete_folders == ["business_date=2026-10-23"]
    assert not any("2026-10-23" in p for p in world["runs"][6].loaded)
    assert "business_date=2026-10-23/orders_20261023.csv" in world["runs"][7].loaded
    dq = world["lake"].read("ops", "dq_results")
    assert dq.filter((F.col("status") == "WAIT") & (F.col("business_date") == "2026-10-23")).count() == 1


def test_changed_file_is_reloaded_and_flagged(world):
    r = world["runs"][8]
    assert r.reloaded_changed == ["business_date=2026-10-17/orders_20261017.csv"]
    assert r.dq_failures >= 1  # checksum and row count no longer match the manifest
    dq = world["lake"].read("ops", "dq_results").filter(F.col("run_id") == r.run_id)
    assert dq.filter(F.col("check_name") == "file_changed_after_delivery").count() == 1
    log = world["lake"].read("ops", "processed_files")
    assert log.filter(F.col("status") == "reloaded_changed_content").count() == 1


def test_every_source_column_is_text(world):
    df = _orders(world)
    for field in df.schema.fields:
        if field.name != "_load_ts":
            assert field.dataType.simpleString() == "string", field.name
    assert df.filter(~F.col("customer_id").rlike(r"^\d{10}$")).count() == 0  # leading zeros kept


def test_dirty_rows_are_kept_exactly(world):
    dups, bad = [], []
    for i in (13, 14, 18, 19, 20, 21, 22):  # 17 is excluded: it was reloaded in run8
        day = date(2026, 10, i)
        for d in _manifest(world, day)["dirt"]:
            if d["file"] != "orders":
                continue
            if d["dirt"] == "duplicate_line":
                dups += [(day.isoformat(), k) for k in d["keys"]]
            if d["dirt"] in ("invalid_date", "negative_quantity_on_invoice"):
                bad += [(day.isoformat(), k) for k in d["keys"]]
    assert dups and bad, "scenario must contain duplicates and invalid rows"
    df = _orders(world).withColumn("k", F.concat_ws("/", "invoice_number", "invoice_item"))
    counts = {(r["_business_date"], r["k"]): r["count"] for r in df.groupBy("_business_date", "k").count().collect()}
    for key in dups:
        assert counts[key] == 2, key  # bronze does not dedupe
    for key in bad:
        assert key in counts, key  # bronze does not quarantine


def test_metadata_on_every_row(world):
    df = _orders(world)
    assert (
        df.filter(F.col("_source_file").isNull() | F.col("_run_id").isNull() | F.col("_load_ts").isNull()).count() == 0
    )
    hist = world["lake"].read("bronze", "invoices_history")
    assert hist.filter(F.col("_business_date").isNotNull()).count() == 0
    assert hist.filter(~F.col("_source_file").startswith("history/")).count() == 0


def test_manifests_table(world):
    mf = world["lake"].read("bronze", "manifests")
    assert mf.filter(F.col("kind") == "daily").select("manifest_path").distinct().count() == 9
    assert mf.filter(F.col("kind").isin("masters", "history")).count() == 2


def test_all_days_accounted_for(world):
    loaded = {r["_business_date"] for r in _orders(world).select("_business_date").distinct().collect()}
    expected = {"2026-10-13", "2026-10-14"} | {(date(2026, 10, 17) + timedelta(days=i)).isoformat() for i in range(7)}
    assert loaded == expected
