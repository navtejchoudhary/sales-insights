"""The one-time copy (pipeline/sap_copy.py): which queries it sends, what it refuses, what it writes.

Most tests need no Spark and no warehouse: a fake source answers the queries from SAP-shaped dummy data. The
last test copies from that fake source and runs the whole client pipeline on the result (Spark).
"""

import dataclasses
import json
import re
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from sales_insights.common.config import load_config
from sales_insights.generator import sap
from sales_insights.pipeline import sap_copy as cp

ROOT = Path(__file__).resolve().parents[1]
AS_OF = date(2026, 10, 1)
SEED = 20261001


def _map(**tables) -> dict:
    base = {
        "billing_items": "src.billing",
        "customer_master": "src.customers",
        "material_master": {"table": "src.materials", "filter": "plant = '2010'"},
        "text_regions": "src.regions",
        "text_sales_offices": "src.offices",
    }
    return {"catalog": "fc", "tables": {**base, **tables}}


def _cfg(source: str = "real", **scope):
    cfg = load_config("local", area="client")
    area = {**cfg.areas["client"], "source": source, "scope": {**cfg.areas["client"]["scope"], **scope}}
    return dataclasses.replace(cfg, areas={**cfg.areas, "client": area})


# ---------------------------------------------------------------------------
# The source map
# ---------------------------------------------------------------------------


def test_the_template_is_a_valid_map_with_placeholders_only():
    raw = json.loads((ROOT / "config" / "source_map.example.json").read_text())
    smap = cp.load_source_map(raw)
    assert set(cp.REQUIRED) <= set(smap.tables)
    text = json.dumps(raw)
    names = [smap.catalog] + [t.table for t in smap.tables.values()]
    assert all(n.startswith("<") for n in names)  # placeholders: no real name in git
    assert not re.search(r"hayleys|zmd|zex|redshift-serverless", text, re.IGNORECASE)


def test_map_needs_the_three_main_tables_and_known_names_only():
    raw = _map()
    del raw["tables"]["material_master"]
    with pytest.raises(ValueError, match="missing"):
        cp.load_source_map(raw)
    with pytest.raises(ValueError, match="unknown logical"):
        cp.load_source_map(_map(sales_orders="src.orders"))
    with pytest.raises(ValueError, match="does not read"):
        cp.load_source_map(_map(customer_master={"table": "src.c", "columns": {"customername": "name1"}}))


def test_map_never_reads_a_personal_column():
    with pytest.raises(ValueError, match="personal"):
        cp.load_source_map(_map(customer_master={"table": "src.c", "columns": {"cityname": "telephonenumber1"}}))
    with pytest.raises(ValueError, match="personal"):
        cp.load_source_map(_map(customer_master={"table": "src.c", "filter": "emailaddress IS NOT NULL"}))
    smap = cp.load_source_map(_map())
    for col in sap.PERSONAL_CUSTOMER_COLUMNS:
        assert col not in smap.tables["customer_master"].columns


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


def test_every_query_names_its_columns_and_billing_is_filtered_at_the_source():
    from sales_insights.pipeline.sap_silver import Scope

    scope = Scope(("2010",), date(2024, 1, 1), date(2026, 1, 31))
    smap = cp.load_source_map(
        _map(
            billing_items={
                "table": "src.billing",
                "columns": {"billingdocumentdate": "billing_date", "salesgroup": None},
            }
        )
    )
    for name in smap.tables:
        sql = cp.select_query(name, smap, scope)
        assert "*" not in sql and sql.count(" AS `") == len(cp.CONTRACT[name]), name
    billing = cp.select_query("billing_items", smap, scope)
    assert "FROM `fc`.`src`.`billing`" in billing
    assert "`billing_date` AS `billingdocumentdate`" in billing and "CAST(NULL AS STRING) AS `salesgroup`" in billing
    assert "WHERE `companycode` IN ('2010')" in billing
    assert (
        "`billing_date` >= TIMESTAMP '2024-01-01 00:00:00' AND `billing_date` < TIMESTAMP '2026-02-01 00:00:00'"
        in billing
    )
    assert "WHERE (plant = '2010')" in cp.select_query("material_master", smap, scope)
    assert "WHERE" not in cp.select_query("customer_master", smap, scope)
    control = cp.control_query(smap, scope)
    assert "COUNT(*)" in control and "GROUP BY `companycode`, `sddocumentcategory`" in control
    assert "`billing_date` >= TIMESTAMP '2024-01-01 00:00:00'" in control


def test_small_tables_are_copied_first_billing_last():
    order = cp.copy_order(cp.load_source_map(_map()))
    assert order[0].startswith("text_") and order[-1] == "billing_items"
    assert order.index("customer_master") < order.index("material_master")


# ---------------------------------------------------------------------------
# Runs against a fake source (SAP-shaped dummy data)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def source():
    return sap.build_sap(SEED, AS_OF, start=date(2026, 9, 15))[0]


class FakeSource:
    """Answers the copy's queries from the dummy tables; counts what was asked."""

    def __init__(self, out: dict, drop_one_billing_row: bool = False):
        self.out, self.asked, self.drop = out, [], drop_one_billing_row
        self.physical = {
            "billing": "billing_items", "customers": "customer_master", "materials": "material_master",
            "regions": "text_regions", "offices": "text_sales_offices",
        }  # fmt: skip

    def __call__(self, sql: str) -> pd.DataFrame:
        self.asked.append(sql)
        table = self.physical[re.search(r"FROM `fc`\.`src`\.`(\w+)`", sql).group(1)]
        df = self.out[table].copy()
        if table == "billing_items":
            dates = df["billingdocumentdate"]
            df = df[(df["companycode"] == "2010") & (dates >= "2026-09-15") & (dates < "2026-10-01")]
            if "GROUP BY" in sql:
                net = df["netamount"].astype(float)
                g = df.assign(net=net).groupby(["companycode", "sddocumentcategory"])
                agg = g.agg(line_count=("net", "size"), netamount_sum=("net", "sum")).reset_index()
                return agg.astype(str)
            if self.drop:
                df = df.iloc[1:]
        cols = re.findall(r" AS `(\w+)`", sql)
        return df[[c for c in cols if c in df.columns]].reset_index(drop=True)


def _run(tmp_path, out, mode="copy", source="real", fetch=None, **kw):
    cfg = _cfg(source, date_from="2026-09-15", date_to="2026-09-30")
    fetch = fetch or FakeSource(out)
    return cp.run_copy(
        cfg, cp.load_source_map(_map()), fetch, tmp_path / "staging", mode, echo=lambda _: None, **kw
    ), fetch


def test_a_dry_run_sends_nothing(tmp_path, source):
    run, fetch = _run(tmp_path, source, mode="dry_run", source="dummy")
    assert fetch.asked == [] and run.written is None
    assert set(run.queries) == set(cp.load_source_map(_map()).tables)


def test_the_pilot_sends_one_small_query_and_writes_nothing(tmp_path, source):
    run, fetch = _run(tmp_path, source, mode="pilot")
    assert len(fetch.asked) == 1 and "text_" in next(iter(run.queries))
    assert run.written is None and not (tmp_path / "staging").exists()


def test_the_copy_writes_the_layout_the_pipeline_loads(tmp_path, source):
    from sales_insights.pipeline import landing as lz

    run, fetch = _run(tmp_path, source)
    assert run.stopped is None and len(fetch.asked) == 6  # 5 tables + the control totals
    staging = tmp_path / "staging"
    manifest = json.loads((staging / "history" / sap.MANIFEST).read_text())
    assert manifest["source_system"] == cp.MANIFEST_SOURCE and manifest["planted_cases"] == {}
    billing = pd.read_csv(staging / "history" / "billing_items.csv", dtype=str, keep_default_na=False)
    assert list(billing.columns) == sap.BILLING_COLUMNS and set(billing["companycode"]) == {"2010"}
    counts = pd.read_csv(staging / "history" / "source_counts.csv", dtype=str)
    assert counts["line_count"].astype(int).sum() == len(billing)
    customers = pd.read_csv(staging / "masters" / "customer_master.csv", dtype=str)
    assert not set(sap.PERSONAL_CUSTOMER_COLUMNS) & set(customers.columns)
    found = lz.discover(staging, load_config("local", area="client").file_targets)
    assert not found.incomplete_folders and not found.unexpected_files
    assert {f.target for f in found.files} >= {"billing_items", "source_counts", "masters_customer_master"}


def test_the_copy_refuses_dummy_mode_and_a_second_copy(tmp_path, source):
    with pytest.raises(RuntimeError, match="source: dummy"):
        _run(tmp_path, source, source="dummy")
    _run(tmp_path, source)
    with pytest.raises(RuntimeError, match="never copy twice"):
        _run(tmp_path, source)


def test_the_copy_refuses_while_bronze_holds_earlier_rows(tmp_path, source):
    with pytest.raises(RuntimeError, match="bronze still holds"):
        _run(tmp_path, source, bronze_loaded=True)
    assert not (tmp_path / "staging").exists()


def test_databricks_copies_the_real_window_while_a_mac_keeps_dummy_data():
    from sales_insights.pipeline.sap_silver import scope_of

    local, dev = load_config("local", area="client"), load_config("dev", area="client")
    assert local.area_setting("source") == "dummy" and dev.area_setting("source") == "real"
    scope = scope_of(dev)
    assert (scope.company_codes, str(scope.date_from), str(scope.date_to)) == (("2010",), "2024-01-01", "2026-01-31")
    assert scope_of(local).date_from.isoformat() == "2025-04-01"


def test_the_client_customer_master_personal_columns_are_refused():
    for col in ("customerfullname", "organizationbpname1", "taxnumber3", "sortfield", "createdbyuser"):
        with pytest.raises(ValueError, match="personal"):
            cp.load_source_map(_map(customer_master={"table": "src.c", "columns": {"cityname": col}}))


def test_rows_that_differ_from_the_source_totals_stop_the_copy(tmp_path, source):
    run, _ = _run(tmp_path, source, fetch=FakeSource(source, drop_one_billing_row=True))
    assert run.stopped and "differ" in run.stopped
    assert not (tmp_path / "staging" / "history").exists()  # nothing written


def test_a_slow_query_stops_the_copy_before_the_next_one(tmp_path, source):
    run, fetch = _run(tmp_path, source, max_seconds=-1)
    assert run.stopped and len(fetch.asked) == 1 and run.written is None


def test_the_copy_job_is_manual_and_a_dry_run_by_default():
    import yaml

    job = yaml.safe_load((ROOT / "bundle" / "resources" / "jobs.yml").read_text())["resources"]["jobs"]["client_copy"]
    assert "schedule" not in job and "trigger" not in job
    params = {p["name"]: p["default"] for p in job["parameters"]}
    assert params["mode"] == "dry_run" and params["source_map"].startswith("/Volumes/client_dev/raw/output/")
    args = job["tasks"][0]["spark_python_task"]["parameters"]
    assert args[0] == "client_copy" and args[args.index("--area") + 1] == "client"
    assert job["timeout_seconds"] <= 1800


# ---------------------------------------------------------------------------
# Copy, then the whole client pipeline on the copy (Spark)
# ---------------------------------------------------------------------------


def test_the_pipeline_loads_a_real_style_copy(spark, cfg, tmp_path, source):
    from sales_insights.common.lake import Lake
    from sales_insights.pipeline import run_pipeline as rp

    client = _cfg("real", date_from="2026-09-15", date_to="2026-09-30")
    run, _ = _run(tmp_path, source)
    assert run.written
    lake = Lake(spark, client, root=tmp_path / "lake")
    (day,) = rp.run_range(
        spark, client, [date(2026, 9, 30)], staging=tmp_path / "staging", landing=tmp_path / "delivered", lake=lake,
        echo=lambda _: None,
    )  # fmt: skip
    assert day.ok, [st.detail for st in day.steps]
    fates = {r["fate"] for r in lake.read("ops", "sap_row_fates").collect()}
    assert "out_of_scope_company" not in fates  # the copy already kept company 2010 only
