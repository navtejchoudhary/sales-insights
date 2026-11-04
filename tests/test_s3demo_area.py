"""The s3demo area: the sales demo files read from S3 (read-only). It must never generate data, never share a
location with another area, run the sales steps unchanged and only by hand. No Spark needed."""

from datetime import date
from pathlib import Path

import pytest
import yaml

from sales_insights.common.config import load_config
from sales_insights.pipeline import external_source as ext

ROOT = Path(__file__).resolve().parents[1]
FIRST, LAST = date(2026, 10, 1), date(2026, 10, 3)


# ---------------------------------------------------------------------------
# Config: its own catalog and folders, the sales file rules, never generates
# ---------------------------------------------------------------------------


def test_s3demo_on_databricks_has_its_own_catalog_and_reads_the_s3_volume():
    s3 = load_config("dev", area="s3demo")
    assert s3.catalog == "s3demo_dev"
    assert s3.table("gold", "fact_sales") == "s3demo_dev.gold.fact_sales"
    assert s3.table("ops", "processed_files") == "s3demo_dev.ops.processed_files"
    assert s3.staging_path == "/Volumes/s3demo_dev/raw/source"  # the external, read-only volume on S3
    assert s3.landing_path == "/Volumes/s3demo_dev/raw/landing"
    assert s3.output_path == "/Volumes/s3demo_dev/raw/output"


@pytest.mark.parametrize("profile", ["local", "dev"])
def test_s3demo_uses_the_sales_files_and_never_generates(profile):
    sales, s3 = load_config(profile), load_config(profile, area="s3demo")
    assert s3.file_targets == sales.file_targets  # the same files as the sales demo
    assert s3.area_setting("source") == "external"
    for other in ("sales", "inventory", "client"):
        o = load_config(profile, area=other)
        assert (s3.catalog, s3.lake_path) != (o.catalog, o.lake_path), other
        assert s3.staging_path != o.staging_path and s3.landing_path != o.landing_path, other


def test_no_other_area_is_external():
    for area in ("sales", "inventory", "client"):
        assert load_config("dev", area=area).area_setting("source") != "external", area


# ---------------------------------------------------------------------------
# An external source: stop if anything is missing, never write to staging
# ---------------------------------------------------------------------------


def _write(path: Path, text: str = "x\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _delivered_staging(root: Path) -> Path:
    """What the upload holds: masters, history and each day's folder, every one with its manifest."""
    staging = root / "source"
    _write(staging / "masters" / "customers.csv", "customer_id\n1\n")
    _write(staging / "masters" / "masters_manifest.json", "{}")
    _write(staging / "history" / "invoices_202509.csv", "invoice_number\n1\n")
    _write(staging / "history" / "history_manifest.json", "{}")
    d = FIRST
    while d <= LAST:
        _write(staging / f"business_date={d}" / f"orders_{d:%Y%m%d}.csv", "invoice_number\n1\n")
        _write(staging / f"business_date={d}" / f"manifest_{d:%Y%m%d}.json", "{}")
        d = date.fromordinal(d.toordinal() + 1)
    return staging


def _files(folder: Path) -> list[str]:
    return sorted(str(p.relative_to(folder)) for p in folder.rglob("*") if p.is_file())


def test_missing_files_lists_every_manifest_the_run_needs(tmp_path):
    missing = ext.missing_files(tmp_path / "empty", FIRST, LAST)
    assert missing == [
        "masters/masters_manifest.json",
        "history/history_manifest.json",
        "business_date=2026-10-01/manifest_20261001.json",
        "business_date=2026-10-02/manifest_20261002.json",
        "business_date=2026-10-03/manifest_20261003.json",
    ]
    assert ext.missing_files(_delivered_staging(tmp_path), FIRST, LAST) == []


def test_a_missing_day_stops_the_run_before_anything_is_delivered(tmp_path):
    staging = _delivered_staging(tmp_path)
    (staging / "business_date=2026-10-02" / "manifest_20261002.json").unlink()
    before = _files(staging)
    landing = tmp_path / "landing"
    with pytest.raises(FileNotFoundError, match=r"1 source file.*business_date=2026-10-02.*never generates"):
        ext.ensure_external_source(staging, landing, FIRST, LAST)
    assert _files(staging) == before  # nothing generated, nothing written to the source
    assert not landing.exists()  # nothing delivered


def test_a_complete_source_is_delivered_once_and_never_changed(tmp_path):
    staging, landing = _delivered_staging(tmp_path), tmp_path / "landing"
    before = _files(staging)
    assert ext.ensure_external_source(staging, landing, FIRST, LAST) == ["initial delivery of master data and history"]
    assert (landing / "masters" / "masters_manifest.json").is_file()
    assert (landing / "history" / "history_manifest.json").is_file()
    assert not (landing / "business_date=2026-10-01").exists()  # the days come one by one through the drip
    assert ext.ensure_external_source(staging, landing, FIRST, LAST) == []  # second call: nothing to do
    assert _files(staging) == before


# ---------------------------------------------------------------------------
# The job and the setup SQL
# ---------------------------------------------------------------------------


def _job():
    jobs = yaml.safe_load((ROOT / "bundle" / "resources" / "jobs.yml").read_text())["resources"]["jobs"]
    return jobs["s3demo_pipeline"]


def test_s3demo_job_runs_by_hand_only_never_retries_and_publishes_after_the_checks():
    job = _job()
    assert "schedule" not in job and "trigger" not in job  # never runs (or bills) on its own
    assert job["parameters"] == [{"name": "data_until", "default": "2026-10-29"}]  # the last day on S3
    pipeline, publish = job["tasks"]
    for task in (pipeline, publish):
        assert task["max_retries"] == 0 and task["disable_auto_optimization"] is True, task["task_key"]
    p = pipeline["spark_python_task"]["parameters"]
    assert p[:3] == ["pipeline", "--profile", "dev"]
    assert p[p.index("--area") + 1] == "s3demo"
    assert (p[p.index("--from") + 1], p[p.index("--to") + 1]) == ("2026-10-01", "{{job.parameters.data_until}}")
    assert p[p.index("--downstream") + 1] == "last"
    assert "--no-deck" not in p  # the insights step writes the weekly deck
    q = publish["spark_python_task"]["parameters"]
    assert q[0] == "kpis" and q[q.index("--area") + 1] == "s3demo"
    assert publish["depends_on"] == [{"task_key": "pipeline"}] and "run_if" not in publish  # only after a pass


def test_s3demo_setup_sql_matches_the_config_grants_nothing_and_holds_no_bucket():
    sql = (ROOT / "sql" / "setup" / "s3demo_dev.sql").read_text()
    dev = load_config("dev", area="s3demo")
    assert f"CREATE CATALOG IF NOT EXISTS {dev.catalog}" in sql
    for layer in ("raw", "bronze", "silver", "gold", "ops"):
        assert f"CREATE SCHEMA IF NOT EXISTS {dev.catalog}.{layer}" in sql
    for path in (dev.landing_path, dev.output_path):
        volume = ".".join(path.removeprefix("/Volumes/").split("/"))
        assert f"CREATE VOLUME IF NOT EXISTS {volume};" in sql
    source = ".".join(dev.staging_path.removeprefix("/Volumes/").split("/"))
    assert f"CREATE EXTERNAL VOLUME IF NOT EXISTS {source}" in sql  # staging = the S3 folder, read-only
    assert "LOCATION '<S3 URL" in sql  # a placeholder: the bucket name is never committed
    statements = [line for line in sql.splitlines() if line and not line.startswith("--")]
    assert not [s for s in statements if s.startswith("GRANT")]
    assert f"REVOKE BROWSE ON CATALOG {dev.catalog} FROM `account users`;" in sql
    assert dev.catalog not in (ROOT / "sql" / "setup" / "permissions.sql").read_text()


# ---------------------------------------------------------------------------
# The pipeline steps (these import run_pipeline, which needs PySpark installed; no Spark session is started)
# ---------------------------------------------------------------------------


def test_s3demo_runs_the_sales_data_steps_unchanged():
    pytest.importorskip("pyspark")
    from sales_insights.pipeline import run_pipeline as rp

    assert rp.AREA_STEPS["s3demo"] == ["drip", "bronze", "silver", "gold", "reconcile", "kpis", "insights"]
    for step in rp.AREA_STEPS["s3demo"]:
        assert rp.AREA_STEP_FUNCS["s3demo"][step] is rp.STEP_FUNCS[step], step  # the sales functions themselves
    assert rp.AREA_STEP_FUNCS["sales"] is rp.STEP_FUNCS  # sales unchanged


def test_ensure_source_never_generates_for_an_external_area(tmp_path):
    pytest.importorskip("pyspark")
    from sales_insights.pipeline import run_pipeline as rp

    cfg = load_config("local", area="s3demo")
    with pytest.raises(FileNotFoundError, match="never generates"):
        rp.ensure_source(cfg, tmp_path / "source", tmp_path / "landing", LAST, first=FIRST)
    assert not (tmp_path / "source").exists()  # the sales branch would have generated master data here
    staging = _delivered_staging(tmp_path)
    assert rp.ensure_source(cfg, staging, tmp_path / "landing", LAST, first=FIRST) == [
        "initial delivery of master data and history"
    ]


# ---------------------------------------------------------------------------
# KPI layer, dashboard and Genie agent: the sales ones, on s3demo_dev only
# ---------------------------------------------------------------------------


def test_s3demo_publishes_exactly_the_sales_kpis():
    pytest.importorskip("pyspark")
    from sales_insights.semantic import kpis as k

    s3, sales = k.AREAS["s3demo"], k.AREAS["sales"]
    assert (s3.folder, s3.metric_views, s3.agreements, s3.own_gold) == (
        sales.folder, sales.metric_views, sales.agreements, True)  # fmt: skip


def test_s3demo_dashboard_is_the_sales_dashboard_on_s3demo_dev_for_the_owner_only():
    from sales_insights.semantic import dashboards as db

    spec, sales = db.load("s3demo"), db.load("sales")
    assert spec["area"] == "s3demo" and spec["audience"] == []
    assert spec["filters"] == sales["filters"] and spec["pages"][1:] == sales["pages"][1:]
    assert spec["datasets"] == {k: {"same_as": f"sales.{k}"} for k in sales["datasets"]}
    board = (ROOT / "dashboards" / "s3demo.lvdash.json").read_text()
    catalogs = {m.split(".")[0] for m in __import__("re").findall(r"\b\w+_dev\.\w+\.\w+", board)}
    assert catalogs == {"s3demo_dev"}, catalogs  # never sales_dev, inventory_dev or client_dev
    bundle = yaml.safe_load((ROOT / "bundle" / "resources" / "dashboards.yml").read_text())["resources"]
    assert "permissions" not in bundle["dashboards"]["s3demo_dashboard"]
    for name in ("sales", "stock", "ceo", "client"):
        assert "s3demo_dev." not in (ROOT / "dashboards" / f"{name}.lvdash.json").read_text(), name


def test_s3demo_genie_agent_asks_the_sales_questions_on_s3demo_dev():
    from sales_insights.semantic import answer_key as ak
    from sales_insights.semantic import genie_setup as gs

    agent, sales = ak.SPACES["s3demo"], ak.SPACES["sales"]
    assert agent.folder == sales.folder and agent.benchmarks == sales.benchmarks  # the same questions
    assert agent.answer_key != sales.answer_key  # never overwrites genie/answer_key.csv
    tables = gs.table_names(load_config("dev"), agent)
    assert tables and all(t.startswith("s3demo_dev.gold.") for t in tables.values()), tables
    assert gs.benchmarks_file(agent) == "genie_benchmarks_s3demo.csv"
