"""Asset Bundle checks (no Databricks needed): the YAML is valid and points at code that exists."""

import importlib.util
from pathlib import Path

import pytest
import yaml

from sales_insights.common.config import load_config

ROOT = Path(__file__).resolve().parents[1]
JOBS_FILE = ROOT / "bundle" / "resources" / "jobs.yml"
PAUSE = "${var.schedule_pause_status}"  # every schedule follows one switch (default UNPAUSED)


def _launcher():
    spec = importlib.util.spec_from_file_location("bundle_run", ROOT / "bundle" / "run.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _jobs():
    return yaml.safe_load(JOBS_FILE.read_text())["resources"]["jobs"]


def test_bundle_file_includes_the_jobs():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["bundle"]["name"] == "sales-insights"
    assert bundle["include"] == ["bundle/resources/*.yml"]
    assert bundle["targets"]["dev"]["default"] is True
    assert "host" not in bundle["targets"]["dev"].get("workspace", {})  # never commit a workspace URL
    assert bundle["targets"]["dev"].get("mode") != "development"  # that mode pauses the daily schedule


def test_every_task_runs_an_existing_command():
    run = _launcher()
    for job in _jobs().values():
        for task in job["tasks"]:
            t = task["spark_python_task"]
            assert (JOBS_FILE.parent / t["python_file"]).resolve() == (ROOT / "bundle" / "run.py").resolve()
            command = t["parameters"][0]
            assert command in run.COMMANDS
            assert importlib.util.find_spec(run.COMMANDS[command]) is not None
            assert t["parameters"][1:3] == ["--profile", "dev"]


def test_serverless_environment_never_installs_spark():
    for job in _jobs().values():
        deps = job["environments"][0]["spec"]["dependencies"]
        assert not any(d.startswith(("pyspark", "delta-spark", "databricks-connect")) for d in deps)
        assert any(d.startswith("python-pptx") for d in deps)


def test_every_job_has_cost_guards():
    for name, job in _jobs().items():
        assert 0 < job["timeout_seconds"] <= 7200, name  # a stuck run can never bill for more than 2 hours
        assert job["max_concurrent_runs"] == 1, name  # runs never pile up
        assert job["email_notifications"]["on_failure"], name


def test_daily_schedule_is_sri_lanka_time():
    s = _jobs()["sales_daily_pipeline"]["schedule"]
    assert s["timezone_id"] == "Asia/Colombo" and s["pause_status"] == PAUSE


def test_genie_check_runs_weekly_after_the_daily_job():
    job = _jobs()["sales_genie_health"]
    assert job["schedule"]["timezone_id"] == "Asia/Colombo" and job["schedule"]["pause_status"] == PAUSE
    assert job["schedule"]["quartz_cron_expression"] == "0 0 7 ? * MON"  # Monday 07:00, after the 06:30 run
    tasks = {t["task_key"]: t for t in job["tasks"]}
    agents = {}
    for key, task in tasks.items():
        params = task["spark_python_task"]["parameters"]
        assert params[0] == "genie_health" and params[-2:] == ["--target", "90"], key
        agents[params[params.index("--agent") + 1]] = params[params.index("--space-id") + 1]
        if task.get("depends_on"):
            assert task["run_if"] == "ALL_DONE", key  # one agent failing never skips the others
    assert agents == {"sales": "${var.genie_space_id}", "inventory": "${var.inventory_genie_space_id}",
                      "sales_and_stock": "${var.sales_and_stock_genie_space_id}"}  # fmt: skip
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert {v.removeprefix("${var.").removesuffix("}") for v in agents.values()} <= set(bundle["variables"])
    deps = job["environments"][0]["spec"]["dependencies"]
    assert any(d.startswith("databricks-sdk") for d in deps)  # the Genie evaluation API needs a recent SDK


def test_one_switch_pauses_every_schedule():
    """`--var schedule_pause_status=PAUSED` keeps the demo paused through a deploy (cost); the default runs it."""
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["variables"]["schedule_pause_status"]["default"] == "UNPAUSED"
    for f in ("jobs.yml", "alerts.yml"):
        text = (ROOT / "bundle" / "resources" / f).read_text()
        assert "pause_status: UNPAUSED" not in text and "pause_status: PAUSED" not in text, f
    scheduled = [j["schedule"] for j in _jobs().values() if "schedule" in j]
    scheduled += [a["schedule"] for a in _alerts().values()]
    assert scheduled and all(s["pause_status"] == PAUSE for s in scheduled)


def test_scheduled_jobs_use_standard_serverless():
    for name, job in _jobs().items():
        if "schedule" in job:  # nobody waits for a scheduled run: take the cheaper, slower-starting mode
            assert job.get("performance_target") == "STANDARD", name


def test_launcher_takes_the_profile_out():
    run = _launcher()
    assert run.split_profile(["--profile", "dev", "--date", "2026-10-15"]) == ("dev", ["--date", "2026-10-15"])
    assert run.split_profile(["--date", "2026-10-15"]) == (None, ["--date", "2026-10-15"])


def test_dev_profile_writes_to_volumes():
    dev = load_config("dev")
    assert dev.output_path.startswith("/Volumes/sales_dev/")
    assert dev.landing_path.startswith("/Volumes/sales_dev/")
    assert load_config("local").output_path == "output"


def _alerts():
    return yaml.safe_load((ROOT / "bundle" / "resources" / "alerts.yml").read_text())["resources"]["alerts"]


def test_alerts_read_the_latest_insights_run_and_email_the_owner():
    from sales_insights.insights.narrative import ALERT_KINDS
    from sales_insights.insights.stock import ALERT_KINDS as STOCK_ALERT_KINDS

    alerts = _alerts()
    assert set(alerts) == {"sales_revenue_drop", "sales_stock_out", "stock_confirmed_stock_out"}
    kinds = set()
    for name, a in alerts.items():
        q = a["query_text"]
        table = "${var.inventory_catalog}" if name.startswith("stock_") else "${var.catalog}"
        assert f"{table}.gold.insights" in q and "max_by(run_id, generated_ts)" in q, name  # latest run only
        kinds.add(q.split("kind = '")[1].split("'")[0])
        assert a["warehouse_id"] == "${var.warehouse_id}", name  # found by name: no warehouse ID in git
        ev = a["evaluation"]
        assert (ev["source"]["name"], ev["comparison_operator"], ev["threshold"]["value"]["double_value"]) == (
            "findings", "GREATER_THAN", 0)  # fmt: skip
        assert ev["empty_result_state"] == "OK"
        assert ev["notification"]["subscriptions"] == [{"user_email": "${workspace.current_user.userName}"}]
        s = a["schedule"]
        when = (s["quartz_cron_schedule"], s["timezone_id"], s["pause_status"])
        assert when == ("0 0 8 * * ?", "Asia/Colombo", PAUSE), name  # 08:00, after the 06:30 daily job
    assert kinds == {"revenue_drop", "stock_out", "stock_out_confirmed"}
    assert kinds <= set(ALERT_KINDS) | set(STOCK_ALERT_KINDS)


def test_alert_variables_match_the_config():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    v = bundle["variables"]
    assert v["catalog"]["default"] == load_config("dev").catalog
    assert v["inventory_catalog"]["default"] == load_config("dev", area="inventory").catalog
    assert v["warehouse_id"]["lookup"] == {"warehouse": "sales-insights-wh"}


def test_launcher_turns_today_into_the_sri_lanka_date():
    from datetime import UTC, datetime

    run = _launcher()
    late_utc = datetime(2026, 10, 15, 20, 0, tzinfo=UTC)  # 01:30 on 16 Oct in Sri Lanka
    sri_lanka = late_utc.astimezone(run.ZoneInfo(run.TIMEZONE))
    assert run.resolve_today(["--to", "today"], sri_lanka) == ["--to", "2026-10-16"]
    assert run.resolve_today(["--to", "2026-10-29"], sri_lanka) == ["--to", "2026-10-29"]  # a fixed date stays


def _params(job):
    return job["tasks"][0]["spark_python_task"]["parameters"]


@pytest.mark.parametrize("name", ["sales_backfill", "inventory_backfill"])
def test_backfills_run_to_data_until_which_defaults_to_today(name):
    job = _jobs()[name]
    assert job["parameters"] == [{"name": "data_until", "default": "today"}]
    params = _params(job)
    assert params[params.index("--to") + 1] == "{{job.parameters.data_until}}"
    assert params[params.index("--from") + 1] == "2026-10-01"


def test_sales_jobs_never_name_an_area():
    """Sales stays the default everywhere: its jobs run exactly the commands they always ran."""
    for name, job in _jobs().items():
        if name.startswith("sales_"):
            assert "--area" not in _params(job), name


def test_inventory_jobs_run_the_inventory_area_after_sales():
    jobs = _jobs()
    inventory = {n: j for n, j in jobs.items() if n.startswith("inventory_")}
    assert set(inventory) == {"inventory_backfill", "inventory_daily_pipeline", "inventory_publish_semantic"}
    for name, job in inventory.items():
        params = _params(job)
        assert params[params.index("--area") + 1] == "inventory", name
    daily = inventory["inventory_daily_pipeline"]["schedule"]
    assert (daily["timezone_id"], daily["pause_status"]) == ("Asia/Colombo", PAUSE)
    assert daily["quartz_cron_expression"] == "0 0 7 * * ?"  # 07:00: after the 06:30 sales job, whose gold it reads
    assert jobs["sales_daily_pipeline"]["schedule"]["quartz_cron_expression"] == "0 30 6 * * ?"


def test_client_job_runs_by_hand_only_and_publishes_after_the_checks():
    job = _jobs()["client_pipeline"]
    assert "schedule" not in job and "trigger" not in job  # never runs (or bills) on its own
    assert job.get("performance_target") == "STANDARD"
    pipeline, publish = job["tasks"]
    p = pipeline["spark_python_task"]["parameters"]
    assert p[0] == "pipeline" and p[p.index("--area") + 1] == "client"
    q = publish["spark_python_task"]["parameters"]
    assert q[0] == "kpis" and q[q.index("--area") + 1] == "client"
    assert publish["depends_on"] == [{"task_key": "pipeline"}] and "run_if" not in publish  # only after a pass


def test_client_setup_sql_matches_the_config_and_grants_nothing():
    sql = (ROOT / "sql" / "setup" / "client_dev.sql").read_text()
    dev = load_config("dev", area="client")
    assert f"CREATE CATALOG IF NOT EXISTS {dev.catalog}" in sql
    for layer in ("raw", "bronze", "silver", "gold", "ops"):
        assert f"CREATE SCHEMA IF NOT EXISTS {dev.catalog}.{layer}" in sql
    for path in (dev.staging_path, dev.landing_path, dev.output_path):
        volume = ".".join(path.removeprefix("/Volumes/").split("/"))
        assert f"CREATE VOLUME IF NOT EXISTS {volume};" in sql
    statements = [line for line in sql.splitlines() if line and not line.startswith("--")]
    assert not [s for s in statements if s.startswith("GRANT")]  # the client's data is shared with nobody
    assert f"REVOKE BROWSE ON CATALOG {dev.catalog} FROM `account users`;" in sql
    assert dev.catalog not in (ROOT / "sql" / "setup" / "permissions.sql").read_text()


def test_inventory_setup_sql_matches_the_config():
    sql = (ROOT / "sql" / "setup" / "inventory_dev.sql").read_text()
    dev = load_config("dev", area="inventory")
    assert f"CREATE CATALOG IF NOT EXISTS {dev.catalog}" in sql
    for layer in ("raw", "bronze", "silver", "gold", "ops"):
        assert f"CREATE SCHEMA IF NOT EXISTS {dev.catalog}.{layer}" in sql
    for path in (dev.staging_path, dev.landing_path, dev.output_path):
        volume = ".".join(path.removeprefix("/Volumes/").split("/"))  # /Volumes/c/s/v -> c.s.v
        assert f"CREATE VOLUME IF NOT EXISTS {volume};" in sql
