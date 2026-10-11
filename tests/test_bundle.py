"""Asset Bundle checks (no Databricks needed): the YAML is valid and points at code that exists."""

import importlib.util
from pathlib import Path

import yaml

from sales_insights.common.config import load_config

ROOT = Path(__file__).resolve().parents[1]
JOBS_FILE = ROOT / "bundle" / "resources" / "jobs.yml"


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
    assert s["timezone_id"] == "Asia/Colombo" and s["pause_status"] == "UNPAUSED"


def test_genie_check_runs_weekly_after_the_daily_job():
    job = _jobs()["sales_genie_health"]
    assert job["schedule"]["timezone_id"] == "Asia/Colombo" and job["schedule"]["pause_status"] == "UNPAUSED"
    assert job["schedule"]["quartz_cron_expression"] == "0 0 7 ? * MON"  # Monday 07:00, after the 06:30 run
    params = job["tasks"][0]["spark_python_task"]["parameters"]
    assert params[0] == "genie_health" and "${var.genie_space_id}" in params and params[-2:] == ["--target", "90"]
    deps = job["environments"][0]["spec"]["dependencies"]
    assert any(d.startswith("databricks-sdk") for d in deps)  # the Genie evaluation API needs a recent SDK


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

    alerts = _alerts()
    assert set(alerts) == {"sales_revenue_drop", "sales_stock_out"}
    kinds = set()
    for name, a in alerts.items():
        q = a["query_text"]
        assert "${var.catalog}.gold.insights" in q and "max_by(run_id, generated_ts)" in q, name  # latest run only
        kinds.add(q.split("kind = '")[1].split("'")[0])
        assert a["warehouse_id"] == "${var.warehouse_id}", name  # found by name: no warehouse ID in git
        ev = a["evaluation"]
        assert (ev["source"]["name"], ev["comparison_operator"], ev["threshold"]["value"]["double_value"]) == (
            "findings", "GREATER_THAN", 0)  # fmt: skip
        assert ev["empty_result_state"] == "OK"
        assert ev["notification"]["subscriptions"] == [{"user_email": "${workspace.current_user.userName}"}]
        s = a["schedule"]
        when = (s["quartz_cron_schedule"], s["timezone_id"], s["pause_status"])
        assert when == ("0 0 8 * * ?", "Asia/Colombo", "UNPAUSED"), name  # 08:00, after the 06:30 daily job
    assert kinds == {"revenue_drop", "stock_out"} and kinds <= set(ALERT_KINDS)


def test_alert_variables_match_the_config():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    v = bundle["variables"]
    assert v["catalog"]["default"] == load_config("dev").catalog
    assert v["warehouse_id"]["lookup"] == {"warehouse": "sales-insights-wh"}
