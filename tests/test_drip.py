"""Tests for the drip (staging -> landing delivery)."""

import csv
from datetime import date, timedelta

import pytest

from sales_insights.drip import drip
from sales_insights.generator import stories
from sales_insights.generator.simulate import folder_name

MISSING = stories.MISSING_DELIVERY_DAY


def _stage(staging, d):
    folder = staging / folder_name(d)
    folder.mkdir(parents=True)
    stamp = d.strftime("%Y%m%d")
    for name in (f"orders_{stamp}.csv", f"changes_{stamp}.csv", f"manifest_{stamp}.json"):
        (folder / name).write_text(f"{name}\n")


@pytest.fixture
def staging(tmp_path):
    root = tmp_path / "staging"
    for i in range(29):
        _stage(root, date(2026, 10, 1) + timedelta(days=i))
    return root


def test_schedule_dates_make_sense():
    assert MISSING.weekday() == 6  # Sunday: the Monday after also redelivers it
    assert date(2026, 10, 1) <= MISSING < date(2026, 10, 29)


def test_normal_day(staging):
    d = date(2026, 10, 14)  # Wednesday
    actions = drip.plan(d, staging)
    assert [a.kind for a in actions] == ["deliver"]
    assert actions[0].files[-1].startswith("manifest_")  # manifest last


def test_missing_day_then_catch_up(staging):
    assert [a.kind for a in drip.plan(MISSING, staging)] == ["skip"]
    nxt = drip.plan(MISSING + timedelta(days=1), staging)
    assert [(a.kind, a.business_date) for a in nxt][:2] == [
        ("catch_up", MISSING),
        ("deliver", MISSING + timedelta(days=1)),
    ]


def test_monday_redelivers_sunday_orders(staging):
    monday = date(2026, 10, 12)
    assert monday.weekday() == 0
    actions = drip.plan(monday, staging)
    assert [a.kind for a in actions] == ["deliver", "redeliver"]
    assert actions[1].business_date == monday - timedelta(days=1)
    assert actions[1].files == ("orders_20261011.csv",)


def test_execute_copies_and_logs(staging, tmp_path):
    landing = tmp_path / "landing"
    for i in range(29):
        d = date(2026, 10, 1) + timedelta(days=i)
        drip.execute(drip.plan(d, staging), staging, landing)

    delivered = sorted(p.name for p in landing.iterdir() if p.is_dir())
    assert len(delivered) == 29  # the missing day still arrives (a day late)
    with open(landing / drip.LOG_NAME, newline="") as fh:
        log = list(csv.DictReader(fh))
    assert any(r["action"] == "skip" and r["business_date"] == MISSING.isoformat() for r in log)
    assert sum(r["action"] == "redeliver" for r in log) == 4  # Mondays 5, 12, 19, 26 Oct
    day = [r["file"] for r in log if r["business_date"] == "2026-10-14" and r["action"] == "deliver"]
    assert day[-1].startswith("manifest_")


def test_missing_staging_fails_clearly(tmp_path):
    with pytest.raises(FileNotFoundError, match="run the simulator"):
        drip.plan(date(2026, 10, 14), tmp_path / "empty")
