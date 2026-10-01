"""Tests for manifest parsing (plain Python, no Spark)."""

import json
from datetime import date

from sales_insights.generator import history as h
from sales_insights.generator import masters as m
from sales_insights.generator import simulate as s
from sales_insights.pipeline.manifests import DIRT_COLUMNS, TOTALS_COLUMNS, parse_manifest

AS_OF = date(2026, 10, 1)
SEED = 20261001


def test_daily_manifest(tmp_path):
    drop = s.Simulator(SEED, AS_OF).build_day(date(2026, 10, 13))
    folder = s.write_day(drop, tmp_path, SEED, AS_OF)
    content = (folder / "manifest_20261013.json").read_text()
    totals, dirt = parse_manifest("business_date=2026-10-13/manifest_20261013.json", content)
    assert all(len(t) == len(TOTALS_COLUMNS) for t in totals)
    assert all(len(d) == len(DIRT_COLUMNS) for d in dirt)
    orders = [t for t in totals if t[3] == "orders"]
    assert sum(t[5] for t in orders) == drop.control_totals["orders"]["lines"]
    assert round(sum(t[7] for t in orders), 2) == drop.control_totals["orders"]["revenue"]
    invalid = [d for d in dirt if d[4] == "invalid_date"]
    assert len(invalid) == 2 and all(d[3] == "orders" for d in invalid)


def test_history_manifest(tmp_path):
    df = h.build_history(SEED, AS_OF, start=date(2026, 9, 28))
    target = h.write_history(df, tmp_path, SEED, AS_OF)
    totals, dirt = parse_manifest("history/history_manifest.json", (target / "history_manifest.json").read_text())
    assert {t[3] for t in totals} == {"history"}
    assert sum(t[5] for t in totals) == len(df)
    assert dirt == []


def test_masters_manifest(tmp_path):
    tables, log = m.build_masters(SEED, AS_OF)
    target = m.write_masters(tables, log, tmp_path, SEED, AS_OF)
    totals, dirt = parse_manifest("masters/masters_manifest.json", (target / "masters_manifest.json").read_text())
    assert totals == []
    dups = [d for d in dirt if d[4] == "duplicate_older_version"]
    assert len(dups) == 12 and all(d[3] == "customers" for d in dups)
    assert json.loads((target / "masters_manifest.json").read_text())["kind"] == "masters"
