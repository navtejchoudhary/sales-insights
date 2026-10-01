"""Tests for landing-zone discovery (plain Python, no Spark)."""

import json
from datetime import date

import pytest

from sales_insights.generator import simulate as s
from sales_insights.pipeline import landing as lz

AS_OF = date(2026, 10, 1)
SEED = 20261001


def _daily(landing, d, sim, with_manifest=True):
    target = s.write_day(sim.build_day(d), landing, SEED, AS_OF)
    if not with_manifest:
        next(target.glob("manifest_*.json")).unlink()
    return target


@pytest.fixture(scope="module")
def sim():
    return s.Simulator(SEED, AS_OF)


def test_complete_folder_is_discovered(tmp_path, sim):
    _daily(tmp_path, date(2026, 10, 5), sim)
    found = lz.discover(tmp_path)
    targets = sorted(f.target for f in found.files)
    assert targets == ["changes", "manifests", "orders"]
    orders = next(f for f in found.files if f.target == "orders")
    assert orders.business_date == "2026-10-05"
    assert orders.header[0] == "invoice_number"
    assert orders.sha256 == orders.expected_sha256  # file matches what the manifest promised
    assert orders.expected_rows > 0


def test_folder_without_manifest_is_left_alone(tmp_path, sim):
    _daily(tmp_path, date(2026, 10, 5), sim, with_manifest=False)
    found = lz.discover(tmp_path)
    assert found.files == []
    assert found.incomplete_folders == ["business_date=2026-10-05"]


def test_folder_missing_a_listed_file_is_incomplete(tmp_path, sim):
    folder = _daily(tmp_path, date(2026, 10, 5), sim)
    next(folder.glob("changes_*.csv")).unlink()
    assert lz.discover(tmp_path).incomplete_folders == ["business_date=2026-10-05"]


def test_unexpected_files_are_reported_not_loaded(tmp_path, sim):
    folder = _daily(tmp_path, date(2026, 10, 5), sim)
    (folder / "notes.txt").write_text("hello")
    found = lz.discover(tmp_path)
    assert found.unexpected_files == ["business_date=2026-10-05/notes.txt"]
    assert all(not f.rel_path.endswith("notes.txt") for f in found.files)


def test_schema_change_visible_in_header(tmp_path, sim):
    _daily(tmp_path, date(2026, 10, 21), sim)
    _daily(tmp_path, date(2026, 10, 22), sim)
    headers = {f.business_date: f.header for f in lz.discover(tmp_path).files if f.target == "orders"}
    assert "sales_rep_id" not in headers["2026-10-21"]
    assert headers["2026-10-22"][-1] == "sales_rep_id"


def test_targets_for_every_file_type():
    assert lz.target_for("masters", "customers.csv") == "masters_customers"
    assert lz.target_for("history", "invoices_202509.csv") == "invoices_history"
    assert lz.target_for("business_date=2026-10-05", "orders_20261005.csv") == "orders"
    assert lz.target_for("business_date=2026-10-05", "changes_20261005.csv") == "changes"
    with pytest.raises(ValueError):
        lz.target_for("business_date=2026-10-05", "mystery_20261005.csv")


def test_classify_new_seen_changed(tmp_path, sim):
    folder = _daily(tmp_path, date(2026, 10, 5), sim)
    files = lz.discover(tmp_path).files
    new, changed, seen = lz.classify(files, {})
    assert len(new) == 3 and not changed and not seen

    loaded = {f.rel_path: {f.sha256} for f in files}
    new, changed, seen = lz.classify(lz.discover(tmp_path).files, loaded)
    assert not new and not changed and len(seen) == 3  # a redelivery of identical files is skipped

    orders = next(folder.glob("orders_*.csv"))
    orders.write_text(orders.read_text() + orders.read_text().splitlines()[1] + "\n")  # source edits a sent file
    manifest = next(folder.glob("manifest_*.json"))
    m = json.loads(manifest.read_text())
    m["files"][orders.name]["rows"] += 1
    manifest.write_text(json.dumps(m))
    new, changed, seen = lz.classify(lz.discover(tmp_path).files, loaded)
    assert {f.target for f in changed} == {"orders", "manifests"}
