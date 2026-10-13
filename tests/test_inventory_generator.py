"""Tests for the inventory (stock) generator (plain Python, no Spark).

The stock is replayed from the first history day, so the simulator is built once per test run (~20 s),
plus the sales history (~20 s) that the stock movements are checked against.
"""

import hashlib
import json
from datetime import date, timedelta

import pandas as pd
import pytest

from sales_insights.generator import engine as e
from sales_insights.generator import history as h
from sales_insights.generator import inventory as inv
from sales_insights.generator import masters as m
from sales_insights.generator import simulate as s
from sales_insights.generator import stories

AS_OF = date(2026, 10, 1)
SEED = 20261001
DAYS = [AS_OF + timedelta(days=i) for i in range(28)]  # 1-28 Oct: the live period up to the demo
SALES_ISSUE_TYPES = ("601", "602", "653")


@pytest.fixture(scope="module")
def sim():
    return inv.InventorySimulator(SEED, AS_OF)


@pytest.fixture(scope="module")
def history(sim):
    return sim.build_history()


@pytest.fixture(scope="module")
def drops(sim, history):
    return {d: sim.build_day(d) for d in DAYS}


@pytest.fixture(scope="module")
def clean(sim, drops):
    """Every clean movement and closing stock from the first history day to 28 Oct (before any dirt)."""
    mv = pd.DataFrame([r for d in sorted(sim.movements) for r in sim.movements[d]], columns=inv.MOVEMENT_COLUMNS)
    st = pd.DataFrame([r for d in sorted(sim.snapshots) for r in sim.snapshots[d]], columns=inv.STOCK_COLUMNS)
    return mv.assign(q=mv["quantity"].astype(float)), st.assign(
        u=st["unrestricted_qty"].astype(float), t=st["in_transit_qty"].astype(float)
    )


@pytest.fixture(scope="module")
def sales_history():
    return h.build_history(SEED, AS_OF)


def _stock(st, pid, loc, day):
    row = st[(st["product_id"] == pid) & (st["location_id"] == loc) & (st["snapshot_date"] == day.isoformat())]
    return float(row["u"].iloc[0])


def _dirt(drop, kind):
    return next(x for x in drop.dirt if x["dirt"] == kind)


# --- locations master -----------------------------------------------------------


def test_locations_master():
    loc = inv.build_locations(AS_OF)
    assert list(loc.columns) == inv.LOCATION_COLUMNS
    assert loc["location_id"].is_unique
    assert len(loc) == len(m.STORAGE_LOCATIONS) + len(m.SALES_OFFICES) == 17
    depots = loc[loc["location_type"] == "depot"]
    assert sorted(depots["sales_office"]) == sorted(o for o, *_ in m.SALES_OFFICES)  # one depot per sales office
    assert (depots["supplying_location_id"] == inv.HUB).all()
    assert {inv.HUB, inv.RAW_STORE, inv.RETURNS_STORE, inv.BUILDING_STORE} <= set(loc["location_id"])
    assert set(loc["province"]) <= {p for _, _, p, _ in m.DISTRICTS}


def test_locations_file_and_manifest(tmp_path):
    target = inv.write_locations(inv.build_locations(AS_OF), tmp_path, SEED, AS_OF)
    manifest = json.loads((target / "masters_manifest.json").read_text())
    assert manifest["files"]["locations.csv"]["rows"] == 17
    assert (
        manifest["files"]["locations.csv"]["sha256"]
        == hashlib.sha256((target / "locations.csv").read_bytes()).hexdigest()
    )


# --- reproducibility and the sales files ---------------------------------------


def test_any_day_is_identical_whatever_else_was_generated(drops, tmp_path):
    other = inv.InventorySimulator(SEED, AS_OF)
    later_first = other.build_day(date(2026, 10, 20))  # a different order of work
    again = other.build_day(date(2026, 10, 13))
    a = inv.write_day(drops[date(2026, 10, 13)], tmp_path / "a", SEED, AS_OF)
    b = inv.write_day(again, tmp_path / "b", SEED, AS_OF)
    for f in sorted(p.name for p in a.iterdir()):
        assert (a / f).read_bytes() == (b / f).read_bytes(), f
    pd.testing.assert_frame_equal(later_first.movements, drops[date(2026, 10, 20)].movements)


def test_sales_files_are_not_changed_by_the_inventory_generator(tmp_path):
    d = date(2026, 10, 13)
    before = s.write_day(s.Simulator(SEED, AS_OF).build_day(d), tmp_path / "before", SEED, AS_OF)
    other = inv.InventorySimulator(SEED, AS_OF)
    other.run_until(other.window_start + timedelta(days=40))  # shared state would be changed from day one
    after = s.write_day(s.Simulator(SEED, AS_OF).build_day(d), tmp_path / "after", SEED, AS_OF)
    for f in sorted(p.name for p in before.iterdir()):
        assert (before / f).read_bytes() == (after / f).read_bytes(), f
    tables, _ = m.build_masters(SEED, AS_OF)
    assert len(tables["customers"]) == 612 and len(tables["products"]) == 40  # sales masters as before


def test_history_dates_are_rejected(sim):
    with pytest.raises(ValueError):
        sim.build_day(AS_OF - timedelta(days=1))


# --- the stock adds up ----------------------------------------------------------


def test_stock_never_negative(clean):
    _, st = clean
    assert (st["u"] >= 0).all()
    assert (st["t"] >= 0).all()


def test_stock_rolls_forward_from_the_movements(clean):
    """Closing stock of every position on every day = all its movements up to that day (opening 561 included)."""
    mv, st = clean
    daily = mv.groupby(["location_id", "product_id", "posting_date"])["q"].sum().rename("moved").reset_index()
    daily = daily.sort_values("posting_date")
    daily["running"] = daily.groupby(["location_id", "product_id"])["moved"].cumsum()
    merged = pd.merge_asof(
        st.assign(day=pd.to_datetime(st["snapshot_date"])).sort_values("day"),
        daily.assign(day=pd.to_datetime(daily["posting_date"]))[["day", "location_id", "product_id", "running"]],
        on="day",
        by=["location_id", "product_id"],
    )
    merged["running"] = merged["running"].fillna(0.0)
    bad = merged[(merged["running"] - merged["u"]).abs() > 1e-9]
    assert bad.empty, bad.head()


def test_in_transit_equals_open_transfers(clean):
    mv, st = clean
    out = mv[mv["movement_type"] == "313"].assign(loc=lambda x: x["counterpart_location_id"], q=lambda x: -x["q"])
    into = mv[mv["movement_type"] == "315"].assign(loc=lambda x: x["location_id"], q=lambda x: -x["q"])
    flows = pd.concat([out, into])[["loc", "product_id", "posting_date", "q"]]
    day = date(2026, 10, 13).isoformat()
    open_now = flows[flows["posting_date"] <= day].groupby(["loc", "product_id"])["q"].sum()
    snap = st[st["snapshot_date"] == day].set_index(["location_id", "product_id"])["t"]
    for (loc, pid), qty in open_now.items():
        assert snap.get((loc, pid), 0.0) == pytest.approx(qty), (loc, pid)
    assert snap.sum() == pytest.approx(open_now.sum())


def test_opening_balances_only_on_the_first_day(clean, sim):
    mv, _ = clean
    assert set(mv.loc[mv["movement_type"] == "561", "posting_date"]) == {sim.window_start.isoformat()}


# --- consistency with sales -----------------------------------------------------


def test_goods_movements_match_every_sales_line_in_the_history(history, sales_history, sim):
    """Issues, cancellation reversals and returns = invoiced quantities, per day, product and sales office."""
    mv, _ = history
    goods = mv[mv["movement_type"].isin(SALES_ISSUE_TYPES)]
    moved = (
        goods.assign(q=-goods["quantity"].astype(float))
        .groupby(["posting_date", "product_id", "sales_office"])["q"]
        .sum()
    )
    stocked = [pid for pid, home in sim.home.items() if home]
    sales = sales_history[
        sales_history["invoice_type"].isin(["ZAOR", "ZFOC", "ZARE", "S1"]) & sales_history["product_id"].isin(stocked)
    ]
    invoiced = (
        sales.assign(q=sales["quantity"].astype(float))
        .groupby(["invoice_date", "product_id", "sales_office"])["q"]
        .sum()
    )
    invoiced.index.names = moved.index.names
    pd.testing.assert_series_equal(moved.sort_index(), invoiced.sort_index(), check_names=False)


def test_every_sales_movement_points_to_its_invoice_line(history, sales_history):
    mv, _ = history
    goods = mv[mv["movement_type"].isin(SALES_ISSUE_TYPES)]
    refs = set(goods["reference_invoice_number"] + "/" + goods["reference_invoice_item"])
    lines = set(sales_history["invoice_number"] + "/" + sales_history["invoice_item"])
    assert refs <= lines
    assert len(refs) == len(goods)  # one movement per invoice line


def test_services_hold_no_stock(clean, sim):
    mv, st = clean
    services = {pid for pid, home in sim.home.items() if home is None}
    assert services and not (set(mv["product_id"]) | set(st["product_id"])) & services


# --- planted stories ------------------------------------------------------------


def test_story_i1_western_depots_run_out_of_the_hero_product(clean, sim):
    mv, st = clean
    hero, start = stories.HERO_PRODUCT_ID, stories.STOCKOUT_FROM
    assert sim.story_depots == sorted(
        sim.depot_of_office[o] for o, _, _, p in m.SALES_OFFICES if p == stories.STOCKOUT_PROVINCE
    )
    for depot in sim.story_depots:
        assert _stock(st, hero, depot, start - timedelta(days=2)) > 0  # still selling two days before
        for i in range(-1, 6):  # empty from the evening before the stock-out to 28 Oct
            assert _stock(st, hero, depot, start + timedelta(days=i)) == 0, (depot, i)
    assert _stock(st, hero, inv.HUB, start) > 1000  # the hub (Galle plant) still holds stock
    later = mv[(mv["product_id"] == hero) & mv["location_id"].isin(sim.story_depots) & (mv["movement_type"] == "315")]
    assert later["posting_date"].max() == sim.last_transfer_day.isoformat()  # the last transfer; the next is held back


def test_story_i2_substitute_leaves_the_western_depots_faster(clean, sim):
    mv, _ = clean
    sub = mv[(mv["product_id"] == e.STOCKOUT_SUBSTITUTE_ID) & mv["location_id"].isin(sim.story_depots)]
    sub = sub[sub["movement_type"] == "601"]
    before = -sub[sub["posting_date"].between("2026-10-09", "2026-10-22")]["q"].sum() / 14
    after = -sub[sub["posting_date"] >= stories.STOCKOUT_FROM.isoformat()]["q"].sum() / 6
    assert after > before * 1.3


def test_story_i3_overstock_at_the_kandy_plant(clean):
    mv, st = clean
    pid = stories.OVERSTOCK_PRODUCT_ID
    issued = mv[(mv["product_id"] == pid) & (mv["movement_type"] == "601") & (mv["posting_date"] >= "2026-07-28")]
    per_day = -issued["q"].sum() / 90
    held = _stock(st, pid, inv.BUILDING_STORE, date(2026, 10, 26))
    assert held / per_day > 120  # days of cover
    assert _stock(st, pid, inv.BUILDING_STORE, stories.OVERSTOCK_RECEIPT_DATE) > 5 * _stock(
        st, pid, inv.BUILDING_STORE, stories.OVERSTOCK_RECEIPT_DATE - timedelta(days=1)
    )


def test_story_i4_returns_pile_up_fastest_for_the_spike_product(clean):
    _, st = clean
    growth = {
        pid: _stock(st, pid, inv.RETURNS_STORE, date(2026, 9, 30))
        / max(1.0, _stock(st, pid, inv.RETURNS_STORE, date(2026, 6, 30)))
        for pid in (stories.RETURNS_SPIKE_PRODUCT_ID, "HGL1LTR", stories.HERO_PRODUCT_ID)
    }
    assert growth[stories.RETURNS_SPIKE_PRODUCT_ID] > 3
    assert growth[stories.RETURNS_SPIKE_PRODUCT_ID] > 2 * max(growth["HGL1LTR"], growth[stories.HERO_PRODUCT_ID])


# --- daily files: dirt and control totals ---------------------------------------


@pytest.mark.parametrize(
    "kind",
    [
        "late_movement",
        "unknown_location",
        "blank_movement_type",
        "unit_text_variant",
        "duplicate_line",
        "negative_stock",
    ],
)
def test_every_dirt_type_is_planted_and_logged(drops, kind):
    found = [(d, k) for d, drop in drops.items() for k in _dirt(drop, kind)["keys"]]
    assert found, kind
    for d, key in found:
        drop = drops[d]
        if kind == "negative_stock":
            st = drop.stock
            assert key in set(st["snapshot_date"] + "/" + st["location_id"] + "/" + st["product_id"])
        else:
            mv = drop.movements
            assert key in set(mv["material_document"] + "/" + mv["material_document_item"])


def test_dirt_does_what_the_log_says(drops):
    for d, drop in drops.items():
        mv, st = drop.movements, drop.stock
        keys = mv["material_document"] + "/" + mv["material_document_item"]
        for k in _dirt(drop, "unknown_location")["keys"]:
            assert (mv.loc[keys == k, "location_id"] == inv.UNKNOWN_LOCATION_ID).all()
        for k in _dirt(drop, "blank_movement_type")["keys"]:
            assert (mv.loc[keys == k, "movement_type"] == "").all()
        for k in _dirt(drop, "duplicate_line")["keys"]:
            assert (keys == k).sum() == 2
        for k in _dirt(drop, "late_movement")["keys"]:
            assert (mv.loc[keys == k, "posting_date"] < d.isoformat()).all()
        skeys = st["snapshot_date"] + "/" + st["location_id"] + "/" + st["product_id"]
        for k in _dirt(drop, "negative_stock")["keys"]:
            assert (st.loc[skeys == k, "unrestricted_qty"].astype(float) < 0).all()


def test_control_totals_are_the_true_totals(drops):
    for d, drop in drops.items():
        n_invalid = _dirt(drop, "unknown_location")["count"] + _dirt(drop, "blank_movement_type")["count"]
        n_copies = _dirt(drop, "duplicate_line")["count"]
        assert drop.control_totals["movements"]["lines"] == len(drop.movements) - n_invalid - n_copies, d
        assert drop.control_totals["stock"]["rows"] == len(drop.stock) - _dirt(drop, "negative_stock")["count"], d
        assert drop.control_totals["stock"]["by_snapshot_date"].keys() == {d.isoformat()}


def test_every_movement_is_delivered_exactly_once(sim, drops):
    """Late lines move to a later delivery but never disappear or arrive twice (copies aside)."""
    delivered = pd.concat([drop.movements.drop_duplicates() for drop in drops.values()])
    delivered = delivered[delivered["posting_date"] <= "2026-10-26"]  # later lines may still be on their way
    expected = [r for d in DAYS if d <= date(2026, 10, 26) for r in sim.movements[d]]
    assert len(delivered) == len(expected)


def test_daily_files_and_manifest(drops, tmp_path):
    d = date(2026, 10, 13)
    target = inv.write_day(drops[d], tmp_path, SEED, AS_OF)
    manifest = json.loads((target / "manifest_20261013.json").read_text())
    assert set(manifest["files"]) == {"movements_20261013.csv", "stock_20261013.csv"}
    for name, meta in manifest["files"].items():
        assert meta["sha256"] == hashlib.sha256((target / name).read_bytes()).hexdigest()
        assert meta["rows"] == len(pd.read_csv(target / name, dtype=str))
    assert manifest["area"] == "inventory" and manifest["dirt"] == drops[d].dirt


def test_history_files_are_clean_and_add_up(history, tmp_path):
    mv, st = history
    target = inv.write_history(mv, st, tmp_path, SEED, AS_OF)
    manifest = json.loads((target / "history_manifest.json").read_text())
    assert manifest["dirt"] == []
    assert sum(f["rows"] for n, f in manifest["files"].items() if n.startswith("movements_")) == len(mv)
    assert sum(f["rows"] for n, f in manifest["files"].items() if n.startswith("stock_")) == len(st)
    assert manifest["control_totals"]["movements"]["lines"] == len(mv)
    assert manifest["date_to"] == (AS_OF - timedelta(days=1)).isoformat()
    assert len([n for n in manifest["files"] if n.startswith("stock_")]) == 18  # Apr 2025 - Sep 2026
