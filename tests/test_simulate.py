"""Tests for the daily simulator (plain Python, no Spark needed)."""

import json
from datetime import date, timedelta

import pandas as pd
import pytest

from sales_insights.generator import engine as e
from sales_insights.generator import simulate as s
from sales_insights.generator import stories

AS_OF = date(2026, 10, 1)
SEED = 20261001
DAYS = [AS_OF + timedelta(days=i) for i in range(29)]  # 1-29 Oct: the whole live period


@pytest.fixture(scope="module")
def sim():
    return s.Simulator(SEED, AS_OF)


@pytest.fixture(scope="module")
def drops(sim):
    return {d: sim.build_day(d) for d in DAYS}


def _keys(df):
    return set(df["invoice_number"] + "/" + df["invoice_item"])


def _dirt(drop, kind, file="orders"):
    found = [x for x in drop.dirt if x["dirt"] == kind and x["file"] == file]
    return found[0] if found else {"count": 0, "keys": []}


def _all_dirt(drops, kind, file="orders"):
    return [(d, k) for d, drop in drops.items() for k in _dirt(drop, kind, file)["keys"]]


# --- basics -------------------------------------------------------------------


def test_history_dates_are_rejected(sim):
    with pytest.raises(ValueError):
        sim.build_day(AS_OF - timedelta(days=1))


def test_columns_and_schema_change(drops):
    for d, drop in drops.items():
        expected = e.COLUMNS + ([stories.NEW_COLUMN] if d >= stories.SCHEMA_CHANGE_FROM else [])
        assert list(drop.orders.columns) == expected, d
        assert list(drop.changes.columns) == expected, d
    late = drops[stories.SCHEMA_CHANGE_FROM].orders
    assert late[stories.NEW_COLUMN].str.fullmatch(r"SR\d{3}|").all()


def test_every_day_has_orders(drops):
    assert all(len(drop.orders) > 0 for drop in drops.values())


# --- reproducibility ------------------------------------------------------------


def test_any_day_rebuilt_alone_is_identical(sim, tmp_path):
    d = date(2026, 10, 12)
    fresh = s.Simulator(SEED, AS_OF).build_day(d)  # new simulator, no earlier days built
    pd.testing.assert_frame_equal(fresh.orders, sim.build_day(d).orders)
    pd.testing.assert_frame_equal(fresh.changes, sim.build_day(d).changes)
    a = s.write_day(fresh, tmp_path / "a", SEED, AS_OF)
    b = s.write_day(sim.build_day(d), tmp_path / "b", SEED, AS_OF)
    for f in a.iterdir():
        assert f.read_bytes() == (b / f.name).read_bytes()


# --- completeness: nothing lost, nothing delivered twice ---------------------------


def test_every_invoice_arrives_exactly_once(sim, drops):
    raised = set()
    for d in DAYS[:21]:  # invoices raised 1-21 Oct have all arrived by 29 Oct (max delay 7)
        for lines in sim.invoices_on(d).values():
            raised |= {f"{x.line['invoice_number']}/{x.line['invoice_item']}" for x in lines}
    seen: dict[str, int] = {}
    for drop in drops.values():
        for k in _keys(drop.orders.drop_duplicates(["invoice_number", "invoice_item"])):
            seen[k] = seen.get(k, 0) + 1
    assert raised <= set(seen)
    assert max(seen.values()) == 1


def test_late_arrivals_within_seven_days(sim, drops):
    late = []
    for d, drop in drops.items():
        for inv_date in drop.control_totals["orders"]["by_invoice_date"]:
            if inv_date < d.isoformat():
                late.append((d, inv_date))
                assert (d - date.fromisoformat(inv_date)).days <= s.MAX_LATE_DAYS
                assert inv_date >= AS_OF.isoformat()  # history invoices never arrive late
    assert late, "no late arrivals in the live period"


# --- changes -----------------------------------------------------------------------


def test_changes_are_dated_on_their_business_day(drops):
    for d, drop in drops.items():
        assert (drop.changes["invoice_date"] == d.isoformat()).all()
        assert set(drop.changes["invoice_type"]) <= {"ZARE", "S1", "ZACR"}


def test_changes_reference_real_invoices_including_history(sim, drops):
    from_history = 0
    for d, drop in drops.items():
        known = set()
        for back in range(s.LOOKBACK_DAYS + 1):
            known |= set(sim.invoices_on(d - timedelta(days=back)))
        refs = set(drop.changes["reference_invoice_number"])
        assert refs <= known, d
        from_history += sum(1 for r in refs if r[1:7] < AS_OF.strftime("%y%m%d"))
    assert from_history > 0, "no October change reverses a September (history) invoice"


def test_change_types_all_occur(drops):
    types = set().union(*(set(drop.changes["invoice_type"]) for drop in drops.values()))
    assert types == {"ZARE", "S1", "ZACR"}


# --- dirt -------------------------------------------------------------------------------


def test_duplicates_present_and_logged(drops):
    found = _all_dirt(drops, "duplicate_line")
    assert found
    for d, key in found:
        df = drops[d].orders
        assert ((df["invoice_number"] + "/" + df["invoice_item"]) == key).sum() == 2


def test_invalid_dates_unparseable(drops):
    found = _all_dirt(drops, "invalid_date")
    assert found
    for d, key in found:
        df = drops[d].orders
        row = df[(df["invoice_number"] + "/" + df["invoice_item"]) == key].iloc[0]
        assert pd.to_datetime(row["invoice_date"], format="%Y-%m-%d", errors="coerce") is pd.NaT


def test_negative_quantity_on_invoice(drops):
    found = _all_dirt(drops, "negative_quantity_on_invoice")
    assert found
    for d, key in found:
        df = drops[d].orders
        row = df[(df["invoice_number"] + "/" + df["invoice_item"]) == key].iloc[0]
        assert row["invoice_type"] == "ZAOR" and float(row["quantity"]) < 0


def test_blank_fields_and_city_variants(drops):
    blanks = [x for drop in drops.values() for x in drop.dirt if x["dirt"].startswith("blank_")]
    assert {x["dirt"] for x in blanks} >= {"blank_plant", "blank_sales_office", "blank_customer_group"}
    assert _all_dirt(drops, "city_text_variant")


# --- control totals -------------------------------------------------------------------


def test_control_totals_are_true_business_totals(drops):
    for d, drop in drops.items():
        invalid = set(_dirt(drop, "invalid_date")["keys"]) | set(_dirt(drop, "negative_quantity_on_invoice")["keys"])
        clean = drop.orders.drop_duplicates(["invoice_number", "invoice_item"])
        clean = clean[~(clean["invoice_number"] + "/" + clean["invoice_item"]).isin(invalid)]
        expected = drop.control_totals["orders"]
        assert len(clean) == expected["lines"], d
        assert clean["revenue"].astype(float).sum() == pytest.approx(expected["revenue"], abs=0.01), d


# --- story 7: live stockout ---------------------------------------------------------------


def test_stockout_in_western_from_story_date(sim):
    western = set(sim.mi.customers.loc[sim.mi.customers["province"] == stories.STOCKOUT_PROVINCE, "customer_id"])

    def hero_lines(days, pid=stories.HERO_PRODUCT_ID):
        return sum(
            1
            for d in days
            for lines in sim.invoices_on(d).values()
            for x in lines
            if x.pid == pid and x.line["customer_id"] in western
        )

    before = [d for d in DAYS if d < stories.STOCKOUT_FROM]
    after = [d for d in DAYS if d >= stories.STOCKOUT_FROM]
    assert hero_lines(before) > 10
    assert hero_lines(after) == 0
    assert hero_lines(after, e.STOCKOUT_SUBSTITUTE_ID) > 0  # some buyers switch pack size


# --- files ----------------------------------------------------------------------------------


def test_written_day(tmp_path, sim):
    d = stories.SCHEMA_CHANGE_FROM
    target = s.write_day(sim.build_day(d), tmp_path, SEED, AS_OF)
    stamp = d.strftime("%Y%m%d")
    assert target.name == f"business_date={d.isoformat()}"
    manifest = json.loads((target / f"manifest_{stamp}.json").read_text())
    for name, info in manifest["files"].items():
        df = pd.read_csv(target / name, dtype=str, keep_default_na=False)
        assert len(df) == info["rows"]
        assert df.shape[1] == info["columns"]
        assert df["customer_id"].str.fullmatch(r"\d{10}").all()
    assert stories.NEW_COLUMN in manifest["columns_added_to_reference"]
    manifest_time = (target / f"manifest_{stamp}.json").stat().st_mtime_ns
    assert all((target / n).stat().st_mtime_ns <= manifest_time for n in manifest["files"])
