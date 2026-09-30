"""Tests for the master data generator (plain Python, no Spark needed)."""

import json
from datetime import date

import pandas as pd
import pytest

from sales_insights.generator import masters as m
from sales_insights.generator import stories

AS_OF = date(2026, 10, 1)
SEED = 20261001


@pytest.fixture(scope="module")
def clean():
    tables, log = m.build_masters(SEED, AS_OF, dirt=False)
    assert log == []
    return tables


@pytest.fixture(scope="module")
def dirty():
    return m.build_masters(SEED, AS_OF, dirt=True)


# --- clean data -------------------------------------------------------------


def test_row_counts(clean):
    assert len(clean["regions"]) == 25
    assert len(clean["customers"]) == 600
    assert len(clean["products"]) == len(m.PRODUCTS)
    assert len(clean["sales_offices"]) == 11
    assert len(clean["sales_reps"]) == 36


@pytest.mark.parametrize(
    "table,key",
    [
        ("regions", ["region"]),
        ("cities", ["city"]),
        ("customers", ["customer_id"]),
        ("products", ["product_id"]),
        ("sales_offices", ["sales_office"]),
        ("sales_reps", ["sales_rep_id"]),
        ("storage_locations", ["plant", "storage_location"]),
    ],
)
def test_clean_keys_unique(clean, table, key):
    df = clean[table]
    assert df[key].notna().all().all()
    assert not df.duplicated(key).any()


def test_customer_ids_are_10_digit_sap_numbers(clean):
    ids = clean["customers"]["customer_id"]
    assert ids.str.fullmatch(r"\d{10}").all()


def test_every_customer_code_resolves(clean):
    c = clean["customers"]
    assert set(c["region"]) <= set(clean["regions"]["region"])
    assert set(c["city"]) <= set(clean["cities"]["city"])
    assert set(c["sales_office"]) <= set(clean["sales_offices"]["sales_office"])
    assert set(c["distribution_channel"]) <= set(clean["distribution_channels"]["distribution_channel"])
    assert set(c["customer_group"]) <= set(clean["customer_groups"]["customer_group"])
    assert set(c["payer_customer_id"]) <= set(c["customer_id"])


def test_city_belongs_to_customer_region(clean):
    merged = clean["customers"].merge(clean["cities"], on="city", suffixes=("", "_city"))
    assert len(merged) == len(clean["customers"])
    assert (merged["region"] == merged["region_city"]).all()


def test_all_provinces_have_customers(clean):
    prov = clean["customers"].merge(clean["regions"], on="region")["province"]
    assert prov.nunique() == 9


def test_every_product_code_resolves(clean):
    p = clean["products"]
    assert set(p["product_group"]) <= set(clean["product_groups"]["product_group"])
    assert set(p["product_type"]) <= set(clean["product_types"]["product_type"])
    assert set(p["division"]) <= set(clean["divisions"]["division"])
    assert set(p["plant"]) <= set(clean["plants"]["plant"])


def test_cost_below_price(clean):
    p = clean["products"]
    assert (p["standard_cost"].astype(float) < p["list_price"].astype(float)).all()


def test_story_settings_point_at_real_data(clean):
    p = clean["products"].set_index("product_id")
    assert p.loc[stories.HERO_PRODUCT_ID, "product_group"] == stories.PRICE_RISE_PRODUCT_GROUP
    assert p.loc[stories.NEW_PRODUCT_ID, "launch_date"] == stories.NEW_PRODUCT_LAUNCH.isoformat()
    assert stories.RETURNS_SPIKE_PRODUCT_ID in p.index
    provinces = set(clean["regions"]["province"])
    assert {stories.WEAK_PROVINCE, stories.STOCKOUT_PROVINCE} <= provinces


def test_supermarket_branches_share_payer(clean):
    mt = clean["customers"][clean["customers"]["distribution_channel"] == "30"]
    assert len(mt) > 0
    assert mt["payer_customer_id"].nunique() <= len(m.CHAIN_NAMES)


# --- dirt ---------------------------------------------------------------------


def _log(dirty_log, table, kind):
    entries = [d for d in dirty_log if d["table"] == table and d["dirt"] == kind]
    assert len(entries) == 1, f"{table}.{kind} not logged"
    assert entries[0]["count"] > 0
    return entries[0]


def test_dirt_city_variants(clean, dirty):
    tables, log = dirty
    entry = _log(log, "customers", "city_text_variant")
    canonical = set(clean["cities"]["city"])
    c = tables["customers"]
    for cid in entry["customer_ids"]:
        rows = c[c["customer_id"].str.zfill(10) == cid]
        assert (~rows["city"].isin(canonical)).any(), f"{cid} city not dirtied"


def test_dirt_blank_region_and_group(dirty):
    tables, log = dirty
    c = tables["customers"]
    assert (c["region"] == "").sum() >= _log(log, "customers", "blank_region")["count"]
    assert (c["customer_group"] == "").sum() >= _log(log, "customers", "blank_customer_group")["count"]


def test_dirt_duplicates_older_version(dirty):
    tables, log = dirty
    entry = _log(log, "customers", "duplicate_older_version")
    c = tables["customers"]
    assert len(c) == 600 + entry["count"]
    for cid in entry["customer_ids"]:
        versions = c[c["customer_id"] == cid].sort_values("last_updated_timestamp")
        assert len(versions) == 2
        assert versions.iloc[0]["last_updated_timestamp"] < versions.iloc[1]["last_updated_timestamp"]


def test_dirt_ids_without_leading_zeros(dirty):
    tables, log = dirty
    entry = _log(log, "customers", "id_without_leading_zeros")
    ids = set(tables["customers"]["customer_id"])
    for cid in entry["customer_ids"]:
        assert cid.lstrip("0") in ids and cid not in ids


def test_dirt_products(dirty):
    tables, log = dirty
    p = tables["products"]
    assert (p["product_group"] == "").sum() == _log(log, "products", "blank_product_group")["count"]
    _log(log, "products", "description_case_variant")
    story_ids = {stories.HERO_PRODUCT_ID, stories.NEW_PRODUCT_ID, stories.RETURNS_SPIKE_PRODUCT_ID}
    assert (p[p["product_id"].isin(story_ids)]["product_group"] != "").all()


def test_dirt_does_not_change_clean_values(clean, dirty):
    """Dirt uses its own random stream, so untouched rows equal the clean rows."""
    c_clean = clean["products"].set_index("product_id")
    c_dirty = dirty[0]["products"].set_index("product_id")
    assert (c_clean["list_price"] == c_dirty.loc[c_clean.index, "list_price"]).all()


# --- reproducibility and output -----------------------------------------------


def test_same_seed_same_output():
    a, la = m.build_masters(SEED, AS_OF)
    b, lb = m.build_masters(SEED, AS_OF)
    assert la == lb
    for name in a:
        pd.testing.assert_frame_equal(a[name], b[name])


def test_written_files_identical_on_rerun(tmp_path):
    for run in ("a", "b"):
        tables, log = m.build_masters(SEED, AS_OF)
        m.write_masters(tables, log, tmp_path / run, SEED, AS_OF)
    for f in (tmp_path / "a" / "masters").iterdir():
        assert f.read_bytes() == (tmp_path / "b" / "masters" / f.name).read_bytes()


def test_manifest_matches_files(tmp_path):
    tables, log = m.build_masters(SEED, AS_OF)
    target = m.write_masters(tables, log, tmp_path, SEED, AS_OF)
    manifest = json.loads((target / "masters_manifest.json").read_text())
    assert manifest["dirt"] == log
    for fname, info in manifest["files"].items():
        df = pd.read_csv(target / fname, dtype=str, keep_default_na=False)
        assert len(df) == info["rows"]


def test_leading_zeros_survive_csv(tmp_path):
    tables, log = m.build_masters(SEED, AS_OF, dirt=False)
    target = m.write_masters(tables, log, tmp_path, SEED, AS_OF)
    c = pd.read_csv(target / "customers.csv", dtype=str, keep_default_na=False)
    assert c["customer_id"].str.fullmatch(r"\d{10}").all()
    r = pd.read_csv(target / "regions.csv", dtype=str)
    assert "05" in set(r["region"])
