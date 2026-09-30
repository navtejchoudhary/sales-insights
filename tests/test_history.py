"""Tests for the invoice history generator (plain Python, no Spark needed).

The full 18-month build takes ~20 seconds, so it is built once per test run.
"""

import json
from datetime import date

import pandas as pd
import pytest

from sales_insights.generator import history as h
from sales_insights.generator import masters as m
from sales_insights.generator import stories

AS_OF = date(2026, 10, 1)
SEED = 20261001


@pytest.fixture(scope="module")
def df():
    return h.build_history(SEED, AS_OF)


@pytest.fixture(scope="module")
def x(df):
    regions, _ = m.build_masters(SEED, AS_OF, dirt=False)
    province = dict(regions["regions"][["region", "province"]].values)
    return df.assign(
        rev=df["revenue"].astype(float),
        qty=df["quantity"].astype(float),
        date=pd.to_datetime(df["invoice_date"]),
        province=df["region"].map(province),
    )


@pytest.fixture(scope="module")
def clean_masters():
    tables, _ = m.build_masters(SEED, AS_OF, dirt=False)
    return tables


# --- shape and keys -----------------------------------------------------------


def test_columns_match_reference_plus_additions(df):
    assert list(df.columns) == h.REFERENCE_COLUMNS + h.ADDED_COLUMNS
    assert len(h.REFERENCE_COLUMNS) == 74


def test_date_window(df):
    start, end = h.history_window(AS_OF)
    assert df["invoice_date"].min() == start.isoformat()
    assert df["invoice_date"].max() == end.isoformat()


def test_line_key_unique(df):
    assert not df.duplicated(["invoice_number", "invoice_item"]).any()


def test_volume_is_realistic(df):
    days = df["invoice_date"].nunique()
    assert 35_000 < len(df) < 60_000
    assert days >= 540


def test_all_codes_resolve_to_masters(df, clean_masters):
    assert set(df["customer_id"]) <= set(clean_masters["customers"]["customer_id"])
    assert set(df["product_id"]) <= set(clean_masters["products"]["product_id"])
    assert set(df["sales_office"]) <= set(clean_masters["sales_offices"]["sales_office"])
    assert df["customer_id"].str.fullmatch(r"\d{10}").all()


def test_no_sale_before_launch_or_customer_creation(x, clean_masters):
    launch = clean_masters["products"].set_index("product_id")["launch_date"]
    created = clean_masters["customers"].set_index("customer_id")["created_date"]
    assert (x["invoice_date"] >= x["product_id"].map(launch)).all()
    assert (x["invoice_date"] >= x["customer_id"].map(created)).all()


# --- document types and signs (copied from the reference) ---------------------


def test_document_type_signs(x):
    sales = x[x["invoice_type"] == "ZAOR"]
    assert (sales["qty"] > 0).all() and (sales["rev"] > 0).all()
    returns = x[x["invoice_type"] == "ZARE"]
    assert (returns["qty"] < 0).all() and (returns["rev"] < 0).all()
    assert (returns["sales_amount"] == "0.00").all()
    assert (returns["credit_amount"].astype(float) == -returns["rev"]).all()
    cancels = x[x["invoice_type"] == "S1"]
    assert (cancels["qty"] < 0).all() and (cancels["net_sales"] == "0.00").all()
    corrections = x[x["invoice_type"] == "ZACR"]
    assert len(corrections) > 0
    assert (corrections["qty"] == 0).all() and (corrections["rev"] < 0).all()
    foc = x[x["invoice_type"] == "ZFOC"]
    assert (foc["rev"] == -foc["tax_amount"].astype(float)).all()


def test_returns_and_cancellations_point_to_earlier_invoices(df):
    sales = df[df["invoice_type"] == "ZAOR"].drop_duplicates("invoice_number").set_index("invoice_number")
    for doc in ("ZARE", "S1", "ZACR"):
        rev = df[df["invoice_type"] == doc]
        assert len(rev) > 0
        assert rev["reference_invoice_number"].isin(sales.index).all()
        orig = sales.loc[rev["reference_invoice_number"]]
        assert (rev["invoice_date"].to_numpy() >= orig["invoice_date"].to_numpy()).all()
        assert (rev["customer_id"].to_numpy() == orig["customer_id"].to_numpy()).all()


def test_cancelled_invoices_marked(df):
    cancelled = set(df.loc[df["invoice_type"] == "S1", "reference_invoice_number"])
    originals = df[df["invoice_number"].isin(cancelled)]
    assert (originals["billing_status"] == "C").all()


def test_cancellation_nets_to_zero(x):
    cancels = x[x["invoice_type"] == "S1"]
    for ref, lines in list(cancels.groupby("reference_invoice_number"))[:25]:
        orig = x[x["invoice_number"] == ref]
        assert abs(orig["rev"].sum() + lines["rev"].sum()) < 0.05


def test_fiscal_calendar(df):
    sample = df.drop_duplicates("invoice_date").set_index("invoice_date")
    assert sample.loc["2025-04-01", ["fiscal_year", "fiscal_period", "fiscal_month"]].tolist() == ["2025", "1", "Apr"]
    assert sample.loc["2026-03-31", ["fiscal_year", "fiscal_period", "fiscal_month"]].tolist() == ["2025", "12", "Mar"]


def test_export_sales_are_zero_rated(df):
    export = df[(df["distribution_channel"] == "20") & (df["invoice_type"] == "ZAOR")]
    assert len(export) > 0 and (export["tax_amount"] == "0.00").all()


# --- stories ------------------------------------------------------------------


def test_story1_season_peaks(x):
    monthly = x.groupby(x["date"].dt.to_period("M"))["rev"].sum()
    maha = monthly[[p.month in stories.MAHA_PEAK_MONTHS for p in monthly.index]].mean()
    off = monthly[[p.month in (6, 7, 8) for p in monthly.index]].mean()
    assert maha > off * 1.2


def test_story2_few_products_dominate(x):
    rev = x[x["invoice_type"] == "ZAOR"].groupby("product_id")["rev"].sum().sort_values(ascending=False)
    assert rev.head(round(len(rev) * 0.2)).sum() / rev.sum() > 0.55
    top_units = x[x["invoice_type"] == "ZAOR"].groupby("product_id")["rev"].size().idxmax()
    assert top_units == stories.HERO_PRODUCT_ID


def test_story3_weak_province_is_weakest(x):
    sales = x[x["invoice_type"] == "ZAOR"]
    per_customer = sales.groupby("province")["rev"].sum() / sales.groupby("province")["customer_id"].nunique()
    assert per_customer.idxmin() == stories.WEAK_PROVINCE


def test_story4_price_rise(x):
    hero = x[(x["invoice_type"] == "ZAOR") & (x["product_group"] == stories.PRICE_RISE_PRODUCT_GROUP)]
    list_price = hero["condition_rate"].astype(float)
    cut = pd.Timestamp(stories.PRICE_RISE_FROM)
    for pid, g in hero.groupby("product_id"):
        before = list_price[g.index][g["date"] < cut]
        after = list_price[g.index][g["date"] >= cut]
        if len(before) and len(after):
            assert after.iloc[0] / before.iloc[0] == pytest.approx(1 + stories.PRICE_RISE_PCT / 100, rel=1e-3), pid


def test_story5_new_product_ramps_up(x):
    new = x[(x["invoice_type"] == "ZAOR") & (x["product_id"] == stories.NEW_PRODUCT_ID)]
    assert new["date"].min() >= pd.Timestamp(stories.NEW_PRODUCT_LAUNCH)
    lines = new.groupby(new["date"].dt.to_period("M")).size()
    assert len(lines) >= 3
    assert lines.iloc[-1] > lines.iloc[1] > lines.iloc[0]


def test_story6_returns_spike(x):
    pid, cut = stories.RETURNS_SPIKE_PRODUCT_ID, pd.Timestamp(stories.RETURNS_SPIKE_FROM)
    sold = x[(x["invoice_type"] == "ZAOR") & (x["product_id"] == pid)]
    ret = x[(x["invoice_type"] == "ZARE") & (x["product_id"] == pid)]
    before = len(ret[ret["date"] < cut]) / len(sold[sold["date"] < cut])
    after = len(ret[ret["date"] >= cut]) / len(sold[sold["date"] >= cut])
    assert after > 4 * before


def test_story7_hero_sells_in_stockout_province_before_stockout(x):
    hero = x[(x["product_id"] == stories.HERO_PRODUCT_ID) & (x["province"] == stories.STOCKOUT_PROVINCE)]
    assert len(hero) > 100  # so the live stockout will be a visible drop


# --- reproducibility and output -------------------------------------------------


def test_same_seed_same_output():
    start = date(2026, 8, 1)
    a = h.build_history(SEED, AS_OF, start=start)
    b = h.build_history(SEED, AS_OF, start=start)
    pd.testing.assert_frame_equal(a, b)


def test_written_files_and_manifest(tmp_path):
    part = h.build_history(SEED, AS_OF, start=date(2026, 8, 20))
    target = h.write_history(part, tmp_path, SEED, AS_OF)
    manifest = json.loads((target / "history_manifest.json").read_text())
    assert set(manifest["files"]) == {"invoices_202608.csv", "invoices_202609.csv"}
    back = pd.concat(pd.read_csv(target / f, dtype=str, keep_default_na=False) for f in sorted(manifest["files"]))
    assert len(back) == manifest["control_totals"]["lines"]
    assert back["revenue"].astype(float).sum() == pytest.approx(manifest["control_totals"]["revenue"], abs=0.01)
    day = back[back["invoice_date"] == "2026-09-15"]
    assert manifest["control_totals"]["by_invoice_date"]["2026-09-15"]["lines"] == len(day)
    assert back["customer_id"].str.fullmatch(r"\d{10}").all()  # leading zeros survive CSV
