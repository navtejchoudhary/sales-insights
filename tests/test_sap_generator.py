"""Tests for the SAP-shaped dummy data (plain Python, no Spark needed).

The full build takes ~40 seconds, so it is built once per test run. Every planted real-world case
must appear in the data exactly as the manifest logs it (docs/sap_input_contract.md).
"""

from collections import Counter
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from sales_insights.generator import sap

AS_OF = date(2026, 10, 1)
SEED = 20261001
REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def built():
    return sap.build_sap(SEED, AS_OF)


@pytest.fixture(scope="module")
def b(built):
    return built[0]["billing_items"]


@pytest.fixture(scope="module")
def log(built):
    return built[1]


def _logged(log, case):
    return [x for x in log if x["case"] == case]


def test_billing_items_have_exactly_the_contract_columns(b):
    assert list(b.columns) == sap.BILLING_COLUMNS
    assert len(sap.BILLING_COLUMNS) == 41  # 39 SAP fields + _fivetran_synced + _fivetran_deleted
    contract = (REPO / "docs" / "sap_input_contract.md").read_text(encoding="utf-8")
    missing = [c for c in sap.BILLING_COLUMNS if f"`{c}`" not in contract]
    assert missing == []


def test_amounts_and_quantities_are_positive_numbers_like_sap_stores_them(b):
    for col in ("netamount", "grossamount", "taxamount", "costamount", "billingquantity"):
        values = b[col].astype(float)
        assert (values >= 0).all(), col


def test_only_known_document_types_and_categories(b):
    types = {t: c for t, c, _, _ in sap.DOCUMENT_TYPES}
    assert set(b["billingdocumenttype"]) <= set(types)
    assert (b["sddocumentcategory"] == b["billingdocumenttype"].map(types)).all()
    assert set(b["sddocumentcategory"]) == {"M", "N", "O", "P", "S", "U"}


def test_every_planted_case_is_present(log):
    cases = Counter(x["case"] for x in log)
    expected = {
        "pro_forma",
        "down_payment",
        "credit_memo_cancellation",
        "debit_memo",
        "usd_invoice",
        "older_version",
        "deleted_in_source",
        "missing_billing_date",
        "impossible_billing_date",
        "customer_missing_in_master",
    }
    assert set(cases) == expected
    assert all(n > 0 for n in cases.values())


def test_pro_formas_are_category_u_copies_of_an_invoice(b, log):
    docs = {x["billingdocument"] for x in _logged(log, "pro_forma")}
    pf = b[b["billingdocument"].isin(docs)]
    assert set(pf["billingdocumenttype"]) == {"F5"} and set(pf["sddocumentcategory"]) == {"U"}
    assert pf["referencesddocument"].isin(set(b["billingdocument"])).all()


def test_down_payments_are_faz_fas_pairs(b, log):
    docs = {x["billingdocument"] for x in _logged(log, "down_payment")}
    assert set(b[b["billingdocument"].isin(docs)]["billingdocumenttype"]) == {"FAZ", "FAS"}


def test_credit_memo_cancellations_point_at_an_existing_credit_memo(b, log):
    docs = {x["billingdocument"] for x in _logged(log, "credit_memo_cancellation")}
    s2 = b[b["billingdocument"].isin(docs)]
    assert set(s2["sddocumentcategory"]) == {"S"}
    credit_memos = set(b[b["sddocumentcategory"] == "O"]["billingdocument"])
    assert s2["cancelledbillingdocument"].isin(credit_memos).all()


def test_debit_memos_are_category_p(b, log):
    docs = {x["billingdocument"] for x in _logged(log, "debit_memo")}
    assert set(b[b["billingdocument"].isin(docs)]["sddocumentcategory"]) == {"P"}


def test_older_versions_share_the_key_with_an_older_sync_time(b, log):
    key = ["billingdocument", "billingdocumentitem"]
    dup = b[b.duplicated(key, keep=False)]
    assert len(dup) == 2 * len(_logged(log, "older_version"))
    for _, g in dup.groupby(key):
        assert g["_fivetran_synced"].nunique() == 2


def test_deleted_rows_are_flagged_not_removed(b, log):
    assert (b["_fivetran_deleted"] == "true").sum() == len(_logged(log, "deleted_in_source"))


def test_missing_and_impossible_dates(b, log):
    assert (b["billingdocumentdate"] == "").sum() == len(_logged(log, "missing_billing_date"))
    years = b["billingdocumentdate"].str[:4]
    future = b[(years != "") & (years > "2030")]
    assert len(future) == len(_logged(log, "impossible_billing_date"))
    assert set(future["billingdocumentdate"].str[:4].astype(int)) <= set(sap.IMPOSSIBLE_YEARS)


def test_usd_invoices_are_rare_and_carry_an_exchange_rate(b, log):
    usd = b[b["transactioncurrency"] == "USD"]
    assert 0 < len(usd) < 0.01 * len(b)
    assert (usd["accountingexchangerate"].astype(float) > 1).all()
    assert set(b[b["transactioncurrency"] == "LKR"]["accountingexchangerate"]) == {"1.00000"}
    invoices = {x["billingdocument"] for x in _logged(log, "usd_invoice")}
    # every USD document is a logged export invoice or a document derived from one (pro-forma, debit memo...)
    derived = usd[~usd["billingdocument"].isin(invoices)]
    assert invoices <= set(usd["billingdocument"])
    assert derived["referencesddocument"].isin(invoices).all()


def test_free_of_charge_lines_have_no_net_value(b):
    foc = b[b["billingdocumenttype"] == "ZFOC"]
    assert len(foc) > 0 and (foc["netamount"].astype(float) == 0).all()


def test_older_documents_keep_fiscal_year_0000(b):
    assert (b["fiscalyear"] == "0000").any() and (b["fiscalyear"] != "0000").any()


def test_a_second_company_is_present_for_the_scope_filter(b):
    assert {"2010", "4010"} <= set(b["companycode"])


def test_one_billed_customer_is_missing_from_the_customer_master(built, b, log):
    customers = built[0]["customer_master"]
    orphan = _logged(log, "customer_missing_in_master")[0]["customer"]
    assert orphan in set(b["soldtoparty"]) and orphan not in set(customers["customer"])


def test_customer_master_has_obviously_fake_personal_columns(built):
    customers = built[0]["customer_master"]
    assert list(customers.columns) == sap.CUSTOMER_COLUMNS
    assert set(sap.PERSONAL_CUSTOMER_COLUMNS) <= set(customers.columns)
    assert customers["emailaddress"].str.endswith("@example.com").all()
    assert customers["telephonenumber1"].str.startswith("000-").all()


def test_text_tables_have_english_and_german_rows(built):
    for name, (code_col, _) in sap.TEXT_TABLES.items():
        t = built[0][f"text_{name}"]
        assert set(t["language"]) == {"E", "D"}, name
        assert not t.duplicated(["language", code_col]).any(), name


def test_source_counts_add_up_to_the_billing_items(built, b):
    counts = built[0]["source_counts"]
    assert counts["line_count"].sum() == len(b)
    assert round(counts["netamount_sum"].astype(float).sum(), 2) == round(b["netamount"].astype(float).sum(), 2)


def test_same_seed_gives_identical_data():
    one, _ = sap.build_sap(SEED, AS_OF, start=date(2026, 9, 1))
    two, _ = sap.build_sap(SEED, AS_OF, start=date(2026, 9, 1))
    for name in one:
        pd.testing.assert_frame_equal(one[name], two[name])


def test_written_files_and_manifest_last(tmp_path):
    out, log = sap.build_sap(SEED, AS_OF, start=date(2026, 9, 15))
    target = sap.write_sap(out, log, tmp_path / "sap", SEED, AS_OF)
    files = sorted(p.name for p in target.iterdir())
    assert "sap_manifest.json" in files and "billing_items.csv" in files
    assert len(files) == len(out) + 1
