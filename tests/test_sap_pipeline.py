"""Client area: SAP-shaped billing data through bronze -> silver -> gold -> reconcile (docs/sap_input_contract.md).

Part 1: no Spark (config, orchestration rules).
Part 2: every silver rule on a tiny hand-made table: one test per planted real-world case.
Part 3: SAP-shaped dummy data for July-September 2026 (about 8,000 rows; every planted case except the rare
        impossible dates, covered in part 2) through the whole client pipeline (run_pipeline), proved against
        the generator's log of planted cases and the source's own control totals.
"""

import dataclasses
from datetime import date
from decimal import Decimal

import pytest

from sales_insights.common.config import load_config
from sales_insights.generator import sap
from sales_insights.pipeline import run_pipeline as rp

AS_OF = date(2026, 10, 1)
SEED = 20261001
SCOPE_DATE = date(2026, 9, 30)


# ---------------------------------------------------------------------------
# Part 1: config and orchestration (no Spark)
# ---------------------------------------------------------------------------


def test_client_area_has_its_own_folders_catalog_and_scope():
    local, dev = load_config("local", area="client"), load_config("dev", area="client")
    assert local.lake_path == "lake/client" and local.staging_path == "staging/client"
    assert dev.catalog == "client_dev" and dev.table("gold", "fact_sales") == "client_dev.gold.fact_sales"
    assert local.file_targets == {"billing": "billing_items", "source": "source_counts"}

    from sales_insights.pipeline.sap_silver import scope_of

    scope = scope_of(local)
    assert scope.company_codes == ("2010",) and scope.currency == "LKR" and scope.excluded_types == ()
    assert (scope.date_from, scope.date_to) == (date(2025, 4, 1), date(2026, 9, 30))


def test_the_pipeline_text_tables_are_the_contract_ones():
    from sales_insights.pipeline.sap_silver import TEXT_TABLES

    assert TEXT_TABLES == sap.TEXT_TABLES


def test_client_area_steps():
    assert rp.AREA_STEPS["client"] == ["bronze", "silver", "gold", "reconcile", "kpis"]
    assert set(rp.AREA_STEPS["client"]) == set(rp.AREA_STEP_FUNCS["client"])
    assert rp.AREA_STEP_FUNCS["client"]["bronze"] is rp._bronze  # same bronze and gold code as sales
    assert rp.AREA_STEP_FUNCS["client"]["gold"] is rp._gold


def _with_source(cfg, source: str):
    areas = {**cfg.areas, "client": {**cfg.areas["client"], "source": source}}
    return dataclasses.replace(cfg, areas=areas)


def test_real_source_never_generates_dummy_data(tmp_path):
    cfg = _with_source(load_config("local", area="client"), "real")
    with pytest.raises(FileNotFoundError, match="never generates"):
        rp.ensure_client_source(cfg, tmp_path / "staging", tmp_path / "landing")
    assert not (tmp_path / "staging").exists()


def test_landed_files_are_delivered_once_and_never_regenerated(tmp_path):
    cfg = _with_source(load_config("local", area="client"), "real")
    out, log = sap.build_sap(SEED, AS_OF, start=SCOPE_DATE)
    sap.write_sap(out, log, tmp_path / "staging", SEED, AS_OF)
    done = rp.ensure_client_source(cfg, tmp_path / "staging", tmp_path / "landing")
    assert done == ["initial delivery of the SAP files"]
    assert (tmp_path / "landing" / "history" / "billing_items.csv").exists()
    assert rp.ensure_client_source(cfg, tmp_path / "staging", tmp_path / "landing") == []


# ---------------------------------------------------------------------------
# Part 2: silver rules on tiny tables
# ---------------------------------------------------------------------------

LINE = {
    "billingdocument": "9000000001",
    "billingdocumentitem": "10",
    "billingdocumenttype": "ZAOR",
    "billingdocumentcategory": "L",
    "sddocumentcategory": "M",
    "billingdocumentdate": "2026-05-03 00:00:00",
    "creationdate": "2026-05-03 00:00:00",
    "lastchangedate": "2026-05-03 00:00:00",
    "billingdocumentiscancelled": None,
    "cancelledbillingdocument": None,
    "referencesddocument": None,
    "referencesddocumentitem": None,
    "salesdocument": "1000000001",
    "salesdocumentitem": "10",
    "companycode": "2010",
    "salesorganization": "2010",
    "distributionchannel": "10",
    "division": "04",
    "salesoffice": "C001",
    "salesgroup": None,
    "salesdistrict": "05",
    "plant": "2010",
    "storagelocation": "0001",
    "soldtoparty": "1000123",
    "customergroup": "01",
    "material": "MC6001LT",
    "materialgroup": "2CHE03",
    "billingquantity": "2.000",
    "billingquantityunit": "CS",
    "billingquantityinbaseunit": "2.000",
    "baseunit": "CS",
    "netamount": "1000.00",
    "grossamount": "1180.00",
    "taxamount": "180.00",
    "costamount": "600.00",
    "transactioncurrency": "LKR",
    "accountingexchangerate": "1.00000",
    "fiscalyear": "2026",
    "fiscalperiod": "002",
    "_fivetran_synced": "2026-05-03 10:00:00.000",
    "_fivetran_deleted": "false",
    "_source_file": "history/billing_items.csv",
    "_business_date": None,
    "_load_ts": "2026-10-28 06:00:00",
}
SCOPE_KW = {"company_codes": ("2010",), "date_from": date(2025, 4, 1), "date_to": date(2026, 9, 30)}


def line(number: str = "9000000001", item: str = "10", **kw) -> dict:
    return {**LINE, "billingdocument": number, "billingdocumentitem": item, **kw}


def frame(spark, rows: list[dict]):
    """Small all-text table, like bronze (every column STRING)."""
    cols = list(rows[0])
    schema = ", ".join(f"`{c}` string" for c in cols)
    return spark.createDataFrame([tuple(r.get(c) for c in cols) for r in rows], schema)


@pytest.fixture(scope="module")
def ss():
    from sales_insights.pipeline import sap_silver

    return sap_silver


def _fates(spark, ss, rows):
    fated = ss.assign_fates(frame(spark, rows), ss.Scope(**SCOPE_KW))
    return {
        (r["billingdocument"], r["billingdocumentitem"], r["_fivetran_synced"]): (r["_fate"], r["_reasons"])
        for r in fated.collect()
    }


def test_every_row_gets_exactly_one_fate_in_the_contract_order(spark, ss):
    rows = [
        line("9000000001"),  # kept
        line("9000000002", _fivetran_synced="2026-05-04 09:00:00.000", netamount="970.00"),  # older version
        line("9000000002", _fivetran_synced="2026-05-05 09:00:00.000"),  # newest version: kept
        line("9000000003", _fivetran_deleted="true"),
        line("9000000004", companycode="4010"),
        line("5000000001", billingdocumenttype="F5", sddocumentcategory="U"),  # pro-forma
        line("2000000001", billingdocumenttype="FAZ"),  # down payment
        line("9000000005", billingdocumentdate=None),  # missing date
        line("9000000006", billingdocumentdate="4334-05-03 00:00:00"),  # typo year
        line("9000000007", billingdocumentdate="1999-12-31 00:00:00"),
        line("9000000008", billingdocumentdate="2024-12-01 00:00:00"),  # outside the window
        line("9000000009", material=" "),
        line("9000000010", netamount="12a"),
        line("9000000011", sddocumentcategory="X"),
        line("9000000012", transactioncurrency="USD", accountingexchangerate=None),
        line("9000000013", billingquantity="abc"),
        line("9000000014", billingdocumentdate=None, _fivetran_deleted="true"),  # first rule wins
    ]
    f = _fates(spark, ss, rows)
    by_doc = {}
    for (doc, _, synced), v in f.items():
        by_doc.setdefault(doc, {})[synced] = v
    assert len(f) == len(rows)

    def only(doc):
        (v,) = by_doc[doc].values()
        return v

    assert only("9000000001") == ("kept", "")
    assert by_doc["9000000002"] == {
        "2026-05-04 09:00:00.000": ("older_version", ""),
        "2026-05-05 09:00:00.000": ("kept", ""),
    }
    assert only("9000000003")[0] == "deleted_in_source"
    assert only("9000000004")[0] == "out_of_scope_company"
    assert only("5000000001")[0] == "not_revenue" and only("2000000001")[0] == "not_revenue"
    assert only("9000000005") == ("quarantined", "invalid_invoice_date")
    assert only("9000000006") == ("quarantined", "invalid_invoice_date")
    assert only("9000000007") == ("quarantined", "invalid_invoice_date")
    assert only("9000000008")[0] == "outside_window"
    assert only("9000000009") == ("quarantined", "missing_key")
    assert only("9000000010") == ("quarantined", "invalid_amount")
    assert only("9000000011") == ("quarantined", "unknown_document_category")
    assert only("9000000012") == ("quarantined", "invalid_amount")
    assert only("9000000013") == ("quarantined", "invalid_quantity")
    assert only("9000000014")[0] == "deleted_in_source"


def test_a_custom_type_excluded_by_config_is_not_revenue(spark, ss):
    rows = [line("9000000001"), line("9100000001", billingdocumenttype="ZCF2")]
    scope = ss.Scope(**SCOPE_KW, excluded_types=("ZCF2",))
    fated = {r["billingdocument"]: r["_fate"] for r in ss.assign_fates(frame(spark, rows), scope).collect()}
    assert fated == {"9000000001": "kept", "9100000001": "not_revenue"}
    default = {
        r["billingdocument"]: r["_fate"] for r in ss.assign_fates(frame(spark, rows), ss.Scope(**SCOPE_KW)).collect()
    }
    assert default["9100000001"] == "kept"  # nothing is excluded unless config says so


def test_fate_counts_add_up_to_the_rows_and_find_a_missing_one(spark, ss):
    rows = [line("9000000001"), line("9000000002", companycode="4010"), line("9000000003", billingdocumentdate=None)]
    counts = ss.fate_counts(ss.assign_fates(frame(spark, rows), ss.Scope(**SCOPE_KW)))
    assert sum(r["line_count"] for r in counts.collect()) == 3
    source = frame(
        spark,
        [
            {"companycode": "2010", "billing_month": "2026-05", "sddocumentcategory": "M",
             "line_count": "1", "netamount_sum": "1000.00", "_source_file": "x", "_load_ts": "1"},
            {"companycode": "4010", "billing_month": "2026-05", "sddocumentcategory": "M",
             "line_count": "1", "netamount_sum": "1000.00", "_source_file": "x", "_load_ts": "1"},
            {"companycode": "2010", "billing_month": None, "sddocumentcategory": "M",
             "line_count": "1", "netamount_sum": "1000.00", "_source_file": "x", "_load_ts": "1"},
        ],
    )  # fmt: skip
    typed = ss.typed_source_counts(source)
    assert {r["status"] for r in ss.compare_to_source(typed, counts).collect()} == {"PASS"}
    one_lost = ss.fate_counts(ss.assign_fates(frame(spark, rows[:2]), ss.Scope(**SCOPE_KW)))
    statuses = [r["status"] for r in ss.compare_to_source(typed, one_lost).collect()]
    assert statuses.count("FAIL") == 1


def _lines(spark, ss, rows):
    fated = ss.assign_fates(frame(spark, rows), ss.Scope(**SCOPE_KW))
    out = ss.flag_cancellations(ss.to_invoice_lines(fated.filter("_fate = 'kept'"), ss.Scope(**SCOPE_KW)))
    return {(r["invoice_number"], r["invoice_item"]): r for r in out.collect()}


def test_the_text_null_is_an_empty_value(spark, ss):
    """30 Oct: the real copy wrote the text "null" for columns the source does not have."""
    rows = [line("9000000001", salesgroup="null", referencesddocument="NULL", cancelledbillingdocument="None")]
    got = _lines(spark, ss, rows)[("9000000001", "10")]
    assert got["sales_group"] is None and got["reference_document"] is None
    rows = [line("9000000002", material="nan")]
    fated = ss.assign_fates(frame(spark, rows), ss.Scope(**SCOPE_KW)).collect()
    assert (fated[0]["_fate"], fated[0]["_reasons"]) == ("quarantined", "missing_key")


def test_signs_follow_the_document_category(spark, ss):
    rows = [
        line("9000000001"),
        line("7000000001", billingdocumenttype="ZARE", sddocumentcategory="O", referencesddocument="9000000001"),
        line("8000000001", billingdocumenttype="S1", sddocumentcategory="N", cancelledbillingdocument="9000000001"),
        line("4000000001", billingdocumenttype="S2", sddocumentcategory="S", cancelledbillingdocument="7000000001"),
        line("3000000001", billingdocumenttype="ZADR", sddocumentcategory="P", billingquantity="0.000",
             netamount="20.00", referencesddocument="9000000001"),
        line("9000000002", billingdocumenttype="ZFOC", netamount="0.00"),
    ]  # fmt: skip
    got = _lines(spark, ss, rows)
    assert got[("9000000001", "10")]["revenue"] == Decimal("1000.00")
    assert got[("9000000001", "10")]["quantity"] == Decimal("2.000")
    assert got[("9000000001", "10")]["cost_amount"] == Decimal("600.00")
    assert got[("7000000001", "10")]["revenue"] == Decimal("-1000.00")
    assert got[("7000000001", "10")]["quantity"] == Decimal("-2.000")
    assert got[("7000000001", "10")]["cost_amount"] == Decimal("-600.00")
    assert got[("8000000001", "10")]["revenue"] == Decimal("-1000.00")
    assert got[("4000000001", "10")]["revenue"] == Decimal("1000.00")  # reverses a credit: positive
    assert got[("3000000001", "10")]["revenue"] == Decimal("20.00")  # debit memo: positive
    assert got[("9000000002", "10")]["revenue"] == Decimal("0.00")  # free of charge: quantity counts
    assert got[("9000000002", "10")]["quantity"] == Decimal("2.000")
    assert got[("7000000001", "10")]["reference_invoice_number"] == "9000000001"
    assert got[("4000000001", "10")]["reference_invoice_number"] == "7000000001"
    assert got[("9000000001", "10")]["reference_invoice_number"] is None


def test_foreign_currency_is_converted_with_the_line_rate(spark, ss):
    rows = [
        line("9000000001", transactioncurrency="USD", accountingexchangerate="300.00000", netamount="10.00",
             grossamount="10.00", taxamount="0.00", costamount="4.00"),
        line("9000000002", transactioncurrency="USD", accountingexchangerate="-0.00400", netamount="10.00"),
    ]  # fmt: skip
    got = _lines(spark, ss, rows)
    usd = got[("9000000001", "10")]
    assert (usd["revenue"], usd["cost_amount"], usd["currency"]) == (Decimal("3000.00"), Decimal("1200.00"), "USD")
    assert usd["document_net_amount"] == Decimal("10.00")  # as the source stores it
    assert got[("9000000002", "10")]["revenue"] == Decimal("2500.00")  # indirect quotation: 1 / 0.004


def test_fiscal_year_comes_from_the_date_not_the_source(spark, ss):
    rows = [
        line("9000000001", billingdocumentdate="2025-05-10 00:00:00", fiscalyear="0000", fiscalperiod="000"),
        line("9000000002", billingdocumentdate="2026-02-01 00:00:00", fiscalyear="0000"),
    ]
    got = _lines(spark, ss, rows)
    assert (got[("9000000001", "10")]["fiscal_year"], got[("9000000001", "10")]["fiscal_period"]) == (2025, 2)
    assert (got[("9000000002", "10")]["fiscal_year"], got[("9000000002", "10")]["fiscal_period"]) == (2025, 11)
    assert got[("9000000001", "10")]["invoice_date"] == date(2025, 5, 10)


def test_cancellations_flag_the_original_invoice(spark, ss):
    rows = [
        line("9000000001"),
        line("8000000001", billingdocumenttype="S1", sddocumentcategory="N", cancelledbillingdocument="9000000001"),
        line("9000000002", billingdocumentiscancelled="X"),  # SAP's flag alone
        line("9000000003"),
    ]
    got = _lines(spark, ss, rows)
    assert (got[("9000000001", "10")]["is_cancelled"], got[("9000000001", "10")]["cancelled_by"]) == (
        True,
        "8000000001",
    )
    assert got[("9000000002", "10")]["is_cancelled"] is True
    assert got[("9000000003", "10")]["is_cancelled"] is False
    assert got[("8000000001", "10")]["is_cancelled"] is False  # the cancellation itself


def _text(spark, code_col, name_col, rows):
    return frame(
        spark,
        [{"language": lang, code_col: code, name_col: name, "_source_file": "m", "_load_ts": "1"}
         for lang, code, name in rows],
    )  # fmt: skip


def test_text_lookup_takes_english_names_only(spark, ss):
    raw = _text(spark, "salesoffice", "salesofficename", [("E", "C001", "Colombo"), ("D", "C001", "Colombo (DE)")])
    got = ss.text_lookup(raw, spark, "salesoffice", "salesofficename", "sales_office", "sales_office_name").collect()
    assert [(r["sales_office"], r["sales_office_name"]) for r in got] == [("C001", "Colombo")]
    empty = ss.text_lookup(None, spark, "salesoffice", "salesofficename", "sales_office", "sales_office_name")
    assert empty.count() == 0


def test_customers_never_hold_personal_data_and_billed_gaps_get_an_unknown_member(spark, ss):
    master = frame(
        spark,
        [
            {"customer": "1000123", "customername": "Real Person Stores", "streetname": "No. 5, Main Street",
             "telephonenumber1": "000-1234567", "emailaddress": "someone@example.com", "taxnumber1": "TEST1",
             "cityname": "colombo  1", "postalcode": "00100", "region": "05", "country": "LK",
             "customergroup": "01", "salesoffice": "C001", "salesdistrict": "05", "distributionchannel": "10",
             "salesorganization": "2010", "division": "04", "payercustomer": "1000123",
             "creationdate": "2020-01-01 00:00:00", "_fivetran_synced": "2026-01-01 00:00:00.000",
             "_fivetran_deleted": "false", "_source_file": "m", "_load_ts": "1"},
        ],
    )  # fmt: skip
    lines = spark.createDataFrame([("0001000123",), ("0009999999",)], "customer_id string")
    groups = spark.createDataFrame([("01", "Dealer")], "customer_group string, customer_group_name string")
    regions = spark.createDataFrame(
        [("05", "Colombo", "Unassigned")], "region string, district_name string, province string"
    )
    out = ss.build_customers(master, lines, groups, regions)
    assert not set(ss.PERSONAL_COLUMNS) & set(out.columns)
    rows = {r["customer_id"]: r.asDict() for r in out.collect()}
    known, unknown = rows["0001000123"], rows["0009999999"]
    assert known["customer_name"] == "Customer 0001000123" and known["in_master"]
    assert (known["customer_group_name"], known["district_name"], known["city"]) == ("Dealer", "Colombo", "COLOMBO 01")
    assert unknown["customer_name"].startswith("Unknown customer") and not unknown["in_master"]
    assert unknown["customer_group_name"] == "Unassigned"
    values = " ".join(str(v) for r in rows.values() for v in r.values())
    for personal in ("Real Person", "Main Street", "000-1234567", "@example.com", "TEST1"):
        assert personal not in values


def test_products_get_names_and_unknown_members(spark, ss):
    master = frame(
        spark,
        [
            {"product": "MC6001LT", "productdescription": "mancozeb 1lt", "producttype": "FERT",
             "productgroup": None, "division": "04", "baseunit": "CS", "plant": "2010",
             "salesorganization": "2010", "creationdate": "2021-01-01 00:00:00",
             "_fivetran_synced": "2026-01-01 00:00:00.000", "_fivetran_deleted": "false",
             "_source_file": "m", "_load_ts": "1"},
        ],
    )  # fmt: skip
    lines = spark.createDataFrame(
        [("MC6001LT", "2CHE03", "2010"), ("MC6001LT", "2CHE03", "2010"), ("XX1", None, "2010")],
        "product_id string, product_group string, company_code string",
    )
    groups = spark.createDataFrame([("2CHE03", "FUNGICIDE")], "product_group string, product_group_name string")
    types = spark.createDataFrame([("FERT", "Finished goods")], "product_type string, product_type_name string")
    divisions = spark.createDataFrame([("04", "Crop protection")], "division string, division_name string")
    rows = {r["product_id"]: r for r in ss.build_products(master, lines, groups, types, divisions).collect()}
    p = rows["MC6001LT"]
    assert (p["product_description"], p["product_group"], p["product_group_name"]) == (
        "MANCOZEB 1LT",
        "2CHE03",
        "FUNGICIDE",
    )
    assert (p["product_type_name"], p["division_name"], p["company_code"]) == (
        "Finished goods",
        "Crop protection",
        "2010",
    )
    assert rows["XX1"]["product_description"] == "UNKNOWN PRODUCT XX1" and not rows["XX1"]["in_master"]


def test_gold_labels_the_two_sap_document_types_and_uses_the_line_cost(spark, ss):
    from sales_insights.pipeline import gold as g

    rows = [
        line("9000000001"),
        line("3000000001", billingdocumenttype="ZADR", sddocumentcategory="P", netamount="20.00", costamount="0.00"),
        line("4000000001", billingdocumenttype="S2", sddocumentcategory="S", cancelledbillingdocument="7000000001"),
    ]
    fated = ss.assign_fates(frame(spark, rows), ss.Scope(**SCOPE_KW))
    lines = ss.flag_cancellations(ss.to_invoice_lines(fated, ss.Scope(**SCOPE_KW)))
    products = spark.createDataFrame([("MC6001LT", Decimal("99.00"))], "product_id string, standard_cost decimal(18,2)")
    fact = {r["invoice_number"]: r for r in g.build_fact_sales(lines, products).collect()}
    assert fact["3000000001"]["document_type"] == "Debit memo"
    assert fact["4000000001"]["document_type"] == "Credit memo cancellation"
    assert fact["9000000001"]["cost_amount"] == Decimal("600.00")  # SAP's cost, not 2 x 99.00
    assert fact["9000000001"]["margin_amount"] == Decimal("400.00")


def test_gold_counts_every_invoice_type_document_and_labels_client_types(spark, ss):
    """30 Oct: the real data has ZCF2, ZAEP, ZFV1, ZFV2 (category M): invoiced revenue, labelled Other (<type>)."""
    from sales_insights.pipeline import gold as g

    rows = [
        line("9000000001"),
        line("9100000001", billingdocumenttype="ZCF2"),
        line("8000000001", billingdocumenttype="S1", sddocumentcategory="N", cancelledbillingdocument="9000000001"),
    ]
    fated = ss.assign_fates(frame(spark, rows), ss.Scope(**SCOPE_KW))
    lines = ss.flag_cancellations(ss.to_invoice_lines(fated, ss.Scope(**SCOPE_KW)))
    products = spark.createDataFrame([("MC6001LT", Decimal("1.00"))], "product_id string, standard_cost decimal(18,2)")
    fact = {r["invoice_number"]: r for r in g.build_fact_sales(lines, products).collect()}
    assert fact["9100000001"]["document_type"] == "Other (ZCF2)"
    assert fact["9100000001"]["invoiced_revenue_amount"] == Decimal("1000.00")
    assert fact["9100000001"]["sold_quantity"] == Decimal("2.000")
    assert fact["8000000001"]["cancellations_amount"] == Decimal("-1000.00")


# ---------------------------------------------------------------------------
# Part 3: the whole client pipeline on SAP-shaped dummy data (July-September 2026)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def world(spark, cfg, tmp_path_factory):
    from sales_insights.common.lake import Lake

    client = _with_source(cfg.for_area("client"), "real")  # the files are written here; never generated
    root = tmp_path_factory.mktemp("client")
    staging, landing = root / "staging", root / "landing"
    out, log = sap.build_sap(SEED, AS_OF, start=date(2026, 7, 1))
    sap.write_sap(out, log, staging, SEED, AS_OF)
    lake = Lake(spark, client, root=root / "lake")
    results = rp.run_range(
        spark, client, [SCOPE_DATE], staging=staging, landing=landing, lake=lake, echo=lambda _: None
    )
    return {"results": results, "lake": lake, "out": out, "log": log}


def _keys(world, case):
    return {(x["billingdocument"], x["billingdocumentitem"]) for x in world["log"] if x["case"] == case}


def _docs(world, case):
    return {x["billingdocument"] for x in world["log"] if x["case"] == case}


def _fact(world):
    return world["lake"].read("gold", "fact_sales")


def test_every_step_passes(world):
    (day,) = world["results"]
    assert [st.step for st in day.steps] == ["bronze", "silver", "gold", "reconcile", "kpis"]
    assert day.ok, [st.detail for st in day.steps]


def test_no_failed_data_quality_check(world):
    from pyspark.sql import functions as F

    dq = world["lake"].read("ops", "dq_results")
    assert dq.filter(F.col("status") == "FAIL").count() == 0
    names = {r["check_name"] for r in dq.collect()}
    assert {"copy_complete_vs_source_counts", "source_to_silver", "silver_to_gold", "no_personal_columns"} <= names


def test_every_source_row_has_exactly_one_fate(world):
    fates = world["lake"].read("ops", "sap_row_fates").collect()
    assert sum(r["line_count"] for r in fates) == len(world["out"]["billing_items"])
    assert sum(r["line_count"] for r in fates if r["fate"] == "kept") == _fact(world).count()


def test_reconciliation_passes_for_every_group(world):
    from pyspark.sql import functions as F

    rec = world["lake"].read("ops", "sap_reconciliation")
    assert rec.filter(F.col("status") == "FAIL").count() == 0
    assert {r["check_name"] for r in rec.collect()} == {"source_to_silver", "silver_to_gold"}


def test_pro_formas_down_payments_and_other_companies_are_not_in_gold(world):
    from pyspark.sql import functions as F

    fact = _fact(world)
    excluded = _docs(world, "pro_forma") | _docs(world, "down_payment")
    assert excluded and fact.filter(F.col("invoice_number").isin(*excluded)).count() == 0
    assert {r["company_code"] for r in fact.select("company_code").distinct().collect()} == {"2010"}
    assert fact.filter(F.col("invoice_type").isin("F5", "FAZ", "FAS")).count() == 0


def test_deleted_lines_and_older_versions_are_gone(world):
    from pyspark.sql import functions as F

    fact = _fact(world)
    deleted = _keys(world, "deleted_in_source")
    got = {(r["invoice_number"], r["invoice_item"]) for r in fact.select("invoice_number", "invoice_item").collect()}
    assert deleted and not (deleted & got)
    older = _keys(world, "older_version")
    assert older
    assert fact.groupBy("invoice_number", "invoice_item").count().filter(F.col("count") > 1).count() == 0


def test_bad_dates_are_quarantined_never_in_gold(world):
    bad = _keys(world, "missing_billing_date") | _keys(world, "impossible_billing_date")
    quarantined = {
        (r["invoice_number"], r["invoice_item"]) for r in world["lake"].read("silver", "quarantine").collect()
    }
    got = {
        (r["invoice_number"], r["invoice_item"])
        for r in _fact(world).select("invoice_number", "invoice_item").collect()
    }
    assert not (bad & got)
    assert quarantined <= bad  # nothing else is quarantined in this data
    assert all(r["reasons"] == "invalid_invoice_date" for r in world["lake"].read("silver", "quarantine").collect())


def test_credit_memo_cancellations_and_debit_memos_count_positive(world):
    from pyspark.sql import functions as F

    fact = _fact(world)
    s2 = fact.filter(F.col("document_type") == "Credit memo cancellation")
    assert s2.count() > 0 and s2.filter(F.col("net_revenue_amount") < 0).count() == 0
    dm = fact.filter(F.col("document_type") == "Debit memo")
    assert dm.filter(F.col("net_revenue_amount") < 0).count() == 0
    assert fact.filter(F.col("document_type").isNull()).count() == 0


def test_usd_lines_are_converted_to_rupees(world):
    from pyspark.sql import functions as F

    usd = world["lake"].read("silver", "invoice_lines").filter(F.col("currency") == "USD").collect()
    assert usd
    for r in usd:
        sign = 1 if r["sd_document_category"] in ("M", "P", "S") else -1
        assert abs(r["revenue"] - r["document_net_amount"] * r["exchange_rate"] * sign) <= Decimal("0.01")


def test_customer_missing_from_the_master_is_an_unknown_member(world):
    from pyspark.sql import functions as F

    (orphan,) = [x["customer"] for x in world["log"] if x["case"] == "customer_missing_in_master"]
    dim = world["lake"].read("gold", "dim_customer")
    row = dim.filter(F.col("customer_id") == orphan).collect()
    assert len(row) == 1 and row[0]["customer_name"].startswith("Unknown customer")
    assert _fact(world).join(dim, "customer_id", "left_anti").count() == 0


def test_no_personal_data_reaches_silver_or_gold(world):
    lake = world["lake"]
    personal = world["out"]["customer_master"][sap.PERSONAL_CUSTOMER_COLUMNS]
    samples = {str(v) for col in personal.columns for v in personal[col].head(20)}
    for layer, table in (("silver", "customers"), ("gold", "dim_customer")):
        df = lake.read(layer, table)
        assert not set(sap.PERSONAL_CUSTOMER_COLUMNS) & set(df.columns)
        text = " ".join(str(v) for r in df.collect() for v in r)
        assert not [s for s in samples if s in text], table


def test_the_client_area_uses_the_sales_kpis_and_metric_view():
    from sales_insights.semantic import kpis as k

    client = k.AREAS["client"]
    assert client.own_gold and client.metric_views == ("sales_metrics",)
    assert client.folder == k.AREAS["sales"].folder and client.agreements == k.AREAS["sales"].agreements


def test_kpi_sql_matches_the_metric_view_on_sap_data(world, spark):
    from sales_insights.semantic import kpis as k

    results = k.check_against_metric_view(spark, world["lake"])
    assert results and all(rows > 0 and bad == 0 for _, rows, bad in results), results


def test_every_client_dashboard_dataset_runs(world, spark):
    """Every dataset of the Sales (SAP data) dashboard runs on the client tables (metric view compiled locally)
    and returns the columns its widgets show."""
    from sales_insights.semantic import dashboards as db

    tables = db.lake_names(None, None, client_lake=world["lake"], area="client")
    assert tables["fact_sales"] == world["lake"].sql_name("gold", "fact_sales")
    spec = db.load("client")
    results = {}
    for key, ds in db.datasets(spec).items():
        df = spark.sql(db.local_sql(ds, tables, db.filter_defaults(spec)))
        results[key] = (set(df.columns), df.collect())
    for page in spec["pages"]:
        for w in page["widgets"]:
            kind = db.widget_kind(w)
            if kind == "text":
                continue
            body = w[kind]
            shown = {c["field"] for c in body.get("columns", [])}
            shown |= {body[x]["field"] for x in ("x", "y", "color") if x in body}
            shown |= {b["field"] for b in body.get("bars", []) + body.get("line", [])}
            shown |= {body["value"]} if "value" in body else set()
            assert shown <= results[body["dataset"]][0], (body["dataset"], shown - results[body["dataset"]][0])
    for key in ("trust", "headline_mtd", "monthly_by_document_type", "row_fates", "reconciliation", "data_checks"):
        assert results[key][1], f"{key} returned no rows"
    fates = {r["what_happened"]: r["row_count"] for r in results["row_fates"][1]}
    assert fates["In silver and gold"] == _fact(world).count()
    assert "Debit memo" in {r["document_type"] for r in results["monthly_by_document_type"][1]}
