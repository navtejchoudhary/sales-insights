"""Silver for SAP-shaped billing data (the client area; rules: docs/sap_input_contract.md).

    uv run python -m sales_insights.pipeline.sap_silver          # client area, after bronze

Reads the raw SAP tables in bronze (every value as text) and writes the SAME silver tables the sales
pipeline writes, so gold, the metric views, the dashboards and Genie work unchanged:

    silver.invoice_lines            one row per billing line that counts: signed, in the company currency
    silver.customers                one row per customer, NO personal columns; "Unknown" members for billed
    silver.products                 customers / materials missing from the masters
    silver.sales_offices, silver.regions, silver.distribution_channels    from the English text tables
    silver.quarantine               bad rows with their reasons and the raw row
    ops.source_counts               the source's own control totals (as copied)
    ops.sap_row_fates               what happened to every bronze row, per company, month and category
    ops.sap_billing_rows            the bronze rows with their fate (computed once per run, for tracing a row)

Every bronze billing row gets exactly ONE fate, checked in this order:

    older_version         an older copy of the same line (same document + item, older sync time)
    deleted_in_source     deleted in SAP (_fivetran_deleted)
    out_of_scope_company  company code not in scope (config: areas.client.scope)
    not_revenue           pro-forma (category U), down payment (FAZ / FAS), or a type listed in
                          scope.excluded_types (a custom type the business does not count as sales)
    quarantined           missing key, impossible date, bad number, unknown category
    outside_window        billing date outside the scope window
    kept                  -> silver.invoice_lines

So source rows = sum of all fates, and reconciliation (sap_reconcile.py) can prove that every row of the
source is accounted for: in silver, or filtered for a stated reason.

Like the sales silver: a FULL REBUILD on every run, every rule a small DataFrame -> DataFrame function.
"""

from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_insights.common.config import Config
from sales_insights.common.lake import Lake
from sales_insights.pipeline import bronze as b
from sales_insights.pipeline import silver as sv

KEY = ["billingdocument", "billingdocumentitem"]
# sddocumentcategory -> sign in silver (SAP stores positive numbers; the category gives the meaning)
SIGNS = {"M": 1, "P": 1, "S": 1, "O": -1, "N": -1}
NOT_REVENUE_CATEGORIES = ["U"]  # pro-forma invoices
NOT_REVENUE_TYPES = ["FAZ", "FAS"]  # down-payment request and its cancellation
SALES_CATEGORY, CANCELLATION_CATEGORY = "M", "N"
KEPT, QUARANTINED = "kept", "quarantined"
FATES = [
    "older_version",
    "deleted_in_source",
    "out_of_scope_company",
    "not_revenue",
    QUARANTINED,
    "outside_window",
    KEPT,
]
EARLIEST_DATE = "2000-01-01"  # an earlier billing date is a typo
FUTURE_DAYS = 365  # a billing date more than a year ahead is a typo (the source has years like 4334)

# Text (lookup) tables: logical name -> (code column, name column), one row per code and language
TEXT_TABLES = {
    "company_codes": ("companycode", "companycodename"),
    "sales_orgs": ("salesorganization", "salesorganizationname"),
    "sales_offices": ("salesoffice", "salesofficename"),
    "distribution_channels": ("distributionchannel", "distributionchannelname"),
    "divisions": ("division", "divisionname"),
    "customer_groups": ("customergroup", "customergroupname"),
    "material_groups": ("materialgroup", "materialgroupname"),
    "material_types": ("producttype", "producttypename"),
    "plants": ("plant", "plantname"),
    "regions": ("region", "regionname"),
    "document_types": ("billingdocumenttype", "billingdocumenttypename"),
}
TEXT_LANGUAGE = "E"
# Personal columns of the customer master: never selected, so they can never reach silver or gold
PERSONAL_COLUMNS = ["customername", "streetname", "telephonenumber1", "emailaddress", "taxnumber1"]
UNASSIGNED = sv.UNASSIGNED
MONEY, QTY, RATE = "DECIMAL(18,2)", "DECIMAL(18,3)", "DECIMAL(18,5)"
BUCKET = ["company_code", "billing_month", "sd_document_category"]  # grain of the control totals


@dataclass(frozen=True)
class Scope:
    """Which rows count (config.yaml: areas.<area>.scope)."""

    company_codes: tuple[str, ...]
    date_from: date
    date_to: date
    currency: str = "LKR"
    excluded_types: tuple[str, ...] = ()  # billing types the business does not count as sales (default none)


def scope_of(cfg: Config) -> Scope:
    spec = cfg.area_setting("scope")
    if not spec:
        raise ValueError(f"Area '{cfg.area}' has no scope: in config.yaml (company_codes, date_from, date_to)")
    return Scope(
        company_codes=tuple(str(c) for c in spec["company_codes"]),
        date_from=date.fromisoformat(str(spec["date_from"])),
        date_to=date.fromisoformat(str(spec["date_to"])),
        currency=str(spec.get("currency", "LKR")),
        excluded_types=tuple(str(t) for t in spec.get("excluded_types") or ()),
    )


@dataclass
class SapSilverRun:
    run_id: str
    rows: dict[str, int] = field(default_factory=dict)
    fates: dict[str, int] = field(default_factory=dict)
    quarantined: int = 0
    dq_failures: int = 0
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Small helpers (text in, typed value or NULL out; never an error)
# ---------------------------------------------------------------------------


# Text that means "no value" when a file passed through another tool (seen in the real copy, 30 Oct: "null")
NULL_TEXTS = ("null", "none", "nan", "<na>")


def blank(c: Column) -> Column:
    return c.isNull() | (F.trim(c) == "") | F.lower(F.trim(c)).isin(*NULL_TEXTS)


def txt(col: str) -> Column:
    """Trimmed text, NULL when empty."""
    return F.when(~blank(F.col(col)), F.trim(F.col(col)))


def dec(col: str, sql_type: str) -> Column:
    return F.expr(f"try_cast(trim(`{col}`) AS {sql_type})")


def date_part(col: str) -> Column:
    """The date of a source timestamp, WITHOUT a time-zone shift (the first 10 characters)."""
    return F.expr(f"try_cast(substring(`{col}`, 1, 10) AS DATE)")


def synced_ts(col: str = "_fivetran_synced") -> Column:
    return F.expr(f"try_cast(`{col}` AS TIMESTAMP)")


def is_deleted(col: str = "_fivetran_deleted") -> Column:
    return F.coalesce(F.lower(F.trim(F.col(col))).isin("true", "t", "1", "yes", "x"), F.lit(False))


def billing_month(col: str = "billingdocumentdate") -> Column:
    """YYYY-MM as the source's control totals group it (the raw text, so even a typo year has a month)."""
    return F.coalesce(F.substring(F.col(col), 1, 7), F.lit(""))


def newest_first(df: DataFrame, keys: list[str]) -> DataFrame:
    """Number the copies of each key, newest sync time first (_rn = 1 is the line as SAP holds it now)."""
    order = [synced_ts().desc_nulls_last(), F.col("_fivetran_synced").desc_nulls_last()]
    order += [F.col(c).desc_nulls_last() for c in ("_load_ts", "_source_file") if c in df.columns]
    return df.withColumn("_rn", F.row_number().over(Window.partitionBy(*keys).orderBy(*order)))


def most_common(df: DataFrame, key: str, cols: list[str]) -> DataFrame:
    """Per key, the most frequent combination of `cols` (ties: the highest values)."""
    counts = df.filter(F.col(cols[0]).isNotNull()).groupBy(key, *cols).count()
    return sv.latest(counts, [key], ["count", *cols]).drop("count")


# ---------------------------------------------------------------------------
# Billing items: one fate per row
# ---------------------------------------------------------------------------


def quarantine_reasons(df: DataFrame, scope: Scope) -> Column:
    """';'-separated reasons a row cannot be trusted ('' when it is fine)."""
    d = date_part("billingdocumentdate")
    missing_key = (
        blank(F.col("billingdocument"))
        | blank(F.col("billingdocumentitem"))
        | blank(F.col("soldtoparty"))
        | blank(F.col("material"))
    )
    bad_date = d.isNull() | (d < F.lit(EARLIEST_DATE).cast("date")) | (d > F.date_add(F.current_date(), FUTURE_DAYS))
    foreign = F.coalesce(txt("transactioncurrency"), F.lit(scope.currency)) != F.lit(scope.currency)
    rate = dec("accountingexchangerate", RATE)
    optional_bad = [~blank(F.col(c)) & dec(c, MONEY).isNull() for c in ("grossamount", "taxamount", "costamount")]
    bad_amount = dec("netamount", MONEY).isNull() | (foreign & (rate.isNull() | (rate == 0)))
    for bad in optional_bad:
        bad_amount = bad_amount | bad
    category = txt("sddocumentcategory")
    unknown_category = category.isNull() | ~category.isin(*SIGNS, *NOT_REVENUE_CATEGORIES)
    return F.concat_ws(
        ";",
        F.when(missing_key, F.lit("missing_key")),
        F.when(bad_date, F.lit("invalid_invoice_date")),
        F.when(dec("billingquantity", QTY).isNull(), F.lit("invalid_quantity")),
        F.when(bad_amount, F.lit("invalid_amount")),
        F.when(unknown_category, F.lit("unknown_document_category")),
    )


def assign_fates(raw: DataFrame, scope: Scope) -> DataFrame:
    """Every bronze billing row with its fate (_fate), quarantine reasons (_reasons) and billing_month.

    Only the newest load of each file counts (a re-sent file replaces its earlier load, as in sales).
    """
    df = newest_first(sv.latest_file_versions(raw), KEY)
    keyed = ~blank(F.col("billingdocument")) & ~blank(F.col("billingdocumentitem"))
    d = date_part("billingdocumentdate")
    company = F.coalesce(txt("companycode"), F.lit(""))
    df = df.withColumn("_reasons", quarantine_reasons(df, scope))
    fate = (
        F.when(keyed & (F.col("_rn") > 1), F.lit("older_version"))
        .when(is_deleted(), F.lit("deleted_in_source"))
        .when(~company.isin(*scope.company_codes), F.lit("out_of_scope_company"))
        .when(
            txt("sddocumentcategory").isin(*NOT_REVENUE_CATEGORIES)
            | txt("billingdocumenttype").isin(*NOT_REVENUE_TYPES, *scope.excluded_types),
            F.lit("not_revenue"),
        )
        .when(F.col("_reasons") != "", F.lit(QUARANTINED))
        .when(~d.between(F.lit(scope.date_from), F.lit(scope.date_to)), F.lit("outside_window"))
        .otherwise(F.lit(KEPT))
    )
    return df.withColumn("_fate", fate).withColumn("billing_month", billing_month()).drop("_rn")


def fate_counts(fated: DataFrame) -> DataFrame:
    """Lines and source net amount per (company, billing month, category, fate): reconciles to the source."""
    return fated.groupBy(
        F.coalesce(txt("companycode"), F.lit("")).alias("company_code"),
        "billing_month",
        F.coalesce(txt("sddocumentcategory"), F.lit("")).alias("sd_document_category"),
        F.col("_fate").alias("fate"),
    ).agg(
        F.count(F.lit(1)).cast("long").alias("line_count"),
        F.sum(F.coalesce(dec("netamount", MONEY), F.lit(0))).cast("DECIMAL(28,2)").alias("netamount_sum"),
    )


def typed_source_counts(raw: DataFrame) -> DataFrame:
    """The source's control totals as copied (bronze text) -> typed, same grain as fate_counts."""
    df = sv.latest_file_versions(raw)
    return df.select(
        F.coalesce(txt("companycode"), F.lit("")).alias("company_code"),
        F.coalesce(txt("billing_month"), F.lit("")).alias("billing_month"),
        F.coalesce(txt("sddocumentcategory"), F.lit("")).alias("sd_document_category"),
        dec("line_count", "BIGINT").alias("line_count"),
        dec("netamount_sum", "DECIMAL(28,2)").alias("netamount_sum"),
    )


def compare_to_source(source: DataFrame, counts: DataFrame) -> DataFrame:
    """Every row copied? Bronze rows (all fates) vs the source's totals per bucket; a missing bucket counts 0."""
    actual = counts.groupBy(*BUCKET).agg(
        F.sum("line_count").alias("actual_lines"), F.sum("netamount_sum").alias("actual_amount")
    )
    expected = source.groupBy(*BUCKET).agg(
        F.sum("line_count").alias("expected_lines"), F.sum("netamount_sum").alias("expected_amount")
    )
    j = expected.join(actual, BUCKET, "full_outer")
    for c in ("expected_lines", "actual_lines", "expected_amount", "actual_amount"):
        j = j.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
    ok = (F.col("expected_lines") == F.col("actual_lines")) & (
        F.abs(F.col("expected_amount") - F.col("actual_amount")) <= F.lit(0.01)
    )
    return j.withColumn("status", F.when(ok, F.lit("PASS")).otherwise(F.lit("FAIL")))


# ---------------------------------------------------------------------------
# Billing items -> invoice lines (the columns gold reads, named as in the sales silver)
# ---------------------------------------------------------------------------


def to_invoice_lines(kept: DataFrame, scope: Scope, fiscal_start_month: int = 4) -> DataFrame:
    """Signed, typed lines in the company currency. Source amounts are positive; the category gives the sign."""
    category = txt("sddocumentcategory")
    sign = F.coalesce(*[F.when(category == k, F.lit(v)) for k, v in SIGNS.items()])
    currency = F.coalesce(txt("transactioncurrency"), F.lit(scope.currency))
    raw_rate = dec("accountingexchangerate", RATE)
    # SAP shows an indirect quotation as a negative rate: -0.00333 means 1 / 0.00333
    rate = (
        F.when(currency == F.lit(scope.currency), F.lit(1).cast(RATE))
        .when(raw_rate < 0, F.lit(1) / F.abs(raw_rate))
        .otherwise(raw_rate)
    )

    def money(col: str) -> Column:
        return F.round(F.coalesce(dec(col, MONEY), F.lit(0)) * rate * sign, 2).cast(MONEY)

    d = date_part("billingdocumentdate")
    month = F.month(d)
    m0 = F.lit(fiscal_start_month)
    cancels_or_credits = category.isin("N", "S")
    return kept.select(
        txt("billingdocument").alias("invoice_number"),
        txt("billingdocumentitem").alias("invoice_item"),
        txt("billingdocumenttype").alias("invoice_type"),
        txt("billingdocumentcategory").alias("invoice_category"),
        category.alias("sd_document_category"),
        d.alias("invoice_date"),
        date_part("creationdate").alias("created_date"),
        date_part("lastchangedate").alias("last_changed_date"),
        # fiscal year/period from the date: the source leaves 0000 on older documents
        F.when(month >= m0, F.year(d)).otherwise(F.year(d) - 1).cast("int").alias("fiscal_year"),
        ((month - m0 + F.lit(12)) % F.lit(12) + F.lit(1)).cast("int").alias("fiscal_period"),
        txt("companycode").alias("company_code"),
        txt("salesorganization").alias("sales_org"),
        txt("distributionchannel").alias("distribution_channel"),
        txt("division").alias("division"),
        txt("salesoffice").alias("sales_office"),
        txt("salesgroup").alias("sales_group"),
        txt("salesdistrict").alias("region"),
        txt("plant").alias("plant"),
        txt("storagelocation").alias("storage_location"),
        F.lpad(txt("soldtoparty"), 10, "0").alias("customer_id"),
        txt("customergroup").alias("customer_group"),
        txt("material").alias("product_id"),
        txt("materialgroup").alias("product_group"),
        (F.coalesce(dec("billingquantity", QTY), F.lit(0)) * sign).cast(QTY).alias("quantity"),
        txt("billingquantityunit").alias("sales_unit"),
        (F.coalesce(dec("billingquantityinbaseunit", QTY), F.lit(0)) * sign).cast(QTY).alias("base_quantity"),
        txt("baseunit").alias("base_unit"),
        money("netamount").alias("revenue"),
        # Gross = net + VAT, never the source's grossamount: the client's SAP leaves it 0.00 on every line (30 Oct)
        (money("netamount") + money("taxamount")).cast(MONEY).alias("gross_revenue"),
        money("taxamount").alias("tax_amount"),
        money("costamount").alias("cost_amount"),
        currency.alias("currency"),
        raw_rate.alias("exchange_rate"),
        dec("netamount", MONEY).alias("document_net_amount"),  # as in the source: unsigned, document currency
        txt("salesdocument").alias("sales_order_number"),
        txt("salesdocumentitem").alias("sales_order_item"),
        txt("referencesddocument").alias("reference_document"),
        txt("referencesddocumentitem").alias("reference_item"),
        F.when(cancels_or_credits, txt("cancelledbillingdocument"))
        .when(category.isin("O", "P"), txt("referencesddocument"))
        .alias("reference_invoice_number"),
        F.coalesce(F.upper(txt("billingdocumentiscancelled")) == "X", F.lit(False)).alias("source_cancelled_flag"),
        synced_ts().alias("_source_synced_ts"),
        "_source_file",
        F.expr("try_cast(_business_date AS DATE)").alias("_business_date"),
        "_load_ts",
    )


def flag_cancellations(lines: DataFrame) -> DataFrame:
    """Mark invoice lines that were cancelled (an N document reverses them, or SAP's own flag is set).

    Revenue still nets through the cancellation rows, as in the sales pipeline: never filter these out.
    """
    cancels = (
        lines.filter(
            (F.col("sd_document_category") == CANCELLATION_CATEGORY) & F.col("reference_invoice_number").isNotNull()
        )
        .select(F.col("reference_invoice_number").alias("invoice_number"), F.col("invoice_number").alias("_by"))
        .dropDuplicates(["invoice_number"])
    )
    out = lines.join(cancels, "invoice_number", "left")
    cancelled = (F.col("sd_document_category") == SALES_CATEGORY) & (
        F.col("_by").isNotNull() | F.col("source_cancelled_flag")
    )
    return (
        out.withColumn("is_cancelled", cancelled)
        .withColumn("cancelled_by", F.when(cancelled, F.col("_by")))
        .withColumn("arrival_delay_days", F.lit(None).cast("int"))
        .drop("_by")
    )


def enrich(lines: DataFrame, customers: DataFrame) -> DataFrame:
    """Fill blank codes on the line from the customer master (district, sales office, customer group)."""
    cust = customers.select(
        "customer_id",
        F.col("region").alias("_c_region"),
        F.col("sales_office").alias("_c_office"),
        F.col("customer_group").alias("_c_group"),
    )
    e = lines.join(F.broadcast(cust), "customer_id", "left")
    return (
        e.withColumn("region", F.coalesce(F.col("region"), F.col("_c_region")))
        .withColumn("sales_office", F.coalesce(F.col("sales_office"), F.col("_c_office")))
        .withColumn("customer_group", F.coalesce(F.col("customer_group"), F.col("_c_group")))
        .drop("_c_region", "_c_office", "_c_group")
    )


def quarantine_frame(rejected: DataFrame, run_id: str) -> DataFrame:
    """Same shape as the sales silver.quarantine."""
    src = [c for c in rejected.columns if not c.startswith("_") and c != "billing_month"]
    return rejected.select(
        txt("billingdocument").alias("invoice_number"),
        txt("billingdocumentitem").alias("invoice_item"),
        F.col("_reasons").alias("reasons"),
        "_source_file",
        F.expr("try_cast(_business_date AS DATE)").alias("business_date"),
        "_load_ts",
        F.to_json(F.struct(*[F.col(f"`{c}`") for c in src])).alias("raw_row"),
        F.current_timestamp().alias("quarantined_ts"),
        F.lit(run_id).alias("run_id"),
    )


# ---------------------------------------------------------------------------
# Masters and text tables
# ---------------------------------------------------------------------------


def text_lookup(
    raw: DataFrame | None, spark: SparkSession, code_col: str, name_col: str, code: str, name: str
) -> DataFrame:
    """English names from a SAP text table: one row per code. Missing table -> empty lookup (names Unassigned)."""
    if raw is None:
        return spark.createDataFrame([], f"`{code}` string, `{name}` string")
    t = sv.latest_delivery(raw).filter(F.upper(txt("language")) == TEXT_LANGUAGE)
    t = t.select(txt(code_col).alias(code), txt(name_col).alias(name)).filter(F.col(code).isNotNull())
    return sv.latest(t, [code], [name])


def regions_lookup(texts: DataFrame, country: str) -> DataFrame:
    """District code -> name. Province is not in SAP: Unassigned until the business gives the mapping."""
    return texts.select(
        "region",
        F.coalesce(F.col("district_name"), F.lit(UNASSIGNED)).alias("district_name"),
        F.lit(UNASSIGNED).alias("province"),
        F.lit(country).alias("country"),
    )


def build_customers(raw: DataFrame, lines: DataFrame, groups: DataFrame, regions: DataFrame) -> DataFrame:
    """One row per customer, latest version. Personal columns are never selected; the display name is the id.

    Billed customers missing from the master get an "Unknown" member, so no line loses its customer.
    """
    c = raw.withColumn("customer_id", F.lpad(txt("customer"), 10, "0")).filter(F.col("customer_id").isNotNull())
    c = newest_first(c, ["customer_id"]).filter(F.col("_rn") == 1)
    known = c.select(
        "customer_id",
        F.concat(F.lit("Customer "), F.col("customer_id")).alias("customer_name"),
        F.concat(F.lit("Customer "), F.col("customer_id")).alias("customer_full_name"),
        txt("customergroup").alias("customer_group"),
        F.coalesce(sv.city_key(txt("cityname")), F.lit(UNASSIGNED)).alias("city"),
        F.coalesce(txt("salesdistrict"), txt("region")).alias("region"),
        txt("salesoffice").alias("sales_office"),
        txt("distributionchannel").alias("distribution_channel"),
        txt("salesorganization").alias("sales_org"),
        F.lpad(txt("payercustomer"), 10, "0").alias("payer_customer_id"),
        date_part("creationdate").alias("created_date"),
        is_deleted().alias("deleted_in_source"),
        synced_ts().alias("last_updated_timestamp"),
        F.lit(True).alias("in_master"),
    )
    unknown = (
        lines.select("customer_id")
        .distinct()
        .join(known.select("customer_id"), "customer_id", "left_anti")
        .select(
            "customer_id",
            F.concat(F.lit("Unknown customer "), F.col("customer_id")).alias("customer_name"),
            F.concat(F.lit("Unknown customer "), F.col("customer_id")).alias("customer_full_name"),
            F.lit(UNASSIGNED).alias("city"),
            F.lit(False).alias("deleted_in_source"),
            F.lit(False).alias("in_master"),
        )
    )
    out = known.unionByName(unknown, allowMissingColumns=True)
    out = out.join(F.broadcast(groups), "customer_group", "left").join(F.broadcast(regions), "region", "left")
    return out.select(
        "customer_id",
        "customer_name",
        "customer_full_name",
        "customer_group",
        F.coalesce(F.col("customer_group_name"), F.lit(UNASSIGNED)).alias("customer_group_name"),
        "city",
        "region",
        F.coalesce(F.col("district_name"), F.lit(UNASSIGNED)).alias("district_name"),
        F.coalesce(F.col("province"), F.lit(UNASSIGNED)).alias("province"),
        "sales_office",
        "distribution_channel",
        "sales_org",
        "payer_customer_id",
        "created_date",
        F.lit(False).alias("city_mapped"),  # no canonical city list for the source yet
        "in_master",
        "deleted_in_source",
        "last_updated_timestamp",
    )


def build_products(
    raw: DataFrame, lines: DataFrame, groups: DataFrame, types: DataFrame, divisions: DataFrame
) -> DataFrame:
    """One row per material, latest version, names from the text tables; Unknown members for billed gaps.

    Group and company fall back to the most common values on the material's billing lines.
    """
    p = raw.withColumn("product_id", txt("product")).filter(F.col("product_id").isNotNull())
    p = newest_first(p, ["product_id"]).filter(F.col("_rn") == 1)
    known = p.select(
        "product_id",
        F.upper(txt("productdescription")).alias("product_description"),
        txt("productgroup").alias("product_group"),
        txt("producttype").alias("product_type"),
        txt("division").alias("division"),
        txt("plant").alias("plant"),
        txt("baseunit").alias("base_unit"),
        date_part("creationdate").alias("launch_date"),
        is_deleted().alias("deleted_in_source"),
        synced_ts().alias("last_updated_timestamp"),
        F.lit(True).alias("in_master"),
    )
    seen = most_common(lines, "product_id", ["product_group", "company_code"]).select(
        "product_id", F.col("product_group").alias("_l_group"), F.col("company_code").alias("_l_company")
    )
    unknown = (
        lines.select("product_id")
        .distinct()
        .join(known.select("product_id"), "product_id", "left_anti")
        .select(
            "product_id",
            F.concat(F.lit("UNKNOWN PRODUCT "), F.col("product_id")).alias("product_description"),
            F.lit(False).alias("deleted_in_source"),
            F.lit(False).alias("in_master"),
        )
    )
    out = known.unionByName(unknown, allowMissingColumns=True).join(seen, "product_id", "left")
    out = out.withColumn("product_group", F.coalesce(F.col("product_group"), F.col("_l_group")))
    out = (
        out.join(F.broadcast(groups), "product_group", "left")
        .join(F.broadcast(types), "product_type", "left")
        .join(F.broadcast(divisions), "division", "left")
    )
    return out.select(
        "product_id",
        F.coalesce(F.col("product_description"), F.col("product_id")).alias("product_description"),
        "product_group",
        F.coalesce(F.col("product_group_name"), F.lit(UNASSIGNED)).alias("product_group_name"),
        "product_type",
        F.coalesce(F.col("product_type_name"), F.lit(UNASSIGNED)).alias("product_type_name"),
        "division",
        F.coalesce(F.col("division_name"), F.lit(UNASSIGNED)).alias("division_name"),
        F.col("_l_company").alias("company_code"),
        "plant",
        "base_unit",
        F.lit(None).cast(MONEY).alias("list_price"),  # not in the billing contract (pricing tables: later)
        F.lit(None).cast(MONEY).alias("standard_cost"),  # cost comes from the billing line itself
        "launch_date",
        "in_master",
        "deleted_in_source",
        "last_updated_timestamp",
    )


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


def _read(lake: Lake, name: str) -> DataFrame | None:
    return lake.read("bronze", name) if lake.exists("bronze", name) else None


def _require(lake: Lake, name: str) -> DataFrame:
    if not lake.exists("bronze", name):
        raise FileNotFoundError(f"bronze.{name} is missing: the SAP masters have not been loaded")
    return lake.read("bronze", name)


def run_sap_silver(spark: SparkSession, cfg: Config, lake: Lake | None = None) -> SapSilverRun:
    lake = lake or Lake(spark, cfg)
    scope = scope_of(cfg)
    run = SapSilverRun(run_id=f"sap-silver-{datetime.now(UTC):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}")
    now = datetime.now(UTC)
    dq: list[tuple] = []

    def check(name: str, target: str, expected, actual, ok: bool, warn_only: bool = False) -> None:
        status = "PASS" if ok else ("WARN" if warn_only else "FAIL")
        dq.append((run.run_id, now, "silver", name, target, None, None, str(expected), str(actual), status))
        if status == "WARN":
            run.warnings.append(f"{name}: {actual}")

    # 1. Every bronze row gets one fate; the fates must add up to the source's own totals
    fated = assign_fates(lake.read("bronze", "billing_items"), scope)
    lake.overwrite(fated.drop("_run_id", "_rescued_data"), "ops", "sap_billing_rows")  # computed once, read 4x
    fated = lake.read("ops", "sap_billing_rows")
    counts = fate_counts(fated).withColumn("run_id", F.lit(run.run_id))
    lake.overwrite(counts, "ops", "sap_row_fates")
    counts = lake.read("ops", "sap_row_fates")
    run.fates = {r["fate"]: int(r["n"]) for r in counts.groupBy("fate").agg(F.sum("line_count").alias("n")).collect()}

    source_raw = _read(lake, "source_counts")
    if source_raw is None:
        check("copy_complete_vs_source_counts", "bronze.billing_items", "source_counts delivered", "missing", False)
    else:
        lake.overwrite(typed_source_counts(source_raw), "ops", "source_counts")
        result = compare_to_source(lake.read("ops", "source_counts"), counts)
        bad = result.filter(F.col("status") == "FAIL").count()
        check("copy_complete_vs_source_counts", "bronze.billing_items", 0, f"{bad} bucket(s) differ", bad == 0)

    # 2. Quarantine
    rejected = fated.filter(F.col("_fate") == QUARANTINED)
    lake.overwrite(quarantine_frame(rejected, run.run_id), "silver", "quarantine")
    run.quarantined = lake.read("silver", "quarantine").count()

    # 3. Lookups from the English text tables
    def lookup(name: str, code: str, label: str) -> DataFrame:
        code_col, name_col = TEXT_TABLES[name]
        return text_lookup(_read(lake, f"masters_text_{name}"), spark, code_col, name_col, code, label)

    offices = lookup("sales_offices", "sales_office", "sales_office_name")
    channels = lookup("distribution_channels", "distribution_channel", "distribution_channel_name")
    regions = regions_lookup(lookup("regions", "region", "district_name"), cfg.business.get("country", "LK"))
    for name, df in (("sales_offices", offices), ("distribution_channels", channels), ("regions", regions)):
        lake.overwrite(df, "silver", name)
        run.rows[name] = lake.read("silver", name).count()
    regions = lake.read("silver", "regions")

    # 4. Lines (before enrichment, for the Unknown members), customers, products, then enriched lines
    m0 = int(cfg.business.get("fiscal_year_start_month", 4))
    base = to_invoice_lines(fated.filter(F.col("_fate") == KEPT), scope, m0)
    customers = build_customers(
        _require(lake, "masters_customer_master"),
        base,
        lookup("customer_groups", "customer_group", "customer_group_name"),
        regions.select("region", "district_name", "province"),
    )
    lake.overwrite(customers, "silver", "customers")
    customers = lake.read("silver", "customers")
    products = build_products(
        _require(lake, "masters_material_master"),
        base,
        lookup("material_groups", "product_group", "product_group_name"),
        lookup("material_types", "product_type", "product_type_name"),
        lookup("divisions", "division", "division_name"),
    )
    lake.overwrite(products, "silver", "products")
    products = lake.read("silver", "products")

    lines = flag_cancellations(enrich(base, customers))
    lake.overwrite(lines, "silver", "invoice_lines")
    lines = lake.read("silver", "invoice_lines")
    for name, df in (("invoice_lines", lines), ("customers", customers), ("products", products)):
        run.rows[name] = df.count()

    # 5. Checks
    kept = run.fates.get(KEPT, 0)
    check(
        "lines_equal_kept_rows",
        "silver.invoice_lines",
        kept,
        run.rows["invoice_lines"],
        kept == run.rows["invoice_lines"],
    )
    dup_keys = run.rows["invoice_lines"] - lines.select("invoice_number", "invoice_item").distinct().count()
    check("line_keys_unique", "silver.invoice_lines", 0, dup_keys, dup_keys == 0)
    for col in ("invoice_date", "customer_id", "product_id", "company_code", "sd_document_category"):
        nulls = lines.filter(F.col(col).isNull()).count()
        check(f"no_blank_{col}", "silver.invoice_lines", 0, nulls, nulls == 0)
    for table, key, df in (("customers", "customer_id", customers), ("products", "product_id", products)):
        distinct = df.select(key).distinct().count()
        check(f"{key}s_unique", f"silver.{table}", run.rows[table], distinct, distinct == run.rows[table])
    leaked = sorted(set(PERSONAL_COLUMNS) & set(customers.columns))
    check("no_personal_columns", "silver.customers", "none", leaked or "none", not leaked)
    for table, key, df in (("customers", "customer_id", customers), ("products", "product_id", products)):
        missing = lines.select(key).distinct().join(df.select(key), key, "left_anti").count()
        check(f"every_line_has_a_{key}", f"silver.{table}", 0, missing, missing == 0)

    # Counted, not failed (contract: "Checks (counted, not quarantined)")
    def info(name: str, target: str, n: int) -> None:
        check(name, target, 0, n, n == 0, warn_only=True)

    info("lines_in_foreign_currency", "silver.invoice_lines", lines.filter(F.col("currency") != scope.currency).count())
    info("unknown_customers", "silver.customers", customers.filter(~F.col("in_master")).count())
    info("unknown_products", "silver.products", products.filter(~F.col("in_master")).count())
    info("lines_without_district", "silver.invoice_lines", lines.filter(F.col("region").isNull()).count())
    negative = fated.filter(
        (F.col("_fate") == KEPT) & ((dec("netamount", MONEY) < 0) | (dec("billingquantity", QTY) < 0))
    ).count()
    info("negative_values_in_source", "bronze.billing_items", negative)

    lake.append(spark.createDataFrame(dq, b.DQ_SCHEMA), *b.DQ_RESULTS)
    run.dq_failures = sum(1 for r in dq if r[-1] == "FAIL")
    return run


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    parser = argparse.ArgumentParser(description="Rebuild the client area's silver tables from SAP-shaped bronze.")
    parser.add_argument("--area", default="client", help="business area with SAP-shaped data (default client)")
    args = parser.parse_args(argv)
    cfg = load_config(area=args.area)
    run = run_sap_silver(get_spark(cfg, app_name="sap-silver"), cfg)
    print(f"  run {run.run_id}")
    for fate in FATES:
        print(f"    {fate:<22} {run.fates.get(fate, 0):,} rows")
    for name, rows in sorted(run.rows.items()):
        print(f"    silver.{name:<24} {rows:,} rows")
    for w in run.warnings:
        print(f"  note: {w}")
    print(f"  data-quality failures: {run.dq_failures}" + ("  <-- check ops.dq_results" if run.dq_failures else ""))


if __name__ == "__main__":
    main()
