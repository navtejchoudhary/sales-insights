"""SAP-shaped dummy data for the real-data phase (see docs/sap_input_contract.md).

The real source is a warehouse holding raw SAP tables, copied there by a replication tool (Fivetran).
This module writes the SAME shape from our dummy business, so the pipeline can be built and proven
without reading a single row from the real source:

    staging/sap/billing_items.csv        the 40 contract columns, SAP field names, one row per SAP record
    staging/sap/customer_master.csv      sold-to customers incl. fake personal columns (never stored by silver)
    staging/sap/material_master.csv      materials
    staging/sap/text_<name>.csv          lookup tables: code, language (E and D), name
    staging/sap/source_counts.csv        control totals as the real copy will record them
    staging/sap/sap_manifest.json        written LAST: rows, SHA-256, every planted case

How it is built: the invoice lines come from the shared engine (generator/engine.py, same stories as the
MVP) and are reshaped to SAP fields: amounts and quantities become positive (SAP stores the meaning in
`sddocumentcategory`), dates become timestamps, `_fivetran_synced` / `_fivetran_deleted` are added.
Then the real-world cases we found in the source are planted, each at a fixed rate and logged:

    pro-forma copies of invoices (category U)        down-payment request + its cancellation (FAZ/FAS)
    cancellations of credit memos (S2, category S)   debit memos (ZADR, category P)
    a few export invoices in USD with a rate         older versions of a line (same key, older sync time)
    lines deleted in SAP (_fivetran_deleted)         missing and impossible billing dates (e.g. year 4334)
    fiscal year left as 0000 on older documents      lines of a second company code (from the engine)
    one billed customer missing from the customer master

Deterministic: every choice is seeded from what it is about (invoice number, line), never from a shared
stream, so the output is identical on every run. Plain Python (NumPy, pandas), like the other generators.

Run:
    uv run python -m sales_insights.generator.sap
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from sales_insights.generator import engine as e
from sales_insights.generator import history as h
from sales_insights.generator import masters as m

# The 40 billing-item columns of the contract, in contract order (SAP field names, lower case)
BILLING_COLUMNS = [
    "billingdocument",
    "billingdocumentitem",
    "billingdocumenttype",
    "billingdocumentcategory",
    "sddocumentcategory",
    "billingdocumentdate",
    "creationdate",
    "lastchangedate",
    "billingdocumentiscancelled",
    "cancelledbillingdocument",
    "referencesddocument",
    "referencesddocumentitem",
    "salesdocument",
    "salesdocumentitem",
    "companycode",
    "salesorganization",
    "distributionchannel",
    "division",
    "salesoffice",
    "salesgroup",
    "salesdistrict",
    "plant",
    "storagelocation",
    "soldtoparty",
    "customergroup",
    "material",
    "materialgroup",
    "billingquantity",
    "billingquantityunit",
    "billingquantityinbaseunit",
    "baseunit",
    "netamount",
    "grossamount",
    "taxamount",
    "costamount",
    "transactioncurrency",
    "accountingexchangerate",
    "fiscalyear",
    "fiscalperiod",
    "_fivetran_synced",
    "_fivetran_deleted",
]

CUSTOMER_COLUMNS = [
    "customer",
    "customername",  # personal / identifying: silver never stores it
    "streetname",  # personal
    "telephonenumber1",  # personal
    "emailaddress",  # personal
    "taxnumber1",  # personal (VAT number)
    "cityname",
    "postalcode",
    "region",
    "country",
    "customergroup",
    "salesoffice",
    "salesdistrict",
    "distributionchannel",
    "salesorganization",
    "division",
    "payercustomer",
    "creationdate",
    "_fivetran_synced",
    "_fivetran_deleted",
]
PERSONAL_CUSTOMER_COLUMNS = ["customername", "streetname", "telephonenumber1", "emailaddress", "taxnumber1"]

MATERIAL_COLUMNS = [
    "product",
    "productdescription",
    "producttype",
    "productgroup",
    "division",
    "baseunit",
    "plant",
    "salesorganization",
    "creationdate",
    "_fivetran_synced",
    "_fivetran_deleted",
]

# Text (lookup) tables: logical name -> (code column, name column). One row per code and language.
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
LANGUAGES = ("E", "D")  # the source keeps English and German texts; the pipeline uses E

DOCUMENT_TYPES = [
    # type, category (sddocumentcategory), billing category, name
    ("ZAOR", "M", "L", "Standard invoice"),
    ("ZFOC", "M", "L", "Free of charge delivery"),
    ("ZARE", "O", "L", "Return credit memo"),
    ("ZACR", "O", "A", "Price correction credit"),
    ("ZADR", "P", "A", "Debit memo"),
    ("S1", "N", "L", "Invoice cancellation"),
    ("S2", "S", "L", "Credit memo cancellation"),
    ("F5", "U", "A", "Pro forma invoice"),
    ("FAZ", "M", "P", "Down payment request"),
    ("FAS", "N", "P", "Cancel down payment request"),
]

# Planted cases (share of invoices or lines; each one is logged in the manifest) -------------------
PRO_FORMA_RATE = 0.03  # of invoices: a pro-forma copy (F5) of the invoice, same lines
DOWN_PAYMENT_RATE = 0.002  # of invoices: a down-payment request (FAZ) and its cancellation (FAS)
CREDIT_CANCEL_RATE = 0.10  # of return credit memos: cancelled by an S2 a few days later
DEBIT_MEMO_RATE = 0.002  # of invoices: a debit memo (ZADR) for a surcharge
OLDER_VERSION_RATE = 0.01  # of lines: an older version of the same line is still in the table
DELETED_RATE = 0.001  # of lines: deleted in SAP (flag set, row kept)
MISSING_DATE_RATE = 0.0002  # of lines: billing date empty
IMPOSSIBLE_DATE_RATE = 0.0002  # of lines: billing date with a typo year
IMPOSSIBLE_YEARS = [2033, 2048, 2201, 3024, 4322, 4334]  # as seen in the source
USD_SHARE_OF_EXPORTS = 0.02  # of export invoices (channel 20): billed in USD (rare in the source)
USD_RATE_BASE = 300.0  # LKR per USD (varies a little by month)
FISCAL_ZERO_BEFORE_DAYS = 365  # documents older than this keep fiscal year 0000 (as in the source)

# Document numbers for the planted documents: first digit + digits 2-10 of the document they relate to
PREFIX = {"F5": "5", "FAZ": "2", "FAS": "1", "ZADR": "3", "S2": "4"}


def _rng(seed: int, kind: int, *ids: int) -> np.random.Generator:
    return np.random.default_rng([seed, kind, *ids])


def _ts(d: date | datetime) -> str:
    """Timestamp text as the source renders it (dates are midnight, no time zone)."""
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return f"{d.isoformat()} 00:00:00"


def _abs(x: str, digits: int) -> str:
    return f"{abs(float(x)):.{digits}f}"


def _sap_line(line: dict, synced: datetime, as_of: date) -> dict:
    """One engine line (reference extract shape) -> one SAP billing item (positive values, SAP names)."""
    d = date.fromisoformat(line["invoice_date"])
    net = abs(float(line["revenue"]))
    tax = abs(float(line["tax_amount"]))
    cost = abs(float(line["revenue"]) - float(line["profit"]))
    free_of_charge = line["invoice_type"] == "ZFOC"
    if free_of_charge:
        net = 0.0  # SAP: no net value on free goods; the VAT the company bears stays in taxamount
    changed = datetime.fromisoformat(line["last_updated_timestamp"])
    old = (as_of - d).days > FISCAL_ZERO_BEFORE_DAYS
    fy, fp, _ = e.fiscal(d)
    reverses = line["reference_invoice_number"]
    is_cancellation = line["invoice_type"] == "S1"
    return {
        "billingdocument": line["invoice_number"],
        "billingdocumentitem": line["invoice_item"],
        "billingdocumenttype": line["invoice_type"],
        "billingdocumentcategory": line["invoice_category"],
        "sddocumentcategory": line["sd_document_category"],
        "billingdocumentdate": _ts(d),
        "creationdate": _ts(d),
        "lastchangedate": _ts(changed.date()),
        "billingdocumentiscancelled": "X" if line["billing_status"] == "C" else "",
        "cancelledbillingdocument": reverses if is_cancellation else "",
        "referencesddocument": reverses if reverses and not is_cancellation else "",
        "referencesddocumentitem": line["invoice_item"] if reverses and not is_cancellation else "",
        "salesdocument": "1" + line["invoice_number"][1:],
        "salesdocumentitem": line["invoice_item"],
        "companycode": line["company_code"],
        "salesorganization": line["sales_org"],
        "distributionchannel": line["distribution_channel"],
        "division": line["division"],
        "salesoffice": line["sales_office"],
        "salesgroup": "",
        "salesdistrict": line["region"],
        "plant": line["plant"],
        "storagelocation": line["storage_location"],
        "soldtoparty": line["customer_id"],
        "customergroup": line["customer_group"],
        "material": line["product_id"],
        "materialgroup": line["product_group"],
        "billingquantity": _abs(line["quantity"], 3),
        "billingquantityunit": line["billing_quantity_unit"],
        "billingquantityinbaseunit": _abs(line["quantity"], 3),
        "baseunit": line["base_unit"],
        "netamount": f"{net:.2f}",
        "grossamount": f"{net + tax:.2f}",
        "taxamount": f"{tax:.2f}",
        "costamount": f"{cost:.2f}",
        "transactioncurrency": line["document_currency"],
        "accountingexchangerate": "1.00000",
        "fiscalyear": "0000" if old else str(fy),
        "fiscalperiod": "000" if old else f"{fp:03d}",
        "_fivetran_synced": _ts(synced),
        "_fivetran_deleted": "false",
    }


def _synced(seed: int, number: str, item: str, changed: str) -> datetime:
    """The replication tool copies a change a few hours after it happens."""
    rng = _rng(seed, 21, int(number), int(item))
    return datetime.fromisoformat(changed) + timedelta(minutes=int(rng.integers(30, 6 * 60)))


def _related(row: dict, doc_type: str, number: str, on: date, synced: datetime) -> dict:
    """A planted document derived from an existing line (pro-forma, debit memo, S2, down payment)."""
    category, billing_category = {t: (c, b) for t, c, b, _ in DOCUMENT_TYPES}[doc_type]
    out = dict(row)
    out.update(
        billingdocument=number,
        billingdocumenttype=doc_type,
        billingdocumentcategory=billing_category,
        sddocumentcategory=category,
        billingdocumentdate=_ts(on),
        creationdate=_ts(on),
        lastchangedate=_ts(on),
        billingdocumentiscancelled="",
        cancelledbillingdocument="",
        referencesddocument=row["billingdocument"],
        referencesddocumentitem=row["billingdocumentitem"],
        _fivetran_synced=_ts(synced),
    )
    return out


def _scale_money(row: dict, factor: float) -> None:
    for col in ("netamount", "grossamount", "taxamount", "costamount"):
        row[col] = f"{float(row[col]) * factor:.2f}"


def plant_cases(rows: list[dict], seed: int, as_of: date) -> tuple[list[dict], list[dict]]:
    """Add the real-world cases to the SAP billing items. Returns (rows, log of every planted case)."""
    end = as_of - timedelta(days=1)
    log: list[dict] = []
    out: list[dict] = []
    by_doc: dict[str, list[dict]] = {}
    for r in rows:
        by_doc.setdefault(r["billingdocument"], []).append(r)

    for number, lines in by_doc.items():
        first = lines[0]
        doc_type = first["billingdocumenttype"]
        on = date.fromisoformat(first["billingdocumentdate"][:10])
        rng = _rng(seed, 20, int(number))
        u = rng.random(4)

        # A few export invoices are billed in USD (all lines of the document)
        if first["distributionchannel"] == "20" and rng.random() < USD_SHARE_OF_EXPORTS:
            rate = USD_RATE_BASE + (on.month - 6) * 1.5
            for r in lines:
                _scale_money(r, 1 / rate)
                r["transactioncurrency"] = "USD"
                r["accountingexchangerate"] = f"{rate:.5f}"
            log.append({"case": "usd_invoice", "billingdocument": number, "lines": len(lines)})

        out.extend(lines)
        stamp = datetime.fromisoformat(first["_fivetran_synced"])
        if doc_type == "ZAOR":
            if u[0] < PRO_FORMA_RATE:
                pf = PREFIX["F5"] + number[1:]
                out.extend(_related(r, "F5", pf, on, stamp) for r in lines)
                log.append({"case": "pro_forma", "billingdocument": pf, "lines": len(lines)})
            if u[1] < DOWN_PAYMENT_RATE:
                for t in ("FAZ", "FAS"):
                    dp = PREFIX[t] + number[1:]
                    row = _related(first, t, dp, on, stamp)
                    _scale_money(row, 0.3)
                    row["billingquantity"] = row["billingquantityinbaseunit"] = "0.000"
                    out.append(row)
                    log.append({"case": "down_payment", "billingdocument": dp, "lines": 1})
            if u[2] < DEBIT_MEMO_RATE:
                dm = PREFIX["ZADR"] + number[1:]
                when = min(on + timedelta(days=int(rng.integers(3, 20))), end)
                row = _related(first, "ZADR", dm, when, stamp + timedelta(days=(when - on).days))
                _scale_money(row, 0.02)
                row["billingquantity"] = row["billingquantityinbaseunit"] = "0.000"
                out.append(row)
                log.append({"case": "debit_memo", "billingdocument": dm, "lines": 1})
        elif doc_type == "ZARE" and u[3] < CREDIT_CANCEL_RATE:
            when = on + timedelta(days=int(rng.integers(1, 10)))
            if when <= end:
                s2 = PREFIX["S2"] + number[1:]
                for r in lines:
                    row = _related(r, "S2", s2, when, stamp + timedelta(days=(when - on).days))
                    row["referencesddocument"] = row["referencesddocumentitem"] = ""
                    row["cancelledbillingdocument"] = number
                    out.append(row)
                log.append({"case": "credit_memo_cancellation", "billingdocument": s2, "lines": len(lines)})

    final: list[dict] = []
    for r in out:
        rng = _rng(seed, 22, int(r["billingdocument"]), int(r["billingdocumentitem"]))
        u = rng.random(4)
        key = {"billingdocument": r["billingdocument"], "billingdocumentitem": r["billingdocumentitem"]}
        if u[0] < OLDER_VERSION_RATE:  # an older version of the line is still in the table
            older = dict(r)
            older["_fivetran_synced"] = _ts(
                datetime.fromisoformat(r["_fivetran_synced"]) - timedelta(days=int(rng.integers(1, 6)))
            )
            _scale_money(older, 0.97)
            final.append(older)
            log.append({"case": "older_version", **key})
        if u[1] < DELETED_RATE:
            r["_fivetran_deleted"] = "true"
            log.append({"case": "deleted_in_source", **key})
        if u[2] < MISSING_DATE_RATE:
            r["billingdocumentdate"] = ""
            log.append({"case": "missing_billing_date", **key})
        elif u[2] < MISSING_DATE_RATE + IMPOSSIBLE_DATE_RATE:
            year = IMPOSSIBLE_YEARS[int(rng.integers(len(IMPOSSIBLE_YEARS)))]
            r["billingdocumentdate"] = f"{year}{r['billingdocumentdate'][4:]}"
            log.append({"case": "impossible_billing_date", "year": year, **key})
        final.append(r)
    return final, log


def build_billing_items(seed: int, as_of: date, start: date | None = None) -> tuple[pd.DataFrame, list[dict]]:
    """SAP billing items for the history window (or from `start`, for fast tests) + the planted-case log."""
    lines = h.build_history(seed, as_of, start).to_dict("records")
    rows = [
        _sap_line(ln, _synced(seed, ln["invoice_number"], ln["invoice_item"], ln["last_updated_timestamp"]), as_of)
        for ln in lines
    ]
    rows, log = plant_cases(rows, seed, as_of)
    df = pd.DataFrame(rows, columns=BILLING_COLUMNS)
    order = ["billingdocument", "billingdocumentitem", "_fivetran_synced"]
    return df.sort_values(order, kind="stable").reset_index(drop=True), log


def build_customer_master(tables: dict[str, pd.DataFrame], seed: int) -> pd.DataFrame:
    rows = []
    for c in tables["customers"].to_dict("records"):
        rng = _rng(seed, 30, int(c["customer_id"]))
        n = int(c["customer_id"]) % 100000
        rows.append(
            {
                "customer": c["customer_id"],
                "customername": c["customer_full_name"],
                "streetname": f"No. {int(rng.integers(1, 400))}, Main Street",
                "telephonenumber1": f"000-{n:07d}",  # obviously fake
                "emailaddress": f"customer{n}@example.com",
                "taxnumber1": f"TEST{n:09d}",
                "cityname": c["city"],
                "postalcode": f"{int(rng.integers(10000, 99999))}",
                "region": c["region"],
                "country": c["country"],
                "customergroup": c["customer_group"],
                "salesoffice": c["sales_office"],
                "salesdistrict": c["region"],
                "distributionchannel": c["distribution_channel"],
                "salesorganization": c["sales_org"],
                "division": "04",
                "payercustomer": c["payer_customer_id"],
                "creationdate": _ts(date.fromisoformat(c["created_date"])),
                "_fivetran_synced": _ts(datetime.fromisoformat(c["last_updated_timestamp"]) + timedelta(hours=2)),
                "_fivetran_deleted": "false",
            }
        )
    return pd.DataFrame(rows, columns=CUSTOMER_COLUMNS)


def build_material_master(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = [
        {
            "product": p["product_id"],
            "productdescription": p["product_description"],
            "producttype": p["product_type"],
            "productgroup": p["product_group"],
            "division": p["division"],
            "baseunit": p["base_unit"],
            "plant": p["plant"],
            "salesorganization": p["company_code"],
            "creationdate": _ts(date.fromisoformat(p["launch_date"])),
            "_fivetran_synced": _ts(datetime.fromisoformat(p["last_updated_timestamp"]) + timedelta(hours=2)),
            "_fivetran_deleted": "false",
        }
        for p in tables["products"].to_dict("records")
    ]
    return pd.DataFrame(rows, columns=MATERIAL_COLUMNS)


def build_text_tables(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Lookup tables in SAP's text-table shape: code, language, name (English and a German copy)."""
    pairs = {
        "company_codes": tables["companies"][["company_code", "company_name"]].values,
        "sales_orgs": tables["companies"][["sales_org", "sales_org_name"]].values,
        "sales_offices": tables["sales_offices"][["sales_office", "sales_office_name"]].values,
        "distribution_channels": tables["distribution_channels"].values,
        "divisions": tables["divisions"].values,
        "customer_groups": tables["customer_groups"].values,
        "material_groups": tables["product_groups"].values,
        "material_types": tables["product_types"].values,
        "plants": tables["plants"][["plant", "plant_name"]].values,
        "regions": tables["regions"][["region", "district_name"]].values,
        "document_types": [(t, name) for t, _, _, name in DOCUMENT_TYPES],
    }
    out = {}
    for name, (code_col, name_col) in TEXT_TABLES.items():
        rows = [
            {"language": lang, code_col: code, name_col: text if lang == "E" else f"{text} (DE)"}
            for code, text in pairs[name]
            for lang in LANGUAGES
        ]
        out[name] = pd.DataFrame(rows, columns=["language", code_col, name_col])
    return out


def source_counts(billing: pd.DataFrame) -> pd.DataFrame:
    """Control totals as the real copy records them from the source: per company, billing month and
    document category, the number of lines and the sum of netamount (as stored, every row counted)."""
    month = billing["billingdocumentdate"].str[:7].where(billing["billingdocumentdate"] != "", "")
    g = billing.assign(billing_month=month, net=billing["netamount"].astype(float)).groupby(
        ["companycode", "billing_month", "sddocumentcategory"], dropna=False
    )
    out = g.agg(line_count=("net", "size"), netamount_sum=("net", "sum")).reset_index()
    out["netamount_sum"] = out["netamount_sum"].map(lambda x: f"{x:.2f}")
    return out.sort_values(["companycode", "billing_month", "sddocumentcategory"], kind="stable").reset_index(drop=True)


def build_sap(seed: int, as_of: date, start: date | None = None) -> tuple[dict[str, pd.DataFrame], list[dict]]:
    """Every SAP-shaped table + the planted-case log."""
    tables, _ = m.build_masters(seed, as_of, dirt=False)
    billing, log = build_billing_items(seed, as_of, start)
    customers = build_customer_master(tables, seed)

    # One billed customer is missing from the customer master (the pipeline gives it an "Unknown" member)
    billed = sorted(set(billing["soldtoparty"]) & set(customers["customer"]))
    orphan = billed[int(_rng(seed, 31).integers(len(billed)))]
    customers = customers[customers["customer"] != orphan].reset_index(drop=True)
    log.append({"case": "customer_missing_in_master", "customer": orphan})

    out = {
        "billing_items": billing,
        "customer_master": customers,
        "material_master": build_material_master(tables),
        **{f"text_{k}": v for k, v in build_text_tables(tables).items()},
    }
    out["source_counts"] = source_counts(billing)
    return out, log


def write_sap(out: dict[str, pd.DataFrame], log: list[dict], target: Path, seed: int, as_of: date) -> Path:
    target.mkdir(parents=True, exist_ok=True)
    files = {}
    for name, df in out.items():
        path = target / f"{name}.csv"
        df.to_csv(path, index=False, lineterminator="\n")
        files[path.name] = {"rows": len(df), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    cases: dict[str, int] = {}
    for item in log:
        cases[item["case"]] = cases.get(item["case"], 0) + 1
    manifest = {
        "kind": "sap",
        "source_system": "SAP billing items via a replication tool (simulated)",
        "as_of": as_of.isoformat(),
        "seed": seed,
        "contract": "docs/sap_input_contract.md",
        "personal_columns": {"customer_master": PERSONAL_CUSTOMER_COLUMNS},
        "files": files,
        "planted_cases": cases,
        "planted": log,
    }
    (target / "sap_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return target


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config

    cfg = load_config()
    parser = argparse.ArgumentParser(description="Generate SAP-shaped dummy data (real-data phase).")
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=date.fromisoformat(cfg.business["simulation_start"]),
        help="Data ends the day before (YYYY-MM-DD).",
    )
    parser.add_argument("--seed", type=int, default=cfg.business["random_seed"])
    parser.add_argument("--out", type=Path, default=Path(cfg.path(cfg.staging_path)) / "sap")
    args = parser.parse_args(argv)

    out, log = build_sap(args.seed, args.as_of)
    target = write_sap(out, log, args.out, args.seed, args.as_of)
    billing = out["billing_items"]
    print(f"  {len(billing):,} billing items, {billing['billingdocument'].nunique():,} documents")
    print(f"  by category {billing['sddocumentcategory'].value_counts().sort_index().to_dict()}")
    print(f"  planted {json.loads((target / 'sap_manifest.json').read_text())['planted_cases']}")
    print(f"SAP-shaped data written to {target}")


if __name__ == "__main__":
    main()
