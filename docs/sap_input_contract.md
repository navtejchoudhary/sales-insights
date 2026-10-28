# SAP input contract (real-data phase)

What the pipeline expects when it reads **real SAP sales data** instead of the dummy files: which tables,
which columns, which rules. Written with **SAP's standard field names** only; the source system's own
table names, the client's name and any connection details are never in this repo (see "Where the real
names live").

Status: draft (28 Oct 2026). Billing items are complete and have SAP-shaped dummy data (below); the master tables are completed once their column
lists are known. Design choices: `docs/decisions.md` (28 Oct 2026).

## How the data arrives

```
SAP ──(replication tool, e.g. Fivetran)──► source warehouse (raw SAP tables, one row per SAP record)
        │  read-only: copied once for the agreed history window, later only new/changed rows
        ▼
bronze (as received + load columns) ──► silver (rules below) ──► gold (same star schema as the MVP)
```

- The source keeps SAP's own field names in lower case (`billingdocument`, `soldtoparty` …).
- The replication tool adds `_fivetran_synced` (when the row was last copied) and `_fivetran_deleted`
  (deleted in SAP). Rows are the latest version of each SAP record, updated in place.
- Scope comes from config, never from code: company code(s), history window (`from`, `to`).
  Real-data phase: one company code, 25 months.

## Tables

| Logical name | SAP content | Grain | Used for |
|---|---|---|---|
| `billing_items` | Billing document items (invoices, credit/debit memos, cancellations, pro-formas) | billing document × item | `fact_sales` |
| `customer_master` | Customer (sold-to) master | customer | `dim_customer`, `dim_region` |
| `material_master` | Material master | material (× plant / sales org, to confirm) | `dim_product` |
| `company_codes`, `sales_orgs`, `sales_offices`, `distribution_channels`, `divisions`, `customer_groups`, `material_groups`, `material_types`, `plants`, `document_types` | Text (lookup) tables: code → name, one row per language | code × language | names in the dimensions (language `E`) |

Not needed in the first version: sales order items, delivery items, pricing conditions (the billing item
already carries net, gross, tax and cost amounts). Stock tables: later, if the source has them.

## Billing items: columns we read (39 SAP fields + 2 replication columns, of about 160)

Only these columns are copied. Types are as the source delivers them; silver casts them.

| SAP field (source) | Type in source | Our column (silver/gold) | Notes |
|---|---|---|---|
| `billingdocument` | string | `invoice_number` | key part 1 |
| `billingdocumentitem` | string | `invoice_item` | key part 2 |
| `billingdocumenttype` | string | `invoice_type` | e.g. ZAOR, ZARE, S1, F5 |
| `billingdocumentcategory` | string | `invoice_category` | |
| `sddocumentcategory` | string | `sd_document_category` | **drives the sign rule** (below) |
| `billingdocumentdate` | timestamp | `invoice_date` (DATE) | take the date part **without a time-zone shift** |
| `creationdate` | timestamp | `created_date` (DATE) | |
| `lastchangedate` | timestamp | `last_changed_date` (DATE) | |
| `billingdocumentiscancelled` | string | `is_cancelled` | flag on the original invoice |
| `cancelledbillingdocument` | string | `reference_invoice_number` (on cancellations) | the invoice a cancellation reverses |
| `referencesddocument`, `referencesddocumentitem` | string | `reference_document`, `reference_item` | delivery or original invoice |
| `salesdocument`, `salesdocumentitem` | string | `sales_order_number`, `sales_order_item` | |
| `companycode` | string | `company_code` | scope filter |
| `salesorganization` | string | `sales_org_code` | |
| `distributionchannel` | string | `distribution_channel_code` | |
| `division` | string | `division_code` | |
| `salesoffice` | string | `sales_office_code` | |
| `salesgroup` | string | `sales_group_code` | |
| `salesdistrict` | string | `sales_district_code` | the customer's region comes from the customer master |
| `plant` | string | `plant_code` | |
| `storagelocation` | string | `storage_location` | |
| `soldtoparty` | string | `customer_id` | keep leading zeros |
| `customergroup` | string | `customer_group_code` | |
| `material` | string | `product_id` | |
| `materialgroup` | string | `product_group_code` | |
| `billingquantity`, `billingquantityunit` | decimal(33,3), string | `quantity`, `sales_unit` | sign from the rule below |
| `billingquantityinbaseunit`, `baseunit` | decimal(33,3), string | `base_quantity`, `base_unit` | |
| `netamount` | decimal(33,2) | `net_revenue_amount` | sign from the rule below |
| `grossamount` | decimal(33,2) | `gross_revenue_amount` | |
| `taxamount` | decimal(33,2) | `tax_amount` | |
| `costamount` | decimal(33,2) | `cost_amount` | `margin_amount` = net − cost |
| `transactioncurrency` | string | `currency` | almost all LKR; see open questions |
| `accountingexchangerate` | decimal(33,5) | `exchange_rate` | for the rare foreign-currency lines |
| `fiscalyear`, `fiscalperiod` | string | (not used) | can be `0000`; we derive fiscal year/period from `invoice_date` (April–March, labelled by start year) |
| `_fivetran_synced` | timestamp | `_source_synced_ts` | newest version wins; watermark for "only new rows" |
| `_fivetran_deleted` | boolean | (filter) | deleted rows are dropped and counted |

**Never copied** (personal or free text): VAT registration numbers, payment references, item text, and any
other column not listed above.

## Sign rules (by SAP document category)

SAP stores amounts and quantities as positive numbers; the meaning comes from the document category.

| `sddocumentcategory` | Meaning | Sign in silver | Counts as |
|---|---|---|---|
| `M` | Invoice | + | sales |
| `P` | Debit memo | + | sales |
| `O` | Credit memo (return, refund, price correction) | − | returns / credits |
| `N` | Cancellation of an invoice | − | cancellation (the original invoice stays in; together they net to zero) |
| `S` | Cancellation of a credit memo | + | reverses a credit |
| `U` | Pro-forma invoice | excluded | not revenue |

- **Down-payment requests and their cancellations** (SAP types `FAZ`, `FAS`) are excluded.
- **Type-level overrides** come from config, for example a custom invoice type the business does not count
  as sales. Default: none. The real-data phase has one to confirm with the business.
- **Free-of-charge lines** (category `M`, net amount 0): quantity counts, revenue does not.
- A cancelled original invoice is **not** removed: its cancellation (`N`) reverses it, as in the MVP.

## Rows filtered out (counted in `ops`, not quarantined)

| Rule | Why |
|---|---|
| `companycode` not in scope | other companies share the same tables |
| `invoice_date` outside the history window | only the agreed window is loaded |
| `_fivetran_deleted = true` | deleted in SAP |
| category `U` or down-payment types | not revenue |
| older versions of the same key | duplicates: keep the newest `_fivetran_synced` per (`billingdocument`, `billingdocumentitem`) |

## Rows quarantined (`silver.quarantine`, with reasons)

| Reason | Condition |
|---|---|
| `missing_key` | document, item, customer or material empty |
| `invalid_invoice_date` | date empty, before 2000-01-01 or more than one year in the future (the source has such typos, e.g. year 4334) |
| `invalid_quantity` / `invalid_amount` | not a number, or does not fit DECIMAL(18,3) / DECIMAL(18,2) |
| `unknown_document_category` | category not in the sign table |

Checks (counted, not quarantined): negative amounts or quantities in the source, lines in a currency other
than the company currency, customers or materials missing from the masters (they get an "Unknown" member).

## Control totals (reconciliation)

When the data is copied, the source's own totals are saved in `ops.source_counts`: per billing month and
document category, the number of lines and the sum of `netamount`. Silver (before filters) must match them
exactly; gold must match the source minus the filtered rows. A mismatch stops publishing, as in the MVP.

## Master tables (to be completed)

| Table | Fields we need | Never stored |
|---|---|---|
| `customer_master` | customer id, customer group, region (district), city, country, sales office, distribution channel, payer, created date | names, street, phone, e-mail, tax/VAT numbers, bank details |
| `material_master` | material id, description, material group, material type, division, base unit, plant / sales org | (none expected) |
| text tables | code, language, name | (none) |

## Dummy data in this shape

`uv run python -m sales_insights.generator.sap` writes the same tables from our dummy business to the
client area's staging folder (CSV, every value as text, the way bronze stores it), so the pipeline is built
and tested without reading the real source. The layout is the one every area uses, and the real one-time
copy must write it too:

```
staging/client/history/billing_items.csv, source_counts.csv, sap_manifest.json (written last)
staging/client/masters/customer_master.csv, material_master.csv, text_<name>.csv, sap_manifest.json (last)
```

Run the whole chain (generates and delivers the files the first time, only while `source: dummy`):

```
uv run python -m sales_insights.pipeline.run_pipeline --area client --date 2026-09-30
```

Steps: bronze (same code as sales) → silver (`pipeline/sap_silver.py`) → gold (same code) → reconcile
(`pipeline/sap_reconcile.py`). Scope (company codes, date window, currency) is in `config.yaml`
(`areas.client.scope`). Same stories as the MVP (generator/engine.py), reshaped to SAP fields,
plus every case found in the real source, at a fixed rate and logged in `sap_manifest.json`:

| Planted case | Rate | The pipeline must |
|---|---|---|
| Pro-forma copy of an invoice (F5, category U) | 3% of invoices | exclude it |
| Down-payment request + its cancellation (FAZ/FAS) | 0.2% of invoices | exclude both |
| Cancellation of a credit memo (S2, category S) | 10% of credit memos | add it back (+) |
| Debit memo (ZADR, category P) | 0.2% of invoices | count it (+) |
| Export invoice billed in USD, with exchange rate | 2% of export invoices | keep, flag the currency |
| Older version of a line (same key, older sync time) | 1% of lines | keep only the newest |
| Line deleted in SAP (`_fivetran_deleted`) | 0.1% of lines | drop and count |
| Missing / impossible billing date (e.g. year 4334) | 0.02% / 0.02% of lines | quarantine |
| Fiscal year `0000` on documents older than a year | all older documents | ignore the field |
| A second company code | from the engine | filter out |
| A billed customer missing from the customer master | 1 customer | "Unknown" member |
| Personal columns in the customer master (fake values) | every row | never store them |

## What silver does with every source row

Every bronze billing row gets exactly ONE fate, checked in this order, counted per company, billing month
and category in `ops.sap_row_fates`:

| Fate | Rule |
|---|---|
| `older_version` | another copy of the same document + item has a newer `_fivetran_synced` |
| `deleted_in_source` | `_fivetran_deleted` is true |
| `out_of_scope_company` | company code not in `scope.company_codes` |
| `not_revenue` | category `U` or type `FAZ` / `FAS` |
| `quarantined` | a quarantine reason above (row and reasons in `silver.quarantine`) |
| `outside_window` | billing date outside `scope.date_from` … `scope.date_to` |
| `kept` | goes to `silver.invoice_lines`: signed, converted to the company currency with the line's rate (a negative SAP rate is an indirect quotation: 1 / rate), fiscal year and period from the date |

Customers: the display name is `Customer <id>`; personal columns are never selected. Customers and
materials billed but missing from the masters get an `Unknown …` member. Province is `Unassigned` until
the business gives the district → province list (open question 3).

Reconciliation (fails the run): source totals = bronze rows (all fates), source − filtered rows = silver
(lines and source net amount per company, month and category), silver = gold (lines and net revenue per
invoice date and type). Results in `ops.sap_reconciliation`.

`source_counts.csv` holds the control totals exactly as the real copy will record them (per company,
billing month and document category: lines and sum of `netamount`, every row counted).

## Where the real names live

The pipeline reads **logical names** (`billing_items`, `customer_master` …). The physical schema and table
names of the source, and the connection, are given at run time (Databricks job parameters or a git-ignored
local file). They are never committed: the repo is public.

## Open questions (for the business)

1. One custom invoice type: count as sales or not (type-level override)?
2. Foreign-currency lines: convert to the company currency with `accountingexchangerate`, or report separately?
3. Region: customer master region vs the line's sales district: which does the business use for "province"?
4. Is the source refreshed again (the feed stopped in early 2026), so that a daily update makes sense?
