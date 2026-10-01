# Pipeline

```
staging/ --drip--> landing/ --bronze--> bronze.* --silver--> silver.* --gold--> gold.* --> reconciliation
```

## Daily run (local)

```bash
uv run python -m sales_insights.drip.drip --initial          # once: master data + history backfill
uv run python -m sales_insights.drip.drip --date 2026-10-01  # each business day
uv run python -m sales_insights.pipeline.bronze              # loads whatever is new in landing/
uv run python -m sales_insights.pipeline.silver              # rebuilds clean silver tables from bronze
uv run python -m sales_insights.pipeline.gold                # rebuilds the gold star schema from silver
uv run python -m sales_insights.pipeline.reconcile           # gold vs manifests; exit code 1 if anything fails
```

Tables live under `lake/<schema>/<table>` locally (path-based Delta) and as `sales_dev.<schema>.<table>` on
Databricks. Code never uses either directly: it goes through `common/lake.py` (`Lake.read`, `Lake.append`, ...).

## Bronze (`pipeline/bronze.py`, discovery in `pipeline/landing.py`)

Loads files **exactly as received**: every source column stays text, nothing is cleaned.

| Table | Source | Notes |
|---|---|---|
| `bronze.orders` | `landing/business_date=*/orders_*.csv` | `sales_rep_id` appears from 22 Oct |
| `bronze.changes` | `landing/business_date=*/changes_*.csv` | returns, cancellations, price corrections |
| `bronze.invoices_history` | `landing/history/invoices_*.csv` | one-time backfill |
| `bronze.masters_<name>` | `landing/masters/<name>.csv` | 14 master and lookup tables |
| `bronze.manifests` | every manifest | raw JSON in `content` |
| `ops.processed_files` | bronze's own log | one row per loaded file: path, SHA-256, rows, status |
| `ops.dq_results` | checks from every layer | PASS / FAIL / WARN / WAIT |

Metadata on every bronze row: `_source_file` (path in landing), `_business_date` (daily files only),
`_load_ts`, `_run_id`, `_rescued_data` (always empty locally; Auto Loader fills it on Databricks).

Rules:

- **Exactly once**: a file is loaded once per (path, SHA-256). An identical redelivery is skipped.
  If the source changes a file it already sent, bronze loads it again, marks it
  `reloaded_changed_content` and writes a WARN (silver keeps the newest load).
- **Complete folders only**: a folder loads when its manifest and every file it lists exist.
  Otherwise bronze writes a WAIT row and tries again next run.
- **Schema evolution**: new source columns are added to the table (`mergeSchema`), never an error.
- **Manifest checks**: row count and checksum per file into `ops.dq_results`.

## Silver (`pipeline/silver.py`, manifest parsing in `pipeline/manifests.py`)

Turns raw text into clean, typed tables you can trust. **Rebuilt in full every run** (see decisions log):
the same bronze always gives the same silver, and a rerun is harmless.

| Table | What it holds |
|---|---|
| `silver.invoice_lines` | history + orders + changes, one row per `invoice_number` + `invoice_item`, typed and enriched |
| `silver.quarantine` | rows that failed validation: key, `reasons`, `business_date`, source file, the original row as JSON |
| `silver.customers` | one row per customer (latest version), canonical city, region, province |
| `silver.products` | one row per product (latest version), product group always filled |
| `silver.<lookup>` | regions, cities, plants, sales offices, sales reps and the other lookup masters |
| `ops.manifest_totals` | what each manifest promised: lines, invoices, revenue, tax per file kind and invoice date |
| `ops.manifest_dirt` | every dirty row each manifest says it injected (file, dirt type, key) |

Order of the line rules (each is a small, separately tested function):

1. **Newest file version**: if the source re-sent a file with new content, only its newest load is used
   (a row the source removed disappears too). Bronze keeps both loads.
2. **Dedupe** on `invoice_number` + `invoice_item`: one copy survives (`duplicate_line`).
3. **Validate** → quarantine with reasons: `invalid_invoice_date` (dd/mm/yyyy, month 13, 0000-00-00),
   `negative_quantity_on_invoice` (ZAOR/ZFOC with quantity < 0), `invalid_quantity`, `invalid_revenue`,
   `unknown_invoice_type`, `missing_key`. Returns (ZARE) and cancellations (S1) are allowed to be negative.
4. **Types**: dates `DATE`, money `DECIMAL(18,2)`, quantities and weights `DECIMAL(18,3)`, rates `DECIMAL(18,6)`.
   Casts use `try_cast`, so bad text becomes NULL instead of crashing (Spark 4 runs in ANSI mode).
5. **Enrich** from master data: customer IDs padded to 10 digits; city and region from the customer master
   (fixes `city_text_variant`); blank sales office, customer group, product group and plant filled
   (`blank_*`); blank customer group name becomes `Unassigned`.
6. **Flags**: `is_cancelled` + `cancelled_by` on sales lines an S1 later reversed (revenue still nets
   through the S1 rows, so nothing is double-counted); `arrival_delay_days` = business date − invoice date
   (late arrivals; NULL for history).

Master rules: customer IDs padded to 10 digits, names trimmed, latest version per key by
`last_updated_timestamp` (`duplicate_older_version`), city spelling mapped to `cities.csv`
(`COLOMBO - 10.` / `Colombo 10` / `colombo  10` → `COLOMBO 10`), blank region taken from the city,
blank product group taken from the group most often seen for that product in invoice lines,
product descriptions upper-cased.

Checks written to `ops.dq_results` (layer `silver`): customer IDs unique, every customer city mapped,
product groups filled, **quarantine equals the manifests' invalid rows exactly**, no blank required fields
(sales office, plant, product group, customer group name, invoice date, region), line keys unique.

The tests prove more against the manifests: silver's daily revenue and line count equal the manifests'
control totals exactly, history equals the history file, and every dirty row listed is fixed or quarantined.

## Gold (`pipeline/gold.py`, descriptions in `pipeline/gold_model.py`)

A **star schema**: one fact table of transactions surrounded by dimension tables that describe them.
This is the standard shape for BI tools and for Genie, because every question becomes
"sum a measure from the fact, grouped by attributes of the dimensions".

```
            dim_date (date)        dim_customer (customer_id)
                     \              /
   dim_channel ---- gold.fact_sales ---- dim_product (product_id)
                     /
            dim_region (district_code)
```

| Table | Grain (one row per) | Rows (1-5 Oct data) |
|---|---|---|
| `gold.fact_sales` | invoice line | same as `silver.invoice_lines` |
| `gold.dim_customer` | customer | 600 |
| `gold.dim_product` | product | 40 |
| `gold.dim_region` | district (with province) | 25 |
| `gold.dim_channel` | distribution channel | 3 |
| `gold.dim_date` | calendar day, whole fiscal years | 730 for FY2025-FY2026 |

Key design points:

- **One revenue measure that is always right**: `net_revenue_amount`. Returns, cancellations and price
  corrections are negative rows, so a plain SUM over any period is true net revenue. Its four parts
  (`invoiced_revenue_amount`, `returns_amount`, `cancellations_amount`, `price_corrections_amount`) always
  add up to it, which makes questions like "how much did returns cost us?" a single SUM.
- **Margin** from the product's standard cost: `cost_amount` = quantity x standard cost,
  `margin_amount` = net revenue - cost (draft KPI, to confirm with Anil).
- **Clear names**: codes end `_code`, money ends `_amount`, dates end `_date`; SAP names are kept in the
  descriptions so people who know SAP can still find them.
- **Descriptions everywhere**: every table and column has a plain-English description in `gold_model.py`.
  Locally they are stored as Delta column comments; on Databricks `Lake.describe` also runs
  `COMMENT ON TABLE` and `ALTER COLUMN ... COMMENT`, which is what Genie and Catalog Explorer read.
  `docs/gold_model.md` (the data dictionary) is generated from the same file; a test fails if it is stale.
- **dim_date** covers whole fiscal years (April-March), with fiscal period and quarter, Monday-start weeks
  and the Sri Lankan cultivation season (Maha Sep-Mar, Yala May-Aug, April in between; set in config).
- **Full rebuild** each run, like silver.

## Reconciliation (`pipeline/reconcile.py`)

Proves gold matches what the source system says it sent. For every
(delivery, file kind, invoice date) the manifests promise lines, invoices and revenue; gold must match
each within `reconciliation_tolerance_pct` (0.1%). The manifests already leave out the duplicate and
invalid rows they injected, so a correct pipeline matches **exactly**; the tolerance only absorbs rounding.

- `ops.reconciliation`: one row per compared group per run (kept, so you can see history)
- `ops.dq_results`: one PASS/FAIL row per delivery (`check_name = 'reconciliation'`)
- The command exits with code 1 and prints the failing groups, so a scheduled job turns red.

A group missing on either side fails too (gold has rows nobody sent, or lost rows that were sent).
