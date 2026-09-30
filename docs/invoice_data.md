# Invoice data (history and daily drops)

Both come from one shared engine (`generator/engine.py`), so they always agree about every invoice.

| Feed | Command | Output |
|---|---|---|
| History (18 months before `simulation_start`) | `uv run python -m sales_insights.generator.history` | `staging/history/invoices_YYYYMM.csv` + `history_manifest.json` |
| Daily drops (from `simulation_start`) | `uv run python -m sales_insights.generator.simulate --date YYYY-MM-DD` (or `--from … --to …`) | `staging/business_date=YYYY-MM-DD/` with `orders_YYYYMMDD.csv`, `changes_YYYYMMDD.csv`, `manifest_YYYYMMDD.json` |
| Delivery | `uv run python -m sales_insights.drip.drip --date YYYY-MM-DD` | copies that day to `landing/`, manifest last; log in `landing/_delivery_log.csv` |

`simulation_start` lives in `config/config.yaml` (2026-10-01). Never change it mid-project: every date and number depends on it.

## Daily files

- **orders**: normal invoices (`ZAOR`) and free-of-charge lines (`ZFOC`) that *arrived* that day. About 3% of invoices arrive 1–7 days after their `invoice_date` (live days only; history invoices never arrive late).
- **changes**: returns (`ZARE`), cancellations (`S1`) and price corrections (`ZACR`) dated that day, against any invoice from the last 45 days, including history invoices.
- **manifest**: file rows, column counts and SHA-256; control totals per `invoice_date`; the dirt log.

**Control totals are the true business totals**: they exclude the injected duplicate copies and invalid rows, which are listed line by line under `dirt`. Reconciliation must match them exactly (0.1% tolerance).

## Columns

The **74 columns of the reference SAP order-to-cash extract, in the same order**, plus one addition:

| Added column | Why |
|---|---|
| `reference_invoice_number` | The invoice a return (`ZARE`) or cancellation (`S1`) reverses (SAP reference document). Without it, silver cannot apply "latest change wins". Empty on normal invoices. |

Read every column as text (IDs and codes keep leading zeros). Amounts have 2 decimals, quantities and weights 3.

## Document types (sign rules copied from the reference)

| `invoice_type` | Meaning | `sd_document_category` | quantity | revenue | Other |
|---|---|---|---|---|---|
| ZAOR | Standard invoice | M | + | + | `sales_amount` = revenue |
| ZFOC | Free-of-charge delivery | M | + | = −tax | Company bears the VAT on free goods |
| ZARE | Return credit memo | O | − | − | `credit_amount` = −revenue, `sales_amount` 0 |
| S1 | Invoice cancellation | N | − | − | `net_sales` and `sales_amount` 0; in history the original gets `billing_status` C |
| ZACR | Price correction (credit for a pricing error) | O | 0 | − | `credit_amount` = −revenue, `sales_amount` 0, `invoice_category` A |

**Revenue KPI = sum of `revenue` over all rows**: returns and cancellations net out automatically.

## Document numbers (10 digits, derived so any day can be rebuilt alone)

| Document | Pattern | Example |
|---|---|---|
| Invoice (ZAOR/ZFOC) | `9` + yymmdd + 3-digit sequence | 9261023004 |
| Return (ZARE) | `7` + the invoice's last 9 digits | 7261023004 |
| Cancellation (S1) | `8` + the invoice's last 9 digits | 8261023004 |
| Price correction (ZACR) | `6` + the invoice's last 9 digits | 6261023004 |

## Other rules

- VAT 18% on domestic sales; export sales (channel 20) are zero-rated.
- `condition_rate` = list price per sales unit on that date (weedicides 8% lower before 1 Apr 2026).
- `condition_amount` = list value including VAT (our definition; the reference's meaning is unclear).
- `cumulative_order_qty_sales_unit` = absolute quantity.
- Fiscal year April–March, labelled by its start year; `fiscal_period` 1 = April.
- History has no data-quality dirt. Dirt arrives in the daily drops (see `story_sheet.md`).
- From 22 Oct a new last column `sales_rep_id` appears in daily files (schema evolution).

## Manifest control totals

`history_manifest.json` and each daily manifest hold line count, document count, revenue and tax: in total, by invoice type and **per `invoice_date`** (`by_invoice_date`). The reconciliation step compares gold against these per-day totals (tolerance 0.1%).
