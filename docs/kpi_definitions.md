# KPI definitions (DRAFT — confirm with Anil on Fri 2 Oct)

Each KPI becomes one Unity Catalog metric view, used by both the dashboard and Genie, so the AI never invents its own maths. Until then, the same formulas live as SQL in `sql/kpis/`.

| KPI | Draft definition | Status |
|---|---|---|
| Revenue | Sum of `revenue` (net of tax, LKR) on invoice lines; cancellations (`S1`) and return credit memos (`ZARE`) count as negative | Confirm with Anil |
| Gross margin % | (Revenue − sum of `quantity` × `standard_cost`) ÷ Revenue; the reference's `profit` column is used to cross-check | Confirm with Anil |
| Growth % | Revenue vs previous period and vs same period last year | Confirm which comparison is the default |
| Attach rate | Draft: share of invoices containing a crop chemical (`ZFRT`) that also contain an add-on item (sprayer, seedling tray, `ZTRD` in groups `2CHE99`/`2HAF09`) | **Open: Anil must define "attach"** |
| Invoices | Count of distinct `invoice_number` | Standard |
| Units | Sum of `quantity` | Standard |
| Average invoice value | Revenue ÷ Invoices | Standard |

## Rules for every KPI

- Money is DECIMAL(18,2); percentages are rounded to 2 decimals in outputs.
- Dates use the business date in Sri Lanka time (Asia/Colombo). Fiscal year runs April–March, labelled by its start year (April 2026 = FY2026 period 1). Currency LKR.
- A KPI is "done" only when the local SQL and the metric view give identical numbers on the same data.
