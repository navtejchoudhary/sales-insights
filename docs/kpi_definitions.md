# KPI definitions (DRAFT — confirm with Anil on Fri 2 Oct)

Each KPI becomes one Unity Catalog metric view, used by both the dashboard and Genie, so the AI never invents its own maths. Until then, the same formulas live as SQL in `sql/kpis/`.

| KPI | Draft definition | Status |
|---|---|---|
| Revenue | Sum of `net_amount`, excluding cancelled lines; returns count as negative | Confirm with Anil |
| Gross margin % | (Revenue − sum of `quantity` × `unit_cost`) ÷ Revenue | Confirm with Anil |
| Growth % | Revenue vs previous period and vs same period last year | Confirm which comparison is the default |
| Attach rate | Share of orders with a primary product that also contain an accessory line | **Open: Anil must define "attach"** |
| Orders | Count of distinct orders | Standard |
| Units | Sum of `quantity` | Standard |
| Average order value | Revenue ÷ Orders | Standard |

## Rules for every KPI

- Money is DECIMAL(18,2); percentages are rounded to 2 decimals in outputs.
- Dates use the business date in IST. Fiscal year runs April–March.
- A KPI is "done" only when the local SQL and the metric view give identical numbers on the same data.
