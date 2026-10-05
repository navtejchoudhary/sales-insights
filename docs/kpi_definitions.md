# KPI definitions

Every KPI is defined **once**, as a measure in the metric view `sql/metric_views/sales_metrics.yaml`.
On Databricks that file becomes the Unity Catalog metric view `gold.sales_metrics`, which the dashboard,
Genie and alerts all query, so no tool (and no AI) invents its own maths.

The hand-written KPI SQL in `sql/kpis/` is the readable, portable version of the same formulas
(useful for the deck job, checks, and anyone without metric views). The plan's rule holds:
**a KPI is done only when the KPI SQL and the metric view give identical numbers on the same data.**
`uv run python -m sales_insights.semantic.kpis` checks exactly that, and the tests check it on a tiny world
where every number was worked out by hand (`tests/test_kpis.py`).

Status: **Standard** = the usual industry definition. **Working** = our chosen business logic for the MVP; it can change later
with a small edit in the metric view and the KPI SQL (the agreement check proves both still match).

| KPI (measure) | Definition | Status |
|---|---|---|
| Net revenue (`net_revenue`) | Sum of `net_revenue_amount`: invoices minus returns, cancellations and price corrections, excluding VAT, LKR | Working |
| Invoiced revenue (`invoiced_revenue`) | Revenue on invoices and free-of-charge lines before any credit notes | Standard |
| Returns / cancellations / price corrections (`*_value`) | Value reversed by each credit-note type, shown as a positive number | Standard |
| Returns rate % (`returns_rate_pct`) | Returns ÷ invoiced revenue × 100 | Standard |
| Units (`units`) | Net cases: sold minus returned and cancelled | Standard |
| Invoices (`invoice_count`) | Distinct paid invoices (`ZAOR`) that were not cancelled. Free-of-charge deliveries and credit notes are not invoices | Working |
| Average invoice value (`average_invoice_value`) | Net revenue ÷ invoices | Working |
| Active customers (`active_customers`) | Customers with at least one paid, non-cancelled invoice in the period | Standard |
| Gross margin (`gross_margin`) | Net revenue − standard cost (quantity × product standard cost) | Working (the reference's `profit` column is an alternative) |
| Gross margin % (`gross_margin_pct`) | Gross margin ÷ net revenue × 100 | Working |
| Growth % (`mom_growth_pct`, `yoy_growth_pct` in `kpi_sales_monthly`) | (This month − comparison) ÷ \|comparison\| × 100, vs previous month and same month last year | Working (both shown; the insights job leads with same period last year) |
| Attach rate % (`kpi_attach_rate_monthly`) | Of paid invoices containing a crop chemical (`ZFRT`), the share also containing an add-on (`ZTRD` in groups `2CHE99`/`2HAF09`: sprayers, seedling trays) | Working (may be redefined later) |

Growth and attach rate are only in the KPI SQL for now: growth needs window measures (newer runtimes;
to try on 15 Oct) and attach rate needs invoice-level logic that a simple measure cannot express.

## KPI views (`sql/kpis/`)

| View | Grain | Used for |
|---|---|---|
| `kpi_sales_daily` | day | daily trend, alerts |
| `kpi_sales_monthly` | month | headline numbers, growth vs last month / last year |
| `kpi_sales_by_province_monthly` | province × month | strong and weak areas (story: Southern) |
| `kpi_product_monthly` | product × month | top movers, price effects (story: weedicide), returns problems (story: HGL400ML) |
| `kpi_attach_rate_monthly` | month | attach rate |

## Metric view fields (what you can slice by)

Time: invoice date, month, week, fiscal year / period / quarter, cultivation season.
Customer: province, district, city, customer, customer group, sales office, channel, sales rep (from 22 Oct).
Product: product, product group, product type, division. Document type.

## Rules for every KPI

- Money is DECIMAL(18,2); percentages are rounded to 2 decimals in outputs.
- Dates are invoice dates in Sri Lanka time (Asia/Colombo). Fiscal year April–March, labelled by its start year
  (April 2026 = FY2026 period 1). Currency LKR.
- Divisions use `NULLIF(…, 0)`, so an empty period gives an empty value instead of an error.
- Never change a formula in only one place: edit the metric view AND the KPI SQL; the check fails otherwise.

## Can change later

On 5 Oct 2026 the sponsor asked for the business logic to be shown working rather than finalised: the
definitions above are the MVP's working definitions. Points that may be revisited with the business later:

1. Revenue: net of returns, excluding VAT (current choice).
2. Margin: standard cost (current) or the source's `profit` column.
3. Attach rate: which products count as an add-on.
4. Growth: month-on-month or year-on-year as the default view (both are computed).
