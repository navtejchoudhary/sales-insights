-- KPI: sales by calendar month, with growth vs the previous month and the same month last year.
-- Grain: one row per month. Formulas must match sql/metric_views/sales_metrics.yaml (tests check this).
-- Status of each KPI (standard / working): docs/kpi_definitions.md
WITH monthly AS (
  SELECT
    d.month_start_date                                   AS month,
    d.fiscal_year_label                                  AS fiscal_year,
    d.fiscal_period,
    SUM(f.net_revenue_amount)                            AS net_revenue,
    SUM(f.invoiced_revenue_amount)                       AS invoiced_revenue,
    -SUM(f.returns_amount)                               AS returns_value,
    -SUM(f.cancellations_amount)                         AS cancellations_value,
    -SUM(f.price_corrections_amount)                     AS price_corrections_value,
    SUM(f.quantity)                                      AS units,
    COUNT(DISTINCT CASE WHEN f.invoice_type = 'ZAOR' AND NOT f.is_cancelled THEN f.invoice_number END) AS invoice_count,
    COUNT(DISTINCT CASE WHEN f.invoice_type = 'ZAOR' AND NOT f.is_cancelled THEN f.customer_id END)    AS active_customers,
    SUM(f.margin_amount)                                 AS gross_margin
  FROM ${fact_sales} f
  JOIN ${dim_date} d ON f.invoice_date = d.date
  GROUP BY d.month_start_date, d.fiscal_year_label, d.fiscal_period
)
SELECT
  m.month,
  m.fiscal_year,
  m.fiscal_period,
  m.net_revenue,
  m.invoiced_revenue,
  m.returns_value,
  m.cancellations_value,
  m.price_corrections_value,
  m.units,
  m.invoice_count,
  m.active_customers,
  ROUND(m.net_revenue / NULLIF(m.invoice_count, 0), 2)                        AS average_invoice_value,
  m.gross_margin,
  ROUND(m.gross_margin / NULLIF(m.net_revenue, 0) * 100, 2)                   AS gross_margin_pct,
  ROUND(m.returns_value / NULLIF(m.invoiced_revenue, 0) * 100, 2)             AS returns_rate_pct,
  p.net_revenue                                                                AS prev_month_net_revenue,
  ROUND((m.net_revenue - p.net_revenue) / NULLIF(ABS(p.net_revenue), 0) * 100, 2) AS mom_growth_pct,
  y.net_revenue                                                                AS last_year_net_revenue,
  ROUND((m.net_revenue - y.net_revenue) / NULLIF(ABS(y.net_revenue), 0) * 100, 2) AS yoy_growth_pct
FROM monthly m
LEFT JOIN monthly p ON p.month = ADD_MONTHS(m.month, -1)
LEFT JOIN monthly y ON y.month = ADD_MONTHS(m.month, -12)
