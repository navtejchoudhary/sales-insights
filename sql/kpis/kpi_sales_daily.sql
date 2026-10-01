-- KPI: sales by invoice date (daily trend, alerts). Grain: one row per day with sales activity.
-- Formulas must match sql/metric_views/sales_metrics.yaml (tests check this).
SELECT
  f.invoice_date,
  SUM(f.net_revenue_amount)                            AS net_revenue,
  SUM(f.invoiced_revenue_amount)                       AS invoiced_revenue,
  -SUM(f.returns_amount)                               AS returns_value,
  SUM(f.quantity)                                      AS units,
  COUNT(DISTINCT CASE WHEN f.invoice_type = 'ZAOR' AND NOT f.is_cancelled THEN f.invoice_number END) AS invoice_count,
  COUNT(DISTINCT CASE WHEN f.invoice_type = 'ZAOR' AND NOT f.is_cancelled THEN f.customer_id END)    AS active_customers,
  SUM(f.margin_amount)                                 AS gross_margin
FROM ${fact_sales} f
GROUP BY f.invoice_date
