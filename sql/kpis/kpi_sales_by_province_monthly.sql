-- KPI: sales by province and month (where are we strong or weak?).
-- Grain: one row per province per month. Formulas must match sql/metric_views/sales_metrics.yaml.
SELECT
  d.month_start_date                                   AS month,
  c.province,
  SUM(f.net_revenue_amount)                            AS net_revenue,
  SUM(f.quantity)                                      AS units,
  COUNT(DISTINCT CASE WHEN f.invoice_type = 'ZAOR' AND NOT f.is_cancelled THEN f.invoice_number END) AS invoice_count,
  COUNT(DISTINCT CASE WHEN f.invoice_type = 'ZAOR' AND NOT f.is_cancelled THEN f.customer_id END)    AS active_customers,
  ROUND(SUM(f.net_revenue_amount) / NULLIF(
    COUNT(DISTINCT CASE WHEN f.invoice_type = 'ZAOR' AND NOT f.is_cancelled THEN f.customer_id END), 0), 2)
                                                       AS revenue_per_active_customer,
  ROUND(SUM(f.margin_amount) / NULLIF(SUM(f.net_revenue_amount), 0) * 100, 2) AS gross_margin_pct
FROM ${fact_sales} f
JOIN ${dim_date} d ON f.invoice_date = d.date
LEFT JOIN ${dim_customer} c ON f.customer_id = c.customer_id
GROUP BY d.month_start_date, c.province
