-- KPI: product performance by month (top movers, price effects, returns problems).
-- Grain: one row per product per month. Formulas must match sql/metric_views/sales_metrics.yaml.
SELECT
  d.month_start_date                                   AS month,
  p.product_id,
  p.product_description                                AS product,
  p.product_group_name                                 AS product_group,
  SUM(f.net_revenue_amount)                            AS net_revenue,
  SUM(f.invoiced_revenue_amount)                       AS invoiced_revenue,
  -SUM(f.returns_amount)                               AS returns_value,
  SUM(f.quantity)                                      AS units,
  ROUND(-SUM(f.returns_amount) / NULLIF(SUM(f.invoiced_revenue_amount), 0) * 100, 2) AS returns_rate_pct,
  ROUND(SUM(f.net_revenue_amount) / NULLIF(SUM(f.quantity), 0), 2)                   AS net_price_per_unit
FROM ${fact_sales} f
JOIN ${dim_date} d ON f.invoice_date = d.date
LEFT JOIN ${dim_product} p ON f.product_id = p.product_id
GROUP BY d.month_start_date, p.product_id, p.product_description, p.product_group_name
