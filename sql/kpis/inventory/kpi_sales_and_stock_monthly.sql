-- KPI: sales next to depot stock, per month and province.
-- Grain: one row per month and province. Sales columns add up over the month; depot stock is the stock on the
-- month's last day (the latest day for the current month). Formulas must match
-- sql/metric_views/sales_and_stock.yaml (tests check this).
WITH month_rows AS (
  SELECT d.month_start_date AS month, l.province, f.*
  FROM ${fact_sales_stock_daily} f
  JOIN ${dim_date} d ON f.activity_date = d.date
  LEFT JOIN ${dim_location} l ON f.location_id = l.location_id
),
last_days AS (
  SELECT month, province, MAX(activity_date) AS stock_date
  FROM month_rows
  GROUP BY month, province
)
SELECT
  r.month,
  r.province,
  ld.stock_date,
  SUM(r.net_revenue_amount)                                                  AS net_revenue,
  SUM(r.sales_quantity)                                                      AS units_sold,
  SUM(r.invoiced_quantity)                                                   AS units_invoiced,
  SUM(r.issued_quantity)                                                     AS issued_quantity,
  COUNT_IF(r.is_stock_out)                                                   AS stock_out_days,
  SUM(CASE WHEN r.activity_date = ld.stock_date THEN r.stock_quantity END)   AS depot_stock,
  SUM(CASE WHEN r.activity_date = ld.stock_date THEN r.in_transit_quantity END) AS depot_in_transit,
  COUNT_IF(r.activity_date = ld.stock_date AND r.is_stock_out)              AS stock_out_positions,
  ROUND(SUM(CASE WHEN r.activity_date = ld.stock_date THEN r.stock_quantity END)
        / NULLIF(SUM(CASE WHEN r.activity_date = ld.stock_date THEN r.avg_daily_issued_quantity_28d END), 0), 2)
                                                                             AS depot_days_of_cover
FROM month_rows r
JOIN last_days ld ON r.month = ld.month AND r.province <=> ld.province
GROUP BY r.month, r.province, ld.stock_date
