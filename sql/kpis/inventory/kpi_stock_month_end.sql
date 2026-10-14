-- KPI: month-end stock per location and product, plus the month's issues and stock-out days.
-- Grain: one row per month, location and product. "Month end" = the last stock date of that month for the
-- position (the latest day for the current month). Formulas must match sql/metric_views/inventory_metrics.yaml
-- (tests check this).
WITH month_rows AS (
  SELECT d.month_start_date AS month, s.*
  FROM ${fact_stock_daily} s
  JOIN ${dim_date} d ON s.snapshot_date = d.date
),
months AS (
  SELECT
    month,
    location_id,
    product_id,
    MAX(snapshot_date)           AS month_end_date,
    SUM(issued_quantity)         AS issued_quantity,
    COUNT_IF(is_stock_out)       AS stock_out_days
  FROM month_rows
  GROUP BY month, location_id, product_id
)
SELECT
  m.month,
  m.location_id,
  m.product_id,
  m.month_end_date,
  s.stock_quantity                                                    AS stock_on_hand,
  s.stock_value_amount                                                AS stock_value,
  s.in_transit_quantity                                               AS in_transit,
  s.avg_daily_issued_quantity_28d                                     AS avg_daily_issued_28d,
  ROUND(s.stock_quantity / NULLIF(s.avg_daily_issued_quantity_28d, 0), 2) AS days_of_cover,
  s.is_stock_out,
  s.is_low_cover,
  m.issued_quantity,
  m.stock_out_days
FROM months m
JOIN ${fact_stock_daily} s
  ON s.location_id = m.location_id AND s.product_id = m.product_id AND s.snapshot_date = m.month_end_date
