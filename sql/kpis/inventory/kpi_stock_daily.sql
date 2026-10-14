-- KPI: stock per day and province (every location in the province: depots and plant stores).
-- Grain: one row per stock date and province. Formulas must match sql/metric_views/inventory_metrics.yaml
-- (tests check this). Stock columns are levels: never add them up over days.
SELECT
  s.snapshot_date,
  l.province,
  SUM(s.stock_quantity)                                                       AS stock_on_hand,
  SUM(s.stock_value_amount)                                                   AS stock_value,
  SUM(s.in_transit_quantity)                                                  AS in_transit,
  SUM(s.avg_daily_issued_quantity_28d)                                        AS avg_daily_issued_28d,
  ROUND(SUM(s.stock_quantity) / NULLIF(SUM(s.avg_daily_issued_quantity_28d), 0), 2) AS days_of_cover,
  COUNT_IF(s.is_stock_out)                                                    AS stock_out_positions,
  COUNT_IF(s.is_low_cover)                                                    AS low_cover_positions,
  COUNT_IF(s.avg_daily_issued_quantity_28d > 0)                               AS active_positions,
  ROUND(COUNT_IF(s.is_stock_out) / NULLIF(COUNT_IF(s.avg_daily_issued_quantity_28d > 0), 0) * 100, 2)
                                                                              AS stock_out_rate_pct,
  SUM(s.issued_quantity)                                                      AS issued_quantity
FROM ${fact_stock_daily} s
LEFT JOIN ${dim_location} l ON s.location_id = l.location_id
GROUP BY s.snapshot_date, l.province
