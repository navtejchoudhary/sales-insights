-- KPI (DRAFT, definition open with Anil): attach rate by month.
-- Draft definition: of the paid, non-cancelled invoices that contain a crop chemical (product type ZFRT),
-- the share that also contain an add-on item (trading goods ZTRD in groups 2CHE99 or 2HAF09:
-- sprayers, seedling trays). Not in the metric view: it needs invoice-level logic.
WITH invoice_flags AS (
  SELECT
    d.month_start_date                                   AS month,
    f.invoice_number,
    MAX(CASE WHEN p.product_type_code = 'ZFRT' THEN 1 ELSE 0 END)                       AS has_chemical,
    MAX(CASE WHEN p.product_type_code = 'ZTRD'
              AND p.product_group_code IN ('2CHE99', '2HAF09') THEN 1 ELSE 0 END)       AS has_add_on
  FROM ${fact_sales} f
  JOIN ${dim_date} d ON f.invoice_date = d.date
  LEFT JOIN ${dim_product} p ON f.product_id = p.product_id
  WHERE f.invoice_type = 'ZAOR' AND NOT f.is_cancelled
  GROUP BY d.month_start_date, f.invoice_number
)
SELECT
  month,
  SUM(has_chemical)                                                AS chemical_invoices,
  SUM(CASE WHEN has_chemical = 1 AND has_add_on = 1 THEN 1 ELSE 0 END) AS chemical_invoices_with_add_on,
  ROUND(SUM(CASE WHEN has_chemical = 1 AND has_add_on = 1 THEN 1 ELSE 0 END)
        / NULLIF(SUM(has_chemical), 0) * 100, 2)                   AS attach_rate_pct
FROM invoice_flags
GROUP BY month
