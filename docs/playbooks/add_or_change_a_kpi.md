# Playbook: add or change a KPI

Every KPI is defined ONCE, as a measure in a metric view. Dashboards, Genie and the alerts read it from
there, so a formula changes in one place. The readable SQL twin in `sql/kpis/` must give the same numbers.

| Metric view | File | Twin SQL | Grain |
|---|---|---|---|
| `sales_metrics` | `sql/metric_views/sales_metrics.yaml` | `sql/kpis/*.sql` | invoice line |
| `inventory_metrics` | `sql/metric_views/inventory_metrics.yaml` | `sql/kpis/inventory/kpi_stock_*.sql` | location x product x day |
| `sales_and_stock` | `sql/metric_views/sales_and_stock.yaml` | `sql/kpis/inventory/kpi_sales_and_stock_monthly.sql` | day x sales office x product |

## Steps

1. Agree the definition in words first (what is counted, what is excluded, which date). Add or change the
   row in `docs/kpi_definitions.md` with status **Standard** or **Working** (our business choice).
2. Add the measure to the metric view YAML (metric view YAML 1.1): `name`, `expr`, `display_name`,
   `comment` (Genie reads it), `synonyms` (words people use). A ratio refers to other measures with
   `MEASURE(other)`, so the parts are defined once too.
3. **Stock is a level:** a stock measure needs a window `order: <date>`, `range: current`,
   `semiadditive: last`, so a month shows its last day, never a sum of days.
4. Change the twin SQL in `sql/kpis/` the same way.
5. `uv run pytest -m "not spark"` then `uv run pytest`. `tests/test_kpis.py` checks the formulas on a tiny
   world worked out by hand; the agreement check compares the metric view (compiled locally by
   `semantic/metric_views.py`) with the twin SQL row by row.
6. Use it: a dashboard dataset (`change_a_dashboard.md`), a Genie benchmark question with its true SQL
   (`tune_a_genie_agent.md`), and an insights rule if it should raise a finding (`docs/insights.md`).
7. Publish on Databricks after the merge:
   ```bash
   databricks bundle deploy -t dev --profile sales-dev
   databricks bundle run -t dev sales_publish_semantic --profile sales-dev        # or inventory_publish_semantic
   ```
   It recreates the metric view and prints `every KPI matches the metric view`.
8. One row in `docs/decisions.md` if the definition is a business choice (for example VAT treatment).

## Traps

- A KPI computed in a dashboard's plain SQL fails `test_kpis_come_from_the_metric_views`: ask the metric view.
- Percentages are rounded only where they are shown, never inside the measure.
- Money stays DECIMAL(18,2) in tables; cast to DOUBLE only for display.
