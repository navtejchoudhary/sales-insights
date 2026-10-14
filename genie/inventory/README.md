# Genie agent "Stock" (inventory area)

Answers stock questions: stock on hand, stock value, days of cover, stock-outs, issues, receipts and transfers.
Same preparation as the sales agent (`genie/README.md`): one definition per KPI in a metric view, column
descriptions in Unity Catalog, a few instructions, taught examples, and scored benchmarks.

| File | What it is |
|---|---|
| `space.yaml` | Title, description, 6 data assets, instructions, sample questions, hidden columns, taught examples |
| `benchmarks.yaml` | 25 questions with ground-truth SQL, each with 2 rewordings; 5 test planted stories I1-I4 |
| `answer_key.csv` | Generated: expected answers on the current data (`answer_key --genie inventory`, or the pipeline's answer_key step) |

The KPI definitions are in `sql/metric_views/inventory_metrics.yaml`. **Stock is a level**: its measures are
window measures that give the stock on the last day of a period, never a sum over days. Table and column
descriptions live in `src/sales_insights/pipeline/inventory_model.py`.

## Regenerate the answer key

```bash
uv run python -m sales_insights.semantic.answer_key --genie inventory
```
Story checks (INV14-INV18) must say PASS, or PENDING before their date (INV14 from 22 Oct, INV16 from 25 Oct).

## Create the agent on Databricks (about 30 minutes, after inventory runs there)

1. Run the inventory pipeline and `uv run python -m sales_insights.semantic.kpis --area inventory` on
   Databricks, so the inventory gold tables, their comments and the metric views
   `inventory_dev.gold.inventory_metrics` and `inventory_dev.gold.sales_and_stock` exist.
2. On the Mac: `uv run python -m sales_insights.semantic.genie_setup --profile dev --genie inventory`. It writes
   `output/genie_setup_inventory.md` (every item, with real table names) and `output/genie_benchmarks_inventory.csv`.
3. **Genie > New**: add the 6 data assets, hide the listed columns, paste title, description, instructions and
   the 5 sample questions, add the 5 taught example queries (sheet section 7).
4. Load the benchmarks: `uv run python -m sales_insights.semantic.genie_sync --space-id <Agent ID>
   --csv output/genie_benchmarks_inventory.csv`, then **Run all benchmarks** in **Chat** mode. Target 90%.
5. Record the score in `docs/decisions.md`. Fix misses in this order: field or synonym in the metric view >
   column description > example query > (last resort) one more instruction.

If the metric view's window measures are refused on our workspace, keep the agent on the tables and the KPI
views `kpi_stock_month_end` and `kpi_stock_daily`, which hold the same numbers (proved by the kpis step).
