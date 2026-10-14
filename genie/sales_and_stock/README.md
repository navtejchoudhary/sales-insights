# Genie agent "Sales & Stock" (combined)

Answers questions that need sales and stock together, above all the demo question
**"Why did sales of MC6001LT stop in Western province?"** (benchmark SS01). It stands in for a supervisor agent
over the Sales and Stock agents, which our workspace does not offer.

| File | What it is |
|---|---|
| `space.yaml` | Title, description, 6 data assets, instructions, sample questions, hidden columns, taught examples |
| `benchmarks.yaml` | 15 questions with ground-truth SQL, each with 2 rewordings; 5 test planted stories (7 + I1, I2, I3, I4) |
| `answer_key.csv` | Generated: `answer_key --genie sales_and_stock`, or the inventory pipeline's answer_key step |

Its main table is `gold.fact_sales_stock_daily` (inventory gold): one row per day, sales office and product,
the office's sales next to the stock of the depot that serves it. Both facts are summed to that grain before
they are joined, so no sale is counted twice; net revenue there equals `fact_sales`. KPIs:
`sql/metric_views/sales_and_stock.yaml`.

SS01 is deliberately **not** a taught example: the demo must show Genie reasoning from the data, not recalling
a stored answer. Expected answer: no MC6001LT invoiced in Western since 23 Oct; the Colombo and Negombo depots
have held zero since the end of 22 Oct; the Galle hub still holds several thousand cases; the last transfer to
those depots was on 23 Sep; Western buyers moved to the 4-litre pack (MC6004LT).

## Create the agent

Same steps as the Stock agent (`genie/inventory/README.md`), with `--genie sales_and_stock`:
```bash
uv run python -m sales_insights.semantic.genie_setup --profile dev --genie sales_and_stock
uv run python -m sales_insights.semantic.genie_sync --space-id <Agent ID> --csv output/genie_benchmarks_sales_and_stock.csv
```
