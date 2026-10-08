# Genie: preparation and Day 15 setup

Genie (AI/BI Genie, now called Genie Agents in Databricks) answers business questions in plain English by
writing SQL over the data we give it. How good it is depends on what we prepare: clean tables with
descriptions, one definition per KPI, a few clear instructions, and worked examples for tricky questions.
We also measure it: a fixed list of questions with known correct answers (benchmarks).

| File | What it is |
|---|---|
| `space.yaml` | Everything to paste into the Genie agent: title, description, 5 data assets, instructions, sample questions, which questions become example SQL |
| `benchmarks.yaml` | 30 questions with ground-truth SQL, each with 2 rewordings; 10 of them test a planted story |
| `trusted_queries.yaml` | 6 verified queries with parameters (Genie trusted assets) for the most common questions |
| `answer_key.csv` | Generated: the expected answer to each question on the current data (`uv run python -m sales_insights.semantic.answer_key`) |

Column and table descriptions are NOT here: they live in `src/sales_insights/pipeline/gold_model.py` and are
written to Unity Catalog as comments when gold is built. The metric view carries the KPI definitions,
display names and synonyms. Genie reads both automatically.

## Before the demo: regenerate the answer key

Answers change as daily data arrives, so regenerate after each pipeline run:
```bash
uv run python -m sales_insights.semantic.answer_key
```
Every story check must say PASS (or PENDING before its date). A FAIL means the data does not tell the story
any more: fix it before the demo, not during it.

## Day 15+: create the Genie agent (about 45 minutes)

1. Run the pipeline and `uv run python -m sales_insights.semantic.kpis` on Databricks so gold tables, comments
   and the metric view `sales_dev.gold.sales_metrics` exist.
2. On the Mac: `uv run python -m sales_insights.semantic.genie_setup --profile dev`. It writes
   `output/genie_setup.md` (every item below, with real table names) and `output/genie_benchmarks.csv`.
   Paste from that sheet, never retype SQL or table names.
3. In the workspace: **Genie > New**. Add the 5 data assets; hide the listed columns.
4. Paste `title`, `description` and `instructions`. Add the 5 `sample_questions`.
5. Add the 6 **trusted queries** (sheet section 6) as example SQL queries with parameters: set each parameter's
   type and paste its comment. Answers that use them are labelled **Trusted**.
6. Add the 5 taught **example queries** (sheet section 7).
7. Load the benchmarks from `genie_benchmarks.csv` (25 untaught questions x 3 wordings; demo-week ones join after
   their data date) with `uv run python -m sales_insights.semantic.genie_sync --space-id <Agent ID>`, then
   **Run all benchmarks** with **Mode: Chat** (Agent mode writes reports that the grader cannot compare).
8. Record the score in `docs/decisions.md`. Target: 90% or better. For each miss, find the cause and fix it in
   this order: period flag or field missing > column description or synonym > trusted or example query >
   (last resort) one more instruction. Run again.

## What makes Genie accurate here (layers)

| Layer | Where | What it prevents |
|---|---|---|
| Period flags (`is_last_complete_month`, `is_month_to_date`, ...) | `gold.dim_date`, metric view | wrong "last month", part-month vs whole-month, calendar vs fiscal year |
| One KPI definition each | metric view `sales_metrics` | home-made revenue or invoice formulas |
| Synonyms + hidden columns | metric view, `space.yaml` | "region" grouped by district, VAT-inclusive revenue |
| Trusted queries | `trusted_queries.yaml` | errors on the most common questions; answers labelled Trusted |
| Reworded benchmarks | `benchmarks.yaml` `paraphrases` | a score that only holds for our own wording |

## How to score a question by hand

Ask Genie the question exactly as written, compare with `expected_answer` in `answer_key.csv`:
correct = same numbers (rounding aside) for the same period and grouping. Note the miss and the reason.

## Keeping the score honest (weekly check)

The job `[dev] sales-insights-genie-health` runs every Monday at 07:00 (after the daily pipeline):
it evaluates every benchmark question in Chat mode, appends the score to `sales_dev.ops.genie_accuracy`
and fails below 90%, which sends the failure email. Run it any time with
`databricks bundle run -t dev sales_genie_health`. Score history:
`SELECT * FROM sales_dev.ops.genie_accuracy ORDER BY checked_ts DESC`.
