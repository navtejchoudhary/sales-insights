# Genie: preparation and Day 15 setup

Genie (AI/BI Genie, now called Genie Agents in Databricks) answers business questions in plain English by
writing SQL over the data we give it. How good it is depends on what we prepare: clean tables with
descriptions, one definition per KPI, a few clear instructions, and worked examples for tricky questions.
We also measure it: a fixed list of questions with known correct answers (benchmarks).

| File | What it is |
|---|---|
| `space.yaml` | Everything to paste into the Genie agent: title, description, 5 data assets, instructions, sample questions, which questions become example SQL |
| `benchmarks.yaml` | 30 questions with ground-truth SQL; 10 of them test a planted story |
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

## Day 15+: create the Genie agent (about 30 minutes)

1. Run the pipeline and `uv run python -m sales_insights.semantic.kpis` on Databricks so gold tables, comments
   and the metric view `sales_dev.gold.sales_metrics` exist.
2. In the workspace: **Genie > New**. Add the 5 data assets listed under `data` in `space.yaml`.
3. Paste `title`, `description` and `instructions`. Add the 5 `sample_questions`.
4. For each entry in `example_queries`, add an example SQL query: the question, and the SQL of that benchmark
   from `answer_key.csv` (column `ground_truth_sql`, with `sales_dev.gold.` table names).
5. Add the remaining 25 questions as **benchmarks** (question + ground-truth SQL). Run the benchmark.
6. Record the score in `docs/decisions.md`. Target: 80% or better. For each miss, prefer fixing a column
   description, synonym or example query over adding more text instructions.

## How to score a question by hand

Ask Genie the question exactly as written, compare with `expected_answer` in `answer_key.csv`:
correct = same numbers (rounding aside) for the same period and grouping. Note the miss and the reason.
