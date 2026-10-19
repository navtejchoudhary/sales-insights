# Playbook: tune a Genie agent

Goal: an agent answers at least 90% of its benchmark questions right, in **Chat** mode, and stays there
(the weekly check scores it every Monday). This loop took the Sales & Stock agent from 58% to 97-100% in
two rounds and the Stock agent from 68% to 97%.

| Agent | Folder | `--genie` name | Agent ID |
|---|---|---|---|
| Sales | `genie/` | `sales` | `genie_space_id` in `databricks.yml` |
| Stock | `genie/inventory/` | `inventory` | `inventory_genie_space_id` |
| Sales & Stock | `genie/sales_and_stock/` | `sales_and_stock` | `sales_and_stock_genie_space_id` |

Each folder: `space.yaml` (instructions, sample questions, hidden columns), `benchmarks.yaml` (questions,
two paraphrases each, the true SQL, and a `check` for planted stories), `README.md` (first-time setup).

## 1. Score it

1. Agent > **Benchmarks** > mode **Chat** > **Run all** (10-30 min, depending on the number of questions).
2. Open the finished run; copy the code after `/benchmarks/` in the page address (the evaluation ID).
3. Export every answer:
   ```bash
   uv run --with databricks-sdk python -m sales_insights.semantic.genie_failures <agent id> <evaluation id>
   ```
   It prints GOOD / BAD / NEEDS_REVIEW per question and writes `output/genie_eval_<evaluation id>.json`.

## 2. Find out why each question failed

For every non-GOOD answer compare Genie's SQL with the expected SQL (both are in the JSON) and sort it:

| What you see | Kind | Fix |
|---|---|---|
| Same numbers, different rounding or extra decimals | Rounding | Expected SQL and instructions both unrounded (per-day averages, days of cover) |
| Right numbers, different layout (extra breakdown, daily rows, extra columns) | Answer shape | Reword the question ("in total", "one row", "each depot") and/or the shape rule in `space.yaml` |
| Different period or window than intended | Ambiguous wording | Say the window in the question ("the 30 days before", "from 23 Oct up to the latest day") |
| Wrong column, wrong movement type, name match instead of a code | Real mistake | One short instruction line naming the right column or code |
| A "why" question answered well in prose | Not scorable as a table | Score the facts behind it as separate columns; demo the "why" live |
| NEEDS_REVIEW and the answer is right | Grader limitation | Mark it correct in the UI, and remove the cause (usually rounding), because the weekly job counts it as wrong |

Do not "teach to the test" by pasting expected SQL into instructions; fix the rule the mistake revealed.

## 3. Change the files, then test

- Edit `genie/<agent>/benchmarks.yaml` and/or `space.yaml`. Keep instructions short (one line per rule).
- `uv run pytest -m "not spark"`, then the full `uv run pytest` (the Spark tests run every expected SQL
  on generated data and check every planted-story `check`).
- Add a row to `docs/decisions.md`: round, score before, what changed and why.

## 4. Load it into the agent

```bash
uv run python -m sales_insights.semantic.genie_setup --profile dev --genie <name> --as-of 2026-10-29
uv run python -m sales_insights.semantic.genie_sync --space-id <agent id> --csv output/genie_benchmarks_<name>.csv
uv run python -c "import yaml; print(yaml.safe_load(open('genie/<folder>/space.yaml'))['instructions'].strip())" | pbcopy
```
- `--as-of` must be the latest data date; without it, questions about later dates are left out.
- The sync prints `benchmarks: N -> N` and saves a backup of the agent in `output/`.
- Paste the instructions: agent > **Configure** > **Instructions** > select all > paste > **Save**
  (the API sync covers benchmarks; instructions are pasted by hand).
- Run the benchmarks again (step 1). Stop tuning at a stable 90%+; variation of 1-2 questions between
  runs is normal.

## 5. Keep it there

The job `[dev] sales-insights-genie-health` (Monday 07:00) runs all three agents' benchmarks and writes
`sales_dev.ops.genie_accuracy` (one row per agent); the Company overview shows the weakest agent. A task
fails, and emails, when its agent is under 90%. Compare `failed_questions` of the last two rows of an agent:
the same questions failing every week = a rule to fix; different ones = normal variation.
```sql
SELECT checked_ts, COALESCE(agent, 'sales') AS agent, good, total, accuracy_pct, failed_questions
FROM sales_dev.ops.genie_accuracy ORDER BY checked_ts DESC LIMIT 6;
```
