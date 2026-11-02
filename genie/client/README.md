# Genie agent "Sales (SAP data)": the client area's real data

The same approach as the sales agent (`genie/README.md`), on the SAP billing data in `client_dev`
(company 2010, whole months January 2024 to December 2025).

| File | What it is |
|---|---|
| `space.yaml` | Title, description, 6 data assets (`client_dev.gold`), hidden columns, instructions, sample questions, taught examples |
| `benchmarks.yaml` | 16 questions with ground-truth SQL, 2 rewordings each; 4 are taught as examples, 12 are scored (36 wordings) |

**Privacy:** the repo is public. No real value from the client's data (customer numbers, product codes,
figures) goes into these files. The answer key holds real figures, so it is written to `output/`
(git-ignored), never into this folder. Column descriptions come from `pipeline/client_model.py`.

## Setup (about 45 minutes)

1. The client pipeline has run on Databricks (gold, comments and `client_dev.gold.sales_metrics` exist).
2. On the Mac: `uv run python -m sales_insights.semantic.genie_setup --profile dev --genie client`. It writes
   `output/genie_setup_client.md` and `output/genie_benchmarks_client.csv`.
3. In the workspace: **Genie > New**. Add the 6 data assets, hide the listed columns, paste the title,
   description and instructions, add the 5 sample questions and the 4 example queries (sheet sections 1-7).
   Do not share the agent with any group: the client data is owner only.
4. Load the benchmarks: `uv run python -m sales_insights.semantic.genie_sync --space-id <Agent ID> --csv output/genie_benchmarks_client.csv`.
5. **Run all benchmarks** with **Mode: Chat**. Target: 90% or better. For each miss, fix in this order:
   column description (`client_model.py`) > example query > one more instruction. Run again.
