# Sales Insights MVP on Databricks — project guide for Claude Code

Source of truth: "Databricks Insights MVP — Execution Plan" (30 Sep 2026). If this file and the plan disagree, ask the human.

## What we are building
A QuickSight-style insights MVP on Azure Databricks:
- Phase 1 (full): gold sales model, Unity Catalog metric views, AI/BI dashboard, Genie space
- Phase 2 (lite): scheduled insights job — period variance, top movers, `ai_forecast`, `ai_detect_anomalies`, `ai_query` narrative
- Phase 3 (thin): native Agent Bricks SupervisorAgent over Genie + insights table, plus a weekly python-pptx deck JOB (no custom chat app)

Timeline:
- Stage A, 1–14 Oct: build and test everything LOCALLY on Mac (no Databricks)
- Stage B, 15–28 Oct: Azure Databricks trial workspace (created 15 Oct, not before)
- Stage C, 29–30 Oct: demo

## Data: dummy, story-driven, deliberately dirty
- NO real data in the MVP. No S3, Delta Sharing or S/4HANA.
- Generator (plain Python: NumPy, pandas, Faker) writes CSV/JSON like an outside source system.
- Masters once: products, customers, regions/cities, channels, sales reps.
- History: 18 months of order lines ending YESTERDAY (simulated dates are real dates; generator takes an end date).
- Daily drop: `staging/business_date=YYYY-MM-DD/` with
  - `orders_YYYYMMDD.csv` — new order lines
  - `changes_YYYYMMDD.csv` — cancellations, returns, price corrections to older orders
  - `manifest_YYYYMMDD.json` — WRITTEN LAST: row counts, control totals (orders, gross, net revenue), list of dirt injected
- Fixed random seed per date: re-running a day produces identical files.
- `drip --date` copies `staging/<date>/` to `landing/<date>/`, manifest last.
- Planted stories and dirt rates live in `docs/story_sheet.md`. Every dirt injection is logged in the manifest.
- Answer key: `genie/answer_key.csv` — 25–30 questions with expected answers computed from the generator.

## Architecture
landing → bronze → silver → gold → reconciliation → metric views → dashboard / Genie / alerts → insights job → SupervisorAgent + deck job
- bronze: append as-is, all columns as STRING, plus `source_file` and `load_ts`; unknown columns go to a rescued-data column
- silver: cast types, standardise text (e.g. Bengaluru/Bangalore/BLR), dedupe on `order_id` + `line_no`, MERGE change files (latest change wins), bad rows to `silver.quarantine` with a reason
- gold: `fact_sales` (order-line grain) + `dim_product`, `dim_customer`, `dim_region`, `dim_date`, `dim_channel`; rebuild only dates touched by late/changed rows
- reconciliation: gold daily totals vs manifest totals minus quarantined rows; pass within 0.1%, otherwise FAIL loudly; results to `ops.dq_results`
- Locally, a processed-files log stands in for the Auto Loader checkpoint

## PORTABILITY RULES (most important)
1. ALL pipeline code (bronze, silver, gold, reconciliation, KPI SQL) uses PySpark + Delta (`delta-spark`). Never pandas or DuckDB for pipeline logic. Exception: the generator/simulator only (plain Python).
2. Keep IO apart from logic: transform functions take DataFrames and return DataFrames. Paths and catalog names come ONLY from `config/config.yaml` with profiles `local` and `dev`.
3. Same schema names everywhere: `bronze`, `silver`, `gold`, `ops` locally; `sales_dev.bronze` etc. on Databricks.
4. Databricks-only features (Auto Loader, volumes, triggers, metric views, Genie, AI functions) go in thin wrappers, never mixed into transform logic.
5. Library versions are pinned in `pyproject.toml`. Do not upgrade without asking.
6. SQL must be Spark SQL / Databricks SQL compatible.

## Local environment
- Python 3.12 via `uv`; Java 17; PySpark 4.0.x + delta-spark 4.0.x
- Run: `uv run python ...` · Add packages: `uv add ...` (ask first) · Tests: `uv run pytest` · Lint: `uv run ruff check . && uv run ruff format .`
- NEVER install `databricks-connect` in this environment — it conflicts with PySpark. It gets its own separate environment on 15 Oct.

## Coding conventions
- snake_case; keys end `_id`; dates end `_date`; timestamps end `_ts`; money ends `_amount`
- Money DECIMAL(18,2), never float. Quantities DECIMAL(18,3).
- Every gold table and column gets a plain-English comment (Genie depends on them).
- Indian fiscal year: April–March.
- Every dirt type needs a passing unit test. Tests use small in-test fixtures, not full generated history.

## Folder layout
```
config/config.yaml        local + dev profiles (paths, catalog, schema names)
docs/                     story_sheet.md, kpi_definitions.md, decisions.md, day15_checklist.md, setup_mac.md
src/sales_insights/common/     config.py (load_config), spark.py (get_spark) — ALWAYS use these
src/sales_insights/generator/  masters, history, simulate_day, dirt injectors, manifest
src/sales_insights/drip/       staging -> landing copy (manifest last)
src/sales_insights/pipeline/   bronze.py, silver.py, gold.py, reconcile.py, run_pipeline.py
src/sales_insights/insights/   variance, top movers (Phase 2)
src/sales_insights/deck/       python-pptx deck generator (Phase 3)
sql/kpis/                 KPI SQL views
sql/metric_views/         metric view YAML drafts
genie/                    instructions.md, descriptions.yaml, answer_key.csv
bundle/                   databricks.yml (dev + prod targets), job definitions
tests/
staging/ landing/ lake/   LOCAL ONLY, git-ignored
```

## Rules for working with the human
- Before a non-trivial change, give a 3–5 bullet plan and wait for approval.
- Small, reviewable changes on a feature branch; never commit to `main` directly. Suggest a commit message after each task.
- Never hardcode secrets, tokens or workspace URLs.
- If a request conflicts with this file or the execution plan, say so and ask.
