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
- The generator is modelled on a reference SAP O2C billing extract (Sri Lanka, LKR). Keep its column names and code values (10-digit customer IDs, district region codes, sales offices, divisions, material types). The reference file lives in `reference/` and is git-ignored: NEVER commit it or copy rows from it (the repo is public).
- Generator (plain Python: NumPy, pandas) writes CSV/JSON like an outside source system. Use the fixed name lists in `generator/masters.py`, NOT Faker (output must be identical across library versions).
- Story settings live ONLY in `src/sales_insights/generator/stories.py`.
- Masters once: products, customers, regions/cities, channels, sales reps.
- History: 18 months of invoice lines ending YESTERDAY, in the reference extract's 74 columns + `reference_invoice_number` (links ZARE returns and S1 cancellations to the invoice they reverse). See `docs/invoice_data.md` for document types and sign rules.
- Daily drop: `staging/business_date=YYYY-MM-DD/` with
  - `orders_YYYYMMDD.csv` — new order lines
  - `changes_YYYYMMDD.csv` — cancellations, returns, price corrections to older orders
  - `manifest_YYYYMMDD.json` — WRITTEN LAST: row counts, control totals (orders, gross, net revenue), list of dirt injected
- Fixed random seed per date AND per invoice (see engine.py docstring): any day can be rebuilt alone and is byte-identical. Never introduce a shared random stream across days.
- `simulation_start` (config) is fixed at 2026-10-01; generators default to it. Never change it mid-project.
- `drip --date` copies `staging/<date>/` to `landing/<date>/`, manifest last.
- Planted stories and dirt rates live in `docs/story_sheet.md`. Every dirt injection is logged in the manifest.
- Answer key: `genie/benchmarks.yaml` — 30 questions with ground-truth SQL and a check per planted story; `python -m sales_insights.semantic.answer_key` computes `genie/answer_key.csv` from gold (story checks must PASS). Genie agent config: `genie/space.yaml`, setup: `genie/README.md`

## Architecture
landing → bronze → silver → gold → reconciliation → metric views → dashboard / Genie / alerts → insights job → SupervisorAgent + deck job
- bronze: append as-is, all columns as STRING, plus `_source_file`, `_business_date`, `_load_ts`, `_run_id`, `_rescued_data`; exactly-once via `ops.processed_files` (path + SHA-256); new columns added (mergeSchema). See `docs/pipeline.md`
- silver: FULL REBUILD each run (no MERGE; see decisions). Newest load of each file → dedupe on `invoice_number` + `invoice_item` → validate (bad rows to `silver.quarantine` with reasons) → `try_cast` types → enrich from masters (10-digit customer IDs, canonical city from `cities.csv`, blank codes filled) → `is_cancelled`, `arrival_delay_days`. Masters: latest version per key by `last_updated_timestamp`. Manifests parsed into `ops.manifest_totals` / `ops.manifest_dirt`; quarantine must equal the manifests' invalid rows. See `docs/pipeline.md`
- gold: FULL REBUILD. Star schema `fact_sales` (invoice-line grain) + `dim_customer`, `dim_product`, `dim_region` (district → province), `dim_date` (whole fiscal years, cultivation season), `dim_channel`. Every table/column description lives ONLY in `pipeline/gold_model.py` (`with_comments` refuses undocumented columns); regenerate `docs/gold_model.md` after changing it
- reconciliation: per (delivery, file kind, invoice date) gold vs `ops.manifest_totals` (which already exclude injected dirt), lines/invoices/revenue within 0.1%; results to `ops.reconciliation` + `ops.dq_results`; CLI exits 1 on FAIL
- semantic layer: every KPI defined ONCE in `sql/metric_views/sales_metrics.yaml` (metric view YAML 1.1); `sql/kpis/*.sql` is the readable twin; `semantic/kpis.py` proves both agree (locally the metric view is compiled to plain SQL by `semantic/metric_views.py`). Change a formula in BOTH places. See `docs/kpi_definitions.md`
- `ops.processed_files` (bronze's file log) works on both Mac and Databricks volumes; Auto Loader is optional

## PORTABILITY RULES (most important)
1. ALL pipeline code (bronze, silver, gold, reconciliation, KPI SQL) uses PySpark + Delta (`delta-spark`). Never pandas or DuckDB for pipeline logic. Exception: the generator/simulator only (plain Python).
2. Keep IO apart from logic: transform functions take DataFrames and return DataFrames. Paths and catalog names come ONLY from `config/config.yaml` with profiles `local` and `dev`.
3. Same schema names everywhere: `bronze`, `silver`, `gold`, `ops` locally; `sales_dev.bronze` etc. on Databricks.
4. Databricks-only features (Auto Loader, volumes, triggers, metric views, Genie, AI functions) go in thin wrappers, never mixed into transform logic.
7. Read and write tables ONLY through `common/lake.py` (`Lake`): path-based Delta locally, Unity Catalog names on Databricks. Never hardcode a table path or name.
5. Library versions are pinned in `pyproject.toml`. Do not upgrade without asking.
6. SQL must be Spark SQL / Databricks SQL compatible.

## Local environment
- Python 3.12 via `uv`; Java 17; PySpark 4.0.x + delta-spark 4.0.x
- Run: `uv run python ...` · Add packages: `uv add ...` (ask first) · Tests: `uv run pytest` (quick: `uv run pytest -m "not spark"`) · Lint: `uv run ruff check . && uv run ruff format .`
- NEVER install `databricks-connect` in this environment — it conflicts with PySpark. It gets its own separate environment on 15 Oct.

## Coding conventions
- snake_case; keys end `_id`; dates end `_date`; timestamps end `_ts`; money ends `_amount`
- Money DECIMAL(18,2), never float. Quantities DECIMAL(18,3).
- Every gold table and column gets a plain-English comment (Genie depends on them).
- Fiscal year April–March, labelled by its start year (April 2026 = FY2026 period 1), as in the reference extract. Currency LKR. Timezone Asia/Colombo.
- Every dirt type needs a passing unit test. Tests use small in-test fixtures, not full generated history.

## Folder layout
```
config/config.yaml        local + dev profiles (paths, catalog, schema names)
docs/                     story_sheet.md, kpi_definitions.md, decisions.md, day15_checklist.md, setup_mac.md
src/sales_insights/common/     config.py (load_config), spark.py (get_spark) — ALWAYS use these
src/sales_insights/generator/  stories.py (settings), masters.py, engine.py (shared per-day sales + per-invoice fates), history.py, simulate.py (daily drops), dirt.py
src/sales_insights/drip/       drip.py: staging -> landing copy (manifest last; missing day, Monday redelivery)
src/sales_insights/pipeline/   bronze.py, silver.py, gold.py, reconcile.py, run_pipeline.py
src/sales_insights/semantic/    metric_views.py (load/compile/publish metric view), kpis.py (KPI views + agreement check), answer_key.py (Genie benchmarks)
src/sales_insights/insights/   variance, top movers (Phase 2)
src/sales_insights/deck/       python-pptx deck generator (Phase 3)
sql/kpis/                 KPI SQL views
sql/metric_views/         metric view YAML drafts
genie/                    space.yaml (agent config), benchmarks.yaml (questions + SQL), answer_key.csv (generated), README.md (setup)
bundle/                   databricks.yml (dev + prod targets), job definitions
tests/
staging/ landing/ lake/   LOCAL ONLY, git-ignored
```

## Rules for working with the human
- Before a non-trivial change, give a 3–5 bullet plan and wait for approval.
- Small, reviewable changes on a feature branch; never commit to `main` directly. Suggest a commit message after each task.
- Never hardcode secrets, tokens or workspace URLs.
- If a request conflicts with this file or the execution plan, say so and ask.
