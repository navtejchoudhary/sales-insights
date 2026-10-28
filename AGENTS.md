# AGENTS.md: Sales Insights MVP on Azure Databricks

Read this first. It is the single source of truth for any AI coding agent (Claude Code reads it through
`CLAUDE.md`; Codex, Cursor and others read it directly) and for new team members. Why each choice was made:
`docs/decisions.md`. Step-by-step recipes: `docs/playbooks/`. Every other doc: `docs/README.md`.
When this file and a doc disagree, this file and `docs/decisions.md` win; ask the human if still unclear.

## What this is

A QuickSight + Amazon Q equivalent on Azure Databricks, built as an MVP for the sponsor (Anil):
dashboards, plain-English questions (Genie), and a system that says what changed without being asked.
All data is **dummy**, generated to look like the company's SAP extracts (a Sri Lankan crop-protection
business, currency LKR, fiscal year April-March) with planted stories and planted dirt, so every answer is
known in advance.

**Status (19 Oct 2026): built and running** on an Azure Databricks trial workspace (bundle target `dev`).
Data is generated up to 29 Oct 2026 (`data_until`). The Azure credit ends 5 Nov 2026; then the workspace is
deleted and this repo plus `workspace_export/` are the record.

| Part | State |
|---|---|
| Sales and stock pipelines (landing, bronze, silver, gold, checks) | Daily jobs 06:30 and 07:00 Asia/Colombo |
| Metric views | 3: `sales_metrics`, `inventory_metrics`, `sales_and_stock` (+ `sales_metrics` again in `client_dev`) |
| AI/BI dashboards | 3: Sales, Stock, Company overview; global filters Province, Product group (+ Channel on Sales); + Sales (SAP data) for the client area, owner only |
| Genie agents | 3: Sales (93-97%), Stock (90-97%), Sales & Stock (97-100%) on their benchmark questions |
| Weekly Genie check | Job every Monday 07:00 scores all 3 agents; below 90% fails and emails |
| Insights, forecast, alerts | Daily findings table, seasonal forecast, 3 SQL alerts (revenue drop, stock-outs) |
| Weekly deck | python-pptx deck with native charts |
| Access | 3 account groups (`sales_team`, `stock_team`, `management`), read-only grants on gold only |
| Real data (from 28 Oct) | In progress: input contract `docs/sap_input_contract.md`; area `client` (catalog `client_dev`, owner only) runs bronze, silver, gold, reconciliation and the sales KPIs on SAP-shaped dummy data; job `client_pipeline` (manual) and dashboard Sales (SAP data); `docs/playbooks/run_the_client_area.md`. Next: the one-time copy of the client's data |
| Not built | Agent Bricks Supervisor (not available on the workspace), a production target |

## How data flows

```
generator (plain Python) -> staging/ -> drip -> landing/ -> bronze -> silver -> gold -> checks
   -> metric views -> dashboards, Genie agents, alerts
   -> insights job (findings, forecast) -> weekly deck
```
- Two business areas, one pipeline and one catalog each: sales (`sales_dev`) and stock (`inventory_dev`).
  Shared dimensions (product, customer, date) live in the sales gold. `config.yaml` -> `areas:`.
- **bronze:** files appended as-is (all STRING) + `_source_file`, `_business_date`, `_load_ts`, `_run_id`;
  exactly-once by file path + SHA-256 in `ops.processed_files`.
- **silver:** FULL REBUILD every run: newest load per file, dedupe, validate (bad rows to
  `silver.quarantine` with reasons), `try_cast`, enrich from masters.
- **gold:** FULL REBUILD. Star schemas; every table and column described in `pipeline/gold_model.py` and
  `pipeline/inventory_model.py` (Genie relies on these comments).
- **checks:** gold vs the source manifests' control totals per invoice date (0.1%), stock roll-forward,
  stock vs sales cross-check. A failed check stops the run before anything is published.

## Where things live

| Path | What |
|---|---|
| `config/config.yaml` | Profiles `local` and `dev`, catalogs, paths, areas. The ONLY place for names and paths |
| `src/sales_insights/common/` | `load_config`, `get_spark`, `Lake` (all table reads and writes) |
| `src/sales_insights/generator/` | Dummy data. Stories ONLY in `stories.py`; names in `masters.py`; stock in `inventory.py`; SAP-shaped data for the real-data phase in `sap.py` |
| `src/sales_insights/pipeline/` | bronze, silver, gold, reconcile, inventory_*, sap_silver + sap_reconcile (SAP billing data), `run_pipeline.py` (orchestration) |
| `src/sales_insights/semantic/` | metric views, KPI check, Genie (answer key, setup, sync, health, failures), dashboards, export |
| `src/sales_insights/insights/`, `deck/` | Findings, forecast, narrative; weekly PowerPoint |
| `sql/metric_views/*.yaml` | Every KPI defined ONCE; `sql/kpis/` holds the readable SQL twin |
| `sql/setup/` | Catalog setup and `permissions.sql` (grants) |
| `dashboards/<name>.yaml` | Dashboard source; `<name>.lvdash.json` is GENERATED from it |
| `genie/`, `genie/inventory/`, `genie/sales_and_stock/` | Per agent: `space.yaml` (instructions), `benchmarks.yaml` (questions + true SQL) |
| `databricks.yml`, `bundle/` | Asset Bundle: jobs, dashboards, alerts; `bundle/run.py` is the job entry point |
| `workspace_export/` | Genie agents and dashboards as exported from the workspace before deletion (no IDs, URLs or e-mails) |
| `docs/` | Design docs, decisions log, playbooks (`docs/README.md` lists them) |
| `staging/`, `landing/`, `lake/`, `output/` | Local runtime folders, git-ignored |

## Commands

```bash
uv sync                                   # exact versions from uv.lock (Python 3.12, Java 17 needed)
uv run pytest -m "not spark"              # quick tests (seconds)
uv run pytest                             # all tests incl. Spark (~10 min); must pass before every merge
uv run ruff check . && uv run ruff format .
uv run python -m sales_insights.pipeline.run_pipeline --date 2026-10-06        # one day, every step, local
uv run python -m sales_insights.pipeline.run_pipeline --area inventory --date 2026-10-06
uv run python -m sales_insights.pipeline.run_pipeline --area client --date 2026-09-30     # SAP billing data
uv run python -m sales_insights.semantic.dashboards                           # regenerate dashboards/*.lvdash.json
databricks bundle deploy -t dev --profile sales-dev                           # jobs, dashboards, alerts
databricks bundle deploy -t dev --profile sales-dev --var schedule_pause_status=PAUSED   # same, schedules stay paused
```
Genie (setup, benchmark sync, failure export) and deployment steps: `docs/playbooks/`.

## Hard rules

1. **Pipeline logic is PySpark + Delta only** (bronze to KPIs). Never pandas or DuckDB there. The generator
   is plain Python (NumPy, pandas) and must stay deterministic: one seed per date and per invoice, no
   shared random stream, fixed name lists (no Faker). `simulation_start` = 2026-10-01, never changed.
2. **IO apart from logic:** transforms take and return DataFrames. Read and write tables ONLY through
   `Lake`; names and paths ONLY from `config/config.yaml`. Same schema names everywhere.
3. **Databricks-only features** (metric views, Genie, AI functions, alerts, volumes) stay in thin wrappers.
4. **One definition per KPI:** change a formula in the metric view YAML AND its `sql/kpis` twin;
   `semantic/kpis.py` proves they agree.
5. **Generated files are never edited by hand:** `dashboards/*.lvdash.json`, `docs/gold_model.md`,
   `genie/answer_key.csv`. Edit the source, regenerate, commit both.
6. **Security (the repo is public):** no real company data (the reference SAP file stays in git-ignored
   `reference/`); no tokens, `.databrickscfg`, workspace URLs, subscription IDs or e-mails in the repo.
7. Library versions are pinned in `pyproject.toml`; do not upgrade or add packages without asking.
   Never install `databricks-connect` in this environment (it breaks PySpark).
8. Conventions: snake_case; `_id` keys, `_date` dates, `_ts` timestamps, `_amount` money; money
   DECIMAL(18,2), quantities DECIMAL(18,3); fiscal year April-March labelled by its start year
   (April 2026 = FY2026); time zone Asia/Colombo; every dirt type has a unit test with an in-test fixture.

## Lessons learned (each cost us a day once)

- **Dashboard JSON:** copy the shapes of Databricks' own exported dashboards (bundle-examples) and never
  guess keys; a guessed key shows "Invalid widget definition". Tables sort by the dataset's ORDER BY, not
  by a hidden column. Colours come from the theme palette by position (`PALETTE` in `semantic/dashboards.py`).
- **Browser edits are overwritten** by the next `bundle deploy`. Change the YAML instead.
- **Global filters are parameter filters:** the generator adds `(:p = 'All' OR field = :p)` to every
  metric-view dataset; mark forecast-like datasets `company_wide: true`. A field filter would leave the
  KPI tiles unchanged.
- **Genie benchmarks:** load them with `genie_setup --as-of <data date>` (questions about later dates are
  dropped otherwise); run them in **Chat** mode (Agent mode scored lower in our runs); the grader compares
  result tables exactly, so expected answers must have the shape the question asks for, and per-day
  averages and days of cover must be unrounded on both sides. "Needs review" counts as wrong in the weekly
  check. Instructions are pasted into the agent by hand (Configure > Instructions); benchmarks via
  `genie_sync`.
- **Genie instructions that fixed real mistakes:** name the movement types (653 return, 313/315 transfer),
  say "product group = the product_group column, never a name match", name the latest-date rule.
- **Unity Catalog grants need account-level groups;** workspace-local groups cannot be granted.
- **Data date:** the data is fast-forwarded to 29 Oct 2026; "today" in questions means the latest data date.
- **Stock is a level, not a flow:** for a period use its last day, never a sum over days (window measures
  with `semiadditive: last` in the metric views).

## Path to production

The code stays; inputs, workspace and people change. In order: (1) real SAP extracts land daily in the same
file layout with a manifest of control totals; (2) a `prod` bundle target with its own catalogs, jobs run
by a service principal; (3) the three groups mapped to Microsoft Entra ID groups; (4) KPI formulas agreed
with finance (today "Working" definitions in `docs/kpi_definitions.md`); (5) Genie re-tuned on the questions
people actually ask, weekly 90% check kept; (6) more areas via `docs/playbooks/add_a_business_area.md`.

## Glossary

Depot = sales office's stock point (11); hub = Galle main store 2010-0090; returns store 2010-0032;
Maha (Sep-Nov) and Yala (Mar-May) = cultivation seasons; ZAOR invoice, ZARE return, S1 cancellation;
movement 601 goods issue, 602 cancelled delivery back, 653 customer return, 313/315 hub transfer out/in.
