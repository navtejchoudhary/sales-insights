# Sales Insights MVP on Azure Databricks

[![CI](https://github.com/navtejchoudhary/sales-insights/actions/workflows/ci.yml/badge.svg)](https://github.com/navtejchoudhary/sales-insights/actions/workflows/ci.yml)

A Databricks equivalent of **Amazon QuickSight + Amazon Q**: dashboards, answers to plain-English questions
(Genie), and a system that tells you what changed without being asked. Built end to end on **dummy data**
shaped like a company's SAP order-to-cash and stock extracts (a Sri Lankan crop-protection business, LKR),
with stories and data problems planted on purpose, so every answer can be checked.

**AI agents and new developers: read [`AGENTS.md`](AGENTS.md) first**, then [`docs/README.md`](docs/README.md).

## What it does

| Capability | How |
|---|---|
| Fresh, checked data every morning | Daily jobs load new files through bronze, silver and gold; numbers are reconciled with the source's control totals before anyone sees them |
| One definition per KPI | Unity Catalog metric views; dashboards, Genie and alerts all read them |
| Dashboards | Sales, Stock and Company overview (AI/BI), with global filters for province, product group and channel |
| Questions in plain English | 3 Genie agents (Sales, Stock, Sales & Stock), each scored weekly on known answers (90%+ target) |
| Tells you what changed | Daily findings (movers, revenue drops, stock-outs, unusual days), a seasonal forecast, email alerts |
| Weekly report | A PowerPoint deck with native charts, built automatically |
| Access by team | Groups `sales_team`, `stock_team`, `management`; read-only, business tables only |

Example: on 23 Oct 2026 sales of the top weedicide stop in Western province. The Company overview turns
red, the stock-out alert emails, and asking Genie "Why did sales of MC6001LT stop in Western province?"
answers: the Western depots are empty, the hub still holds stock, the last transfer was a month earlier,
and customers switched to the 4-litre pack.

## Architecture

```
generator -> staging/ -> drip -> landing/ -> bronze -> silver -> gold -> checks
   -> metric views -> dashboards, Genie agents, alerts
   -> insights job (findings, forecast) -> weekly deck
```
Two business areas (sales, stock), one catalog and pipeline each, on PySpark + Delta Lake. The same code
runs on a Mac (path-based Delta) and on Databricks (Unity Catalog); jobs, dashboards and alerts are
deployed with a Databricks Asset Bundle. Design decisions and their reasons: [`docs/decisions.md`](docs/decisions.md).

## Status (19 Oct 2026)

Running on an Azure Databricks trial workspace with data up to 29 Oct 2026. 344 automated tests.
Genie benchmark scores: Sales 93-97%, Stock 90-97%, Sales & Stock 97-100%. Not built: the Agent Bricks
Supervisor (not available on the trial workspace), real data, a production target
(see "Path to production" in [`AGENTS.md`](AGENTS.md)).

## Quick start (Mac)

Set up once with [`docs/setup_mac.md`](docs/setup_mac.md) (Python 3.12 via uv, Java 17). Then:

```bash
uv sync                                    # exact package versions from uv.lock
uv run pytest -m "not spark"               # quick tests (seconds)
uv run pytest                              # all tests incl. Spark (about 10 minutes)
uv run python -m sales_insights.pipeline.run_pipeline --date 2026-10-06                   # one day, every step
uv run python -m sales_insights.pipeline.run_pipeline --from 2026-10-06 --to 2026-10-29   # day by day
```
One line per step, stops at the first failed check, report in `output/`. Data generation step by step:
[`docs/pipeline.md`](docs/pipeline.md). On Databricks: [`docs/playbooks/deploy_and_operate.md`](docs/playbooks/deploy_and_operate.md).

## Folder map

| Folder | What lives there |
|---|---|
| `config/` | `config.yaml`: profiles `local` and `dev`, catalogs, paths, business areas |
| `src/sales_insights/` | `generator/`, `drip/`, `pipeline/`, `semantic/`, `insights/`, `deck/`, `common/` |
| `sql/` | Metric views (`metric_views/`), KPI SQL twins (`kpis/`), setup and grants (`setup/`) |
| `dashboards/` | Dashboard YAML sources and the generated `.lvdash.json` files |
| `genie/` | Genie agents: instructions and benchmark questions with their true SQL |
| `bundle/`, `databricks.yml` | Asset Bundle: jobs, dashboards, alerts |
| `workspace_export/` | Genie agents and dashboards exported from the workspace |
| `docs/` | Design docs, decisions log, playbooks ([index](docs/README.md)) |
| `tests/` | Unit and Spark tests |

`staging/`, `landing/`, `lake/` and `output/` are created at runtime and never committed.

## Working rules

Feature branch per change, tests green before merging, never commit to `main` directly. Pipeline code is
PySpark + Delta only; names and paths come only from `config/config.yaml`. No real company data, tokens or
workspace URLs in the repo (it is public). Full rules: [`AGENTS.md`](AGENTS.md).
