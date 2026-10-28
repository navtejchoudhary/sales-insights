# Playbook: deploy, run every day, fix problems, shut down

First deployment from nothing (workspace, catalogs, volumes, groups): `docs/day15_runbook.md`.
This page is for a workspace that already runs.

## Deploy a change

```bash
git checkout main && git pull
databricks bundle validate -t dev --profile sales-dev
databricks bundle deploy -t dev --profile sales-dev
```
While the demo is paused (to save cost), keep it paused through the deploy: add
`--var schedule_pause_status=PAUSED`. Without it every schedule (daily jobs, weekly Genie check, alerts) is
switched back on, because that is the default.

The output lists what changed (`Updated jobs.…`, `Updated dashboards.…`). A deploy that would delete or
recreate a dashboard asks first; recreated dashboards get new links.

## What runs by itself (Asia/Colombo)

| Job | When | What |
|---|---|---|
| `[dev] sales-insights-daily` | 06:30 daily | drip, bronze, silver, gold, reconcile, KPIs, answer key, insights, deck |
| `[dev] inventory-daily` | 07:00 daily | the same for stock, after sales |
| Alerts (3) | 08:00 daily | email when the latest insights hold a revenue drop or a stock-out |
| `[dev] sales-insights-genie-health` | Monday 07:00 | benchmarks of all 3 Genie agents; fails under 90% |
| Backfill and publish jobs | by hand | rebuild from scratch (`data_until`), recreate metric views |

Every job emails on failure. A failed check stops the run before publishing, so dashboards keep the last
checked numbers.

## Morning check (2 minutes)

1. Jobs & Pipelines > Runs: today's runs green.
2. Company overview: "Data status" line shows today's data date and all checks passed.
3. Any alert email: open "What needs attention".

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Reconciliation FAIL | real data problem | `SELECT * FROM sales_dev.ops.reconciliation WHERE status = 'FAIL'`; do not demo until fixed |
| Inventory job: `TABLE_OR_VIEW_NOT_FOUND sales_dev.gold…` | sales not loaded to that date | run `sales_backfill` with the same `data_until` first |
| Inventory checks FAIL after a backfill | the two backfills used different dates | rerun `inventory_backfill` with the sales date |
| Dashboard widget: "Invalid widget definition" | JSON shape not as Databricks exports it | `change_a_dashboard.md`, step 5 |
| Dashboard change gone | edited in the browser, then deployed | change the YAML, regenerate, deploy |
| Grant fails: principal does not exist | group is workspace-local | create the group at account level (`docs/access.md`) |
| Genie score dropped | wording, data date or a real mistake | `tune_a_genie_agent.md` |
| `FIELD_ALREADY_EXISTS` in the SQL editor | a one-time `ALTER TABLE` ran twice | harmless; clear the editor tab |
| `ModuleNotFoundError: sales_insights` in a job | bundle files moved | check the task's `python_file` path |
| Job slow (over 40 min) | serverless cold start + full rebuilds | expected for backfills |

## Before the workspace is deleted

1. Export what was built in the workspace (Genie agents, deployed dashboards), scrubbed of IDs and URLs:
   ```bash
   uv run python -m sales_insights.semantic.export_workspace --profile sales-dev \
     --space sales=<id> --space inventory=<id> --space sales_and_stock=<id>
   ```
   Copy `output/export/*` to `workspace_export/` and commit it.
2. Pause every schedule (jobs and alerts) with one deploy:
   `databricks bundle deploy -t dev --profile sales-dev --var schedule_pause_status=PAUSED`.
3. Azure portal: delete the resource group `sales-insights-rg`; check the credit page a day later.

Rebuild on a new workspace: `docs/day15_runbook.md`, then create the Genie agents from
`workspace_export/genie_*.json` or `genie/*/README.md`.
