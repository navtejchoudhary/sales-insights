# Day 15 runbook: from nothing to a running Databricks pipeline

Goal for Thu 15 Oct: the same pipeline that runs on the Mac runs every morning on Azure Databricks, with the
metric view and Genie on top. Expected effort: half a day. Do the steps in order; each says what you should see.
Record go / no-go results in `docs/day15_checklist.md`.

**Golden rule: the trial clock starts when the workspace is created. Do steps 1-2 only on 15 Oct.**

## 0. The evening before (Mac)

| # | Do | You should see |
|---|---|---|
| 0.1 | `git checkout main && git pull` | up to date |
| 0.2 | `uv run pytest` | all tests pass |
| 0.3 | `uv run python -m sales_insights.pipeline.run_pipeline --from 2026-10-06 --to 2026-10-14` | `DONE: 9 of 9 day(s)` |
| 0.4 | `brew upgrade databricks` then `databricks --version` | a recent version (bundles need 0.218+; the `presets` setting needs a recent one) |

## 1. Azure: protect the credit first

| # | Do | You should see |
|---|---|---|
| 1.1 | Azure portal > **Cost Management > Budgets > Add**: monthly budget USD 50, alerts at 50%, 80%, 100% to your email | budget listed |
| 1.2 | Note the credit balance and expiry (Cost Management > Credits) | write both in `docs/decisions.md` |

## 2. Create the workspace

| # | Do | You should see |
|---|---|---|
| 2.1 | Portal > **Create a resource > Azure Databricks** | create form |
| 2.2 | Resource group `sales-insights-rg`; workspace name `sales-insights-dev`; region **East US 2** (serverless compute and AI functions without extra settings; see decisions); **Pricing tier: Trial (Premium - 14-Days Free DBUs)** | validation passed |
| 2.3 | Create, wait ~5 minutes, **Launch workspace** | Databricks home page |
| 2.4 | **Catalog** in the left menu: a metastore is attached (new workspaces get Unity Catalog automatically) | catalogs listed |

## 3. Catalog, schemas and volumes (SQL editor)

```sql
CREATE CATALOG IF NOT EXISTS sales_dev;
CREATE SCHEMA IF NOT EXISTS sales_dev.raw;
CREATE SCHEMA IF NOT EXISTS sales_dev.bronze;
CREATE SCHEMA IF NOT EXISTS sales_dev.silver;
CREATE SCHEMA IF NOT EXISTS sales_dev.gold;
CREATE SCHEMA IF NOT EXISTS sales_dev.ops;
CREATE VOLUME IF NOT EXISTS sales_dev.raw.staging;
CREATE VOLUME IF NOT EXISTS sales_dev.raw.landing;
CREATE VOLUME IF NOT EXISTS sales_dev.raw.output;
```
You should see: `sales_dev` with 5 schemas and 3 volumes in Catalog Explorer.
If `CREATE CATALOG` is refused (no storage location), use the workspace's own catalog instead: change
`catalog:` and the three `/Volumes/sales_dev/...` paths in the `dev` profile of `config/config.yaml`, commit, continue.

## 4. Connect the Mac to the workspace

```bash
databricks auth login --host https://<your-workspace>.azuredatabricks.net --profile sales-dev
export DATABRICKS_CONFIG_PROFILE=sales-dev
databricks current-user me
```
You should see your user. The token is stored in `~/.databrickscfg` on your Mac only (never commit it).

## 5. Deploy the jobs

```bash
cd ~/Projects/sales-insights
databricks bundle validate
databricks bundle deploy -t dev
```
You should see `Deployment complete!` and, under **Jobs & Pipelines**, three jobs prefixed `[dev] `:
`sales-insights-daily`, `sales-insights-backfill`, `sales-insights-publish-semantic`.

## 6. First load (backfill) and checks

```bash
databricks bundle run -t dev sales_backfill
```
It generates master data, 18 months of history and every day from 1 Oct, delivers them to the landing
volume, and runs the whole pipeline once. Expect 15-40 minutes on serverless. The run log ends with
`DONE: n of n day(s)`. Then, in the SQL editor:

```sql
SELECT status, COUNT(*) FROM sales_dev.ops.reconciliation
WHERE run_id = (SELECT MAX(run_id) FROM sales_dev.ops.reconciliation) GROUP BY status;   -- only PASS
SELECT COUNT(*) FROM sales_dev.gold.fact_sales;                                         -- same as the Mac for the same dates
SELECT kind, COUNT(*) FROM sales_dev.gold.insights GROUP BY kind;
```

## 7. KPI views and metric view

```bash
databricks bundle run -t dev sales_publish_semantic
```
You should see `every KPI matches the metric view`. Then check the metric view:
```sql
SELECT month, MEASURE(net_revenue) AS net_revenue
FROM sales_dev.gold.sales_metrics GROUP BY ALL ORDER BY month;
```
Same numbers as `SELECT month, net_revenue FROM sales_dev.gold.kpi_sales_monthly ORDER BY month`.

## 8. Genie

Follow `genie/README.md` ("Day 15+: create the Genie agent"), then run the 75 benchmarks.
Record the score in `docs/decisions.md` (target 90%).

## 9. Feature go / no-go

Work through `docs/day15_checklist.md`: `ai_query` (narrative model), `ai_forecast`, Supervisor agent,
file-arrival trigger. Each has a fallback written next to it; nothing in the demo depends on a feature
that fails here.

## 10. Daily schedule

The daily job runs at **06:30 Sri Lanka time** and processes that day's date. Check the next morning:
**Jobs & Pipelines > sales-insights-daily > Runs**: green, and the deck appears in the `output` volume.
On failure you get an email (the job notifies the user who deployed it).

## 11. Inventory area (stock), added 15 Oct

Same workspace, its own catalog `inventory_dev` and its own jobs; sales jobs and `sales_dev` are not touched.
Do the steps in order. Use **the same `data_until` date for sales and inventory** (inventory reads the sales gold).

| # | Do | You should see |
|---|---|---|
| 11.1 | SQL editor: run `sql/setup/inventory_dev.sql` | catalog `inventory_dev` with schemas raw, bronze, silver, gold, ops and 3 volumes |
| 11.2 | Mac: `git pull`, then `databricks bundle validate` and `databricks bundle deploy -t dev` | 3 new jobs: `[dev] inventory-backfill`, `[dev] inventory-daily`, `[dev] inventory-publish-semantic` |
| 11.3 | Sales up to the chosen date: `databricks bundle run -t dev sales_backfill --params data_until=<date>` (omit `--params` for today) | `DONE`, reconciliation all PASS, answer key 0 FAIL |
| 11.4 | `databricks bundle run -t dev inventory_backfill --params data_until=<same date>` | `checks … passed`, `kpis 3 KPI views match`, `answer_key … 0 FAIL` |
| 11.5 | `databricks bundle run -t dev inventory_publish_semantic` | `every KPI matches the metric view`; metric views `inventory_dev.gold.inventory_metrics` and `inventory_dev.gold.sales_and_stock` exist |
| 11.6 | SQL editor: the stock-measure check below | September stock = the 30 Sep stock, not 30 days added up |
| 11.7 | From the next morning: `[dev] inventory-daily` runs at 07:00, after the 06:30 sales job | green run, email only on failure |

```sql
-- 11.6: window measure check. The two numbers must be equal.
SELECT MEASURE(stock_on_hand) AS september_stock
FROM inventory_dev.gold.inventory_metrics WHERE month = DATE'2026-09-01';
SELECT SUM(stock_quantity) AS stock_on_30_sep
FROM inventory_dev.gold.fact_stock_daily WHERE snapshot_date = DATE'2026-09-30';
```

**Running the simulated days ahead (`data_until` later than today).** The generator can produce any day, so
a demo can show the planted live stories (new sales rep 22 Oct, stock-out 23 Oct) before the calendar reaches
them. The daily jobs keep working afterwards: a day that is already loaded is not loaded twice.

If 11.5 fails on the metric view (window measures refused), keep going: the KPI views
`inventory_dev.gold.kpi_stock_month_end` and `kpi_stock_daily` hold the same numbers; point Genie and the
dashboard at them and tell Claude the exact error.

## 12. Dashboards and access, added 15 Oct

Full explanation: `docs/access.md`. Do the steps in order (the deploy in 12.3 needs the groups).

| # | Do | You should see |
|---|---|---|
| 12.1 | Settings > Identity and access > Groups > Manage > Add group > Add new: `sales_team`, `stock_team`, `management`; add yourself to `management` | three groups |
| 12.2 | SQL editor: run `sql/setup/permissions.sql`; SQL Warehouses > `sales-insights-wh` > Permissions: the 3 groups, *Can use* | `SHOW GRANTS ON SCHEMA sales_dev.gold;` lists sales_team and management |
| 12.3 | Mac: `git pull`, `databricks bundle deploy -t dev` | `Sales`, `Stock`, `Company overview` under Dashboards |
| 12.4 | Open each dashboard | every widget shows data; "Data status" says all checks passed |
| 12.5 | Each Genie agent > Share: Sales -> sales_team + management; Stock -> stock_team + management; Sales & Stock -> management (*Can run*) | |
| 12.5b | Once, before the next deploy: SQL editor `ALTER TABLE sales_dev.ops.genie_accuracy ADD COLUMNS (agent STRING);` then `databricks bundle deploy`; optionally `databricks bundle run -t dev sales_genie_health` (about 1 hour: all 3 agents) | the weekly check covers the Sales, Stock and Sales & Stock agents; the Company overview shows the weakest score |
| 12.6 | Before deleting the workspace: `uv run python -m sales_insights.semantic.export_workspace --space sales=<id> --space inventory=<id> --space sales_and_stock=<id>` | files in `output/export/` |

## If something goes wrong

| Symptom | Likely cause | Fix |
|---|---|---|
| `bundle validate`: "mode: development cannot set trigger pause status to UNPAUSED" | development mode always pauses schedules | fixed 7 Oct: the dev target has no `mode`, only a `[dev] ` name prefix |
| `environment_version` rejected | serverless environment 6 not offered | set `"5"` (also Python 3.12) in `bundle/resources/jobs.yml`, deploy again |
| Job fails at `ModuleNotFoundError: sales_insights` | bundle files not where `run.py` expects | open the run, check `python_file` path; files are under the bundle's workspace folder |
| `PERMISSION_DENIED` on catalog or volume | catalog created by someone else | `GRANT ALL PRIVILEGES ON CATALOG sales_dev TO <you>` |
| Reconciliation FAILED | a real data problem | read `ops.reconciliation` rows with `status = 'FAIL'`; do not demo until fixed |
| Job slow (> 40 min) | serverless cold start + full rebuilds | fine for the demo; note the time in decisions |
| Inventory job: `TABLE_OR_VIEW_NOT_FOUND sales_dev.gold...` | sales not loaded up to the same date yet | run `sales_backfill` with the same `data_until` first |
| Inventory checks FAIL right after the backfill | the two backfills used different dates | rerun `inventory_backfill` with the sales date |
