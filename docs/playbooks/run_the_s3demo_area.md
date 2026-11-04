# Playbook: run the s3demo area (the sales demo files, read from AWS S3)

The s3demo area proves that the pipeline can take its input straight from an AWS S3 bucket. The files are the
sales demo files (master data, 18-month history, daily drops 1-29 Oct 2026, with their planted dirt), uploaded
once to S3. Databricks reads them read-only and runs the sales steps on them: drip, bronze, silver, gold,
reconciliation, KPI check and insights (findings, forecast and the weekly PowerPoint deck), into its own catalog
`s3demo_dev`, then publishes the KPI views and the metric view `s3demo_dev.gold.sales_metrics`. On top: the
dashboard **Sales (S3 data)** (the Sales dashboard's pages) and a Genie agent with the Sales agent's questions.

What it never does:
- write to S3 (the credential, the external location and the volume are all read-only);
- generate data (`source: external` in `config/config.yaml`): if a file is missing, the run stops and says which;
- touch `sales_dev`, `inventory_dev`, `client_dev` or Redshift (own catalog, own folders, own job);
- run by itself (no schedule) or retry by itself (`max_retries: 0`).

Not included: the Genie answer-key step and the SQL alerts (the Sales agent's answer key and the demo alerts
cover the same files). Everything is owner only: no group grants, the dashboard has no `permissions`.

Never put the bucket name, the role ARN or the account ID in git: the repo is public and the bucket is the
client's. They live only in the workspace (credential, external location, volume).

## Once: the connection (Catalog > External Data)

Done on 4 Nov 2026:
1. **Credential** `aws_s3_dummy_readonly`: Create > Create a credential > Storage Credential, type AWS IAM
   Role, the role ARN from the AWS team, Advanced > Read only. Its External ID goes back to the AWS team for the
   role's trust policy. Validate Configuration: Assume Role, Self Assume Role, External ID Condition all green.
2. **External location** `s3_dummy_share`: Create > Create an external location, Storage type Amazon S3, URL =
   the uploaded folder, credential `aws_s3_dummy_readonly`, Read only. Read, List and Path Exists green. The
   "file events" error (Azure queue vs S3) is expected: Force create. File events only speed up Auto Loader,
   which this pipeline does not use.
3. Check: `LIST '<the folder URL>sales/';` shows `masters/`, `history/` and 29 `business_date=...` folders.

Source: [Connect to an AWS S3 external location (read-only)](https://learn.microsoft.com/azure/databricks/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual).

## Once: the catalog

Databricks > SQL Editor: paste `sql/setup/s3demo_dev.sql`. Replace the placeholder in the last statement with
the S3 URL of the sales folder (ends in `/sales/`), then **Run all**. Do not save that URL into git.

Check:
```sql
SHOW GRANTS ON CATALOG s3demo_dev;     -- no rows: only you
LIST '/Volumes/s3demo_dev/raw/source/'; -- masters/, history/, business_date=2026-10-01/ ... 2026-10-29/
```

## Deploy and run

```bash
git checkout main && git pull
databricks bundle deploy -t dev --profile sales-dev --var schedule_pause_status=PAUSED
databricks bundle run -t dev s3demo_pipeline --profile sales-dev
```
- `--var schedule_pause_status=PAUSED` keeps the demo's daily jobs, weekly check and alerts paused.
- The run delivers each day from the S3 volume to `raw.landing` (the same drip rules as sales: 18 Oct arrives
  on 19 Oct, Sunday's orders file again on Monday), then bronze, silver, gold and reconcile once at the end.
- Or in the workspace: Jobs & Pipelines > `[dev] s3demo-pipeline` > Run now.
- Two tasks: `pipeline` (the steps above, about 10-15 minutes), then `publish` (KPI views + metric view), which
  starts only when every check of `pipeline` passed.

## The weekly deck

The insights step writes `/Volumes/s3demo_dev/raw/output/weekly_deck_2026-10-29.pptx` (data date in the name):
Catalog > s3demo_dev > raw > output > the file > Download. Same slides as the sales deck: KPI cards, trend,
outlook with the forecast, what moved revenue, alerts, summary, how the numbers were checked.

## The dashboard

Dashboards > **Sales (S3 data)** (deployed with the bundle, owner only). The Sales dashboard's three pages on
`s3demo_dev`; it shows data once the job (both tasks) has run.

## The Genie agent (by hand, once)

1. Write the setup sheet and benchmarks with the S3 tables:
   ```bash
   uv run python -m sales_insights.semantic.genie_setup --profile dev --genie s3demo --as-of 2026-10-29
   ```
   -> `output/genie_setup_s3demo.md` and `output/genie_benchmarks_s3demo.csv`.
2. Genie Agents > New: title **Sales Analytics (S3 data)**, then follow the sheet: data assets (all in
   `s3demo_dev.gold`; never `client_src_catalog`), hidden columns, instructions, sample questions, trusted and
   example queries.
3. Benchmarks (the Agent ID is on the About tab):
   ```bash
   uv run python -m sales_insights.semantic.genie_sync --space-id <agent id> --csv output/genie_benchmarks_s3demo.csv
   ```
   Run them in **Chat** mode. The questions and data are the Sales agent's, so its score (93-97%) is the yardstick.

## Check the result

```sql
-- every file loaded once
SELECT target_table, COUNT(*) AS files, SUM(rows) AS rows FROM s3demo_dev.ops.processed_files GROUP BY ALL;
-- every check of the run
SELECT layer, check_name, status, COUNT(*) AS results FROM s3demo_dev.ops.dq_results GROUP BY ALL ORDER BY 1, 2;
-- the planted dirt was caught
SELECT reasons, COUNT(*) AS rows FROM s3demo_dev.silver.quarantine GROUP BY reasons ORDER BY rows DESC;
-- the same numbers as the sales demo, which was built from the same files
SELECT 's3demo' AS area, COUNT(*) AS lines, SUM(net_revenue_amount) AS net_revenue FROM s3demo_dev.gold.fact_sales
UNION ALL
SELECT 'sales_dev', COUNT(*), SUM(net_revenue_amount) FROM sales_dev.gold.fact_sales WHERE invoice_date <= '2026-10-29';
```
The last query compares with `sales_dev` only by reading it. The two match when `sales_dev` was loaded up to
29 Oct 2026; if it was loaded further, compare up to the same date.

## If it fails

| Message | Fix |
|---|---|
| `N source file(s) not found in /Volumes/s3demo_dev/raw/source` | The volume points at the wrong folder, or files are missing in S3: `LIST` the volume and compare with the upload |
| `PERMISSION_DENIED` / `Access Denied` reading the volume | Catalog > External Data > the location > Test connection; ask the AWS team to check the role and bucket policy |
| `source file(s) not found` although `LIST '/Volumes/s3demo_dev/raw/source/'` shows the files in SQL | The job's Python cannot read the S3-backed volume as a folder. Fallback without a code change: `DROP VOLUME s3demo_dev.raw.source; CREATE VOLUME s3demo_dev.raw.source;` (a normal managed volume, same path), then copy the files in once from a notebook: `dbutils.fs.cp("<the S3 sales folder URL>", "/Volumes/s3demo_dev/raw/source/", recurse=True)` and run the job again |
| A bronze file check fails (row count or checksum) | A file changed in the upload (opened and re-saved?). Upload it again from the original zip, then follow "Reload one file" in `run_the_client_area.md` with `s3demo_dev` |

## Start again from nothing

```sql
DROP SCHEMA IF EXISTS s3demo_dev.bronze CASCADE;
DROP SCHEMA IF EXISTS s3demo_dev.silver CASCADE;
DROP SCHEMA IF EXISTS s3demo_dev.gold CASCADE;
DROP SCHEMA IF EXISTS s3demo_dev.ops CASCADE;
```
Then empty the `raw.landing` volume (Catalog > s3demo_dev > raw > landing > select all > Delete), run
`sql/setup/s3demo_dev.sql` again and run the job. The S3 files are never touched.
