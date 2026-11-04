# Playbook: run the s3demo area (the sales demo files, read from AWS S3)

The s3demo area proves that the pipeline can take its input straight from an AWS S3 bucket. The files are the
sales demo files (master data, 18-month history, daily drops 1-29 Oct 2026, with their planted dirt), uploaded
once to S3. Databricks reads them read-only and runs the sales steps on them: drip, bronze, silver, gold and
reconciliation, into its own catalog `s3demo_dev`.

What it never does:
- write to S3 (the credential, the external location and the volume are all read-only);
- generate data (`source: external` in `config/config.yaml`): if a file is missing, the run stops and says which;
- touch `sales_dev`, `inventory_dev`, `client_dev` or Redshift (own catalog, own folders, own job);
- run by itself (no schedule) or retry by itself (`max_retries: 0`).

Not included: KPI views, metric view, dashboard, Genie, insights. Those read or publish the sales area's own
tables; the gold tables are there to query directly.

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
