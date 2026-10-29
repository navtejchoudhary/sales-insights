# Playbook: copy the client's SAP data once (and load it)

The client's data is copied ONCE from their warehouse (Redshift Serverless) into the client area, then the same
pipeline that was proven on SAP-shaped dummy data loads it (`docs/playbooks/run_the_client_area.md`).
Contract (tables, columns, rules): `docs/sap_input_contract.md`. Safety rules: never `SELECT *`, read only,
company and window filters at the source, no personal columns, nothing real in git.

## What it costs on the source

The source bills per second of query time (60-second minimum per query, nothing while idle), about
**$0.375 per RPU-hour** in US East. The copy is about 10 minutes of querying, run once:

| Base capacity of their workgroup | Expected (12 min incl. checks) | With buffer (15 min) |
|---|---|---|
| 128 RPU | about $9.60 | $12 |
| 32 RPU | about $2.40 | $3 |
| 8 RPU | about $0.60 | $0.75 |

Nothing is sent to the source before the approved estimate. Browsing the foreign catalog in Catalog Explorer
also sends queries: don't.

## 1. Connection (once, in the browser)

Catalog > External data > Connections > Create connection, type Redshift: the host, port and the READ-ONLY
user from the client. Type the password only into this form (never in chat, a file or the SQL editor). Then
create the foreign catalog from it, and remove the default access the wizard adds:
```sql
REVOKE BROWSE ON CATALOG <foreign_catalog> FROM `account users`;
SHOW GRANTS ON CATALOG <foreign_catalog>;          -- expect: no rows
SHOW GRANTS ON CONNECTION <connection>;            -- expect: no rows
```

## 2. Clear the dummy data (Databricks only, $0)

Real and dummy rows never share a table. SQL editor, one statement at a time:
```sql
DROP SCHEMA IF EXISTS client_dev.bronze CASCADE;
DROP SCHEMA IF EXISTS client_dev.silver CASCADE;
DROP SCHEMA IF EXISTS client_dev.gold CASCADE;
DROP SCHEMA IF EXISTS client_dev.ops CASCADE;
```
Then run `sql/setup/client_dev.sql` again (it recreates them; `IF NOT EXISTS` leaves the rest). Terminal:
```bash
databricks fs rm -r dbfs:/Volumes/client_dev/raw/staging/masters --profile sales-dev
databricks fs rm -r dbfs:/Volumes/client_dev/raw/staging/history --profile sales-dev
databricks fs rm -r dbfs:/Volumes/client_dev/raw/delivered/masters --profile sales-dev
databricks fs rm -r dbfs:/Volumes/client_dev/raw/delivered/history --profile sales-dev
databricks fs rm dbfs:/Volumes/client_dev/raw/delivered/_delivery_log.csv --profile sales-dev
```
Never touch `client_dev.raw.landing` (the external inbox).

## 3. Switch the config to the real data (a small PR)

In `config/config.yaml`, `areas.client`: `source: real`, `scope.date_from: "2024-01-01"`,
`scope.date_to: "2026-01-31"`, and `scope.excluded_types` with the type the business does not count as sales.
Merge, then `databricks bundle deploy -t dev --profile sales-dev --var schedule_pause_status=PAUSED`.
With `source: real` the pipeline never generates dummy data, and the copy refuses to run without it.

## 4. The source map (real names, never in git)

Copy `config/source_map.example.json` to `source_map.json` (git-ignored), fill in the foreign catalog, the
schema.table of each logical table, and the column names that differ from the contract (SAP standard names
need no entry; `null` = not in the source). Upload it:
```bash
databricks fs cp source_map.json dbfs:/Volumes/client_dev/raw/output/source_map.json --profile sales-dev --overwrite
```
The copy refuses a map that would read a personal column (names, street, phone, e-mail, tax or VAT number,
free text).

## 5. Dry run ($0), pilot (1 query), copy (once)

```bash
databricks bundle run -t dev client_copy --profile sales-dev
```
Prints every query; nothing is sent to the source. Check: named columns only, company and date filter on every
billing query, the material filter, no personal columns.
```bash
databricks bundle run -t dev client_copy --profile sales-dev --params mode=pilot
```
Copies nothing, times ONE small lookup table. If it took more than about 30 seconds, stop and re-plan.
```bash
databricks bundle run -t dev client_copy --profile sales-dev --params mode=copy
```
Small tables first, billing items last, then the source's own totals. It stops before the next query when one
took longer than 2 minutes, and stops without writing anything when the copied rows differ from the source's
totals. It refuses to run twice.

## 6. Load it

```bash
databricks bundle run -t dev client_pipeline --profile sales-dev
```
Bronze, silver, gold, reconciliation, KPI check, publish: the dashboard Sales (SAP data) now shows the real data.
Review `client_dev.silver.quarantine` and `client_dev.ops.sap_row_fates` before showing anyone.
