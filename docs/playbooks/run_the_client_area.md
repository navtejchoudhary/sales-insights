# Playbook: run the client area (SAP billing data) on Databricks

The client area loads SAP billing data into its own catalog, `client_dev`, and shows it on the dashboard
**Sales (SAP data)**. Today it runs on SAP-shaped dummy data (`source: dummy` in `config/config.yaml`); after
the one-time copy of the client's data the same steps load the real files (`source: real`).
Rules and tables: `docs/sap_input_contract.md`.

Cost: nothing runs on its own. The job has no schedule; it runs only when you start it (about 10 minutes of
serverless compute per run). The dashboard and the SQL editor use the SQL warehouse, which stops by itself
when idle.

## Once: create the catalog

1. Log in (the token expires):
   ```bash
   databricks auth login --profile sales-dev
   ```
2. Databricks > SQL Editor: paste `sql/setup/client_dev.sql`, then **Run all**. It creates the catalog, its
   schemas and volumes, and grants nothing: only you can see it. If the last line (REVOKE BROWSE) says there
   was nothing to revoke, that is fine.
3. Check: `SHOW GRANTS ON CATALOG client_dev;` lists only you (the owner).

## Deploy and run

```bash
git checkout main && git pull
databricks bundle deploy -t dev --profile sales-dev --var schedule_pause_status=PAUSED
databricks bundle run -t dev client_pipeline --profile sales-dev
```
- `--var schedule_pause_status=PAUSED` keeps the demo's daily jobs, weekly check and alerts paused (cost).
  Leave it out only when the demo should run every day again.
- The run has two tasks. **pipeline** prints one line per step: bronze, silver, gold, reconcile, kpis, each
  `ok`. **publish** creates `client_dev.gold.sales_metrics` (the metric view) and the `kpi_*` views. It
  starts only when every check of the pipeline passed.
- On the dummy data the numbers are the same as on a Mac: 45,282 source rows, 42,604 lines in gold,
  14 quarantined, every reconciliation group matching.

## Look at it

Databricks > Dashboards > **[dev] Sales (SAP data)**: page 1 KPIs and revenue by SAP document type, page 2
products and customers, page 3 "Can we trust the numbers?" (what happened to every source row, the
reconciliation, the data checks). Only you can open it: the bundle gives it no group permissions.

## If something fails

| Message | Meaning | Do |
|---|---|---|
| `CATALOG_NOT_FOUND client_dev` | setup not run | step "Once" above |
| `the copied SAP files have not landed yet` | `source: real` and no files in the volume | copy the files first (R phase); never switch back to dummy |
| `FAILED: ... see ops.dq_results` (silver) | a data check failed | `SELECT * FROM client_dev.ops.dq_results WHERE status = 'FAIL'` |
| `FAILED: ... see ops.sap_reconciliation` | a row went missing between source and gold | `SELECT * FROM client_dev.ops.sap_reconciliation WHERE status = 'FAIL'` |
| `KPI SQL and metric view differ` | a KPI formula changed in one place only | `docs/playbooks/add_or_change_a_kpi.md` |

Run it again after a fix: bronze loads each file only once, and silver, gold and the checks rebuild in full.
