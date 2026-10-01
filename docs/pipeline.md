# Pipeline

```
staging/ --drip--> landing/ --bronze--> bronze.* --silver--> silver.* --gold--> gold.* --> reconciliation
```

## Daily run (local)

```bash
uv run python -m sales_insights.drip.drip --initial          # once: master data + history backfill
uv run python -m sales_insights.drip.drip --date 2026-10-01  # each business day
uv run python -m sales_insights.pipeline.bronze              # loads whatever is new in landing/
```

Tables live under `lake/<schema>/<table>` locally (path-based Delta) and as `sales_dev.<schema>.<table>` on
Databricks. Code never uses either directly: it goes through `common/lake.py` (`Lake.read`, `Lake.append`, ...).

## Bronze (`pipeline/bronze.py`, discovery in `pipeline/landing.py`)

Loads files **exactly as received**: every source column stays text, nothing is cleaned.

| Table | Source | Notes |
|---|---|---|
| `bronze.orders` | `landing/business_date=*/orders_*.csv` | `sales_rep_id` appears from 22 Oct |
| `bronze.changes` | `landing/business_date=*/changes_*.csv` | returns, cancellations, price corrections |
| `bronze.invoices_history` | `landing/history/invoices_*.csv` | one-time backfill |
| `bronze.masters_<name>` | `landing/masters/<name>.csv` | 14 master and lookup tables |
| `bronze.manifests` | every manifest | raw JSON in `content` |
| `ops.processed_files` | bronze's own log | one row per loaded file: path, SHA-256, rows, status |
| `ops.dq_results` | checks from every layer | PASS / FAIL / WARN / WAIT |

Metadata on every bronze row: `_source_file` (path in landing), `_business_date` (daily files only),
`_load_ts`, `_run_id`, `_rescued_data` (always empty locally; Auto Loader fills it on Databricks).

Rules:

- **Exactly once**: a file is loaded once per (path, SHA-256). An identical redelivery is skipped.
  If the source changes a file it already sent, bronze loads it again, marks it
  `reloaded_changed_content` and writes a WARN (silver keeps the newest load).
- **Complete folders only**: a folder loads when its manifest and every file it lists exist.
  Otherwise bronze writes a WAIT row and tries again next run.
- **Schema evolution**: new source columns are added to the table (`mergeSchema`), never an error.
- **Manifest checks**: row count and checksum per file into `ops.dq_results`.
