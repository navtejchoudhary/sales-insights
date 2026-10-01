# Sales Insights MVP

A QuickSight-style insights MVP on Azure Databricks: plain-English questions (Genie), a governed KPI layer (metric views), an AI/BI dashboard, automatic weekly insights and a generated PowerPoint deck.

Plan: *Databricks Insights MVP — Execution Plan* (30 Sep 2026). Demo: 29–30 Oct 2026.

## How the data flows

```
generator -> staging/ -> drip -> landing/ -> bronze -> silver -> gold -> reconciliation
                                                                  -> metric views -> dashboard / Genie
                                                                  -> insights job -> deck job
```

- **Stage A (1–14 Oct):** everything runs on a Mac with PySpark + Delta Lake.
- **Stage B (15–28 Oct):** the same code runs on the Azure Databricks trial workspace.

## Quick start (Mac)

New to the project? Follow `docs/setup_mac.md` first. Then:

```bash
uv sync                 # install the exact package versions from uv.lock
uv run pytest           # all tests should pass
```

## Generate data

```bash
uv run python -m sales_insights.generator.masters    # master data -> staging/masters/ (see docs/master_data.md)
uv run python -m sales_insights.generator.history    # 18-month invoices -> staging/history/ (see docs/invoice_data.md)
uv run python -m sales_insights.generator.simulate --from 2026-10-01 --to 2026-10-29   # daily drops -> staging/business_date=.../
uv run python -m sales_insights.drip.drip --initial                                  # deliver masters + history once -> landing/
uv run python -m sales_insights.drip.drip --date 2026-10-01                           # deliver one day -> landing/
uv run python -m sales_insights.pipeline.bronze                                        # load new landing files -> bronze (see docs/pipeline.md)
uv run python -m sales_insights.pipeline.silver                                        # rebuild clean silver tables from bronze
```

## Folder map

| Folder | What lives there |
|---|---|
| `config/` | `config.yaml` with `local` and `dev` profiles (paths, catalog, schema names) |
| `docs/` | Story sheet, KPI definitions, decisions log, Day 15 checklist, setup guide |
| `src/sales_insights/common/` | Config loader and the one `get_spark()` everyone uses |
| `src/sales_insights/generator/` | Dummy data: masters, 18-month history, daily files, dirt, manifest |
| `src/sales_insights/drip/` | Copies one day from `staging/` to `landing/`, manifest last |
| `src/sales_insights/pipeline/` | Bronze, silver, gold, reconciliation |
| `src/sales_insights/insights/` | Period variance, top movers (Phase 2) |
| `src/sales_insights/deck/` | python-pptx deck generator (Phase 3) |
| `sql/` | KPI views and metric view YAML drafts |
| `genie/` | Genie instructions, column descriptions, answer key |
| `bundle/` | Databricks Asset Bundle (from 15 Oct) |
| `tests/` | Unit tests (`uv run pytest`) |

`staging/`, `landing/` and `lake/` are created at runtime and never committed.

## Working rules

- Feature branch per task; every merge reviewed by a senior developer. Never commit to `main` directly.
- Pipeline code: PySpark + Delta only. Generator: plain Python. See `CLAUDE.md`.
- Paths and table names come only from `config/config.yaml`.
- Every dirt type the generator injects has a unit test.
