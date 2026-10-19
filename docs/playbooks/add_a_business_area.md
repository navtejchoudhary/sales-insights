# Playbook: add a business area (finance, procurement, ...)

The stock area (`inventory`) is the worked example: it was added beside sales in four days without
changing a sales number. Design and reasoning: `docs/multiple_gold_layers.md` (section 5 sketches finance /
receivables). One area = one catalog, one pipeline, one metric view, one Genie agent, one dashboard, one group.

## Steps (each a separate PR, tests green at every step)

1. **Decide the grain and the stories.** Which fact table type (transaction, periodic snapshot,
   accumulating snapshot), which source files, which planted stories and dirt (`docs/story_sheet.md`).
   Get approval, then add a row to `docs/decisions.md`.
2. **Config:** a new entry under `areas:` in `config/config.yaml` (catalog `<area>_dev`, its file types).
   `cfg.for_area("<area>")` gives the area's `Lake`. Sales must stay byte-for-byte unchanged.
3. **Generator:** `generator/<area>.py`, reading the sales output where the area follows sales (stock
   issues follow invoices), never changing it. Settings in `generator/stories.py`. Deterministic seeds.
   Every dirt type logged in the manifest and covered by a unit test.
4. **Pipeline:** bronze is reused as is; `pipeline/<area>_silver.py`, `<area>_gold.py`, `<area>_model.py`
   (every table and column described), `<area>_checks.py` (totals vs source, the area's own invariant,
   a cross-check with sales). `run_pipeline --area <area>` runs it.
5. **Semantic:** `sql/metric_views/<area>_metrics.yaml` + twin SQL; add the gold tables to
   `AREA_GOLD_TABLES` in `semantic/metric_views.py`.
6. **Databricks:** `sql/setup/<area>_dev.sql` (catalog, schemas, volumes); jobs `<area>_backfill`,
   `<area>_daily_pipeline` (after sales), `<area>_publish_semantic` in `bundle/resources/jobs.yml`;
   grants in `sql/setup/permissions.sql` for a new group; runbook section in `docs/day15_runbook.md`.
7. **Genie:** `genie/<area>/` (space.yaml, benchmarks.yaml with story checks), an entry in `SPACES`
   (`semantic/answer_key.py`), then tune it (`tune_a_genie_agent.md`) and add it to the weekly check
   (`sales_genie_health` tasks + a `databricks.yml` variable).
8. **Dashboard:** `dashboards/<area>.yaml`, add to `NAMES`, the bundle and `export_workspace.DASHBOARD_NAMES`.
9. Update the status table in `AGENTS.md`.

## Rules that made the stock area safe

- Shared dimensions (product, customer, date) stay in the sales gold; areas read them, never copy them.
- A combined view of two areas (sales next to stock) is its own gold table summed to the shared grain
  first (Kimball drill-across), never a join of two facts in a metric view.
- Only days both areas cover are kept in a combined table, so a late area never shows as zero.
