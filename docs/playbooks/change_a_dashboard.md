# Playbook: change a dashboard (chart, tile, filter)

Dashboards are code. Edit `dashboards/<name>.yaml`; `semantic/dashboards.py` writes `<name>.lvdash.json`;
the Asset Bundle deploys it. A change made in the browser is overwritten by the next deploy.

| File | Dashboard | Audience |
|---|---|---|
| `dashboards/sales.yaml` | Sales (3 pages) | `sales_team`, `management` |
| `dashboards/stock.yaml` | Stock (1 page) | `stock_team`, `management` |
| `dashboards/ceo.yaml` | Company overview (1 page) | `management` |

## The YAML in one minute

- `datasets:` one query each.
  - `metric_view:` + `fields` + `measures` (+ `where`) asks a metric view with `MEASURE()`. Use this for
    every KPI, so the dashboard, Genie and the alerts share one definition. `select` / `group_by` /
    `order_by` / `limit` reshape the answer; `then` wraps it once more (`{previous}`).
  - `sql:` plain SQL, only for what is not a KPI (findings, forecast, data checks). `${fact_sales}` and the
    other placeholders become real table names from `config/config.yaml`.
  - `same_as: <dashboard>.<dataset>` reuses another dashboard's dataset.
- `pages:` > `widgets:` fill a 6-column grid left to right; `size: [width, height]`. Widget kinds: `text`,
  `counter`, `table`, `bar`, `line`, `combo`. A widget shows columns its dataset returns.
- `filters:` global filters (filter icon, top left). Each has `param`, `title`, `values` (a dataset whose
  first row is `All`) and `fields` (which field of which metric view it filters). The generator adds
  `(:param = 'All' OR <field> = :param)` to every metric-view dataset of those metric views, so tiles and
  charts recalculate. `company_wide: true` on a dataset keeps it out (the forecast); say "company total"
  in its title. A `sql` dataset takes part only by writing `:param` itself.

## Colours (one meaning each; `docs/access.md` "Colours")

Green on track, amber watch, red act now, rose serious (stock health), blue an actual figure with no
target, light blue forecast, grey context. Counters: `status: [["<", -5, red], ...]` (first match wins,
operators `<` and `>` only). Bars: `color: {field, colors: {Growing: green, ...}}`. Table text:
`color_by` on a column. Status always has words too (titles, "Act now"), never colour alone.

## Steps

1. Edit the YAML. For a new KPI first add it to the metric view (`add_or_change_a_kpi.md`).
2. `uv run python -m sales_insights.semantic.dashboards` (rewrites all `.lvdash.json`).
3. `uv run pytest -m "not spark"`: files up to date, every widget's column exists, grid has no overlap,
   filters reach the right datasets, no KPI computed in plain SQL. Then the full `uv run pytest`: every
   dataset runs on generated data, with filters at `All` and at one province.
4. Commit the YAML and the JSON together; after the merge `databricks bundle deploy -t dev --profile sales-dev`.
5. Open the dashboard, refresh, try a filter. If a widget says "Invalid widget definition", compare its
   JSON with the same widget type in Databricks' exported dashboards
   (github.com/databricks/bundle-examples, `knowledge_base/dashboard_nyc_taxi`, `contrib/*/dashboards`)
   and copy their shape into `_widget()`; never guess keys.

A deploy that removes a dashboard asks `Would you like to proceed? [y/N]` and lists it; check that only
the dashboards you meant are listed (a recreated dashboard gets a new link).
