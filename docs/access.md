# Dashboards, access and export

Three dashboards, three Genie agents, three groups. Everything a group may see is granted to the group,
never to a person: adding someone to `stock_team` is all it takes to give them the stock dashboard, the
Stock agent and the stock tables.

| Group | Dashboards | Genie agents | Tables (read-only) |
|---|---|---|---|
| `sales_team` | Sales | Sales | `sales_dev.gold`, `sales_dev.ops` |
| `stock_team` | Stock | Stock | `inventory_dev.gold`, `inventory_dev.ops`, `sales_dev.gold.dim_product`, `sales_dev.gold.dim_date` |
| `management` | Sales, Stock, Company overview | Sales, Stock, Sales & Stock | both `gold` and `ops` schemas |

Nobody gets bronze or silver. Dashboards run with each viewer's own rights (`embed_credentials: false`), so a
dashboard or agent can never show more than the viewer's grants allow. Sources: Databricks docs,
[Manage groups](https://learn.microsoft.com/azure/databricks/admin/users-groups/manage-groups) (groups made in
the workspace settings are account groups; workspace-local groups cannot be granted Unity Catalog access) and
[Dashboard datasets](https://learn.microsoft.com/azure/databricks/dashboards/manage/data-modeling/datasets)
(metric views in dashboard SQL are read with `MEASURE()`).

## The dashboards

| Dashboard | Pages | What it answers |
|---|---|---|
| Sales | How are we doing? / Where the money comes from / Customers and data trust | Month to date vs the same days last year, the forecast to March, today's findings; provinces, product groups, top products, channels; top customers, returns and margin by month; the data checks and the dirty data caught |
| Stock | Stock today | Stock value, cover, stock-outs and low cover now; stock-outs per day; value by location; depot cover by product group; stock-out days by depot; today's stock findings |
| Company overview | Company overview | Fiscal year to date revenue and margin, stock today, the most important sales and stock findings, revenue trend and forecast, sales next to depot stock per province |

Every KPI comes from a metric view (`sales_metrics`, `inventory_metrics`, `sales_and_stock`), the same
definitions Genie and the alerts use; plain SQL only reads the insights, the forecast and the data checks.
Each dashboard has a "Data status" line: data date and how many checks against the source passed.

The dashboards are code: `dashboards/<name>.yaml` (datasets + widgets) ->
`uv run python -m sales_insights.semantic.dashboards` -> `dashboards/<name>.lvdash.json` -> deployed by the
Asset Bundle (`bundle/resources/dashboards.yml`). Tests run every dataset on real tables and check that each
widget shows a column its dataset returns. Change a dashboard in the YAML, not in the browser: the next deploy
overwrites browser edits.

## Steps (once)

1. **Create the groups** (as workspace admin): user menu > Settings > Identity and access > Groups > Manage >
   Add group > Add new: `sales_team`, `stock_team`, `management`. Add members: open the group > Members >
   Add members. (Membership can take a few minutes to apply.)
2. **Grant the tables**: SQL editor, paste and run `sql/setup/permissions.sql`. Check:
   `SHOW GRANTS ON SCHEMA sales_dev.gold;`
3. **Warehouse**: SQL Warehouses > `sales-insights-wh` > Permissions > add the three groups with *Can use*.
4. **Dashboards**: `databricks bundle deploy -t dev` creates the three dashboards (Workspace > your bundle
   folder > resources, or Dashboards) and shares them with their groups (*Can run*). Open each one once and
   check every widget shows data.
5. **Genie agents**: open each agent > Share: Sales -> `sales_team`, `management`; Stock -> `stock_team`,
   `management`; Sales & Stock -> `management`; permission *Can run*.

To test as a group member without a second login: Workspace > a dashboard > Share shows who can open it, and
`SHOW GRANTS ON SCHEMA inventory_dev.gold;` shows who can read the tables.

## Export before the workspace is deleted

```
uv run python -m sales_insights.semantic.export_workspace \
    --space sales=<agent id> --space inventory=<agent id> --space sales_and_stock=<agent id>
```
Writes `output/export/genie_<name>.json` (each agent's full definition: data, instructions, examples,
benchmarks) and `output/export/dashboard_<name>.lvdash.json` (importable in any workspace), without IDs,
the workspace URL or e-mail addresses. The agent ID is on the agent's About tab.
