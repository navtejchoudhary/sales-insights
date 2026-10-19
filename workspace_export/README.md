# Workspace export

What was built in the Databricks workspace itself, saved before the trial workspace is deleted (5 Nov 2026):
the three Genie agents (`genie_<name>.json`: data, instructions, example queries, benchmarks) and the
deployed dashboards (`dashboard_<name>.lvdash.json`). Workspace IDs, URLs and e-mail addresses are removed.

Made with `src/sales_insights/semantic/export_workspace.py` (steps: `docs/playbooks/deploy_and_operate.md`,
"Before the workspace is deleted"). The source of truth stays in the repo (`genie/`, `dashboards/`); these
files show what the workspace actually held, and a dashboard file can be imported into any workspace
(Dashboards > Create > Import).

## Contents (exported 19 Oct 2026)

| File | What | Notes |
|---|---|---|
| `genie_sales.json` | Sales agent "Sri Lankan Crop Protection Sales Analytics" | 72 benchmark questions, instructions, example SQL |
| `genie_inventory.json` | Stock agent "Sri Lankan Crop Protection Stock Analytics" | 60 benchmark questions |
| `genie_sales_and_stock.json` | Sales & Stock agent "Crop Protection Sales and Inventory" | 36 benchmark questions |
| `dashboard_sales.lvdash.json` | Sales dashboard | identical to `dashboards/sales.lvdash.json` |
| `dashboard_stock.lvdash.json` | Stock dashboard | identical to `dashboards/stock.lvdash.json` |
| `dashboard_ceo.lvdash.json` | Company overview | identical to `dashboards/ceo.lvdash.json` |

Checked before committing: no workspace URL or number, e-mail address, user name, or agent, warehouse or
dashboard ID. The remaining 32-character `id` values inside the agents are the agents' own ids for each
question and instruction; they mean nothing outside the agent and give no access.
Agent scores at export: Sales 93-97%, Stock 97% (58/60), Sales & Stock 97-100%.
