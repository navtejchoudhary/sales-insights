# Workspace export

What was built in the Databricks workspace itself, saved before the trial workspace is deleted (5 Nov 2026):
the three Genie agents (`genie_<name>.json`: data, instructions, example queries, benchmarks) and the
deployed dashboards (`dashboard_<name>.lvdash.json`). Workspace IDs, URLs and e-mail addresses are removed.

Made with `src/sales_insights/semantic/export_workspace.py` (steps: `docs/playbooks/deploy_and_operate.md`,
"Before the workspace is deleted"). The source of truth stays in the repo (`genie/`, `dashboards/`); these
files show what the workspace actually held, and a dashboard file can be imported into any workspace
(Dashboards > Create > Import).
