-- One-time setup of the inventory area on Databricks (run in the SQL editor before the first deploy).
-- Same layout as sales_dev (docs/day15_runbook.md step 3); nothing here touches sales_dev.
-- Names must match config/config.yaml (areas: inventory: dev:).
CREATE CATALOG IF NOT EXISTS inventory_dev COMMENT 'Inventory area: stock movements and daily stock (dummy SAP MM data)';
CREATE SCHEMA IF NOT EXISTS inventory_dev.raw    COMMENT 'Volumes for the files: staging (generator), landing (delivered), output';
CREATE SCHEMA IF NOT EXISTS inventory_dev.bronze COMMENT 'Files loaded as received, every column as text';
CREATE SCHEMA IF NOT EXISTS inventory_dev.silver COMMENT 'Cleaned, typed, deduplicated; bad rows in silver.quarantine';
CREATE SCHEMA IF NOT EXISTS inventory_dev.gold   COMMENT 'Stock tables, metric views and KPI views for dashboards and Genie';
CREATE SCHEMA IF NOT EXISTS inventory_dev.ops    COMMENT 'File log, checks and data-quality results';
CREATE VOLUME IF NOT EXISTS inventory_dev.raw.staging;
CREATE VOLUME IF NOT EXISTS inventory_dev.raw.landing;
CREATE VOLUME IF NOT EXISTS inventory_dev.raw.output;
