-- One-time setup of the client area on Databricks (run in the SQL editor before the first client run).
-- SAP billing data (docs/sap_input_contract.md): SAP-shaped dummy data first, the client's data after the copy.
-- Same layout as inventory_dev; nothing here touches sales_dev or inventory_dev.
-- Names must match config/config.yaml (areas: client: dev:).
--
-- ACCESS: nobody but the owner. There are NO grants in this file and none in permissions.sql: the client's
-- data is not shared with the demo groups. Check afterwards with: SHOW GRANTS ON CATALOG client_dev;
-- (expected: only the owner). If the catalog was ever created from Catalog Explorer with "All account users"
-- ticked, the REVOKE below removes that default browse access.
CREATE CATALOG IF NOT EXISTS client_dev COMMENT 'Client area: SAP billing data (SAP-shaped dummy data until the one-time copy)';
CREATE SCHEMA IF NOT EXISTS client_dev.raw    COMMENT 'Volumes: landing (external inbox, read-only), staging (copy or generator), delivered, output';
CREATE SCHEMA IF NOT EXISTS client_dev.bronze COMMENT 'SAP tables as received, every column as text';
CREATE SCHEMA IF NOT EXISTS client_dev.silver COMMENT 'Signed, typed, one row per billing line; bad rows in silver.quarantine';
CREATE SCHEMA IF NOT EXISTS client_dev.gold   COMMENT 'Sales star schema, KPI views and the metric view sales_metrics';
CREATE SCHEMA IF NOT EXISTS client_dev.ops    COMMENT 'File log, row fates, control totals, reconciliation, data-quality results';
-- The pipeline's own volumes (config.yaml areas.client.dev). client_dev.raw.landing is NOT one of them: it is
-- the external, read-only inbox for the real upload (created on 26 Oct with its storage credential), and the
-- pipeline never writes to it.
CREATE VOLUME IF NOT EXISTS client_dev.raw.staging;
CREATE VOLUME IF NOT EXISTS client_dev.raw.delivered;
CREATE VOLUME IF NOT EXISTS client_dev.raw.output;
REVOKE BROWSE ON CATALOG client_dev FROM `account users`;
