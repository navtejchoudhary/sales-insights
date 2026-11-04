-- One-time setup of the s3demo area on Databricks (run in the SQL editor before the first s3demo run).
-- The sales demo files, uploaded to an AWS S3 bucket, read from there (docs/playbooks/run_the_s3demo_area.md).
-- Same layout as inventory_dev; nothing here touches sales_dev, inventory_dev or client_dev.
-- Names must match config/config.yaml (areas: s3demo: dev:).
--
-- BEFORE YOU RUN: the storage credential and the read-only external location on the S3 folder must exist and
-- pass "Test connection" (Catalog > External Data). Then replace the placeholder in the LAST statement with the
-- S3 URL of the uploaded sales folder (ending in /sales/). Do not save the real URL back into git: the repo is
-- public and the bucket name is the client's.
--
-- ACCESS: nobody but the owner (no grants here or in permissions.sql). Check with SHOW GRANTS ON CATALOG s3demo_dev;
CREATE CATALOG IF NOT EXISTS s3demo_dev COMMENT 'S3 test: the sales demo files read from an AWS S3 bucket (read-only)';
CREATE SCHEMA IF NOT EXISTS s3demo_dev.raw    COMMENT 'Volumes: source (external, read-only, on S3), landing (delivered), output';
CREATE SCHEMA IF NOT EXISTS s3demo_dev.bronze COMMENT 'Files loaded as received, every column as text';
CREATE SCHEMA IF NOT EXISTS s3demo_dev.silver COMMENT 'Cleaned, typed, deduplicated; bad rows in silver.quarantine';
CREATE SCHEMA IF NOT EXISTS s3demo_dev.gold   COMMENT 'Sales star schema built from the S3 files';
CREATE SCHEMA IF NOT EXISTS s3demo_dev.ops    COMMENT 'File log, control totals, reconciliation and data-quality results';
-- The pipeline's own volumes. It reads raw.source and never writes there (the S3 credential is read-only anyway).
CREATE VOLUME IF NOT EXISTS s3demo_dev.raw.landing;
CREATE VOLUME IF NOT EXISTS s3demo_dev.raw.output;
REVOKE BROWSE ON CATALOG s3demo_dev FROM `account users`;
-- The staging folder = an EXTERNAL volume on the S3 folder. Replace the placeholder, then run this statement.
CREATE EXTERNAL VOLUME IF NOT EXISTS s3demo_dev.raw.source
  LOCATION '<S3 URL of the uploaded sales folder, e.g. s3://bucket/prefix/sales/>'
  COMMENT 'The sales demo files on S3, read-only';
