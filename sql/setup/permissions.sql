-- Who may read what: three groups, least privilege, read-only. Run once in the Databricks SQL editor
-- (as the owner of both catalogs) AFTER creating the three groups (docs/access.md, step 1).
--
--   sales_team   the sales gold (tables, metric view, KPI views, insights) + sales data checks
--   stock_team   the stock gold + stock data checks + the two shared master tables it joins to
--                (product and calendar, which live in the sales gold)
--   management   everything both teams see (the CEO dashboard and the Sales & Stock agent read both catalogs)
--
-- Nobody gets bronze or silver: raw and half-cleaned data stay with the pipeline (its owner).
-- Grants on a schema also cover the tables and views created in it later (the daily rebuilds keep them).
-- The groups also need: CAN USE on the SQL warehouse, CAN RUN on their Genie agent, and their dashboard
-- (granted by the Asset Bundle, bundle/resources/dashboards.yml). Steps: docs/access.md.
-- Check with: SHOW GRANTS ON SCHEMA sales_dev.gold;

-- ---------------------------------------------------------------- sales_team
GRANT USE CATALOG ON CATALOG sales_dev TO `sales_team`;
GRANT USE SCHEMA, SELECT ON SCHEMA sales_dev.gold TO `sales_team`;
GRANT USE SCHEMA, SELECT ON SCHEMA sales_dev.ops TO `sales_team`;

-- ---------------------------------------------------------------- stock_team
GRANT USE CATALOG ON CATALOG inventory_dev TO `stock_team`;
GRANT USE SCHEMA, SELECT ON SCHEMA inventory_dev.gold TO `stock_team`;
GRANT USE SCHEMA, SELECT ON SCHEMA inventory_dev.ops TO `stock_team`;
GRANT USE CATALOG ON CATALOG sales_dev TO `stock_team`;
GRANT USE SCHEMA ON SCHEMA sales_dev.gold TO `stock_team`;
GRANT SELECT ON TABLE sales_dev.gold.dim_product TO `stock_team`;
GRANT SELECT ON TABLE sales_dev.gold.dim_date TO `stock_team`;

-- ---------------------------------------------------------------- management
GRANT USE CATALOG ON CATALOG sales_dev TO `management`;
GRANT USE SCHEMA, SELECT ON SCHEMA sales_dev.gold TO `management`;
GRANT USE SCHEMA, SELECT ON SCHEMA sales_dev.ops TO `management`;
GRANT USE CATALOG ON CATALOG inventory_dev TO `management`;
GRANT USE SCHEMA, SELECT ON SCHEMA inventory_dev.gold TO `management`;
GRANT USE SCHEMA, SELECT ON SCHEMA inventory_dev.ops TO `management`;
