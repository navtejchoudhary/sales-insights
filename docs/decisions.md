# Decisions log

One line per decision: date, decision, why, who. Newest at the bottom.

| Date | Decision | Why | Who |
|---|---|---|---|
| 30 Sep 2026 | Dummy, story-driven, dirty data only for the MVP | No dependency on real data access; stories give known answers for testing Genie | Navtej |
| 30 Sep 2026 | Azure pay-as-you-go + Databricks Trial (Premium 14-day) tier, workspace created 15 Oct | Free Trial subscription can't run Azure Databricks; keeps the 14 free days for Databricks-only work | Navtej |
| 30 Sep 2026 | PySpark + Delta locally (not pandas/DuckDB) for pipeline code | Same code runs on Databricks unchanged | Navtej |
| 30 Sep 2026 | Python 3.12, PySpark 4.0.x, delta-spark 4.0.x, Java 17 | Matching pair; compare with serverless versions on 15 Oct | Navtej |
| 30 Sep 2026 | Databricks Connect in a separate environment (15 Oct) | It conflicts with PySpark in the same environment | Navtej |
| 30 Sep 2026 | Git: `main` + feature branches, senior review on every merge | Quality and traceability | Navtej |
