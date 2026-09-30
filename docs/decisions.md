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
| 1 Oct 2026 | Generator uses fixed name lists, not Faker | Same seed must give byte-identical files; Faker output can change between versions | Navtej |
| 1 Oct 2026 | Model the dummy data on the reference SAP O2C extract: Sri Lanka, LKR, SAP column names and codes (replaces the fictional India electronics setting) | Demo looks like the company's own data; real data can replace it with little mapping | Navtej |
| 1 Oct 2026 | Stories v2: Southern weak, Weedicide +8% from 1 Apr 2026, new HEX1LTR (15 Jun 2026), HGL400ML returns spike (1 Jul 2026), MC6001LT stockout in Western from 23 Oct | Known answers for Genie tests; kept in `generator/stories.py` | Navtej |
| 1 Oct 2026 | Reference extract kept in git-ignored `reference/`; never committed | Repo is public | Navtej |
| 1 Oct 2026 | Invoice history uses the reference's 74 columns plus one added column `reference_invoice_number` | Returns and cancellations must point at the invoice they reverse | Navtej |
| 1 Oct 2026 | Shared engine with per-day and per-invoice random seeds; document numbers derived from date and invoice | Any day rebuilds alone and identically; history and daily files never disagree | Navtej |
| 1 Oct 2026 | Daily manifest control totals exclude injected duplicates and invalid rows | Reconciliation can demand an exact match; dirt is listed row by row | Navtej |
| 1 Oct 2026 | Added price-correction credit memos (ZACR, as in the reference) to history and daily changes | Plan requires price corrections to older orders | Navtej |
