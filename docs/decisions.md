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
| 1 Oct 2026 | Local Delta tables are path-based (lake/<schema>/<table>) behind a `Lake` helper; Unity Catalog names on Databricks | A plain local Spark session forgets registered tables; a local Hive metastore locks when tests and runs overlap | Navtej |
| 1 Oct 2026 | Bronze exactly-once via our own file log (path + SHA-256) instead of relying on Auto Loader | Same code and tests on Mac and Databricks volumes; Auto Loader stays optional | Navtej |
| 1 Oct 2026 | Silver is rebuilt in full each run instead of incremental MERGE (as the plan first said) | ~45k lines rebuild in seconds; deterministic, rerun-safe and far simpler to test. Revisit if volume grows 100x | Navtej |
| 1 Oct 2026 | A re-sent file with changed content replaces its earlier load in silver (bronze keeps both) | Source corrections, including removed rows, flow through without manual clean-up | Navtej |
| 1 Oct 2026 | Bad rows are quarantined with reasons, never fixed by guessing; the manifests' dirt list is the test oracle | Quarantine must match the source's own list exactly, so correctness is proven, not assumed | Navtej |
| 1 Oct 2026 | Local Spark driver memory 2 GB | Bronze ran at 95% of the 1 GB default heap | Navtej |
| 1 Oct 2026 | Gold is rebuilt in full each run (not only the dates touched by late or changed rows, as the plan said) | Same reasoning as silver: seconds at this size, always consistent, simpler to test | Navtej |
| 1 Oct 2026 | Gold measure names follow our conventions (`net_revenue_amount`, `district_code`); SAP names live in the column descriptions | Genie and business users understand plain names; SAP users still find their terms | Navtej |
| 1 Oct 2026 | Revenue split into four signed parts (invoiced, returns, cancellations, price corrections) that always sum to net revenue | Common questions become one SUM; impossible to double count | Navtej |
| 1 Oct 2026 | One description file (`gold_model.py`) feeds column comments, the data dictionary and later Genie | Written once, never drifts | Navtej |
| 1 Oct 2026 | Reconciliation per delivery x file x invoice date, exit code 1 on failure | Pinpoints exactly where a mismatch is; a scheduled job fails loudly | Navtej |
| 1 Oct 2026 | Cultivation seasons in config: Maha Sep-Mar, Yala May-Aug, April 'Inter-season' (source: Wikipedia, Agriculture in Sri Lanka) | Seasonality is a key story; Genie can group by season | Navtej |
| 1 Oct 2026 | KPIs defined once in the metric view YAML; KPI SQL kept as the readable twin; a check proves both give identical numbers | Plan rule: a KPI is done only when SQL and metric view agree. Also a fallback if metric views misbehave on the trial | Navtej |
| 1 Oct 2026 | Metric view tested locally by compiling it to plain Spark SQL (fact LEFT JOIN dims, MEASURE() expanded) | Real metric views exist only on Databricks; the local twin catches formula mistakes two weeks earlier | Navtej |
| 1 Oct 2026 | Invoice count = distinct paid (ZAOR) non-cancelled invoices; free-of-charge and credit notes excluded (draft) | Average invoice value should not be diluted by free goods or credit notes | Navtej |
| 1 Oct 2026 | Growth and attach rate only in KPI SQL for now | Growth needs window measures (newer runtime, test 15 Oct); attach needs invoice-level logic | Navtej |
