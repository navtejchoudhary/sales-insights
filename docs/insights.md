# Insights job and weekly deck

```bash
uv run python -m sales_insights.insights.job            # findings -> gold.insights, deck -> output/weekly_deck_<date>.pptx
uv run python -m sales_insights.insights.job --no-deck  # findings only
```

Runs after gold. Every number comes from our code over `gold.fact_sales`, with the same formulas as the
metric view. On Databricks an AI model may reword the summary, but it may not add or change a number:
if any number in its text is not in our facts, the job silently keeps the original sentences.

## 1. Like-for-like periods (`insights/periods.py`)

| Comparison | Example on 5 Oct 2026 |
|---|---|
| Month to date vs same days last month | 1-5 Oct 2026 vs 1-5 Sep 2026 |
| Month to date vs same days last year | 1-5 Oct 2026 vs 1-5 Oct 2025 |
| Last 7 days vs the 7 days before | 29 Sep-5 Oct vs 22-28 Sep |
| Last complete month vs same month last year | Sep 2026 vs Sep 2025 |
| Last complete month vs the month before | Sep 2026 vs Aug 2026 |

Never a part month against a whole month: that is what produced the misleading -86% in the KPI view.

## 2. Findings (`insights/analysis.py`)

| Kind | Rule | Severity |
|---|---|---|
| Headline | 6 measures (net revenue, units, invoices, active customers, margin %, returns %) for each comparison; percentages change in points | 2 if revenue moved 10%+, else 3 |
| Mover | Top 3 risers and fallers by change in net revenue, per province, product group, product, customer group, channel; changes under 1% of total revenue are ignored as noise; says what share of the total movement each explains | 2 if 30%+ of the movement or 25%+ change |
| Possible stock-out | A product that sold on 80%+ of the 28 days up to its last sale in a province, then nothing for 3+ days (chance of that by luck: well under 1%). Measured from the last sale, so a long stock-out stays reported and the message says when the last sale was | 1 |
| Unusual day | Daily net revenue 3+ standard deviations from the same weekday over the previous 8 weeks | 2 |
| Revenue drop | Net revenue of the last 7 days 10%+ below the same 7 days 52 weeks earlier (same weekdays, same season). Compared with last year, not last week: a fall from a pre-season peak week is normal | 1 |
| Forecast | This month and the total to the end of the fiscal year (see section 3) | 3 |

Findings are ranked (alerts first) and appended to `gold.insights` with a `run_id`, so the Supervisor
agent (Phase 3) and Genie can answer "what changed this week?" from a table.

## 3. Forecast (`insights/forecast.py`)

| Step | Rule |
|---|---|
| Growth | Net revenue of the fiscal year so far (complete months) / the same months last year. Fewer than 3 complete months (April-June): the last 6 complete months instead |
| Forecast | Same month last year x growth, for every month from the current one to March (at least 3 months) |
| Range | Same month last year x (growth -/+ one standard deviation of the month-by-month growth) |

Why not `ai_forecast`: with 18 months of history it cannot learn the Maha/Yala seasons and drew a
near-straight line (tested on Day 15). Starting from last year's real months keeps the peaks and the
December dip. The forecast assumes this year's growth continues; weather, prices or a competitor can
change that, which is why it shows a range.

Output: `gold.forecast` (rebuilt every run, one row per month, every column described for Genie), one
insight sentence for this month and one for the rest of the fiscal year, and the deck's Outlook slide.

## 4. Alerts (Databricks SQL alerts, `bundle/resources/alerts.yml`)

| Alert | Fires when | Checked |
|---|---|---|
| Sales - revenue drop | the latest insights run has a `revenue_drop` finding | 08:00 daily (after the 06:30 job) |
| Sales - possible stock-out | the latest insights run has a `stock_out` finding | 08:00 daily |

The rules are the Python rules above (unit-tested); the alerts only count findings in `gold.insights`, so
an alert, the deck and Genie never disagree. One email when an alert starts, one when it is back to
normal. Story 7 (MC600 out of stock in Western from 23 Oct) should trigger the stock-out alert on about
26 Oct. Both alerts run at the same minute, so the SQL warehouse wakes once a day.

## 5. Narrative (`insights/narrative.py`)

The 6 most important findings become the summary. Locally these are the template sentences. On Databricks,
`ai_query` with `business.narrative_model` rewrites them for management, then the job checks that every
number in the AI's text appears in our facts; otherwise the template sentences are kept.

## 6. Weekly deck (`deck/deck.py`, python-pptx)

8 slides: title, at a glance (4 KPI cards, month to date vs last year, plus the 3 key points), revenue
trend (13 complete months), outlook (last 6 complete months + the forecast to March), what moved revenue (change by province and product group), alerts, summary,
about these numbers (definitions, late-arrival note, reconciliation result). Charts are native PowerPoint
charts, so values can be clicked and restyled. On Databricks the job writes the deck to a volume.

## Not built locally (tried on Day 15)

`ai_forecast` and `ai_detect_anomalies` are Databricks-only functions. `ai_forecast` works on the
workspace but is not used (see section 3). `ai_detect_anomalies` needs the Predictive AI Functions preview,
which is not yet enabled; the unusual-day rule above covers the demo stories.
