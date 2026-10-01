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
| Possible stock-out | A product that sold on 80%+ of the previous 28 days in a province, then nothing for 3 days (chance of that by luck: well under 1%) | 1 |
| Unusual day | Daily net revenue 3+ standard deviations from the same weekday over the previous 8 weeks | 2 |

Findings are ranked (alerts first) and appended to `gold.insights` with a `run_id`, so the Supervisor
agent (Phase 3) and Genie can answer "what changed this week?" from a table.

## 3. Narrative (`insights/narrative.py`)

The 6 most important findings become the summary. Locally these are the template sentences. On Databricks,
`ai_query` with `business.narrative_model` rewrites them for management, then the job checks that every
number in the AI's text appears in our facts; otherwise the template sentences are kept.

## 4. Weekly deck (`deck/deck.py`, python-pptx)

7 slides: title, at a glance (4 KPI cards, month to date vs last year, plus the 3 key points), revenue
trend (13 complete months), what moved revenue (change by province and product group), alerts, summary,
about these numbers (definitions, late-arrival note, reconciliation result). Charts are native PowerPoint
charts, so values can be clicked and restyled. On Databricks the job writes the deck to a volume.

## Not built locally (tried on Day 15)

`ai_forecast` and `ai_detect_anomalies` are Databricks-only functions. The local rules above cover the
demo stories; on Day 15 we test whether the AI functions are available on the trial and add them as
extra findings if they are (see `docs/day15_checklist.md`).
