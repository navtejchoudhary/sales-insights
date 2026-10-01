"""Turn ranked insights into a short narrative.

- Everywhere: `summary()` picks the most important insights and returns their template sentences.
  Deterministic, testable, and every number is ours.
- Databricks only: `ai_rewrite()` sends those exact sentences to a Databricks-hosted model through
  ai_query and asks for a short management summary that may not add or change any number. If the call
  fails or AI functions are unavailable on the trial, the template sentences are used unchanged.
"""

from __future__ import annotations

import re

from sales_insights.insights.analysis import Insight, rank

PROMPT = """You write a weekly sales summary for the management of a Sri Lankan crop-protection company.
Rewrite the facts below as at most {n} short bullet points in plain business English.
Rules: use ONLY the facts given; do not add, round differently or invent any number, product, place or cause;
keep every number exactly as written; put alerts first; one sentence per bullet; no greeting or sign-off.

Facts:
{facts}"""


SUMMARY_HEADLINES = ["mtd_vs_last_year", "month_vs_last_year"]  # always lead with the latest period
SUMMARY_COMPARISON = "month_vs_last_year"  # movers are explained on the last complete month
SUMMARY_DIMENSIONS = ["province", "product_group", "product"]  # channel / customer group: too few values to tell much


def summary(insights: list[Insight], n: int = 6) -> list[str]:
    """Alerts first, then net revenue for the latest periods, then the biggest mover per dimension, then the biggest fall."""
    picked = [i for i in rank(insights) if i.kind in ("stock_out", "anomaly")]
    revenue = [i for i in insights if i.kind == "headline" and i.metric == "net_revenue"]
    for key in SUMMARY_HEADLINES:
        picked += [i for i in revenue if i.comparison == key][:1]
    moves = sorted(
        (
            i
            for i in insights
            if i.kind == "mover" and i.comparison == SUMMARY_COMPARISON and i.dimension in SUMMARY_DIMENSIONS
        ),
        key=lambda i: -abs(i.change or 0),
    )
    best = [
        next(i for i in moves if i.dimension == d) for d in SUMMARY_DIMENSIONS if any(i.dimension == d for i in moves)
    ]
    picked += sorted(best, key=lambda i: -abs(i.change or 0))
    fallers = [i for i in moves if (i.change or 0) < 0]
    if fallers and fallers[0] not in picked:
        picked.append(fallers[0])
    return [i.text for i in picked[:n]]


def numbers_in(text: str) -> set[str]:
    return set(re.findall(r"\d[\d,]*\.?\d*", text))


def ai_rewrite(spark, facts: list[str], model: str | None) -> list[str]:
    """Databricks: ai_query rewrite. Falls back to the facts if anything goes wrong or a number changes."""
    if not model or not facts:
        return facts
    prompt = PROMPT.format(n=len(facts), facts="\n".join(f"- {f}" for f in facts))
    try:
        text = spark.sql("SELECT ai_query(:model, :prompt) AS t", args={"model": model, "prompt": prompt}).first()["t"]
    except Exception:  # noqa: BLE001 - any failure (no AI functions, endpoint missing) means: keep our sentences
        return facts
    bullets = [b.strip(" -*•\t") for b in (text or "").splitlines() if b.strip(" -*•\t")]
    # Guard: the AI may only use numbers that appear in our facts.
    allowed = set().union(*(numbers_in(f) for f in facts))
    if not bullets or any(not numbers_in(b) <= allowed for b in bullets):
        return facts
    return bullets
