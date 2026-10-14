"""Genie answer key: run every benchmark question's ground-truth SQL on gold and record the answers.

    uv run python -m sales_insights.semantic.answer_key                          # genie/answer_key.csv
    uv run python -m sales_insights.semantic.answer_key --genie inventory        # genie/inventory/answer_key.csv
    uv run python -m sales_insights.semantic.answer_key --genie sales_and_stock  # genie/sales_and_stock/...

One answer key per Genie agent (SPACES): Sales (genie/), Inventory (genie/inventory/) and the combined
Sales & Stock agent (genie/sales_and_stock/). For each question in the agent's benchmarks.yaml:
  - run its SQL on the current gold tables (local Delta or Unity Catalog)
  - keep the answer (up to 10 rows, as readable text) for scoring Genie by hand or by benchmark
  - evaluate its `check`: PASS means the planted story is visible in the data, FAIL means it is not
    (a generator or pipeline problem to fix BEFORE the demo), PENDING means the data has not reached
    the story's date yet (demo-week stories)
The command exits with code 1 if any check FAILS.
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from string import Template

import yaml

from sales_insights.common.config import REPO_ROOT

GENIE = REPO_ROOT / "genie"
CATEGORIES = {"basic", "trend", "story", "tricky"}
CHECK_NAMES = {"row": None, "rows": None, "date": date, "len": len, "set": set, "all": all, "any": any, "abs": abs}


@dataclass(frozen=True)
class GenieAgent:
    """One Genie agent's files and the area whose gold tables answer its questions."""

    name: str
    folder: Path
    area: str
    data_until_sql: str  # the latest date the agent's data covers (decides PENDING)

    @property
    def benchmarks(self) -> Path:
        return self.folder / "benchmarks.yaml"

    @property
    def answer_key(self) -> Path:
        return self.folder / "answer_key.csv"


SPACES = {
    "sales": GenieAgent("sales", GENIE, "sales", "SELECT MAX(invoice_date) AS d FROM ${fact_sales}"),
    "inventory": GenieAgent(
        "inventory", GENIE / "inventory", "inventory", "SELECT MAX(snapshot_date) AS d FROM ${fact_stock_daily}"
    ),
    "sales_and_stock": GenieAgent(
        "sales_and_stock",
        GENIE / "sales_and_stock",
        "inventory",
        "SELECT MAX(activity_date) AS d FROM ${fact_sales_stock_daily}",
    ),
}


@dataclass(frozen=True)
class Question:
    id: str
    category: str
    question: str
    sql: str
    check: str | None = None
    story: str | None = None
    notes: str | None = None
    needs_data_until: date | None = None
    paraphrases: tuple[str, ...] = ()  # the same question worded the way a manager might ask it


def scored(questions: list[Question], taught: set[str]) -> list[tuple[str, str, str]]:
    """Genie's benchmark set: (benchmark id, wording, question id) for every wording of every question that
    was NOT taught as an example query. Taught questions and their rewordings are left out entirely:
    scoring Genie on what it was shown would overstate its accuracy."""
    out = []
    for q in questions:
        if q.id in taught:
            continue
        wordings = [q.question, *q.paraphrases]
        out += [(q.id if i == 0 else f"{q.id}-{chr(96 + i)}", w, q.id) for i, w in enumerate(wordings)]
    return out


def load(path: Path = GENIE / "benchmarks.yaml") -> list[Question]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))["questions"]
    return [Question(**{**q, "paraphrases": tuple(q.get("paraphrases", ()))}) for q in raw]


def render(q: Question, tables: dict[str, str]) -> str:
    return Template(q.sql).substitute(tables)


def evaluate(q: Question, rows: list[dict], data_until: date | None) -> str:
    """PASS / FAIL / PENDING / '' (no check). A check that raises counts as FAIL."""
    if not q.check:
        return ""
    if q.needs_data_until and (data_until is None or data_until < q.needs_data_until):
        return "PENDING"
    try:
        # names go in the globals so that generator expressions (`all(... for r in rows)`) can see them too
        names = {"__builtins__": {}, **CHECK_NAMES, "row": rows[0] if rows else {}, "rows": rows}
        ok = eval(q.check, names)
    except Exception:  # noqa: BLE001 - a check that cannot even be evaluated (no rows, missing column) fails
        return "FAIL"
    return "PASS" if ok else "FAIL"


def _fmt(v) -> str:
    if isinstance(v, Decimal | float):
        return f"{v:,.2f}"
    return str(v)


def as_text(rows: list[dict], limit: int = 10) -> str:
    """Readable answer: 'province=Southern, revenue_per_active_customer=1,794,371.00 | ...'"""
    text = " | ".join(", ".join(f"{k}={_fmt(v)}" for k, v in r.items()) for r in rows[:limit])
    return text + (f" | ... ({len(rows)} rows)" if len(rows) > limit else "")


def build(
    spark, lake, questions: list[Question] | None = None, agent: GenieAgent = SPACES["sales"], sales_lake=None
) -> list[dict]:
    """Run every question of `agent` on gold. `lake` is the agent's area; other areas also need `sales_lake`."""
    from sales_insights.semantic import metric_views as mvs

    questions = questions or load(agent.benchmarks)
    tables = mvs.table_names(lake) if sales_lake is None else mvs.table_names(lake, sales_lake)
    data_until = spark.sql(Template(agent.data_until_sql).substitute(tables)).collect()[0]["d"]
    out = []
    for q in questions:
        rows = [r.asDict() for r in spark.sql(render(q, tables)).collect()]
        out.append(
            {
                "id": q.id,
                "category": q.category,
                "question": q.question,
                "expected_answer": as_text(rows),
                "story": q.story or "",
                "story_check": evaluate(q, rows, data_until),
                "data_until": str(data_until),
                "notes": q.notes or "",
                "ground_truth_sql": " ".join(q.sql.split()),
            }
        )
    return out


def write_csv(rows: list[dict], path: Path = GENIE / "answer_key.csv") -> Path:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return path


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.lake import Lake
    from sales_insights.common.spark import get_spark

    parser = argparse.ArgumentParser(description="Compute the Genie answer key from gold.")
    parser.add_argument("--genie", default="sales", choices=sorted(SPACES), help="which Genie agent (default sales)")
    agent = SPACES[parser.parse_args(argv).genie]
    cfg = load_config(area=agent.area)
    spark = get_spark(cfg, app_name="answer-key")
    sales_lake = None if cfg.area == "sales" else Lake(spark, cfg.for_area("sales"))
    rows = build(spark, Lake(spark, cfg), agent=agent, sales_lake=sales_lake)
    path = write_csv(rows, agent.answer_key)
    print(f"  {len(rows)} questions, data until {rows[0]['data_until']} -> {path.relative_to(REPO_ROOT)}")
    for r in rows:
        if r["story_check"]:
            print(f"    {r['story_check']:<7} {r['id']} {r['question']}")
    failed = [r for r in rows if r["story_check"] == "FAIL"]
    if failed:
        print(f"  {len(failed)} STORY CHECK(S) FAILED: the data does not tell the planted story. Fix before the demo.")
        sys.exit(1)
    print("  every planted story is visible in the data (or pending its date).")


if __name__ == "__main__":
    main()
