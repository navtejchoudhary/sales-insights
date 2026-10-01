"""Genie answer key: run every benchmark question's ground-truth SQL on gold and record the answers.

    uv run python -m sales_insights.semantic.answer_key      # writes genie/answer_key.csv

For each question in genie/benchmarks.yaml:
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
class Question:
    id: str
    category: str
    question: str
    sql: str
    check: str | None = None
    story: str | None = None
    notes: str | None = None
    needs_data_until: date | None = None


def load(path: Path = GENIE / "benchmarks.yaml") -> list[Question]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))["questions"]
    return [Question(**q) for q in raw]


def render(q: Question, tables: dict[str, str]) -> str:
    return Template(q.sql).substitute(tables)


def evaluate(q: Question, rows: list[dict], data_until: date | None) -> str:
    """PASS / FAIL / PENDING / '' (no check). A check that raises counts as FAIL."""
    if not q.check:
        return ""
    if q.needs_data_until and (data_until is None or data_until < q.needs_data_until):
        return "PENDING"
    try:
        ok = eval(q.check, {"__builtins__": {}}, {**CHECK_NAMES, "row": rows[0] if rows else {}, "rows": rows})
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


def build(spark, lake, questions: list[Question] | None = None) -> list[dict]:
    from sales_insights.semantic import metric_views as mvs

    questions = questions or load()
    tables = mvs.table_names(lake)
    data_until = spark.sql(f"SELECT MAX(invoice_date) AS d FROM {tables['fact_sales']}").collect()[0]["d"]
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

    argparse.ArgumentParser(description="Compute the Genie answer key from gold.").parse_args(argv)
    cfg = load_config()
    spark = get_spark(cfg, app_name="answer-key")
    rows = build(spark, Lake(spark, cfg))
    path = write_csv(rows)
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
