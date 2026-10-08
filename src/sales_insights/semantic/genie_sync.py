"""Replace the benchmark questions of the Genie agent with output/genie_benchmarks.csv, in one go.

    uv run python -m sales_insights.semantic.genie_setup --profile dev              # writes the CSV
    uv run python -m sales_insights.semantic.genie_sync --space-id <id>             # backup + replace + upload

Why: the Genie UI adds benchmarks one at a time (75 questions = hours of copy-paste) and has no CSV import.
The Genie API exports the whole agent as one JSON ("serialized space") and accepts it back as a full
replacement, so this tool:
  1. exports the agent and saves a backup (output/genie_space_backup_<time>.json), to restore if needed;
  2. swaps ONLY the benchmark questions for the CSV rows (instructions, example queries and data untouched);
  3. uploads it and checks the count.
It calls the Databricks CLI (already logged in with `databricks auth login`), so no token is handled here.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from sales_insights.common.config import REPO_ROOT


def benchmark_questions(rows: list[dict]) -> list[dict]:
    """Benchmark entries in the serialized-space format (question + SQL answer), sorted by id."""
    questions = [
        {
            "id": uuid.uuid4().hex,
            "question": [r["question"]],
            "answer": [{"format": "SQL", "content": [r["ground_truth_sql"]]}],
        }
        for r in rows
    ]
    return sorted(questions, key=lambda q: q["id"])


def replace_benchmarks(space: dict, rows: list[dict]) -> dict:
    """A copy of the serialized space with its benchmark questions replaced by `rows`; nothing else changes."""
    new = json.loads(json.dumps(space))
    new.setdefault("benchmarks", {})["questions"] = benchmark_questions(rows)
    return new


def read_rows(csv_path: Path) -> list[dict]:
    with csv_path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _cli(*args: str, profile: str) -> dict:
    cmd = ["databricks", "genie", *args, "--profile", profile, "-o", "json"]
    out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    return json.loads(out or "{}")


def export_space(space_id: str, profile: str) -> dict:
    raw = _cli("get-space", space_id, "--include-serialized-space", profile=profile)
    return json.loads(raw["serialized_space"])


def upload_space(space_id: str, space: dict, profile: str, payload_path: Path) -> None:
    payload_path.write_text(json.dumps({"serialized_space": json.dumps(space)}), encoding="utf-8")
    _cli("update-space", space_id, "--json", f"@{payload_path}", profile=profile)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Replace the Genie agent's benchmarks with the generated CSV.")
    parser.add_argument("--space-id", required=True, help="Genie agent ID (About tab > Agent ID)")
    parser.add_argument("--profile", default="sales-dev", help="Databricks CLI profile (default: sales-dev)")
    parser.add_argument("--csv", default=str(REPO_ROOT / "output" / "genie_benchmarks.csv"))
    args = parser.parse_args(argv)

    out = REPO_ROOT / "output"
    out.mkdir(exist_ok=True)
    rows = read_rows(Path(args.csv))
    space = export_space(args.space_id, args.profile)
    backup = out / f"genie_space_backup_{datetime.now(ZoneInfo('Asia/Colombo')):%Y%m%d_%H%M%S}.json"
    backup.write_text(json.dumps(space, indent=1), encoding="utf-8")
    before = len(space.get("benchmarks", {}).get("questions", []))

    upload_space(args.space_id, replace_benchmarks(space, rows), args.profile, out / "genie_update.json")
    after = len(export_space(args.space_id, args.profile).get("benchmarks", {}).get("questions", []))
    print(f"  backup:     {backup}")
    print(f"  benchmarks: {before} -> {after} (CSV has {len(rows)})")
    if after != len(rows):
        raise SystemExit("benchmark count does not match the CSV: check the agent in the browser")


if __name__ == "__main__":
    main()
