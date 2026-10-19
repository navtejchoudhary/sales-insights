"""Save every result of one Genie benchmark run to a JSON file, to see why questions failed (read-only).

    uv run --with databricks-sdk python -m sales_insights.semantic.genie_failures <agent id> <evaluation id>

<agent id>: the agent's About tab (or databricks.yml variables). <evaluation id>: open the run under
Benchmarks > Evaluations; it is the code after /benchmarks/ in the page address.
Prints one line per question (GOOD / BAD / NEEDS_REVIEW) and writes output/genie_eval_<evaluation id>.json:
for each question the assessment, its reasons, Genie's SQL and the expected SQL. Uses the Databricks CLI
login of profile sales-dev (`databricks auth login`); no token in code. The pipeline does not need
`databricks-sdk`, so it is not a project dependency: `uv run --with` adds it for this one command.
How to use the file: docs/playbooks/tune_a_genie_agent.md.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from sales_insights.common.config import REPO_ROOT


def as_dict(obj):
    return obj.as_dict() if hasattr(obj, "as_dict") else obj


def summary(out: list[dict]) -> tuple[int, int]:
    """(GOOD answers, all answers) of an exported run."""
    return sum(1 for o in out if o["detail"].get("assessment") == "GOOD"), len(out)


def main(argv: list[str] | None = None) -> None:
    from databricks.sdk import WorkspaceClient

    parser = argparse.ArgumentParser(description="Save every result of one Genie benchmark run to JSON.")
    parser.add_argument("space_id", help="Genie agent ID")
    parser.add_argument("run_id", help="evaluation ID: the code after /benchmarks/ in the run's page address")
    parser.add_argument("--profile", default="sales-dev", help="Databricks CLI profile (default: sales-dev)")
    args = parser.parse_args(argv)

    genie = WorkspaceClient(profile=args.profile).genie
    results, token = [], None
    while True:
        page = as_dict(genie.genie_list_eval_results(args.space_id, args.run_id, page_token=token))
        results += page.get("eval_results", [])
        token = page.get("next_page_token")
        if not token:
            break

    out = []
    for r in results:
        detail = as_dict(genie.genie_get_eval_result_details(args.space_id, args.run_id, r["result_id"]))
        out.append({"result": r, "detail": detail})
        print(f"  {detail.get('assessment')!s:<14} {r.get('question', '')[:90]}")

    path = REPO_ROOT / "output" / f"genie_eval_{args.run_id}.json"
    Path(path).parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    good, total = summary(out)
    print(f"\n  {good}/{total} good. Saved: {path}")


if __name__ == "__main__":
    main()
