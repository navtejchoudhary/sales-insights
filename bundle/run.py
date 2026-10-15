"""Databricks entry point for every job: run one of the project's commands from the bundle's files.

    run.py pipeline --profile dev --date 2026-10-15     ->  sales_insights.pipeline.run_pipeline
    run.py pipeline --profile dev --area inventory --from 2026-10-01 --to today
    run.py kpis --profile dev                           ->  sales_insights.semantic.kpis

The bundle uploads the repository as workspace files; this script puts its src/ folder on the Python
path, so config/, sql/ and genie/ are found exactly as on the Mac. `--profile` selects the config profile.
The word `today` in any argument becomes today's date in Sri Lanka, so a job parameter can default to
"today" and still be overridden with a fixed date (e.g. a backfill that runs the simulated days ahead).
"""

import importlib
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TIMEZONE = "Asia/Colombo"  # same as business.timezone in config/config.yaml and every job schedule

COMMANDS = {
    "pipeline": "sales_insights.pipeline.run_pipeline",
    "kpis": "sales_insights.semantic.kpis",
    "insights": "sales_insights.insights.job",
    "answer_key": "sales_insights.semantic.answer_key",
    "genie_health": "sales_insights.semantic.genie_health",
}


def split_profile(args: list[str]) -> tuple[str | None, list[str]]:
    """Take `--profile X` out of the arguments (only the pipeline command understands it itself)."""
    if "--profile" in args:
        i = args.index("--profile")
        return args[i + 1], args[:i] + args[i + 2 :]
    return None, args


def resolve_today(args: list[str], now: datetime | None = None) -> list[str]:
    """Replace the argument `today` with today's date (YYYY-MM-DD, Sri Lanka time)."""
    today = (now or datetime.now(ZoneInfo(TIMEZONE))).date().isoformat()
    return [today if a == "today" else a for a in args]


def main(argv: list[str]) -> None:
    try:
        here = Path(__file__).resolve()
    except NameError:  # some job runners do not set __file__
        here = Path(sys.argv[0]).resolve()
    sys.path.insert(0, str(here.parents[1] / "src"))
    if not argv or argv[0] not in COMMANDS:
        raise SystemExit(f"usage: run.py {{{'|'.join(COMMANDS)}}} [--profile dev] [options]")
    profile, rest = split_profile(resolve_today(argv[1:]))
    if profile:
        os.environ["SALES_PROFILE"] = profile
    importlib.import_module(COMMANDS[argv[0]]).main(rest)


if __name__ == "__main__":
    main(sys.argv[1:])
