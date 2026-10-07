"""Drip: deliver files from staging/ to landing/, like a file feed would.

    uv run python -m sales_insights.drip.drip --initial          # once: master data + history backfill
    uv run python -m sales_insights.drip.drip --date 2026-10-05  # every day

Rules (copied files keep their names; the manifest is always copied LAST):

- Normal day: copy staging/business_date=D/ to landing/business_date=D/.
- Missing day (stories.MISSING_DELIVERY_DAY): nothing arrives that day...
- ...and the next day delivers BOTH folders (catch-up).
- Every Monday (stories.REDELIVERY_WEEKDAY) Sunday's orders file is delivered AGAIN, same name,
  overwriting the landed copy. The pipeline must not load it twice (exactly-once ingestion).

Every action is appended to landing/_delivery_log.csv, so you can see what arrived when.
"""

from __future__ import annotations

import argparse
import csv
import io
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from sales_insights.generator import stories
from sales_insights.generator.simulate import folder_name

LOG_NAME = "_delivery_log.csv"
LOG_HEADER = ["delivered_at", "business_date", "action", "file"]


@dataclass(frozen=True)
class Action:
    business_date: date
    kind: str  # deliver | catch_up | redeliver | skip
    files: tuple[str, ...]


def plan(d: date, staging: Path) -> list[Action]:
    """What the feed delivers on day d (pure function: no copying)."""
    if d == stories.MISSING_DELIVERY_DAY:
        return [Action(d, "skip", ())]
    actions = []
    prev = d - timedelta(days=1)
    if prev == stories.MISSING_DELIVERY_DAY:
        actions.append(Action(prev, "catch_up", _day_files(staging, prev)))
    actions.append(Action(d, "deliver", _day_files(staging, d)))
    if d.weekday() == stories.REDELIVERY_WEEKDAY:
        orders = f"orders_{prev:%Y%m%d}.csv"
        if (staging / folder_name(prev) / orders).exists():
            actions.append(Action(prev, "redeliver", (orders,)))
    return actions


def _day_files(staging: Path, d: date) -> tuple[str, ...]:
    folder = staging / folder_name(d)
    if not folder.exists():
        raise FileNotFoundError(f"{folder} not found - run the simulator for {d} first")
    data = sorted(p.name for p in folder.iterdir() if not p.name.startswith("manifest_"))
    manifests = sorted(p.name for p in folder.iterdir() if p.name.startswith("manifest_"))
    if not manifests:
        raise FileNotFoundError(f"{folder} has no manifest - the simulation of {d} is incomplete")
    return (*data, *manifests)  # manifest last


INITIAL_FOLDERS = ("masters", "history")


def add_to_log(log_path: Path, rows: list[list[str]]) -> None:
    """Add rows to the delivery log by rewriting the whole file in one sequential write.

    Never opens the file in append mode: Unity Catalog volumes (/Volumes/...) refuse appends and
    random writes ("OSError: [Errno 29] Illegal seek"). The log is a few hundred lines, so a full
    rewrite costs nothing and works the same on a Mac and on Databricks.
    """
    old = ""
    if log_path.exists():
        with open(log_path, newline="", encoding="utf-8") as fh:  # newline="" keeps the CSV line endings as written
            old = fh.read()
    buf = io.StringIO()
    writer = csv.writer(buf)
    if not old:
        writer.writerow(LOG_HEADER)
    writer.writerows(rows)
    log_path.write_text(old + buf.getvalue(), encoding="utf-8", newline="")


def deliver_initial(staging: Path, landing: Path) -> list[str]:
    """One-time delivery of master data and the history backfill (manifest last in each folder)."""
    delivered, log = [], []
    landing.mkdir(parents=True, exist_ok=True)
    for folder in INITIAL_FOLDERS:
        src = staging / folder
        if not src.is_dir():
            raise FileNotFoundError(f"{src} not found - run the {folder} generator first")
        files = sorted(src.iterdir(), key=lambda p: (p.name.endswith("manifest.json"), p.name))
        if not files or not files[-1].name.endswith("manifest.json"):
            raise FileNotFoundError(f"{src} has no manifest - generation is incomplete")
        (landing / folder).mkdir(parents=True, exist_ok=True)
        for f in files:
            shutil.copyfile(f, landing / folder / f.name)
            log.append([datetime.now(UTC).isoformat(timespec="seconds"), "", "initial", f"{folder}/{f.name}"])
            delivered.append(f"{folder}/{f.name}")
    add_to_log(landing / LOG_NAME, log)
    return delivered


def execute(actions: list[Action], staging: Path, landing: Path) -> None:
    landing.mkdir(parents=True, exist_ok=True)
    log = []
    for a in actions:
        now = datetime.now(UTC).isoformat(timespec="seconds")
        if not a.files:
            log.append([now, a.business_date.isoformat(), a.kind, ""])
            continue
        target = landing / folder_name(a.business_date)
        target.mkdir(parents=True, exist_ok=True)
        for name in a.files:
            shutil.copyfile(staging / folder_name(a.business_date) / name, target / name)
            log.append([now, a.business_date.isoformat(), a.kind, name])
    add_to_log(landing / LOG_NAME, log)


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config

    cfg = load_config()
    parser = argparse.ArgumentParser(description="Deliver one business day from staging to landing.")
    parser.add_argument("--date", type=date.fromisoformat, help="Deliver one business day")
    parser.add_argument("--initial", action="store_true", help="Deliver master data and history once")
    parser.add_argument("--staging", type=Path, default=Path(cfg.path(cfg.staging_path)))
    parser.add_argument("--landing", type=Path, default=Path(cfg.path(cfg.landing_path)))
    args = parser.parse_args(argv)

    if args.initial:
        files = deliver_initial(args.staging, args.landing)
        print(f"  initial   {len(files)} files: master data and history backfill")
        return
    if not args.date:
        parser.error("give --date YYYY-MM-DD or --initial")
    actions = plan(args.date, args.staging)
    execute(actions, args.staging, args.landing)
    for a in actions:
        what = ", ".join(a.files) if a.files else "nothing (feed missed this day)"
        print(f"  {a.kind:<9} {a.business_date}  {what}")


if __name__ == "__main__":
    main()
