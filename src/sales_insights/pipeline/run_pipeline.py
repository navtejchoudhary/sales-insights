"""The whole daily pipeline in one command, in the right order, stopping at the first failure.

    uv run python -m sales_insights.pipeline.run_pipeline --date 2026-10-06
    uv run python -m sales_insights.pipeline.run_pipeline --from 2026-10-06 --to 2026-10-29             # rehearsal
    uv run python -m sales_insights.pipeline.run_pipeline --from 2026-10-06 --to 2026-10-29 --downstream last

For each business day:
    drip        deliver that day's files to landing/ (missed day, catch-up, Monday redelivery included)
    bronze      load new files exactly once
    silver      clean, dedupe, quarantine                         fails if a data-quality check fails
    gold        star schema
    reconcile   gold vs the source system's own totals           fails if any group is off by > 0.1%
    kpis        KPI SQL must equal the metric view               fails on any difference
    answer_key  Genie answer key + planted-story checks          fails if a story is no longer visible
    insights    findings -> gold.insights (+ deck on the last day)

`--downstream last` runs only drip + bronze for each day and the rest once at the end (much faster;
the final numbers are identical because silver and gold are full rebuilds). One line per step is printed,
and a CSV report of every day goes to output/.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

from sales_insights.common.config import Config
from sales_insights.common.lake import Lake
from sales_insights.drip import drip
from sales_insights.generator import history as h
from sales_insights.generator import masters as m
from sales_insights.generator import simulate as s

STEPS = ["drip", "bronze", "silver", "gold", "reconcile", "kpis", "answer_key", "insights"]


class StepFailed(Exception):
    """A step ran but its own checks failed (the message says which)."""


@dataclass
class StepResult:
    step: str
    ok: bool
    seconds: float
    detail: str


@dataclass
class DayResult:
    business_date: date
    steps: list[StepResult] = field(default_factory=list)
    alerts: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(st.ok for st in self.steps)


def day_range(first: date, last: date) -> list[date]:
    if last < first:
        raise ValueError(f"--to {last} is before --from {first}")
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


# ---------------------------------------------------------------------------
# Making sure the source files exist
# ---------------------------------------------------------------------------


def ensure_source(cfg: Config, staging: Path, landing: Path, until: date) -> list[str]:
    """Generate any missing source files up to `until` and do the one-time initial delivery. Idempotent."""
    seed = int(cfg.business["random_seed"])
    start = date.fromisoformat(cfg.business["simulation_start"])
    done = []
    if not (staging / "masters" / "masters_manifest.json").exists():
        tables, log = m.build_masters(seed, start)
        m.write_masters(tables, log, staging, seed, start)
        done.append("generated master data")
    if not (staging / "history" / "history_manifest.json").exists():
        h.write_history(h.build_history(seed, start), staging, seed, start)
        done.append("generated 18-month history")
    missing = [
        d for d in day_range(start, until) if not (staging / s.folder_name(d) / f"manifest_{d:%Y%m%d}.json").exists()
    ]
    if missing:
        sim = s.Simulator(seed, start)
        for d in missing:
            s.write_day(sim.build_day(d), staging, seed, start)
        done.append(f"generated {len(missing)} daily drop(s)")
    if not (landing / "masters").exists():
        drip.deliver_initial(staging, landing)
        done.append("initial delivery of master data and history")
    return done


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


def _drip(ctx: dict, d: date) -> str:
    actions = drip.plan(d, ctx["staging"])
    drip.execute(actions, ctx["staging"], ctx["landing"])
    return ", ".join(f"{a.kind} {a.business_date:%d %b}" for a in actions)


def _bronze(ctx: dict, d: date) -> str:
    from sales_insights.pipeline.bronze import run_bronze

    r = run_bronze(ctx["spark"], ctx["cfg"], landing=ctx["landing"], lake=ctx["lake"])
    if r.dq_failures:
        raise StepFailed(f"{r.dq_failures} data-quality failure(s), see ops.dq_results")
    waiting = f", waiting for {len(r.incomplete_folders)} incomplete folder(s)" if r.incomplete_folders else ""
    return f"{len(r.loaded) + len(r.reloaded_changed)} file(s) loaded, {sum(r.rows_by_target.values()):,} rows{waiting}"


def _silver(ctx: dict, d: date) -> str:
    from sales_insights.pipeline.silver import run_silver

    r = run_silver(ctx["spark"], ctx["cfg"], lake=ctx["lake"])
    if r.dq_failures:
        raise StepFailed(f"{r.dq_failures} data-quality failure(s), see ops.dq_results")
    return f"{r.rows['invoice_lines']:,} lines, {r.duplicates_removed} duplicates removed, {r.quarantined} quarantined"


def _gold(ctx: dict, d: date) -> str:
    from sales_insights.pipeline.gold import run_gold

    r = run_gold(ctx["spark"], ctx["cfg"], lake=ctx["lake"])
    return f"fact_sales {r.rows['fact_sales']:,} rows"


def _reconcile(ctx: dict, d: date) -> str:
    from sales_insights.pipeline.reconcile import run_reconcile

    r = run_reconcile(ctx["spark"], ctx["cfg"], lake=ctx["lake"])
    if r.failed_groups:
        f = r.failures[0]
        raise StepFailed(
            f"{r.failed_groups} of {r.groups} groups differ, first: {f['business_date']} {f['file_kind']} {f['invoice_date']}"
        )
    return f"{r.groups} of {r.groups} checks passed"


def _kpis(ctx: dict, d: date) -> str:
    from sales_insights.semantic.kpis import check_against_metric_view

    results = check_against_metric_view(ctx["spark"], ctx["lake"])
    bad = [(name, n) for name, rows, n in results if n]
    if bad:
        raise StepFailed(f"KPI SQL and metric view differ: {bad}")
    return f"{len(results)} KPI views match the metric view"


def _answer_key(ctx: dict, d: date) -> str:
    from sales_insights.semantic import answer_key as ak

    rows = ak.build(ctx["spark"], ctx["lake"])
    if ctx["write_answer_key"]:
        ak.write_csv(rows)
    counts = {k: sum(1 for r in rows if r["story_check"] == k) for k in ("PASS", "PENDING", "FAIL")}
    failed = [r["id"] for r in rows if r["story_check"] == "FAIL"]
    if failed and ctx["strict_stories"]:
        raise StepFailed(f"story checks failed: {', '.join(failed)}")
    return f"{counts['PASS']} PASS, {counts['PENDING']} PENDING, {counts['FAIL']} FAIL"


def _insights(ctx: dict, d: date) -> str:
    from sales_insights.insights import job

    out = job.run(
        ctx["spark"], ctx["cfg"], lake=ctx["lake"], deck_dir=ctx["deck_dir"] if ctx["last_day"] == d else None
    )
    ins = out["insights"]
    ctx["alerts"] = [i.text for i in ins if i.kind in ("stock_out", "anomaly")]
    deck = f", deck {Path(out['deck']).name}" if out["deck"] else ""
    return f"{len(ins)} findings, {len(ctx['alerts'])} alert(s){deck}"


STEP_FUNCS: dict[str, Callable[[dict, date], str]] = {
    "drip": _drip, "bronze": _bronze, "silver": _silver, "gold": _gold, "reconcile": _reconcile,
    "kpis": _kpis, "answer_key": _answer_key, "insights": _insights,
}  # fmt: skip


def run_day(ctx: dict, d: date, steps: list[str], echo: Callable[[str], None] = print) -> DayResult:
    result = DayResult(d)
    ctx["alerts"] = []
    for step in steps:
        t0 = time.perf_counter()
        try:
            detail, ok = STEP_FUNCS[step](ctx, d), True
        except StepFailed as e:
            detail, ok = f"FAILED: {e}", False
        except Exception as e:  # noqa: BLE001 - report any crash as a failed step, then stop the run
            detail, ok = f"ERROR: {type(e).__name__}: {e}", False
        st = StepResult(step, ok, time.perf_counter() - t0, detail)
        result.steps.append(st)
        echo(f"    {'ok  ' if ok else 'FAIL'} {step:<11} {st.seconds:6.1f}s  {detail}")
        if not ok:
            break
    result.alerts = list(ctx["alerts"])
    for a in result.alerts:
        echo(f"    ALERT {a}")
    return result


def run_range(
    spark: SparkSession,
    cfg: Config,
    days: list[date],
    *,
    staging: Path,
    landing: Path,
    lake: Lake,
    downstream: str = "each",
    deck_dir: Path | None = None,
    strict_stories: bool = True,
    write_answer_key: bool = True,
    echo: Callable[[str], None] = print,
) -> list[DayResult]:
    for note in ensure_source(cfg, staging, landing, max(days)):
        echo(f"  {note}")
    ctx = {
        "spark": spark, "cfg": cfg, "lake": lake, "staging": staging, "landing": landing, "deck_dir": deck_dir,
        "last_day": max(days), "strict_stories": strict_stories, "write_answer_key": write_answer_key, "alerts": [],
    }  # fmt: skip
    results = []
    for d in days:
        steps = STEPS if downstream == "each" or d == max(days) else STEPS[:2]
        echo(f"  {d:%a %d %b %Y}")
        r = run_day(ctx, d, steps, echo)
        results.append(r)
        if not r.ok:
            echo(f"  STOPPED on {d}: fix the failing step, then run again from this date.")
            break
    return results


def write_report(results: list[DayResult], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["business_date", "ok", "total_seconds", *[f"{s}_detail" for s in STEPS], "alerts"])
        for r in results:
            by = {st.step: st for st in r.steps}
            w.writerow(
                [r.business_date, r.ok, round(sum(st.seconds for st in r.steps), 1),
                 *[by[s].detail if s in by else "" for s in STEPS], " | ".join(r.alerts)]
            )  # fmt: skip
    return path


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    parser = argparse.ArgumentParser(description="Run the whole daily pipeline for one day or a range of days.")
    parser.add_argument("--date", type=date.fromisoformat, help="one business day")
    parser.add_argument("--from", dest="first", type=date.fromisoformat, help="first day of a range")
    parser.add_argument("--to", dest="last", type=date.fromisoformat, help="last day of a range")
    parser.add_argument("--downstream", choices=["each", "last"], default="each",
                        help="run silver..insights every day (each) or only after the last day (last)")  # fmt: skip
    parser.add_argument("--no-deck", action="store_true", help="do not build the PowerPoint deck")
    parser.add_argument("--profile", help="config profile: local (default, or SALES_PROFILE) or dev (Databricks)")
    args = parser.parse_args(argv)
    if args.date:
        days = [args.date]
    elif args.first and args.last:
        days = day_range(args.first, args.last)
    else:
        parser.error("give --date, or --from and --to")

    cfg = load_config(args.profile)
    spark = get_spark(cfg, app_name="pipeline")
    out = Path(cfg.path(cfg.output_path))
    lake = Lake(spark, cfg)
    t0 = time.perf_counter()
    results = run_range(
        spark, cfg, days,
        staging=Path(cfg.path(cfg.staging_path)), landing=Path(cfg.path(cfg.landing_path)), lake=lake,
        downstream=args.downstream, deck_dir=None if args.no_deck else out, write_answer_key=lake.by_path,
    )  # fmt: skip
    report = write_report(results, out / f"pipeline_report_{days[0]}_{days[-1]}.csv")
    ok = results and results[-1].ok and len(results) == len(days)
    print(f"  {'DONE' if ok else 'FAILED'}: {len(results)} of {len(days)} day(s) in {time.perf_counter() - t0:,.0f}s")
    print(f"  report: {report}")
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
