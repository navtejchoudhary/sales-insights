"""The whole daily pipeline in one command, in the right order, stopping at the first failure.

    uv run python -m sales_insights.pipeline.run_pipeline --date 2026-10-06
    uv run python -m sales_insights.pipeline.run_pipeline --from 2026-10-06 --to 2026-10-29             # rehearsal
    uv run python -m sales_insights.pipeline.run_pipeline --from 2026-10-06 --to 2026-10-29 --downstream last
    uv run python -m sales_insights.pipeline.run_pipeline --date 2026-10-06 --area sales    # the default area
    uv run python -m sales_insights.pipeline.run_pipeline --date 2026-10-06 --area inventory   # after sales
    uv run python -m sales_insights.pipeline.run_pipeline --date 2026-09-30 --area client      # SAP billing data

For each business day:
    drip        deliver that day's files to landing/ (missed day, catch-up, Monday redelivery included)
    bronze      load new files exactly once
    silver      clean, dedupe, quarantine                         fails if a data-quality check fails
    gold        star schema
    reconcile   gold vs the source's totals per invoice date      fails if any date is off by > 0.1%
    kpis        KPI SQL must equal the metric view               fails on any difference
    answer_key  Genie answer key + planted-story checks          fails if a story is no longer visible
    insights    findings -> gold.insights (+ deck on the last day)

Inventory area (--area inventory): drip, bronze (same code, inventory files), silver (inventory_silver),
gold (inventory_gold), checks (inventory_checks: source totals, stock roll-forward, cross-check with sales),
kpis (inventory KPI SQL = the inventory_metrics and sales_and_stock metric views), answer_key (the Stock and
Sales & Stock Genie answer keys + stories I1-I4), insights (confirmed stock-outs, low cover, overstock).
It reads the sales gold, so run sales first for the same day. The sales deck shows the latest stock findings.

Client area (--area client, SAP billing data, docs/sap_input_contract.md): bronze (same code), silver
(sap_silver: one fate per source row), gold (same code), reconcile (sap_reconcile: source -> silver -> gold),
kpis (the sales KPI SQL = the sales metric view, on the client gold).
No daily drops: the data is one copy. With `source: dummy` in config.yaml, SAP-shaped dummy files are generated
and delivered the first time; with `source: real` nothing is ever generated and the run stops if the copied
files have not landed. The date only labels the run.

`--downstream last` only delivers (drip) each day's files and runs everything else, bronze included, once
at the end (much faster: bronze checks every landed file each time it runs; the final numbers are identical
because bronze loads each file exactly once and silver and gold are full rebuilds). One line per step is printed,
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
from sales_insights.generator import inventory as inv
from sales_insights.generator import masters as m
from sales_insights.generator import sap
from sales_insights.generator import simulate as s

STEPS = ["drip", "bronze", "silver", "gold", "reconcile", "kpis", "answer_key", "insights"]
INVENTORY_STEPS = ["drip", "bronze", "silver", "gold", "checks", "kpis", "answer_key", "insights"]
CLIENT_STEPS = ["bronze", "silver", "gold", "reconcile", "kpis"]  # kpis: the sales KPI SQL = the metric view
# Business areas this command can run (config.yaml `areas:`). Sales is the default and runs exactly as before.
# Inventory reads the sales gold (shared dimensions, cross-area check), so run sales first each day.
AREA_STEPS = {"sales": STEPS, "inventory": INVENTORY_STEPS, "client": CLIENT_STEPS}


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
    if cfg.area == "inventory":
        return ensure_inventory_source(cfg, staging, landing, until)
    if cfg.area == "client":
        return ensure_client_source(cfg, staging, landing)
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


def ensure_inventory_source(cfg: Config, staging: Path, landing: Path, until: date) -> list[str]:
    """Inventory area: locations master, 18-month history and missing daily drops, then the initial delivery."""
    seed = int(cfg.business["random_seed"])
    start = date.fromisoformat(cfg.business["simulation_start"])
    done = []
    sim = None
    if not (staging / "masters" / "masters_manifest.json").exists():
        sim = sim or inv.InventorySimulator(seed, start)
        inv.write_locations(sim.locations, staging, seed, start)
        done.append("generated the locations master")
    if not (staging / "history" / "history_manifest.json").exists():
        sim = sim or inv.InventorySimulator(seed, start)
        inv.write_history(*sim.build_history(), staging, seed, start)
        done.append("generated 18-month stock history")
    missing = [
        d for d in day_range(start, until) if not (staging / inv.folder_name(d) / f"manifest_{d:%Y%m%d}.json").exists()
    ]
    if missing:
        sim = sim or inv.InventorySimulator(seed, start)
        for d in missing:
            inv.write_day(sim.build_day(d), staging, seed, start)
        done.append(f"generated {len(missing)} daily stock drop(s)")
    if not (landing / "masters").exists():
        drip.deliver_initial(staging, landing)
        done.append("initial delivery of the locations master and stock history")
    return done


def ensure_client_source(cfg: Config, staging: Path, landing: Path) -> list[str]:
    """Client area: SAP-shaped dummy files (only when config says `source: dummy`), then the one-time delivery.

    With `source: real` (or no setting) nothing is generated: real and dummy rows must never mix.
    """
    source = cfg.area_setting("source", "real")
    done = []
    if not (staging / "history" / sap.MANIFEST).exists():
        if source != "dummy":
            raise FileNotFoundError(
                f"{staging / 'history' / sap.MANIFEST} not found: the copied SAP files have not landed yet "
                f"(source: {source} never generates dummy data)"
            )
        seed = int(cfg.business["random_seed"])
        start = date.fromisoformat(cfg.business["simulation_start"])
        out, log = sap.build_sap(seed, start)
        sap.write_sap(out, log, staging, seed, start)
        done.append(f"generated SAP-shaped dummy data ({len(out['billing_items']):,} billing rows)")
    if not (landing / "masters").exists():
        drip.deliver_initial(staging, landing)
        done.append("initial delivery of the SAP files")
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
            f"{r.failed_groups} of {r.groups} invoice dates differ from the source, first: {f['invoice_date']}"
        )
    return f"{r.groups} of {r.groups} checks passed"


def _kpis(ctx: dict, d: date) -> str:
    from sales_insights.semantic.kpis import check_against_metric_view

    sales_lake = None if ctx["cfg"].area == "sales" else ctx["sales_lake"]
    results = check_against_metric_view(ctx["spark"], ctx["lake"], sales_lake)
    bad = [(name, n) for name, rows, n in results if n]
    if bad:
        raise StepFailed(f"KPI SQL and metric view differ: {bad}")
    return f"{len(results)} KPI views match the metric view"


def _answer_key(ctx: dict, d: date) -> str:
    from sales_insights.semantic import answer_key as ak

    rows = []
    for agent in (a for a in ak.SPACES.values() if a.area == ctx["cfg"].area):
        sales_lake = None if agent.area == "sales" else ctx["sales_lake"]
        agent_rows = ak.build(ctx["spark"], ctx["lake"], agent=agent, sales_lake=sales_lake)
        if ctx["write_answer_key"]:
            ak.write_csv(agent_rows, agent.answer_key)
        rows += agent_rows
    counts = {k: sum(1 for r in rows if r["story_check"] == k) for k in ("PASS", "PENDING", "FAIL")}
    failed = [r["id"] for r in rows if r["story_check"] == "FAIL"]
    if failed and ctx["strict_stories"]:
        raise StepFailed(f"story checks failed: {', '.join(failed)}")
    return f"{counts['PASS']} PASS, {counts['PENDING']} PENDING, {counts['FAIL']} FAIL"


def _insights(ctx: dict, d: date) -> str:
    from sales_insights.insights import job
    from sales_insights.insights.narrative import ALERT_KINDS

    out = job.run(
        ctx["spark"], ctx["cfg"], lake=ctx["lake"], deck_dir=ctx["deck_dir"] if ctx["last_day"] == d else None,
        stock_lake=ctx.get("stock_lake"),
    )  # fmt: skip
    ins = out["insights"]
    ctx["alerts"] = [i.text for i in ins if i.kind in ALERT_KINDS]
    deck = f", deck {Path(out['deck']).name}" if out["deck"] else ""
    forecast = f", forecast {out['forecast_months']} month(s)" if out.get("forecast_months") else ""
    return f"{len(ins)} findings, {len(ctx['alerts'])} alert(s){forecast}{deck}"


STEP_FUNCS: dict[str, Callable[[dict, date], str]] = {
    "drip": _drip, "bronze": _bronze, "silver": _silver, "gold": _gold, "reconcile": _reconcile,
    "kpis": _kpis, "answer_key": _answer_key, "insights": _insights,
}  # fmt: skip


def _inventory_silver(ctx: dict, d: date) -> str:
    from sales_insights.pipeline.inventory_silver import run_inventory_silver

    r = run_inventory_silver(ctx["spark"], ctx["cfg"], lake=ctx["lake"], sales_lake=ctx["sales_lake"])
    if r.dq_failures:
        raise StepFailed(f"{r.dq_failures} data-quality failure(s), see ops.dq_results")
    return (
        f"{r.rows['movements']:,} movements, {r.rows['stock']:,} stock rows, {r.duplicates_removed} duplicates "
        f"removed, {r.quarantined} quarantined, {r.units_fixed} units fixed"
    )


def _inventory_gold(ctx: dict, d: date) -> str:
    from sales_insights.pipeline.inventory_gold import run_inventory_gold

    r = run_inventory_gold(ctx["spark"], ctx["cfg"], lake=ctx["lake"], sales_lake=ctx["sales_lake"])
    return (
        f"fact_stock_daily {r.rows['fact_stock_daily']:,} rows, fact_stock_movements {r.rows['fact_stock_movements']:,}"
    )


def _inventory_checks(ctx: dict, d: date) -> str:
    from sales_insights.pipeline.inventory_checks import run_inventory_checks

    r = run_inventory_checks(ctx["spark"], ctx["cfg"], lake=ctx["lake"], sales_lake=ctx["sales_lake"])
    summary = ", ".join(f"{x.name} {x.groups - x.failed}/{x.groups}" for x in r.results.values())
    if not r.ok:
        bad = next(x for x in r.results.values() if not x.ok)
        raise StepFailed(f"{summary}; first failure in {bad.name}: {bad.first_failures[:1]}")
    return f"{summary} passed"


def _inventory_insights(ctx: dict, d: date) -> str:
    from sales_insights.insights import job
    from sales_insights.insights.stock import ALERT_KINDS

    out = job.run_stock(ctx["spark"], ctx["cfg"], lake=ctx["lake"], sales_lake=ctx["sales_lake"])
    ins = out["insights"]
    ctx["alerts"] = [i.text for i in ins if i.kind in ALERT_KINDS]
    counts = {k: sum(1 for i in ins if i.kind == k) for k in ("stock_out_confirmed", "low_cover", "overstock")}
    return (
        f"{counts['stock_out_confirmed']} confirmed stock-out(s), {counts['low_cover']} low cover, "
        f"top {counts['overstock']} overstock"
    )


INVENTORY_STEP_FUNCS: dict[str, Callable[[dict, date], str]] = {
    "drip": _drip, "bronze": _bronze, "silver": _inventory_silver, "gold": _inventory_gold,
    "checks": _inventory_checks, "kpis": _kpis, "answer_key": _answer_key, "insights": _inventory_insights,
}  # fmt: skip


def _sap_silver(ctx: dict, d: date) -> str:
    from sales_insights.pipeline.sap_silver import KEPT, run_sap_silver

    r = run_sap_silver(ctx["spark"], ctx["cfg"], lake=ctx["lake"])
    if r.dq_failures:
        raise StepFailed(f"{r.dq_failures} data-quality failure(s), see ops.dq_results")
    filtered = sum(n for f, n in r.fates.items() if f != KEPT) - r.quarantined
    notes = f"; notes: {', '.join(r.warnings)}" if r.warnings else ""
    return (
        f"{r.rows['invoice_lines']:,} lines, {filtered:,} filtered (counted in ops.sap_row_fates), "
        f"{r.quarantined} quarantined{notes}"
    )


def _sap_reconcile(ctx: dict, d: date) -> str:
    from sales_insights.pipeline.sap_reconcile import run_sap_reconcile

    r = run_sap_reconcile(ctx["spark"], ctx["cfg"], lake=ctx["lake"])
    summary = ", ".join(f"{name} {n - r.failed[name]}/{n}" for name, n in r.groups.items())
    if r.failed_groups:
        raise StepFailed(f"{summary}; see ops.sap_reconciliation")
    return f"{summary} groups match"


CLIENT_STEP_FUNCS: dict[str, Callable[[dict, date], str]] = {
    "bronze": _bronze, "silver": _sap_silver, "gold": _gold, "reconcile": _sap_reconcile, "kpis": _kpis,
}  # fmt: skip
AREA_STEP_FUNCS = {"sales": STEP_FUNCS, "inventory": INVENTORY_STEP_FUNCS, "client": CLIENT_STEP_FUNCS}


def run_day(ctx: dict, d: date, steps: list[str], echo: Callable[[str], None] = print) -> DayResult:
    result = DayResult(d)
    ctx["alerts"] = []
    for step in steps:
        t0 = time.perf_counter()
        try:
            detail, ok = ctx.get("funcs", STEP_FUNCS)[step](ctx, d), True
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
    sales_lake: Lake | None = None,
    stock_lake: Lake | None = None,
) -> list[DayResult]:
    for note in ensure_source(cfg, staging, landing, max(days)):
        echo(f"  {note}")
    area_steps = AREA_STEPS[cfg.area]
    ctx = {
        "spark": spark, "cfg": cfg, "lake": lake, "staging": staging, "landing": landing, "deck_dir": deck_dir,
        "last_day": max(days), "strict_stories": strict_stories, "write_answer_key": write_answer_key, "alerts": [],
        "funcs": AREA_STEP_FUNCS[cfg.area],
        "sales_lake": sales_lake or (Lake(spark, cfg.for_area("sales")) if cfg.area != "sales" else lake),
        "stock_lake": stock_lake,
    }  # fmt: skip
    results = []
    for d in days:
        drip_only = [st for st in area_steps[:1] if st == "drip"]  # the client area has no daily drops
        steps = area_steps if downstream == "each" or d == max(days) else drip_only
        echo(f"  {d:%a %d %b %Y}")
        r = run_day(ctx, d, steps, echo)
        results.append(r)
        if not r.ok:
            echo(f"  STOPPED on {d}: fix the failing step, then run again from this date.")
            break
    return results


def write_report(results: list[DayResult], path: Path, steps: list[str] = STEPS) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["business_date", "ok", "total_seconds", *[f"{s}_detail" for s in steps], "alerts"])
        for r in results:
            by = {st.step: st for st in r.steps}
            w.writerow(
                [r.business_date, r.ok, round(sum(st.seconds for st in r.steps), 1),
                 *[by[s].detail if s in by else "" for s in steps], " | ".join(r.alerts)]
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
    parser.add_argument("--area", default="sales", help="business area (config.yaml areas:), default sales")
    args = parser.parse_args(argv)
    if args.date:
        days = [args.date]
    elif args.first and args.last:
        days = day_range(args.first, args.last)
    else:
        parser.error("give --date, or --from and --to")
    if args.area not in AREA_STEPS:
        parser.error(f"area '{args.area}' has no pipeline steps yet; available: {sorted(AREA_STEPS)}")

    cfg = load_config(args.profile, area=args.area)
    spark = get_spark(cfg, app_name="pipeline")
    out = Path(cfg.path(cfg.output_path))
    lake = Lake(spark, cfg)
    # the sales deck shows the latest stock findings when the inventory area exists
    stock_lake = (
        Lake(spark, cfg.for_area("inventory")) if cfg.area == "sales" and "inventory" in cfg.area_names() else None
    )
    t0 = time.perf_counter()
    results = run_range(
        spark, cfg, days,
        staging=Path(cfg.path(cfg.staging_path)), landing=Path(cfg.path(cfg.landing_path)), lake=lake,
        downstream=args.downstream, deck_dir=None if args.no_deck else out, write_answer_key=lake.by_path,
        stock_lake=stock_lake,
    )  # fmt: skip
    report = write_report(results, out / f"pipeline_report_{days[0]}_{days[-1]}.csv", AREA_STEPS[cfg.area])
    ok = results and results[-1].ok and len(results) == len(days)
    print(f"  {'DONE' if ok else 'FAILED'}: {len(results)} of {len(days)} day(s) in {time.perf_counter() - t0:,.0f}s")
    print(f"  report: {report}")
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
