"""One-time copy of the client's SAP tables from their warehouse into the client area (Lakehouse Federation).

    run.py client_copy --profile dev --source-map /Volumes/client_dev/raw/output/source_map.json            # dry run
    run.py client_copy --profile dev --source-map ... --mode pilot --only text_company_codes   # 1 small table, timed
    run.py client_copy --profile dev --source-map ... --mode copy                              # everything, once

Cost first (docs/playbooks/copy_the_client_data.md): the source bills per second of query time with a 60-second
minimum, so this job
  - reads ONLY the contract columns (docs/sap_input_contract.md), renamed to the contract names, never `*`;
  - filters at the source: company codes and the date window of config.yaml (areas.client.scope);
  - never selects a personal column (refuses a map that would);
  - runs the queries back to back, once; `dry_run` (the default) prints them and runs nothing;
  - stops before the next query when one took longer than --max-seconds (the stop rule, default 120).

What it writes: the same layout as the dummy data (masters/ + history/, manifest last) into the area's staging
folder, so run_pipeline loads the real copy with the code proven on dummy data. The source's control totals are
computed from the copied rows (source_counts.csv, per company, month and category) and checked against an
independent aggregate query on the source (per company and category): any difference stops the copy before a
file is written.

Where the real names live: a JSON "source map" (catalog, tables, column renames) given at run time, kept in the
area's output volume, never in git (the repo is public). Template: config/source_map.example.json.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

from sales_insights.common.config import Config
from sales_insights.generator import sap

MODES = ("dry_run", "pilot", "copy")
REQUIRED = ("billing_items", "customer_master", "material_master")
# Columns the contract reads, per logical table (personal customer columns are not among them)
CONTRACT: dict[str, list[str]] = {
    "billing_items": sap.BILLING_COLUMNS,
    "customer_master": [c for c in sap.CUSTOMER_COLUMNS if c not in sap.PERSONAL_CUSTOMER_COLUMNS],
    "material_master": sap.MATERIAL_COLUMNS,
    **{f"text_{name}": ["language", code, label] for name, (code, label) in sap.TEXT_TABLES.items()},
}
# Source columns that must never be read, whatever the map says (personal data or free text)
PERSONAL = {
    *sap.PERSONAL_CUSTOMER_COLUMNS,
    # seen in the client's customer master (29 Oct): names, tax numbers, search term, the SAP user who created it
    "customerfullname", "organizationbpname1", "organizationbpname2", "taxnumber3", "taxnumber4", "taxnumber5",
    "taxnumber6", "sortfield", "createdbyuser", "addressid", "taxinvoicerepresentativename",
    "name1", "name2", "name3", "name4", "street", "streetname", "housenumber", "telephonenumber2",
    "faxnumber", "mobilephonenumber", "emailaddress", "taxnumber2", "vatregistration", "paymentreference",
    "billingdocumentitemtext", "bankaccount", "iban",
}  # fmt: skip
# Order of the copy: small tables first (a slow small table stops the run before the big ones)
ORDER = ("text", "customer_master", "material_master", "billing_items")
MANIFEST_SOURCE = "SAP tables copied once from the client's warehouse (Lakehouse Federation)"


@dataclass(frozen=True)
class TableSource:
    table: str  # schema.table in the foreign catalog
    columns: dict[str, str | None]  # contract column -> source column (None: not in the source, read as NULL)
    filter: str | None = None  # extra condition in SOURCE column names (e.g. a plant filter on materials)


@dataclass(frozen=True)
class SourceMap:
    catalog: str
    tables: dict[str, TableSource]


@dataclass
class CopyRun:
    mode: str
    queries: dict[str, str] = field(default_factory=dict)
    rows: dict[str, int] = field(default_factory=dict)
    seconds: dict[str, float] = field(default_factory=dict)
    written: Path | None = None
    stopped: str | None = None


# ---------------------------------------------------------------------------
# The source map (real names, given at run time)
# ---------------------------------------------------------------------------


def load_source_map(raw: dict) -> SourceMap:
    """Check the map: required tables present, contract columns only, nothing personal."""
    if not raw.get("catalog"):
        raise ValueError("source map: `catalog` (the foreign catalog of the connection) is missing")
    tables = raw.get("tables") or {}
    missing = [t for t in REQUIRED if t not in tables]
    if missing:
        raise ValueError(f"source map: tables {missing} are missing")
    unknown = sorted(set(tables) - set(CONTRACT))
    if unknown:
        raise ValueError(f"source map: unknown logical tables {unknown}; known: {sorted(CONTRACT)}")
    out = {}
    for name, spec in tables.items():
        if isinstance(spec, str):
            spec = {"table": spec}
        renames = spec.get("columns") or {}
        extra = sorted(set(renames) - set(CONTRACT[name]))
        if extra:
            raise ValueError(f"source map: {name} maps columns the contract does not read: {extra}")
        columns = {c: renames.get(c, c) for c in CONTRACT[name]}
        personal = sorted(src for src in columns.values() if src and src.lower() in PERSONAL)
        if personal:
            raise ValueError(f"source map: {name} would read personal columns {personal}")
        if spec.get("filter") and any(p in spec["filter"].lower() for p in PERSONAL):
            raise ValueError(f"source map: the filter of {name} names a personal column")
        out[name] = TableSource(table=spec["table"], columns=columns, filter=spec.get("filter"))
    return SourceMap(catalog=raw["catalog"], tables=out)


def read_source_map(path: Path) -> SourceMap:
    return load_source_map(json.loads(Path(path).read_text(encoding="utf-8")))


# ---------------------------------------------------------------------------
# Queries (plain strings: printed in a dry run, run as they are in a copy)
# ---------------------------------------------------------------------------


def _ident(name: str) -> str:
    return ".".join(f"`{part}`" for part in name.split("."))


def _quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def billing_filter(src: TableSource, scope) -> str:
    """Company codes and the date window, on the SOURCE columns, as simple comparisons the source can apply
    itself (no functions around the columns, so the filter is pushed down to the warehouse)."""
    company, day = _ident(src.columns["companycode"]), _ident(src.columns["billingdocumentdate"])
    codes = ", ".join(_quote(c) for c in scope.company_codes)
    after = (scope.date_to + timedelta(days=1)).isoformat()
    return f"{company} IN ({codes}) AND {day} >= TIMESTAMP '{scope.date_from} 00:00:00' AND {day} < TIMESTAMP '{after} 00:00:00'"


def where(name: str, src: TableSource, scope) -> str | None:
    conditions = [billing_filter(src, scope)] if name == "billing_items" else []
    if src.filter:
        conditions.append(f"({src.filter})")
    return " AND ".join(conditions) or None


def select_query(name: str, smap: SourceMap, scope) -> str:
    """SELECT <source col> AS <contract col>, ... for one logical table: named columns only, never `*`."""
    src = smap.tables[name]
    # columns the source does not have are not asked for: the copy adds them, empty (see complete_columns)
    cols = [f"{_ident(s)} AS `{c}`" for c, s in src.columns.items() if s]
    sql = f"SELECT {', '.join(cols)}\nFROM {_ident(smap.catalog)}.{_ident(src.table)}"
    condition = where(name, src, scope)
    return sql + (f"\nWHERE {condition}" if condition else "")


def control_query(smap: SourceMap, scope) -> str:
    """Independent totals from the source (same filter as the copy): lines and net amount per company and
    category. Plain GROUP BY on columns: simple enough for the source to compute it itself."""
    src = smap.tables["billing_items"]
    company, category = _ident(src.columns["companycode"]), _ident(src.columns["sddocumentcategory"])
    net = _ident(src.columns["netamount"])
    return (
        f"SELECT {company} AS companycode, {category} AS sddocumentcategory, COUNT(*) AS line_count, "
        f"SUM({net}) AS netamount_sum\nFROM {_ident(smap.catalog)}.{_ident(src.table)}\n"
        f"WHERE {billing_filter(src, scope)}\nGROUP BY {company}, {category}"
    )


def complete_columns(df: pd.DataFrame, src: TableSource) -> pd.DataFrame:
    """Every contract column in contract order; the ones the source does not have are empty."""
    out = df.copy()
    for col in src.columns:
        if col not in out.columns:
            out[col] = None
    return out[list(src.columns)]


def as_text(df: pd.DataFrame) -> pd.DataFrame:
    """Every value as text, every missing value as "" (never the text "None", "nan" or "<NA>")."""
    obj = df.astype(object)
    return obj.where(pd.notna(obj), "").astype(str)


def copy_order(smap: SourceMap) -> list[str]:
    def rank(name: str) -> int:
        return next(i for i, prefix in enumerate(ORDER) if name.startswith(prefix))

    return sorted(smap.tables, key=lambda n: (rank(n), n))


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def totals_agree(copied: pd.DataFrame, control: pd.DataFrame) -> list[str]:
    """Differences between the copied billing rows and the source's own totals ([] = they agree)."""

    def norm(df: pd.DataFrame, net_col: str, count_col: str | None) -> dict:
        net = pd.to_numeric(df[net_col], errors="coerce").fillna(0.0)
        keyed = df.assign(_net=net, _one=1 if count_col is None else pd.to_numeric(df[count_col]))
        g = keyed.groupby([df["companycode"].fillna("").astype(str), df["sddocumentcategory"].fillna("").astype(str)])
        return {k: (int(v["_one"]), round(float(v["_net"]), 2)) for k, v in g[["_one", "_net"]].sum().iterrows()}

    mine = norm(copied, "netamount", None)
    theirs = norm(control, "netamount_sum", "line_count")
    problems = []
    for key in sorted(set(mine) | set(theirs)):
        (a_lines, a_net), (e_lines, e_net) = mine.get(key, (0, 0.0)), theirs.get(key, (0, 0.0))
        if a_lines != e_lines or abs(a_net - e_net) > 0.01:
            problems.append(f"{key}: copied {a_lines} lines / {a_net:.2f}, source {e_lines} lines / {e_net:.2f}")
    return problems


def ready_to_copy(cfg: Config, staging: Path, bronze_loaded: bool = False) -> None:
    """Refuse a copy that could mix real and dummy rows or overwrite an earlier copy."""
    source = cfg.area_setting("source", "real")
    if source != "real":
        raise RuntimeError(
            f"config says `source: {source}`: clear the dummy data and set `source: real` first "
            "(docs/playbooks/copy_the_client_data.md)"
        )
    if (staging / "history" / sap.MANIFEST).exists() or (staging / "masters" / sap.MANIFEST).exists():
        raise RuntimeError(f"{staging} already holds a copy: never copy twice (empty the staging volume to redo it)")
    if bronze_loaded:
        raise RuntimeError(
            "bronze still holds earlier rows (the dummy data?): drop and recreate the client schemas first "
            "(docs/playbooks/copy_the_client_data.md, step 2)"
        )


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

Fetch = Callable[[str], pd.DataFrame]  # SQL -> every column as text (None for NULL)


def run_copy(
    cfg: Config,
    smap: SourceMap,
    fetch: Fetch,
    staging: Path,
    mode: str = "dry_run",
    only: str | None = None,
    max_seconds: float = 120.0,
    echo: Callable[[str], None] = print,
    bronze_loaded: bool = False,
) -> CopyRun:
    from sales_insights.pipeline.sap_silver import scope_of

    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}")
    scope = scope_of(cfg)
    run = CopyRun(mode=mode)
    names = copy_order(smap)
    if only:
        if only not in smap.tables:
            raise ValueError(f"--only {only}: not in the source map ({names})")
        names = [only]
    elif mode == "pilot":
        names = names[:1]  # the smallest table first in the copy order: one short query
    run.queries = {n: select_query(n, smap, scope) for n in names}
    if mode == "copy" and not only:
        run.queries["control_totals"] = control_query(smap, scope)

    if mode == "dry_run":
        echo(f"  DRY RUN: nothing is sent to the source. Scope: companies {list(scope.company_codes)}, "
             f"{scope.date_from} to {scope.date_to}.")  # fmt: skip
        for name, sql in run.queries.items():
            echo(f"\n-- {name}\n{sql};")
        echo(f"\n  {len(run.queries)} queries; a copy also runs the control-totals query. Nothing was run.")
        return run
    if mode == "copy":
        if only:
            raise ValueError("a copy takes every table at once (--only is for the pilot)")
        ready_to_copy(cfg, staging, bronze_loaded)

    frames: dict[str, pd.DataFrame] = {}
    for name, sql in run.queries.items():
        t0 = time.perf_counter()
        df = fetch(sql)
        if name in smap.tables:
            df = complete_columns(df, smap.tables[name])
        run.seconds[name] = round(time.perf_counter() - t0, 1)
        run.rows[name] = len(df)
        frames[name] = df
        echo(f"  {name:<32} {len(df):>9,} rows  {run.seconds[name]:>6.1f}s")
        if run.seconds[name] > max_seconds:
            run.stopped = f"{name} took {run.seconds[name]}s (> {max_seconds}s): stopped before the next query"
            echo(f"  STOP: {run.stopped}. Re-plan before going on (docs/playbooks/copy_the_client_data.md).")
            return run
    if mode == "pilot":
        echo("  PILOT done: nothing was written. Use the time above to confirm the cost estimate.")
        return run

    control = frames.pop("control_totals")
    problems = totals_agree(frames["billing_items"], control)
    if problems:
        run.stopped = "copied rows differ from the source's own totals: " + "; ".join(problems[:5])
        echo(f"  STOP: {run.stopped}. Nothing was written.")
        return run
    out = {name: as_text(df) for name, df in frames.items()}
    out["source_counts"] = sap.source_counts(out["billing_items"])
    run.written = sap.write_sap(out, [], staging, None, datetime.now(UTC).date(), source_system=MANIFEST_SOURCE)
    echo(f"  written to {staging} (masters/ and history/, manifests last). Next: run_pipeline --area client")
    return run


def spark_fetch(spark) -> Fetch:
    """Run a query through Lakehouse Federation; every column as text, the way bronze stores it."""

    def fetch(sql: str) -> pd.DataFrame:
        from pyspark.sql import functions as F

        df = spark.sql(sql)
        return df.select(*[F.col(f"`{c}`").cast("string").alias(c) for c in df.columns]).toPandas()

    return fetch


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config

    parser = argparse.ArgumentParser(description="Copy the client's SAP tables once (dry run by default).")
    parser.add_argument("--area", default="client")
    parser.add_argument("--source-map", type=Path, required=True, help="JSON with the real names (never in git)")
    parser.add_argument("--mode", choices=MODES, default="dry_run")
    parser.add_argument("--only", default="", help="pilot: one logical table, e.g. text_company_codes")
    parser.add_argument("--max-seconds", type=float, default=120.0, help="stop rule per query (default 120)")
    args = parser.parse_args(argv)

    cfg = load_config(area=args.area)
    smap = read_source_map(args.source_map)
    staging = Path(cfg.path(cfg.staging_path))
    fetch: Fetch
    bronze_loaded = False
    if args.mode == "dry_run":
        fetch = lambda sql: pd.DataFrame()  # noqa: E731 - never called in a dry run
    else:
        from sales_insights.common.lake import Lake
        from sales_insights.common.spark import get_spark

        spark = get_spark(cfg, app_name="client-copy")
        fetch = spark_fetch(spark)
        bronze_loaded = Lake(spark, cfg).exists("bronze", "billing_items")
    run = run_copy(
        cfg, smap, fetch, staging, args.mode, args.only or None, args.max_seconds,
        bronze_loaded=args.mode == "copy" and bronze_loaded,
    )  # fmt: skip
    if run.stopped:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
