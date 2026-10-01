"""Bronze: load every new file from landing/ into Delta tables exactly as received.

    uv run python -m sales_insights.pipeline.bronze

What bronze does:
- loads master data, the history backfill and each daily orders/changes file
- keeps EVERY source column as text (leading zeros and bad values survive untouched)
- adds _source_file, _business_date, _load_ts, _run_id and _rescued_data to every row
- loads each file exactly once: ops.processed_files remembers (path, SHA-256) pairs, so a
  redelivered identical file is skipped; a sent file whose content changed is loaded again
  and flagged
- adds new source columns to the table instead of failing (schema evolution)
- checks each file against its manifest (row count, checksum) into ops.dq_results

What bronze does NOT do: dedupe, cast, standardise, quarantine or merge. That is silver.

Portability: file discovery uses plain file paths, which also work on Databricks for
Unity Catalog volumes (/Volumes/...). Tables go through Lake (paths locally, UC names on
Databricks). Auto Loader can replace discovery later without changing the transforms.
"""

from __future__ import annotations

import argparse
import json
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from sales_insights.common.config import Config
from sales_insights.common.lake import Lake
from sales_insights.pipeline import landing as lz

META_COLUMNS = ["_source_file", "_business_date", "_load_ts", "_run_id", "_rescued_data"]

PROCESSED_FILES = ("ops", "processed_files")
DQ_RESULTS = ("ops", "dq_results")

PROCESSED_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType()),
        T.StructField("file_path", T.StringType()),
        T.StructField("target_table", T.StringType()),
        T.StructField("business_date", T.StringType()),
        T.StructField("sha256", T.StringType()),
        T.StructField("rows", T.LongType()),
        T.StructField("status", T.StringType()),  # loaded | reloaded_changed_content
        T.StructField("loaded_ts", T.TimestampType()),
    ]
)

DQ_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType()),
        T.StructField("checked_ts", T.TimestampType()),
        T.StructField("layer", T.StringType()),
        T.StructField("check_name", T.StringType()),
        T.StructField("target", T.StringType()),
        T.StructField("business_date", T.StringType()),
        T.StructField("file_path", T.StringType()),
        T.StructField("expected", T.StringType()),
        T.StructField("actual", T.StringType()),
        T.StructField("status", T.StringType()),  # PASS | FAIL | WARN | WAIT
    ]
)


@dataclass
class BronzeRun:
    run_id: str
    loaded: list[str] = field(default_factory=list)
    reloaded_changed: list[str] = field(default_factory=list)
    skipped_already_loaded: int = 0
    rows_by_target: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    incomplete_folders: list[str] = field(default_factory=list)
    unexpected_files: list[str] = field(default_factory=list)
    dq_failures: int = 0


# ---------------------------------------------------------------------------
# Transforms (DataFrame in, DataFrame out: identical on Mac and Databricks)
# ---------------------------------------------------------------------------


def add_metadata(df: DataFrame, file_map: DataFrame, run_id: str) -> DataFrame:
    """Attach landing path and business date (joined on file name) plus load metadata."""
    file_name = F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1)
    return (
        df.withColumn("_file_name", file_name)
        .join(F.broadcast(file_map), on="_file_name", how="left")
        .drop("_file_name")
        .withColumn("_load_ts", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_rescued_data", F.lit(None).cast("string"))
    )


def read_csv_group(spark: SparkSession, files: list[lz.LandingFile], run_id: str) -> DataFrame:
    """Read files that share one header in a single pass. Every column stays text."""
    header = files[0].header
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .csv([f.abs_path for f in files])
    )
    # Folder names like business_date=... can trigger Spark partition discovery: drop that column.
    if "business_date" in df.columns and "business_date" not in header:
        df = df.drop("business_date")
    file_map = spark.createDataFrame(
        [(Path(f.abs_path).name, f.rel_path, f.business_date) for f in files],
        "_file_name string, _source_file string, _business_date string",
    )
    return add_metadata(df.select(*[F.col(f"`{c}`") for c in header], "_metadata"), file_map, run_id).drop("_metadata")


def manifests_frame(spark: SparkSession, files: list[lz.LandingFile], run_id: str) -> DataFrame:
    rows = []
    for f in files:
        content = Path(f.abs_path).read_text(encoding="utf-8")
        kind = json.loads(content).get("kind")
        rows.append((f.rel_path, kind, f.business_date, content, f.sha256))
    df = spark.createDataFrame(
        rows, "manifest_path string, kind string, business_date string, content string, sha256 string"
    )
    return (
        df.withColumn("_source_file", F.col("manifest_path"))
        .withColumn("_business_date", F.col("business_date"))
        .withColumn("_load_ts", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_rescued_data", F.lit(None).cast("string"))
    )


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


def already_loaded(lake: Lake) -> dict[str, set[str]]:
    if not lake.exists(*PROCESSED_FILES):
        return {}
    loaded: dict[str, set[str]] = defaultdict(set)
    for r in lake.read(*PROCESSED_FILES).select("file_path", "sha256").collect():
        loaded[r["file_path"]].add(r["sha256"])
    return loaded


def run_bronze(spark: SparkSession, cfg: Config, landing: Path | None = None, lake: Lake | None = None) -> BronzeRun:
    lake = lake or Lake(spark, cfg)
    landing = landing or Path(cfg.path(cfg.landing_path))
    run = BronzeRun(run_id=f"bronze-{datetime.now():%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}")

    found = lz.discover(landing)
    run.incomplete_folders, run.unexpected_files = found.incomplete_folders, found.unexpected_files
    new, changed, seen = lz.classify(found.files, already_loaded(lake))
    run.skipped_already_loaded = len(seen)
    changed_paths = {f.rel_path for f in changed}
    dq: list[tuple] = []
    now = datetime.now()

    for folder in run.incomplete_folders:
        dq.append(
            (
                run.run_id,
                now,
                "bronze",
                "folder_complete",
                "",
                folder.replace(lz.DAILY_PREFIX, ""),
                folder,
                "manifest and all listed files",
                "incomplete - not loaded yet",
                "WAIT",
            )
        )
    for path in run.unexpected_files:
        dq.append(
            (
                run.run_id,
                now,
                "bronze",
                "unexpected_file",
                "",
                None,
                path,
                "only manifest-listed files",
                "ignored",
                "WARN",
            )
        )

    # Group by (target table, header) so each group is one Spark read and one Delta append
    groups: dict[tuple[str, tuple[str, ...]], list[lz.LandingFile]] = defaultdict(list)
    for f in new + changed:
        groups[(f.target, f.header)].append(f)

    for (target, _header), files in sorted(groups.items(), key=lambda kv: kv[0][0]):
        if target == "manifests":
            df = manifests_frame(spark, files, run.run_id)
            counts = {f.rel_path: 1 for f in files}
        else:
            df = read_csv_group(spark, files, run.run_id)
            counts = {r["_source_file"]: r["count"] for r in df.groupBy("_source_file").count().collect()}
        lake.append(df, "bronze", target)

        log_rows = []
        for f in files:
            actual = int(counts.get(f.rel_path, 0))
            status = "reloaded_changed_content" if f.rel_path in changed_paths else "loaded"
            log_rows.append((run.run_id, f.rel_path, target, f.business_date, f.sha256, actual, status, datetime.now()))
            (run.reloaded_changed if status != "loaded" else run.loaded).append(f.rel_path)
            run.rows_by_target[target] += actual
            if f.expected_rows is not None:
                ok = actual == f.expected_rows
                dq.append(
                    (
                        run.run_id,
                        now,
                        "bronze",
                        "row_count_vs_manifest",
                        target,
                        f.business_date,
                        f.rel_path,
                        str(f.expected_rows),
                        str(actual),
                        "PASS" if ok else "FAIL",
                    )
                )
            if f.expected_sha256 is not None:
                ok = f.sha256 == f.expected_sha256
                dq.append(
                    (
                        run.run_id,
                        now,
                        "bronze",
                        "checksum_vs_manifest",
                        target,
                        f.business_date,
                        f.rel_path,
                        f.expected_sha256[:12],
                        f.sha256[:12],
                        "PASS" if ok else "FAIL",
                    )
                )
            if status != "loaded":
                dq.append(
                    (
                        run.run_id,
                        now,
                        "bronze",
                        "file_changed_after_delivery",
                        target,
                        f.business_date,
                        f.rel_path,
                        "unchanged",
                        "content changed - loaded again",
                        "WARN",
                    )
                )
        # Record progress per group, so a crash later in the run never reloads finished groups
        lake.append(spark.createDataFrame(log_rows, PROCESSED_SCHEMA), *PROCESSED_FILES)

    if dq:
        lake.append(spark.createDataFrame(dq, DQ_SCHEMA), *DQ_RESULTS)
    run.dq_failures = sum(1 for r in dq if r[-1] == "FAIL")
    return run


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config
    from sales_insights.common.spark import get_spark

    parser = argparse.ArgumentParser(description="Load new landing files into bronze Delta tables.")
    parser.add_argument("--landing", type=Path, help="Override the landing folder")
    args = parser.parse_args(argv)

    cfg = load_config()
    spark = get_spark(cfg, app_name="bronze")
    run = run_bronze(spark, cfg, landing=args.landing)
    print(f"  run {run.run_id}")
    print(
        f"  loaded {len(run.loaded)} files, reloaded (changed) {len(run.reloaded_changed)}, "
        f"skipped (already loaded) {run.skipped_already_loaded}"
    )
    for target, rows in sorted(run.rows_by_target.items()):
        print(f"    bronze.{target:<28} +{rows:,} rows")
    if run.incomplete_folders:
        print(f"  waiting for complete delivery: {', '.join(run.incomplete_folders)}")
    if run.unexpected_files:
        print(f"  ignored unexpected files: {', '.join(run.unexpected_files)}")
    print(f"  data-quality failures: {run.dq_failures}" + ("  <-- check ops.dq_results" if run.dq_failures else ""))


if __name__ == "__main__":
    main()
