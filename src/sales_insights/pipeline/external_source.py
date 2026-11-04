"""Source files that come from outside (config `source: external`): check they are all there, never generate.

Used by the s3demo area: the sales demo files were uploaded once to an S3 bucket, and on Databricks the
area's staging folder is an external, read-only volume on that bucket. The sales area itself generates any
missing file (run_pipeline.ensure_source); an external source must not, because a generated file would mix
invented rows with the delivered ones and would not even be writable (the volume is read-only).

So, before anything is delivered or loaded:
- staging/masters and staging/history must exist, each with its manifest;
- every business day of the run must have its folder with its manifest;
- otherwise the run stops with the list of what is missing (nothing is copied, nothing is loaded).
Then the one-time delivery of master data and history to landing, exactly as for sales (drip.deliver_initial).

Plain Python, no Spark: the checks can be tested anywhere.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from sales_insights.drip import drip
from sales_insights.generator.simulate import folder_name

INITIAL = {"masters": "masters_manifest.json", "history": "history_manifest.json"}


def missing_files(staging: Path, first: date, last: date) -> list[str]:
    """Every manifest the run needs that is not in staging (empty list = all there)."""
    missing = [f"{folder}/{name}" for folder, name in INITIAL.items() if not (staging / folder / name).is_file()]
    d = first
    while d <= last:
        manifest = staging / folder_name(d) / f"manifest_{d:%Y%m%d}.json"
        if not manifest.is_file():
            missing.append(f"{folder_name(d)}/{manifest.name}")
        d += timedelta(days=1)
    return missing


def ensure_external_source(staging: Path, landing: Path, first: date, last: date) -> list[str]:
    """Stop if a delivered file is missing; otherwise do the one-time initial delivery. Never writes to staging."""
    missing = missing_files(staging, first, last)
    if missing:
        shown = ", ".join(missing[:5]) + (f" and {len(missing) - 5} more" if len(missing) > 5 else "")
        raise FileNotFoundError(
            f"{len(missing)} source file(s) not found in {staging}: {shown}. "
            "This area never generates data (source: external): upload the files, or check the volume."
        )
    if (landing / "masters").exists():
        return []
    drip.deliver_initial(staging, landing)
    return ["initial delivery of master data and history"]
