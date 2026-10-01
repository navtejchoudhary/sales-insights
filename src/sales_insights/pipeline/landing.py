"""What is waiting in landing/, and what is new? (plain Python, no Spark)

Rules:
- A folder is COMPLETE only when its manifest exists and every file the manifest lists exists.
  Incomplete folders are left alone (the delivery may still be in progress).
- Only files listed in the manifest are loaded; anything else in the folder is reported.
- A file is NEW if its (path, SHA-256) pair has never been loaded. The same path with the same
  content (a redelivery) is skipped. The same path with DIFFERENT content is loaded again and
  flagged, because the source changed a file it had already sent.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

DAILY_PREFIX = "business_date="
TARGETS = {"orders": "orders", "changes": "changes", "invoices": "invoices_history"}


@dataclass(frozen=True)
class LandingFile:
    rel_path: str  # relative to landing/, e.g. business_date=2026-10-05/orders_20261005.csv
    abs_path: str
    target: str  # bronze table name
    business_date: str | None
    sha256: str
    header: tuple[str, ...]  # CSV column names in file order ((), for manifests)
    expected_rows: int | None  # from the manifest
    expected_sha256: str | None  # from the manifest
    manifest_rel_path: str


@dataclass
class Discovery:
    files: list[LandingFile] = field(default_factory=list)  # data files + manifests of complete folders
    incomplete_folders: list[str] = field(default_factory=list)
    unexpected_files: list[str] = field(default_factory=list)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_header(path: Path) -> tuple[str, ...]:
    with open(path, encoding="utf-8") as fh:
        return tuple(fh.readline().rstrip("\n").split(","))


def target_for(folder: str, file_name: str) -> str:
    stem = file_name.rsplit(".", 1)[0]
    if folder == "masters":
        return f"masters_{stem}"
    prefix = stem.split("_", 1)[0]
    if prefix not in TARGETS:
        raise ValueError(f"Unknown file type: {folder}/{file_name}")
    return TARGETS[prefix]


def _manifest_in(folder: Path) -> Path | None:
    found = sorted(folder.glob("*manifest*.json"))
    return found[0] if found else None


def discover(landing: Path) -> Discovery:
    out = Discovery()
    if not landing.is_dir():
        return out
    folders = [p for p in sorted(landing.iterdir()) if p.is_dir()]
    for folder in folders:
        name = folder.name
        if name not in ("masters", "history") and not name.startswith(DAILY_PREFIX):
            continue
        manifest_path = _manifest_in(folder)
        if manifest_path is None:
            out.incomplete_folders.append(name)
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        listed = manifest.get("files", {})
        if not all((folder / f).exists() for f in listed):
            out.incomplete_folders.append(name)
            continue
        business_date = name[len(DAILY_PREFIX) :] if name.startswith(DAILY_PREFIX) else None
        manifest_rel = f"{name}/{manifest_path.name}"
        for f in sorted(listed):
            p = folder / f
            out.files.append(
                LandingFile(
                    rel_path=f"{name}/{f}",
                    abs_path=str(p),
                    target=target_for(name, f),
                    business_date=business_date,
                    sha256=sha256_of(p),
                    header=read_header(p),
                    expected_rows=listed[f].get("rows"),
                    expected_sha256=listed[f].get("sha256"),
                    manifest_rel_path=manifest_rel,
                )
            )
        out.files.append(
            LandingFile(
                rel_path=manifest_rel,
                abs_path=str(manifest_path),
                target="manifests",
                business_date=business_date,
                sha256=sha256_of(manifest_path),
                header=(),
                expected_rows=None,
                expected_sha256=None,
                manifest_rel_path=manifest_rel,
            )
        )
        known = set(listed) | {manifest_path.name}
        out.unexpected_files += [f"{name}/{p.name}" for p in sorted(folder.iterdir()) if p.name not in known]
    return out


def classify(files: list[LandingFile], loaded: dict[str, set[str]]) -> tuple[list, list, list]:
    """Split into (new, changed_content, already_loaded) using {rel_path: {sha256, ...}} of past loads."""
    new, changed, seen = [], [], []
    for f in files:
        shas = loaded.get(f.rel_path)
        if not shas:
            new.append(f)
        elif f.sha256 in shas:
            seen.append(f)
        else:
            changed.append(f)
    return new, changed, seen
