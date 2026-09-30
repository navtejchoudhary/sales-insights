"""Load config/config.yaml and resolve names for the active profile.

Usage:
    from sales_insights.common.config import load_config
    cfg = load_config()                 # profile from SALES_PROFILE, default "local"
    cfg.table("silver", "orders")       # -> "silver.orders" locally, "sales_dev.silver.orders" on dev
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = REPO_ROOT / "config" / "config.yaml"


@dataclass(frozen=True)
class Config:
    profile: str
    staging_path: str
    landing_path: str
    lake_path: str | None
    catalog: str | None
    schemas: dict[str, str]
    processed_files_log: str | None
    business: dict[str, Any] = field(default_factory=dict)

    def schema(self, layer: str) -> str:
        """Fully qualified schema name for a layer (bronze, silver, gold, ops)."""
        name = self.schemas[layer]
        return f"{self.catalog}.{name}" if self.catalog else name

    def table(self, layer: str, name: str) -> str:
        """Fully qualified table name, e.g. table("gold", "fact_sales")."""
        return f"{self.schema(layer)}.{name}"

    def path(self, relative: str) -> str:
        """Resolve a local relative path against the repo root; leave absolute paths alone."""
        p = Path(relative)
        return str(p if p.is_absolute() else REPO_ROOT / p)


def load_config(profile: str | None = None, config_file: Path | str = DEFAULT_CONFIG) -> Config:
    profile = profile or os.environ.get("SALES_PROFILE", "local")
    with open(config_file, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    profiles = raw.get("profiles", {})
    if profile not in profiles:
        raise ValueError(f"Unknown profile '{profile}'. Available: {sorted(profiles)}")

    p = profiles[profile]
    return Config(
        profile=profile,
        staging_path=p["staging_path"],
        landing_path=p["landing_path"],
        lake_path=p.get("lake_path"),
        catalog=p.get("catalog"),
        schemas=p["schemas"],
        processed_files_log=p.get("processed_files_log"),
        business=raw.get("business", {}),
    )
