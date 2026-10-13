"""Load config/config.yaml and resolve names for the active profile and business area.

Usage:
    from sales_insights.common.config import load_config
    cfg = load_config()                 # profile from SALES_PROFILE, default "local"; area "sales"
    cfg.table("silver", "orders")       # -> "silver.orders" locally, "sales_dev.silver.orders" on dev
    inv = cfg.for_area("inventory")     # same profile, the inventory area's folders and catalog
    inv.table("gold", "fact_stock_daily")   # -> "inventory_dev.gold.fact_stock_daily" on dev

Areas (the `areas:` section of config.yaml): every business area has its own pipeline, folders and
tables. "sales" is the default area and uses the profile settings exactly as they are, so code that never
mentions an area behaves as before. Another area overrides only the settings that differ.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = REPO_ROOT / "config" / "config.yaml"

DEFAULT_AREA = "sales"
# Settings an area may override per profile (everything that says WHERE data lives).
AREA_SETTINGS = (
    "staging_path",
    "landing_path",
    "lake_path",
    "catalog",
    "schemas",
    "processed_files_log",
    "output_path",
)
# Used when config.yaml has no `areas:` section (older config files, tests with a custom file).
SALES_FILE_TARGETS = {"orders": "orders", "changes": "changes", "invoices": "invoices_history"}


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
    output_path: str = "output"  # decks and run reports
    area: str = DEFAULT_AREA
    file_targets: dict[str, str] = field(default_factory=lambda: dict(SALES_FILE_TARGETS))
    areas: dict[str, Any] = field(default_factory=dict, repr=False)  # raw `areas:` section of config.yaml
    profile_settings: dict[str, Any] = field(default_factory=dict, repr=False)  # the profile before any area

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

    def area_names(self) -> list[str]:
        """Every configured area, the default (sales) first."""
        return [DEFAULT_AREA, *sorted(a for a in self.areas if a != DEFAULT_AREA)]

    def for_area(self, name: str | None = None) -> Config:
        """The same profile, with the folders, catalog and file rules of business area `name`.

        Always starts from the profile settings, so for_area("inventory").for_area("sales") is the
        plain sales config again. None or "sales" gives the default area.
        """
        name = name or DEFAULT_AREA
        if name not in self.area_names():
            raise ValueError(f"Unknown area '{name}'. Available: {self.area_names()}")
        spec = self.areas.get(name) or {}
        overrides = spec.get(self.profile) or {}
        if name == DEFAULT_AREA and overrides:
            raise ValueError("The default area (sales) uses the profile settings; change them under profiles:")
        unknown = sorted(set(overrides) - set(AREA_SETTINGS))
        if unknown:
            raise ValueError(f"Area '{name}', profile '{self.profile}': unknown setting(s) {unknown}")
        targets = spec.get("file_targets") or (SALES_FILE_TARGETS if name == DEFAULT_AREA else {})
        settings = {**self.profile_settings, **overrides}
        if name != DEFAULT_AREA:
            # Safety: another area must never write into the sales tables or read the sales landing folder.
            base = self.profile_settings
            if (settings["catalog"], settings["lake_path"]) == (base["catalog"], base["lake_path"]):
                raise ValueError(f"Area '{name}', profile '{self.profile}': set its own catalog or lake_path")
            if settings["landing_path"] == base["landing_path"] or settings["staging_path"] == base["staging_path"]:
                raise ValueError(f"Area '{name}', profile '{self.profile}': set its own staging_path and landing_path")
            if not targets:
                raise ValueError(f"Area '{name}': file_targets is missing")
        return replace(self, area=name, file_targets=dict(targets), **{k: settings[k] for k in AREA_SETTINGS})


def load_config(
    profile: str | None = None, config_file: Path | str = DEFAULT_CONFIG, area: str | None = None
) -> Config:
    profile = profile or os.environ.get("SALES_PROFILE", "local")
    with open(config_file, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    profiles = raw.get("profiles", {})
    if profile not in profiles:
        raise ValueError(f"Unknown profile '{profile}'. Available: {sorted(profiles)}")

    p = profiles[profile]
    settings = {
        "staging_path": p["staging_path"],
        "landing_path": p["landing_path"],
        "lake_path": p.get("lake_path"),
        "catalog": p.get("catalog"),
        "schemas": p["schemas"],
        "processed_files_log": p.get("processed_files_log"),
        "output_path": p.get("output_path", "output"),
    }
    cfg = Config(
        profile=profile,
        business=raw.get("business", {}),
        areas=raw.get("areas") or {},
        profile_settings=settings,
        **settings,
    )
    return cfg.for_area(area)
