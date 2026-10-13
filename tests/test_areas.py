"""Business areas (config.yaml `areas:`): sales stays exactly as before; other areas never share its locations.

Plain Python, no Spark.
"""

import json

import pytest
import yaml

from sales_insights.common import config as c
from sales_insights.common.config import load_config
from sales_insights.pipeline import landing as lz
from sales_insights.pipeline import run_pipeline as rp

PROFILES = ["local", "dev"]


def _raw():
    return yaml.safe_load(c.DEFAULT_CONFIG.read_text(encoding="utf-8"))


def _write(tmp_path, raw):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Sales (the default area) is unchanged
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("profile", PROFILES)
def test_default_area_is_sales_with_the_profile_settings_unchanged(profile):
    cfg = load_config(profile)
    p = _raw()["profiles"][profile]
    assert cfg.area == "sales"
    assert cfg.staging_path == p["staging_path"]
    assert cfg.landing_path == p["landing_path"]
    assert cfg.lake_path == p.get("lake_path")
    assert cfg.catalog == p.get("catalog")
    assert cfg.schemas == p["schemas"]
    assert cfg.processed_files_log == p.get("processed_files_log")
    assert cfg.output_path == p.get("output_path", "output")


@pytest.mark.parametrize("profile", PROFILES)
def test_asking_for_sales_gives_the_same_config(profile):
    cfg = load_config(profile)
    assert cfg.for_area("sales") == cfg
    assert cfg.for_area(None) == cfg
    assert load_config(profile, area="sales") == cfg


def test_sales_file_rules_are_the_ones_landing_always_used():
    assert load_config("local").file_targets == lz.TARGETS
    assert load_config("dev").file_targets == lz.TARGETS
    assert c.SALES_FILE_TARGETS == lz.TARGETS


def test_sales_table_names_unchanged():
    assert load_config("local").table("gold", "fact_sales") == "gold.fact_sales"
    assert load_config("dev").table("gold", "fact_sales") == "sales_dev.gold.fact_sales"


def test_config_file_without_areas_still_loads_as_sales(tmp_path):
    raw = _raw()
    raw.pop("areas")
    cfg = load_config("dev", _write(tmp_path, raw))
    assert cfg.area == "sales"
    assert cfg.catalog == "sales_dev"
    assert cfg.file_targets == lz.TARGETS
    with pytest.raises(ValueError, match="Unknown area"):
        cfg.for_area("inventory")


# ---------------------------------------------------------------------------
# Inventory: its own folders, catalog and file rules
# ---------------------------------------------------------------------------


def test_inventory_on_the_mac_uses_its_own_folders():
    inv = load_config("local").for_area("inventory")
    assert inv.area == "inventory"
    assert inv.staging_path == "staging/inventory"
    assert inv.landing_path == "landing/inventory"
    assert inv.lake_path == "lake/inventory"
    assert inv.processed_files_log == "lake/inventory/ops/_processed_files"
    assert inv.output_path == "output/inventory"
    assert inv.catalog is None
    assert inv.schemas == load_config("local").schemas  # same layer names in every area
    assert inv.file_targets == {"movements": "movements", "stock": "stock"}


def test_inventory_on_databricks_uses_its_own_catalog():
    inv = load_config("dev", area="inventory")
    assert inv.catalog == "inventory_dev"
    assert inv.table("gold", "fact_stock_daily") == "inventory_dev.gold.fact_stock_daily"
    assert inv.table("ops", "processed_files") == "inventory_dev.ops.processed_files"
    assert inv.landing_path == "/Volumes/inventory_dev/raw/landing"
    assert inv.staging_path == "/Volumes/inventory_dev/raw/staging"
    assert inv.business == load_config("dev").business  # fiscal year, timezone, seed: shared


@pytest.mark.parametrize("profile", PROFILES)
def test_every_other_area_stays_out_of_the_sales_locations(profile):
    sales = load_config(profile)
    for name in sales.area_names()[1:]:
        other = sales.for_area(name)
        assert (other.catalog, other.lake_path) != (sales.catalog, sales.lake_path)
        assert other.landing_path != sales.landing_path
        assert other.staging_path != sales.staging_path
        if sales.catalog:  # Databricks: different table names; locally the lake folder differs (above)
            assert other.table("gold", "x") != sales.table("gold", "x")


def test_switching_back_to_sales_restores_it_exactly():
    cfg = load_config("dev")
    assert cfg.for_area("inventory").for_area("sales") == cfg


def test_area_names_list_sales_first():
    assert load_config("local").area_names()[0] == "sales"
    assert "inventory" in load_config("local").area_names()


# ---------------------------------------------------------------------------
# Mistakes in config.yaml are refused, not guessed
# ---------------------------------------------------------------------------


def test_unknown_area_is_refused():
    with pytest.raises(ValueError, match="Unknown area 'hr'"):
        load_config("local").for_area("hr")


def test_unknown_area_setting_is_refused(tmp_path):
    raw = _raw()
    raw["areas"]["inventory"]["dev"]["landing_folder"] = "/Volumes/x"  # typo of landing_path
    with pytest.raises(ValueError, match="unknown setting"):
        load_config("dev", _write(tmp_path, raw), area="inventory")


def test_area_that_would_write_into_sales_tables_is_refused(tmp_path):
    raw = _raw()
    del raw["areas"]["inventory"]["dev"]["catalog"]  # would fall back to sales_dev
    with pytest.raises(ValueError, match="own catalog or lake_path"):
        load_config("dev", _write(tmp_path, raw), area="inventory")


def test_area_that_would_read_the_sales_landing_folder_is_refused(tmp_path):
    raw = _raw()
    del raw["areas"]["inventory"]["local"]["landing_path"]
    with pytest.raises(ValueError, match="own staging_path and landing_path"):
        load_config("local", _write(tmp_path, raw), area="inventory")


def test_sales_settings_cannot_be_overridden_under_areas(tmp_path):
    raw = _raw()
    raw["areas"]["sales"]["dev"] = {"catalog": "other"}
    with pytest.raises(ValueError, match="default area"):
        load_config("dev", _write(tmp_path, raw))


# ---------------------------------------------------------------------------
# Landing uses the area's file rules
# ---------------------------------------------------------------------------


def _inventory_folder(landing):
    folder = landing / "business_date=2026-10-13"
    folder.mkdir(parents=True)
    files = {}
    for name in ("movements_20261013.csv", "stock_20261013.csv"):
        (folder / name).write_text("material_id,quantity\nMC6001LT,5\n", encoding="utf-8")
        files[name] = {"rows": 1, "sha256": lz.sha256_of(folder / name)}
    (folder / "manifest_20261013.json").write_text(json.dumps({"files": files}), encoding="utf-8")


def test_landing_routes_inventory_files_with_the_inventory_rules(tmp_path):
    _inventory_folder(tmp_path)
    found = lz.discover(tmp_path, load_config("local", area="inventory").file_targets)
    assert sorted(f.target for f in found.files) == ["manifests", "movements", "stock"]


def test_sales_rules_refuse_inventory_files(tmp_path):
    _inventory_folder(tmp_path)
    with pytest.raises(ValueError, match="Unknown file type"):
        lz.discover(tmp_path, load_config("local").file_targets)
    with pytest.raises(ValueError, match="Unknown file type"):
        lz.discover(tmp_path)  # no rules given: the sales default, as before


def test_masters_always_go_to_masters_tables():
    assert lz.target_for("masters", "locations.csv", {"stock": "stock"}) == "masters_locations"


# ---------------------------------------------------------------------------
# run_pipeline --area
# ---------------------------------------------------------------------------


def test_pipeline_runs_sales_by_default():
    assert rp.AREA_STEPS["sales"] == rp.STEPS


@pytest.mark.parametrize("area", ["inventory", "hr"])
def test_pipeline_refuses_an_area_without_steps_before_starting_spark(area, capsys):
    with pytest.raises(SystemExit) as stopped:
        rp.main(["--date", "2026-10-13", "--area", area])
    assert stopped.value.code == 2
    assert "has no pipeline steps yet" in capsys.readouterr().err
