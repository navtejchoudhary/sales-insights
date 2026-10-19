"""Dashboards, access and export (no Spark): the dashboard files are up to date and consistent, every
widget shows a column its dataset returns, the bundle shares each dashboard with the right groups, the grants
match the config, and the export removes workspace details. (Every dataset is RUN on real tables in
test_inventory_pipeline.py::test_every_dashboard_dataset_runs.)"""

import json
import re
from pathlib import Path

import pytest
import yaml

from sales_insights.common.config import load_config
from sales_insights.semantic import dashboards as db
from sales_insights.semantic import export_workspace as ex
from sales_insights.semantic import metric_views as mvs

ROOT = Path(__file__).resolve().parents[1]
GROUPS = {"sales_team", "stock_team", "management"}


def _tables():
    return db.config_names(load_config("dev"))


def _widgets(spec):
    for page in spec["pages"]:
        for w in page["widgets"]:
            yield db.widget_kind(w), w


def _shown_fields(kind, w):
    if kind == "counter":
        return [w["value"]]
    if kind == "table":
        return [c["field"] for c in w["columns"]] + ([w["order_by"].split()[0]] if w.get("order_by") else [])
    if kind in ("bar", "line"):
        return [w["x"]["field"], w["y"]["field"]] + ([w["color"]["field"]] if w.get("color") else [])
    if kind == "combo":
        return [w["x"]["field"]] + [b["field"] for b in w["bars"] + w["line"]]
    return []


@pytest.mark.parametrize("name", db.NAMES)
def test_dashboard_files_are_up_to_date(name):
    """dashboards/<name>.lvdash.json is what the YAML gives with the dev names: regenerate after editing."""
    expected = db.render(name, _tables())
    assert (ROOT / "dashboards" / f"{name}.lvdash.json").read_text(encoding="utf-8") == expected, (
        "run: uv run python -m sales_insights.semantic.dashboards"
    )


@pytest.mark.parametrize("name", db.NAMES)
def test_every_widget_shows_a_column_of_its_dataset(name):
    spec = db.load(name)
    data = db.datasets(spec)
    used = set()
    for kind, w in _widgets(spec):
        if kind == "text":
            continue
        body = w[kind]
        assert body["dataset"] in data, (name, body)
        used.add(body["dataset"])
        sql = db.databricks_sql(data[body["dataset"]], _tables())
        for field in _shown_fields(kind, body):
            assert re.search(rf"\b{re.escape(field)}\b", sql), (name, body["dataset"], field)
    used |= {f["values"] for f in spec.get("filters", [])}  # a global filter's list of choices
    assert used == set(data), f"{name}: datasets no widget uses: {set(data) - used}"


@pytest.mark.parametrize("name", db.NAMES)
def test_widgets_fit_the_grid_without_overlapping(name):
    board = json.loads((ROOT / "dashboards" / f"{name}.lvdash.json").read_text())
    for page in board["pages"]:
        cells = set()
        for item in page["layout"]:
            p = item["position"]
            assert p["x"] + p["width"] <= db.GRID_WIDTH
            mine = {(p["x"] + i, p["y"] + j) for i in range(p["width"]) for j in range(p["height"])}
            assert not cells & mine, item["widget"]["name"]
            cells |= mine
    names = [i["widget"]["name"] for page in board["pages"] for i in page["layout"]]
    assert len(names) == len(set(names))


def test_kpis_come_from_the_metric_views():
    """Every KPI on the dashboards is asked from a metric view (one definition); SQL only reads non-KPI tables."""
    kpi_words = re.compile(
        r"\bSUM\(\s*\w*\.?(margin_amount|cost_amount|stock_value_amount|stock_quantity)"
        r"|COUNT\(\s*DISTINCT\s+\w*\.?(invoice_number|customer_id)",
        re.IGNORECASE,
    )
    for name in db.NAMES:
        for key, ds in db.datasets(db.load(name)).items():
            if "sql" in ds:
                assert not kpi_words.search(ds["sql"]), (name, key)
            else:
                assert ds["metric_view"] in db.METRIC_VIEWS, (name, key)


def test_metric_view_datasets_use_measure_and_real_names():
    sql = db.databricks_sql(db.load("stock")["datasets"]["depot_cover_by_group"], _tables())
    assert "MEASURE(`days_of_cover`) AS `days_of_cover`" in sql
    assert "FROM inventory_dev.gold.inventory_metrics" in sql
    assert "WHERE location_type = 'Depot'" in sql and "GROUP BY ALL" in sql
    assert "GROUP BY" not in db.databricks_sql(db.load("stock")["datasets"]["stock_today"], _tables())


def test_where_uses_field_names_and_is_compiled_locally():
    mv = mvs.load("inventory_metrics")
    assert mvs.field_condition(mv, "location_type = 'Depot' AND is_month_to_date") == (
        "(location_dim.location_type) = 'Depot' AND (date_dim.is_month_to_date)"
    )
    assert mvs.field_condition(mv, "location = 'location_type'") == "(location_dim.location_name) = 'location_type'"
    tables = {t: f"t_{t}" for t in [*mvs.GOLD_TABLES, *mvs.AREA_GOLD_TABLES["inventory"]]}
    sql = mvs.compile_query(mv, tables, ["location"], ["stock_on_hand"], "location_type = 'Depot'")
    assert sql.count("WHERE ((location_dim.location_type) = 'Depot')") == 2  # outer query and the last-day subquery
    with pytest.raises(KeyError):
        db.databricks_sql({"metric_view": "inventory_metrics", "fields": ["nope"], "measures": ["stock_value"]}, tables)


def test_colours_keep_one_meaning():
    """Status colours only through checked rules; growth and stock-health values always get the same colour."""
    with pytest.raises(ValueError):
        db._rules([[">=", 0, "green"]])  # only the comparisons seen in Databricks' exported dashboards
    with pytest.raises(ValueError):
        db._rules([["<", 0, "purple"]])
    meaning = {"Growing": "green", "Flat": "amber", "Falling": "red", "New": "blue", "Stock-out": "red",
               "Low cover": "rose", "Healthy": "green", "Overstock": "amber", "Actual": "blue", "Forecast": "lightblue"}  # fmt: skip
    for name in db.NAMES:
        for kind, w in _widgets(db.load(name)):
            colors = (w[kind].get("color") or {}).get("colors", {}) if kind in ("bar", "line") else {}
            for value, color in colors.items():
                assert meaning.get(value, color) == color, (name, value, color)


def test_global_filters_set_a_parameter_on_every_dataset_that_uses_it():
    """Filter test: each global filter lists its choices (first 'All') and sets its parameter on every dataset
    whose SQL uses it, so the KPI tiles recalculate too; dashboards without filters get no filter pane."""
    spec, board = db.load("filter_test"), json.loads((ROOT / "dashboards" / "filter_test.lvdash.json").read_text())
    by_id = {d["name"]: d for d in board["datasets"]}
    (pane,) = [p for p in board["pages"] if p["pageType"] == "PAGE_TYPE_GLOBAL_FILTERS"]
    assert [i["widget"]["spec"]["frame"]["title"] for i in pane["layout"]] == ["Province", "Product group"]
    for item, f in zip(pane["layout"], spec["filters"], strict=True):
        widget = item["widget"]
        assert widget["spec"]["widgetType"] == "filter-single-select"
        choices, *targets = widget["queries"]
        assert by_id[choices["query"]["datasetName"]]["displayName"] == f["values"]
        set_on = {by_id[q["query"]["datasetName"]]["displayName"] for q in targets}
        uses = {d["displayName"] for d in board["datasets"] if f":{f['param']}" in "".join(d["queryLines"])}
        assert set_on == uses == {"headline", "by_month", "by_province", "by_product_group", "top_products"}
        for d in board["datasets"]:
            keywords = [p["keyword"] for p in d.get("parameters", [])]
            assert (f["param"] in keywords) == (d["displayName"] in uses), d["displayName"]
        assert [e.get("parameterName") for e in widget["spec"]["encodings"]["fields"][1:]] == [f["param"]] * len(uses)
    for name in ("sales", "stock", "ceo"):
        other = json.loads((ROOT / "dashboards" / f"{name}.lvdash.json").read_text())
        assert {p["pageType"] for p in other["pages"]} == {"PAGE_TYPE_CANVAS"}
        assert not any("parameters" in d for d in other["datasets"])


def test_local_sql_binds_the_filter_values():
    tables = {t: f"t_{t}" for t in [*mvs.GOLD_TABLES, *mvs.AREA_GOLD_TABLES["inventory"]]}
    tables |= {m: f"t_{m}" for m in db.METRIC_VIEWS}
    ds = db.load("filter_test")["datasets"]["by_province"]
    sql = db.local_sql(ds, tables, {"province": "Western", "product_group": "All"})
    assert ":province" not in sql and ":product_group" not in sql
    assert "('Western' = 'All' OR (customer_dim.province) = 'Western')" in sql
    assert db.bind("x = :p AND y = :pp", {"p": "O'Neil"}) == "x = 'O''Neil' AND y = :pp"
    assert db.filter_defaults(db.load("filter_test")) == {"province": "All", "product_group": "All"}


def test_no_workspace_details_in_the_dashboard_files():
    for path in (ROOT / "dashboards").glob("*.lvdash.json"):
        text = path.read_text()
        assert not ex.WORKSPACE_URL.search(text) and not ex.EMAIL.search(text), path.name


def _bundle_dashboards():
    return yaml.safe_load((ROOT / "bundle" / "resources" / "dashboards.yml").read_text())["resources"]["dashboards"]


def test_bundle_deploys_each_dashboard_to_its_audience():
    bundle = _bundle_dashboards()
    assert set(bundle) == {f"{n}_dashboard" for n in db.NAMES}
    for name in db.NAMES:
        d = bundle[f"{name}_dashboard"]
        assert (ROOT / "bundle" / "resources" / d["file_path"]).resolve() == ROOT / "dashboards" / f"{name}.lvdash.json"
        assert d["warehouse_id"] == "${var.warehouse_id}"
        assert d["embed_credentials"] is False  # viewers query with their own rights
        groups = {p["group_name"] for p in d["permissions"]}
        assert groups == set(db.load(name)["audience"]) and groups <= GROUPS, name
        assert d["display_name"] in ex.DASHBOARD_NAMES and ex.DASHBOARD_NAMES[d["display_name"]] == name


def test_grants_are_read_only_and_match_the_config():
    sql = (ROOT / "sql" / "setup" / "permissions.sql").read_text()
    grants = [line for line in sql.splitlines() if line.startswith("GRANT")]
    assert {re.search(r"TO `(\w+)`", g).group(1) for g in grants} == GROUPS
    privileges = {p.strip() for g in grants for p in g.removeprefix("GRANT ").split(" ON ")[0].split(",")}
    assert privileges == {"USE CATALOG", "USE SCHEMA", "SELECT"}  # read-only
    sales, stock = load_config("dev").catalog, load_config("dev", area="inventory").catalog
    for catalog in (sales, stock):
        assert f"CATALOG {catalog} TO `management`" in sql
        for layer in ("bronze", "silver"):
            assert f"{catalog}.{layer}" not in "\n".join(grants)  # nobody reads raw data
    assert f"ON SCHEMA {sales}.gold TO `sales_team`" in sql and f"{stock}." not in _lines_for(grants, "sales_team")
    assert f"ON SCHEMA {stock}.gold TO `stock_team`" in sql
    assert f"SELECT ON SCHEMA {sales}.gold TO `stock_team`" not in sql  # only the two shared masters
    assert f"SELECT ON TABLE {sales}.gold.dim_product TO `stock_team`" in sql


def _lines_for(grants, group):
    return "\n".join(g for g in grants if g.endswith(f"`{group}`;"))


def test_export_removes_workspace_details():
    raw = {
        "space_id": "01f1abc", "title": "Stock agent", "warehouse_id": "abc123",
        "serialized_space": json.dumps({"data_sources": {"tables": [{"identifier": "inventory_dev.gold.dim_location"}]},
                                        "note": "made by anil@example.com at https://adb-123.4.azuredatabricks.net/x"}),
    }  # fmt: skip
    out = ex.genie_export(raw)
    text = json.dumps(out)
    assert out["title"] == "Stock agent" and "inventory_dev.gold.dim_location" in text
    assert "01f1abc" not in text and "abc123" not in text
    assert "@" not in text and "azuredatabricks" not in text
    assert ex.dashboard_name("[dev] Sales") == "sales" and ex.dashboard_name("Company overview") == "ceo"
    assert ex.dashboard_name("Someone else's dashboard") is None
