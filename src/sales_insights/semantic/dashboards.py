"""AI/BI dashboards as code: dashboards/<name>.yaml -> dashboards/<name>.lvdash.json -> Asset Bundle.

    uv run python -m sales_insights.semantic.dashboards              # rewrite dashboards/*.lvdash.json (dev names)
    databricks bundle deploy -t dev                                  # creates / updates the 3 dashboards

Three dashboards, one per audience (docs/access.md):
    sales.yaml   Sales (3 pages)          sales_team, management
    stock.yaml   Stock (1 page)           stock_team, management
    ceo.yaml     Company overview (1 page) management

Why YAML -> JSON instead of clicking the dashboards together: the dashboard file format (.lvdash.json) is long
and repetitive, but a dashboard is only datasets + widgets on a grid. The YAML keeps the parts a person reviews
(the SQL, which chart shows which column); this module writes the JSON the workspace imports, with the real
table names from config/config.yaml. The bundle deploys it like the jobs, so a deleted dashboard comes back
with one command and every change is reviewed in git.

Two kinds of dataset:
  metric_view  fields + measures (+ where) asked from a Unity Catalog metric view with MEASURE(): every KPI
               keeps its one definition (sql/metric_views/*.yaml), shared with Genie and the alerts.
               `select` / `group_by` / `order_by` / `limit` reshape the answer; `then` wraps it once more
               ({previous} = the query so far). On Databricks the dataset runs against the published view;
               locally `local_sql` compiles the same request into plain Spark SQL (tests run every dataset).
  sql          plain SQL for what is not a KPI: insights, forecast, data checks.
`same_as: <dashboard>.<dataset>` reuses another dashboard's dataset.

Format reference: Databricks bundle examples (knowledge_base/dashboard_nyc_taxi) and the dashboards it
exports; https://docs.databricks.com/aws/en/dev-tools/bundles/resources#dashboards
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from string import Template

import yaml

from sales_insights.common.config import REPO_ROOT, Config
from sales_insights.semantic import metric_views as mvs

DASHBOARDS = REPO_ROOT / "dashboards"
NAMES = ["sales", "stock", "ceo"]
GRID_WIDTH = 6
METRIC_VIEWS = {"sales_metrics": "sales", "inventory_metrics": "inventory", "sales_and_stock": "inventory"}
# Non-gold-model tables the datasets read: placeholder -> (area, layer, table)
EXTRA_TABLES = {
    "sales_insights": ("sales", "gold", "insights"),
    "sales_forecast": ("sales", "gold", "forecast"),
    "sales_reconciliation": ("sales", "ops", "reconciliation"),
    "sales_dq_results": ("sales", "ops", "dq_results"),
    "sales_manifest_dirt": ("sales", "ops", "manifest_dirt"),
    "stock_insights": ("inventory", "gold", "insights"),
    "stock_reconciliation": ("inventory", "ops", "reconciliation"),
    "stock_rollforward": ("inventory", "ops", "stock_rollforward"),
    "stock_cross_checks": ("inventory", "ops", "cross_checks"),
}

NameFn = Callable[[str, str, str], str]  # (area, layer, table) -> SQL name


def table_names(name: NameFn) -> dict[str, str]:
    """Placeholder -> SQL name for every table and metric view a dashboard may read."""
    names = {t: name("sales", "gold", t) for t in mvs.GOLD_TABLES}
    names |= {t: name("inventory", "gold", t) for t in mvs.AREA_GOLD_TABLES["inventory"]}
    names |= {mv: name(area, "gold", mv) for mv, area in METRIC_VIEWS.items()}
    names |= {key: name(*where) for key, where in EXTRA_TABLES.items()}
    return names


def config_names(cfg: Config) -> dict[str, str]:
    """Table names of a config profile (dev: sales_dev.gold.fact_sales, inventory_dev.gold.insights, ...)."""
    return table_names(lambda area, layer, t: cfg.for_area(area).table(layer, t))


def lake_names(sales_lake, stock_lake) -> dict[str, str]:
    """Local table names (delta.`/path`), for running the datasets in tests."""
    lakes = {"sales": sales_lake, "inventory": stock_lake}
    return table_names(lambda area, layer, t: lakes[area].sql_name(layer, t))


# ---------------------------------------------------------------------------
# Specs and datasets
# ---------------------------------------------------------------------------


def load(name: str, folder: Path = DASHBOARDS) -> dict:
    return yaml.safe_load((folder / f"{name}.yaml").read_text(encoding="utf-8"))


def datasets(spec: dict, folder: Path = DASHBOARDS) -> dict[str, dict]:
    """The spec's datasets with `same_as` references resolved."""
    out = {}
    for key, ds in spec["datasets"].items():
        if "same_as" in ds:
            other, ref = ds["same_as"].split(".")
            ds = load(other, folder)["datasets"][ref]
        out[key] = ds
    return out


def _shape(ds: dict, inner: str) -> str:
    """Apply select / group_by / order_by / limit / then around the metric view answer."""
    sql = inner
    if any(k in ds for k in ("select", "group_by", "order_by", "limit")):
        cols = ",\n  ".join(ds.get("select", ["*"]))
        parts = [f"SELECT\n  {cols}\nFROM (\n{_indent(inner)}\n) AS q"]
        if ds.get("group_by"):
            parts.append("GROUP BY " + ", ".join(ds["group_by"]))
        if ds.get("order_by"):
            parts.append(f"ORDER BY {ds['order_by']}")
        if ds.get("limit"):
            parts.append(f"LIMIT {int(ds['limit'])}")
        sql = "\n".join(parts)
    if ds.get("then"):
        sql = ds["then"].strip().replace("{previous}", f"(\n{_indent(sql)}\n) AS previous")
    return sql


def _indent(sql: str) -> str:
    return "\n".join("  " + line for line in sql.splitlines())


def _check_request(ds: dict, mv: mvs.MetricView) -> None:
    unknown = [f for f in ds.get("fields", []) if f not in mv.fields]
    unknown += [m for m in ds.get("measures", []) if m not in mv.measures]
    if unknown:
        raise KeyError(f"{mv.name} has no field or measure {unknown}")


def databricks_sql(ds: dict, tables: dict[str, str]) -> str:
    """The dataset query as Databricks runs it (metric views queried with MEASURE())."""
    if "sql" in ds:
        return Template(ds["sql"]).substitute(tables).strip()
    mv = mvs.load(ds["metric_view"])
    _check_request(ds, mv)
    fields, measures = ds.get("fields", []), ds["measures"]
    cols = [f"`{f}`" for f in fields] + [f"MEASURE(`{m}`) AS `{m}`" for m in measures]
    inner = [f"SELECT {', '.join(cols)}", f"FROM {tables[ds['metric_view']]}"]
    if ds.get("where"):
        inner.append(f"WHERE {ds['where']}")
    if fields:
        inner.append("GROUP BY ALL")
    return _shape(ds, "\n".join(inner))


def local_sql(ds: dict, tables: dict[str, str]) -> str:
    """The same dataset as plain Spark SQL: the metric view request compiled by metric_views.compile_query."""
    if "sql" in ds:
        return Template(ds["sql"]).substitute(tables).strip()
    mv = mvs.load(ds["metric_view"])
    _check_request(ds, mv)
    return _shape(ds, mvs.compile_query(mv, tables, ds.get("fields", []), ds["measures"], ds.get("where")))


# ---------------------------------------------------------------------------
# .lvdash.json
# ---------------------------------------------------------------------------


def _id(*parts: str) -> str:
    """Stable 8-character id (the same file every time, so git shows only real changes)."""
    return hashlib.sha1("/".join(parts).encode()).hexdigest()[:8]


@dataclass
class _Grid:
    x: int = 0
    y: int = 0
    row_height: int = 0

    def place(self, width: int, height: int) -> dict:
        if self.x + width > GRID_WIDTH:
            self.x, self.y, self.row_height = 0, self.y + self.row_height, 0
        pos = {"x": self.x, "y": self.y, "width": width, "height": height}
        self.x += width
        self.row_height = max(self.row_height, height)
        return pos


def _field(name: str, agg: str | None = None, temporal: str | None = None) -> dict:
    if temporal:
        return {"name": f"{temporal}ly({name})" if temporal != "day" else f"daily({name})",
                "expression": f'DATE_TRUNC("{temporal.upper()}", `{name}`)'}  # fmt: skip
    if agg:
        return {"name": f"{agg.lower()}({name})", "expression": f"{agg.upper()}(`{name}`)"}
    return {"name": name, "expression": f"`{name}`"}


def _query(dataset_id: str, fields: list[dict], disaggregated: bool, order_by: str | None = None) -> list[dict]:
    q = {"datasetName": dataset_id, "fields": fields, "disaggregated": disaggregated}
    if order_by:
        col, *direction = order_by.split()
        q["orders"] = [{"direction": (direction or ["ASC"])[0].upper(), "expression": f"`{col}`"}]
    return [{"name": "main_query", "query": q}]


def _frame(w: dict) -> dict:
    frame = {"showTitle": True, "title": w["title"]}
    if w.get("description"):
        frame |= {"showDescription": True, "description": w["description"]}
    return frame


def _column(c: dict, i: int) -> dict:
    number = c.get("type") == "number"
    col = {
        "fieldName": c["field"], "booleanValues": ["false", "true"], "imageUrlTemplate": "{{ @ }}",
        "imageTitleTemplate": "{{ @ }}", "imageWidth": "", "imageHeight": "", "linkUrlTemplate": "{{ @ }}",
        "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": True,
        "type": "float" if number else "string", "displayAs": "number" if number else "string",
        "visible": True, "order": 100000 + i, "title": c.get("title", c["field"]), "displayName": c["field"],
        "allowSearch": False, "alignContent": "right" if number else "left", "allowHTML": False,
        "highlightLinks": False, "useMonospaceFont": False, "preserveWhitespace": False,
    }  # fmt: skip
    if number:
        col["numberFormat"] = c.get("format", "0,0.0")
    if c.get("wide"):
        col["defaultColumnWidth"] = 700
    return col


def _widget(kind: str, w: dict, ds_ids: dict[str, str], name: str) -> dict:
    ds = ds_ids[w["dataset"]]
    if kind == "counter":
        value = {"fieldName": f"max({w['value']})", "displayName": w["title"]}
        if w.get("format") == "compact":
            value["format"] = {"type": "number-plain", "abbreviation": "compact",
                               "decimalPlaces": {"type": "max", "places": 1}}  # fmt: skip
        spec = {"version": 2, "widgetType": "counter", "encodings": {"value": value}}
        queries = _query(ds, [_field(w["value"], "max")], False)
    elif kind == "table":
        fields = [_field(c["field"]) for c in w["columns"]]
        order = w.get("order_by")
        if order and order.split()[0] not in {c["field"] for c in w["columns"]}:
            fields.append(_field(order.split()[0]))  # sort column, not shown
        spec = {"version": 1, "widgetType": "table", "condensed": True, "allowHTMLByDefault": False,
                "encodings": {"columns": [_column(c, i) for i, c in enumerate(w["columns"])]}}  # fmt: skip
        queries = _query(ds, fields, True, order)
    elif kind in ("bar", "line"):
        x, y = w["x"], w["y"]
        xf, yf = _field(x["field"], temporal=x.get("temporal")), _field(y["field"], "sum")
        fields = [xf, yf]
        x_scale = {"type": "temporal"} if x.get("temporal") else {"type": "categorical"}
        if kind == "bar" and not x.get("temporal") and not w.get("color"):
            x_scale["sort"] = {"by": "y-reversed"}
        enc = {
            "x": {"fieldName": xf["name"], "scale": x_scale, "displayName": x.get("title", x["field"])},
            "y": {"fieldName": yf["name"], "scale": {"type": "quantitative"}, "displayName": y.get("title", y["field"])},
        }  # fmt: skip
        if w.get("color"):
            cf = _field(w["color"]["field"])
            fields.append(cf)
            enc["color"] = {"fieldName": cf["name"], "scale": {"type": "categorical"},
                            "displayName": w["color"].get("title", w["color"]["field"])}  # fmt: skip
        spec = {"version": 3, "widgetType": kind, "encodings": enc}
        queries = _query(ds, fields, False)
    elif kind == "combo":
        x = w["x"]
        xf = _field(x["field"])
        bars = [_field(b["field"], "sum") for b in w["bars"]]
        line = [_field(b["field"], "sum") for b in w["line"]]
        enc = {
            "x": {"fieldName": xf["name"], "scale": {"type": "categorical"}, "displayName": x.get("title", x["field"])},
            "label": {"show": False},
            "y": {
                "primary": {"fields": [{"fieldName": f["name"], "displayName": b.get("title", b["field"])}
                                       for f, b in zip(bars, w["bars"], strict=True)], "scale": {"type": "quantitative"}},
                "secondary": {"fields": [{"fieldName": f["name"], "displayName": b.get("title", b["field"])}
                                         for f, b in zip(line, w["line"], strict=True)], "scale": {"type": "quantitative"}},
            },
        }  # fmt: skip
        spec = {"version": 1, "widgetType": "combo", "encodings": enc}
        queries = _query(ds, [xf, *bars, *line], False)
    else:
        raise ValueError(f"unknown widget type {kind!r}")
    spec["frame"] = _frame(w)
    return {"name": name, "queries": queries, "spec": spec}


WIDGET_TYPES = ("text", "counter", "table", "bar", "line", "combo")


def widget_kind(w: dict) -> str:
    (kind,) = [k for k in WIDGET_TYPES if k in w]
    return kind


def lvdash(name: str, tables: dict[str, str], folder: Path = DASHBOARDS) -> dict:
    """The dashboard file the workspace imports."""
    spec = load(name, folder)
    data = datasets(spec, folder)
    ds_ids = {key: _id(name, "dataset", key) for key in data}
    out = {
        "datasets": [
            {
                "name": ds_ids[key],
                "displayName": key,
                "queryLines": databricks_sql(ds, tables).splitlines(keepends=True),
            }
            for key, ds in data.items()
        ],
        "pages": [],
    }
    for p, page in enumerate(spec["pages"]):
        grid, layout = _Grid(), []
        for i, w in enumerate(page["widgets"]):
            kind, (width, height) = widget_kind(w), w["size"]
            wid = _id(name, str(p), str(i))
            if kind == "text":
                widget = {"name": wid, "multilineTextboxSpec": {"lines": w["text"].rstrip("\n").splitlines(True)}}
            else:
                widget = _widget(kind, w[kind], ds_ids, wid)
            layout.append({"widget": widget, "position": grid.place(width, height)})
        out["pages"].append({"name": _id(name, "page", str(p)), "displayName": page["title"], "layout": layout,
                             "pageType": "PAGE_TYPE_CANVAS"})  # fmt: skip
    return out


def render(name: str, tables: dict[str, str], folder: Path = DASHBOARDS) -> str:
    return json.dumps(lvdash(name, tables, folder), indent=2, ensure_ascii=False) + "\n"


def write(cfg: Config, folder: Path = DASHBOARDS) -> list[Path]:
    tables, paths = config_names(cfg), []
    for name in NAMES:
        path = folder / f"{name}.lvdash.json"
        path.write_text(render(name, tables, folder), encoding="utf-8")
        paths.append(path)
    return paths


def main(argv: list[str] | None = None) -> None:
    from sales_insights.common.config import load_config

    parser = argparse.ArgumentParser(description="Write dashboards/*.lvdash.json from dashboards/*.yaml.")
    parser.add_argument("--profile", default="dev", help="config profile whose table names to use (default: dev)")
    args = parser.parse_args(argv)
    for path in write(load_config(args.profile)):
        print(f"  {path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
