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

Global filters (`filters:` in the YAML) are PARAMETER filters: the value goes INTO each query, so KPI totals
recalculate too (a field filter only filters a query's finished rows). Every metric_view dataset gets
(:province = 'All' OR <field> = :province) added to its `where` for each filter whose `fields` names that
metric view, unless the dataset says `company_wide: true` (forecast, findings, data checks). A `sql` dataset
takes part by writing :province itself. Each filter lists its choices from a `values` dataset whose first
row is `All`. https://learn.microsoft.com/azure/databricks/dashboards/manage/filters/

Format reference: Databricks bundle examples (knowledge_base/dashboard_nyc_taxi) and the dashboards it
exports; https://docs.databricks.com/aws/en/dev-tools/bundles/resources#dashboards
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
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
    "stock_manifest_dirt": ("inventory", "ops", "manifest_dirt"),
    "sales_genie_accuracy": ("sales", "ops", "genie_accuracy"),
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
    """The spec's datasets with `same_as` references resolved and this spec's global filters added."""
    out = {}
    for key, ds in spec["datasets"].items():
        if "same_as" in ds:
            other, ref = ds["same_as"].split(".")
            ds = {**load(other, folder)["datasets"][ref], **{k: v for k, v in ds.items() if k != "same_as"}}
        out[key] = with_filters(ds, spec.get("filters", []))
    return out


def with_filters(ds: dict, filters: list[dict]) -> dict:
    """A metric_view dataset with one condition per global filter that covers its metric view, so the filter
    value goes into the query (the default 'All' keeps every row)."""
    if "metric_view" not in ds or ds.get("company_wide"):
        return ds
    conditions = [f"(:{f['param']} = 'All' OR {f['fields'][ds['metric_view']]} = :{f['param']})"
                  for f in filters if ds["metric_view"] in f.get("fields", {})]  # fmt: skip
    if not conditions:
        return ds
    where = " AND ".join(([f"({ds['where']})"] if ds.get("where") else []) + conditions)
    return {**ds, "where": where}


def _shape(ds: dict, inner: str, tables: dict[str, str]) -> str:
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
        then = Template(ds["then"]).substitute(tables).strip()  # may read other tables, e.g. ${sales_forecast}
        sql = then.replace("{previous}", f"(\n{_indent(sql)}\n) AS previous")
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
    return _shape(ds, "\n".join(inner), tables)


def filter_defaults(spec: dict) -> dict[str, str]:
    """Parameter -> default value of the spec's global filters ({"province": "All", ...})."""
    return {f["param"]: f.get("default", "All") for f in spec.get("filters", [])}


def parameters(sql: str, spec: dict) -> list[str]:
    """The spec's filter parameters a query uses (:province -> province), in the spec's order."""
    return [p for p in filter_defaults(spec) if re.search(rf":{p}\b", sql)]


def bind(text: str, values: dict[str, str]) -> str:
    """Parameters replaced by quoted values, as the dashboard does at run time (locally: for the tests)."""
    for p, v in values.items():
        text = re.sub(rf":{p}\b", "'" + str(v).replace("'", "''") + "'", text)
    return text


def local_sql(ds: dict, tables: dict[str, str], params: dict[str, str] | None = None) -> str:
    """The same dataset as plain Spark SQL: the metric view request compiled by metric_views.compile_query.
    `params` gives the filter values (parameter -> value); every :parameter must have one."""
    params = params or {}
    if "sql" in ds:
        return bind(Template(ds["sql"]).substitute(tables).strip(), params)
    mv = mvs.load(ds["metric_view"])
    _check_request(ds, mv)
    where = bind(ds["where"], params) if ds.get("where") else None
    compiled = mvs.compile_query(mv, tables, ds.get("fields", []), ds["measures"], where)
    return bind(_shape(ds, compiled, tables), params)


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


# Colours with a fixed meaning (docs/access.md "Colours"): status green / amber / red (+ rose for "serious"),
# facts blue, forecast light blue, ranges grey. Charts and counters use the dashboard theme's palette by
# position (Databricks default theme), tables and text take the same colours as hex.
PALETTE = {  # name: (theme position, hex)
    "blue": (1, "#077A9D"), "amber": (2, "#FFAB00"), "green": (3, "#00A972"), "red": (4, "#FF3621"),
    "lightblue": (5, "#8BCAE7"), "rose": (6, "#AB4057"), "grey": (9, "#919191"),
}  # fmt: skip
OPERATORS = ("<", ">")  # the comparisons Databricks' exported dashboards use; rules apply first match first


def _theme(color: str) -> dict:
    return {"themeColorType": "visualizationColors", "position": PALETTE[color][0]}


def _rules(rules: list[list]) -> list[tuple[str, str, str]]:
    """[["<", -5, "red"], ...] -> checked (operator, literal, colour) triples."""
    out = []
    for op, value, color in rules:
        if op not in OPERATORS or color not in PALETTE:
            raise ValueError(
                f"colour rule {op} {value} {color}: operator must be < or >, colour one of {list(PALETTE)}"
            )
        out.append((op, str(value), color))
    return out


def _query(dataset_id: str, fields: list[dict], disaggregated: bool) -> list[dict]:
    return [
        {"name": "main_query", "query": {"datasetName": dataset_id, "fields": fields, "disaggregated": disaggregated}}
    ]


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
    if c.get("color_by"):  # colour this column's text from a numeric column of the same row (first match wins)
        by = c["color_by"]
        col["cellFormat"] = {
            "default": {"foregroundColor": PALETTE[by.get("default", "grey")][1]},
            "rules": [{"if": {"column": by["column"], "fn": op, "literal": lit}, "value": {"foregroundColor": PALETTE[color][1]}}
                      for op, lit, color in _rules(by["rules"])],
        }  # fmt: skip
    return col


def _widget(kind: str, w: dict, ds_ids: dict[str, str], name: str) -> dict:
    ds = ds_ids[w["dataset"]]
    if kind == "counter":
        value = {"fieldName": f"max({w['value']})", "displayName": w["title"]}
        if w.get("format") == "compact":
            value["format"] = {"type": "number-plain", "abbreviation": "compact",
                               "decimalPlaces": {"type": "max", "places": 1}}  # fmt: skip
        if w.get("status"):  # traffic light on the number, most severe rule first (first match wins)
            value["style"] = {"rules": [
                {"condition": {"operand": {"type": "data-value", "value": lit}, "operator": op}, "color": _theme(color)}
                for op, lit, color in _rules(w["status"])]}  # fmt: skip
        spec = {"version": 2, "widgetType": "counter", "encodings": {"value": value}}
        queries = _query(ds, [_field(w["value"], "max")], False)
    elif kind == "table":
        # Same settings as the tables in Databricks' own exported dashboards; rows keep the dataset's ORDER BY
        spec = {"version": 1, "widgetType": "table", "invisibleColumns": [], "allowHTMLByDefault": False,
                "itemsPerPage": 25, "paginationSize": "default", "condensed": True, "withRowNumber": False,
                "encodings": {"columns": [_column(c, i) for i, c in enumerate(w["columns"])]}}  # fmt: skip
        queries = _query(ds, [_field(c["field"]) for c in w["columns"]], True)
    elif kind in ("bar", "line"):
        x, y = w["x"], w["y"]
        xf, yf = _field(x["field"], temporal=x.get("temporal")), _field(y["field"], "sum")
        fields = [xf, yf]
        x_scale = {"type": "temporal"} if x.get("temporal") else {"type": "categorical"}
        enc = {
            "x": {"fieldName": xf["name"], "scale": x_scale, "displayName": x.get("title", x["field"])},
            "y": {"fieldName": yf["name"], "scale": {"type": "quantitative"}, "displayName": y.get("title", y["field"])},
        }  # fmt: skip
        if w.get("color"):
            cf = _field(w["color"]["field"])
            fields.append(cf)
            scale = {"type": "categorical"}
            if w["color"].get("colors"):  # fixed colour per value, e.g. Growing -> green
                scale["mappings"] = [{"value": v, "color": _theme(c)} for v, c in w["color"]["colors"].items()]
            enc["color"] = {"fieldName": cf["name"], "scale": scale,
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
                # exactly the shape of the combo charts in Databricks' exported dashboards (fields: fieldName only)
                "primary": {"fields": [{"fieldName": f["name"]} for f in bars], "scale": {"type": "quantitative"}},
                "secondary": {"fields": [{"fieldName": f["name"]} for f in line], "scale": {"type": "quantitative"}},
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


def _parameter(f: dict) -> dict:
    """A dataset parameter set by a single-select filter, as in Databricks' exported dashboards."""
    default = {"dataType": "STRING", "values": [{"value": f.get("default", "All")}]}
    return {"displayName": f["param"], "keyword": f["param"], "dataType": "STRING",
            "defaultSelection": {"values": default}}  # fmt: skip


def _filter_widget(name: str, f: dict, ds_ids: dict[str, str], users: list[str]) -> dict:
    """One global filter: its choices come from the `values` dataset, its value goes to every dataset that
    uses the parameter (same query and encoding shapes as Databricks' exported global filters)."""
    field, param = f.get("field", f["param"]), f["param"]
    choices = ds_ids[f["values"]]
    queries = [{"name": f"dashboards/{name}/datasets/{choices}_{field}",
                "query": {"datasetName": choices, "disaggregated": False, "fields": [
                    {"name": field, "expression": f"`{field}`"},
                    {"name": f"{field}_associativity", "expression": "COUNT_IF(`associative_filter_predicate_group`)"}]}}]  # fmt: skip
    queries += [{"name": f"parameter_dashboards/{name}/datasets/{ds_ids[key]}_{param}",
                 "query": {"datasetName": ds_ids[key], "parameters": [{"name": param, "keyword": param}],
                           "disaggregated": False}} for key in users]  # fmt: skip
    fields = [{"fieldName": field, "queryName": queries[0]["name"]}]
    fields += [{"parameterName": param, "queryName": q["name"]} for q in queries[1:]]
    frame = {"showTitle": True, "title": f["title"]}
    if f.get("description"):
        frame |= {"showDescription": True, "description": f["description"]}
    spec = {"version": 2, "widgetType": "filter-single-select", "encodings": {"fields": fields}, "frame": frame}
    return {"name": _id(name, "filter", param), "queries": queries, "spec": spec}


def lvdash(name: str, tables: dict[str, str], folder: Path = DASHBOARDS) -> dict:
    """The dashboard file the workspace imports."""
    spec = load(name, folder)
    data = datasets(spec, folder)
    ds_ids = {key: _id(name, "dataset", key) for key in data}
    filters = {f["param"]: f for f in spec.get("filters", [])}
    sqls = {key: databricks_sql(ds, tables) for key, ds in data.items()}
    uses = {key: parameters(sql, spec) for key, sql in sqls.items()}
    out = {"datasets": [], "pages": []}
    for key in data:
        dataset = {"name": ds_ids[key], "displayName": key, "queryLines": sqls[key].splitlines(keepends=True)}
        if uses[key]:
            dataset["parameters"] = [_parameter(filters[p]) for p in uses[key]]
        out["datasets"].append(dataset)
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
    if filters:
        layout = [{"widget": _filter_widget(name, f, ds_ids, [k for k in data if p in uses[k]]),
                   "position": {"x": 0, "y": 2 * i, "width": 1, "height": 2}}
                  for i, (p, f) in enumerate(filters.items())]  # fmt: skip
        out["pages"].append({"name": _id(name, "page", "filters"), "displayName": "Global filters",
                             "layout": layout, "pageType": "PAGE_TYPE_GLOBAL_FILTERS"})  # fmt: skip
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
