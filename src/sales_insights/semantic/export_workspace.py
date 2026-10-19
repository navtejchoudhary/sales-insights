"""Export what was built by hand in the workspace (Genie agents) and the deployed dashboards, before the trial
workspace is deleted. Files go to output/export/ (git-ignored), with workspace-specific details removed.

    uv run python -m sales_insights.semantic.export_workspace \
        --space sales=<agent id> --space inventory=<agent id> --space sales_and_stock=<agent id>

For each Genie agent (About tab > Agent ID): output/export/genie_<name>.json - title, description and the full
agent definition (data, instructions, example queries, benchmarks), as the Genie API exports it.
For each dashboard of this project: output/export/dashboard_<name>.lvdash.json - importable in any workspace
(Dashboards > Create > Import), and the same as dashboards/<name>.lvdash.json unless edited in the browser.

Removed from every file: IDs that only mean something in this workspace (agent, dashboard, warehouse IDs,
folders), the workspace URL and e-mail addresses. Nothing secret is read: the Databricks CLI uses the login
made with `databricks auth login`.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

from sales_insights.common.config import REPO_ROOT

DROP_KEYS = {"space_id", "warehouse_id", "dashboard_id", "parent_path", "path", "etag", "create_time",
             "update_time", "lifecycle_state"}  # fmt: skip
WORKSPACE_URL = re.compile(r"https://[\w.-]+\.(azuredatabricks\.net|cloud\.databricks\.com|databricks\.azure\.cn)\S*")
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
DASHBOARD_NAMES = {"Sales": "sales", "Stock": "stock", "Company overview": "ceo", "Filter test": "filter_test"}


def scrub(value):
    """A copy without workspace-specific keys, workspace URLs or e-mail addresses (strings, lists, dicts)."""
    if isinstance(value, dict):
        return {k: scrub(v) for k, v in value.items() if k not in DROP_KEYS}
    if isinstance(value, list):
        return [scrub(v) for v in value]
    if isinstance(value, str):
        return EMAIL.sub("<email>", WORKSPACE_URL.sub("<workspace-url>", value))
    return value


def _cli(*args: str, profile: str) -> dict | list:
    cmd = ["databricks", *args, "--profile", profile, "-o", "json"]
    out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    return json.loads(out or "{}")


def genie_export(raw: dict) -> dict:
    """get-space output -> {title, description, space}: the serialized agent definition as JSON."""
    return scrub({
        "title": raw.get("title"),
        "description": raw.get("description"),
        "space": json.loads(raw.get("serialized_space") or "{}"),
    })  # fmt: skip


def dashboard_name(display_name: str) -> str | None:
    """Our file name for a deployed dashboard ("[dev] Sales" -> "sales"); None for other dashboards."""
    return DASHBOARD_NAMES.get(re.sub(r"^\[\w+\]\s*", "", display_name or ""))


def export(spaces: dict[str, str], profile: str, out: Path) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for name, space_id in spaces.items():
        raw = _cli("genie", "get-space", space_id, "--include-serialized-space", profile=profile)
        path = out / f"genie_{name}.json"
        path.write_text(json.dumps(genie_export(raw), indent=1, ensure_ascii=False), encoding="utf-8")
        written.append(path)
    listed = _cli("lakeview", "list", profile=profile)
    for d in listed if isinstance(listed, list) else listed.get("dashboards", []):
        name = dashboard_name(d.get("display_name", ""))
        if not name:
            continue
        full = _cli("lakeview", "get", d["dashboard_id"], profile=profile)
        body = scrub(json.loads(full.get("serialized_dashboard") or "{}"))
        path = out / f"dashboard_{name}.lvdash.json"
        path.write_text(json.dumps(body, indent=2, ensure_ascii=False), encoding="utf-8")
        written.append(path)
    return written


def _space(text: str) -> tuple[str, str]:
    name, _, space_id = text.partition("=")
    if not name or not space_id:
        raise argparse.ArgumentTypeError("use name=<agent id>, e.g. sales=01f1...")
    return name, space_id


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Export the Genie agents and dashboards to output/export/.")
    parser.add_argument("--space", type=_space, action="append", default=[], help="name=<Genie agent id> (repeat)")
    parser.add_argument("--profile", default="sales-dev", help="Databricks CLI profile (default: sales-dev)")
    parser.add_argument("--out", default=str(REPO_ROOT / "output" / "export"))
    args = parser.parse_args(argv)
    for path in export(dict(args.space), args.profile, Path(args.out)):
        print(f"  {path}")


if __name__ == "__main__":
    main()
