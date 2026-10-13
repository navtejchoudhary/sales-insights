"""Turn manifest JSON into rows (plain Python, no Spark).

Two outputs, used to PROVE the pipeline is right rather than hope it is:
- control totals per (manifest, file kind, invoice_date): the net change each delivery makes; summed per
  invoice date over all deliveries they are what gold must add up to (pipeline/reconcile.py)
- expected dirt per (manifest, file, dirt type, key): what silver must have caught
"""

from __future__ import annotations

import json

TOTALS_COLUMNS = [
    "manifest_path",
    "kind",
    "business_date",
    "file_kind",
    "invoice_date",
    "lines",
    "invoices",
    "revenue",
    "tax_amount",
]
DIRT_COLUMNS = ["manifest_path", "kind", "business_date", "file", "dirt", "key"]


def parse_manifest(manifest_path: str, content: str) -> tuple[list[tuple], list[tuple]]:
    m = json.loads(content)
    kind = m.get("kind")
    business_date = m.get("business_date")
    totals: list[tuple] = []

    def add(file_kind: str, block: dict) -> None:
        for inv_date, t in sorted(block.get("by_invoice_date", {}).items()):
            totals.append(
                (
                    manifest_path,
                    kind,
                    business_date,
                    file_kind,
                    inv_date,
                    int(t["lines"]),
                    int(t["invoices"]),
                    float(t["revenue"]),
                    float(t["tax_amount"]),
                )
            )

    ct = m.get("control_totals", {})
    if kind == "history":
        add("history", ct)
    elif kind == "daily":
        add("orders", ct.get("orders", {}))
        add("changes", ct.get("changes", {}))

    dirt: list[tuple] = []
    for d in m.get("dirt", []):
        file = d.get("file") or d.get("table")
        keys = d.get("keys") or d.get("customer_ids") or d.get("product_ids") or []
        dirt += [(manifest_path, kind, business_date, file, d["dirt"], str(k)) for k in keys]
    return totals, dirt


# ---------------------------------------------------------------------------
# Inventory area (generator/inventory.py manifests)
# ---------------------------------------------------------------------------

INVENTORY_TOTALS_COLUMNS = [
    "manifest_path",
    "kind",
    "business_date",
    "file_kind",  # movements | stock
    "data_date",  # posting_date (movements) or snapshot_date (stock)
    "lines",
    "quantity",  # movements: signed quantity; stock: unrestricted quantity
    "in_transit_quantity",  # stock only (0 for movements)
]


def parse_inventory_manifest(manifest_path: str, content: str) -> tuple[list[tuple], list[tuple]]:
    """Control totals per (manifest, file kind, data date) and expected dirt, from an inventory manifest.

    Like the sales totals they are NET of planted dirt (no invalid rows, no duplicate copies), and a late
    movement is counted in the delivery it arrives in, under its own posting date.
    """
    m = json.loads(content)
    kind, business_date = m.get("kind"), m.get("business_date")
    ct = m.get("control_totals", {})
    totals: list[tuple] = []
    for date_, t in sorted(ct.get("movements", {}).get("by_posting_date", {}).items()):
        totals.append(
            (manifest_path, kind, business_date, "movements", date_, int(t["lines"]), float(t["quantity"]), 0.0)
        )
    for date_, t in sorted(ct.get("stock", {}).get("by_snapshot_date", {}).items()):
        totals.append(
            (
                manifest_path,
                kind,
                business_date,
                "stock",
                date_,
                int(t["rows"]),
                float(t["unrestricted_qty"]),
                float(t["in_transit_qty"]),
            )
        )
    _, dirt = parse_manifest(manifest_path, content)
    return totals, dirt
