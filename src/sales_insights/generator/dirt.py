"""Data-quality dirt for the daily feed.

Each injector takes a DataFrame and a random generator, and returns the dirtied frame plus
one log entry per dirt type naming every affected line (invoice_number + invoice_item).
The daily manifest carries these logs, so silver tests can prove each problem is handled.

Rates follow docs/story_sheet.md.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from sales_insights.generator.masters import _city_variant

DUPLICATE_RATE = 0.015
CITY_VARIANT_RATE = 0.05
NULL_RATE = 0.02
INVALID_RATE = 0.004

# Non-key fields the source sometimes leaves empty (as in the reference extract)
NULLABLE = {
    "sales_office": [],  # the reference keeps sales_office_name even when the code is blank
    "plant": [],
    "storage_location": [],
    "customer_group": ["customer_group_name"],
    "product_group": ["product_group_name"],
}


def _keys(df: pd.DataFrame, idx) -> list[str]:
    return [f"{a}/{b}" for a, b in df.loc[idx, ["invoice_number", "invoice_item"]].itertuples(index=False)]


def _sample(rng: np.random.Generator, idx: pd.Index, rate: float) -> list:
    if len(idx) == 0:
        return []
    k = int(rng.binomial(len(idx), rate))
    return sorted(rng.choice(idx, size=k, replace=False).tolist()) if k else []


def invalid_values(df: pd.DataFrame, rng: np.random.Generator, rate: float = INVALID_RATE):
    """Rows silver must quarantine: unparseable dates, negative quantity on a normal invoice."""
    df = df.copy()
    rows = _sample(rng, df.index, rate)
    bad_date, neg_qty = [], []
    for i in rows:
        if df.at[i, "invoice_type"] == "ZAOR" and rng.random() < 0.5:
            df.at[i, "quantity"] = "-" + df.at[i, "quantity"]
            neg_qty.append(i)
        else:
            y, mth, day = df.at[i, "invoice_date"].split("-")
            df.at[i, "invoice_date"] = [f"{day}/{mth}/{y}", f"{y}-13-{day}", "0000-00-00"][int(rng.integers(3))]
            bad_date.append(i)
    log = [
        {
            "dirt": "invalid_date",
            "count": len(bad_date),
            "keys": _keys(df, bad_date),
            "expect": "quarantine: invoice_date is not a valid yyyy-mm-dd date",
        },
        {
            "dirt": "negative_quantity_on_invoice",
            "count": len(neg_qty),
            "keys": _keys(df, neg_qty),
            "expect": "quarantine: quantity < 0 on invoice type ZAOR",
        },
    ]
    return df, log, _keys(df, rows)


def null_fields(df: pd.DataFrame, rng: np.random.Generator, rate: float = NULL_RATE):
    df = df.copy()
    rows = _sample(rng, df.index, rate)
    by_col: dict[str, list] = {c: [] for c in NULLABLE}
    cols = list(NULLABLE)
    for i in rows:
        col = cols[int(rng.integers(len(cols)))]
        df.loc[i, [col, *NULLABLE[col]]] = ""
        by_col[col].append(i)
    return df, [
        {
            "dirt": f"blank_{col}",
            "count": len(ix),
            "keys": _keys(df, ix),
            "expect": f"fill {col} from the master data (customer or product)",
        }
        for col, ix in by_col.items()
        if ix
    ]


def city_variants(df: pd.DataFrame, rng: np.random.Generator, rate: float = CITY_VARIANT_RATE):
    df = df.copy()
    rows = _sample(rng, df.index, rate)
    for i in rows:
        df.at[i, "city"] = _city_variant(rng, df.at[i, "city"])
    return df, [
        {
            "dirt": "city_text_variant",
            "count": len(rows),
            "keys": _keys(df, rows),
            "expect": "map to the canonical city in cities.csv",
        }
    ]


def duplicates(df: pd.DataFrame, rng: np.random.Generator, rate: float = DUPLICATE_RATE):
    """Exact copies of some lines, inserted right after the original."""
    rows = _sample(rng, df.index, rate)
    if not rows:
        return df, [
            {"dirt": "duplicate_line", "count": 0, "keys": [], "expect": "dedupe on invoice_number + invoice_item"}
        ]
    copies = df.loc[rows]
    out = pd.concat([df, copies]).sort_index(kind="stable").reset_index(drop=True)
    return out, [
        {
            "dirt": "duplicate_line",
            "count": len(rows),
            "keys": _keys(df, rows),
            "expect": "dedupe on invoice_number + invoice_item",
        }
    ]
