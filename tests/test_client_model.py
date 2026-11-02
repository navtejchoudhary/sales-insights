"""The client area's gold descriptions: true for the SAP data, while the sales demo keeps its own (no Spark)."""

from __future__ import annotations

from pathlib import Path

from sales_insights.pipeline import client_model as cm
from sales_insights.pipeline import gold_model as gm

ROOT = Path(__file__).resolve().parents[1]

# Phrases that describe only the dummy data: example codes, number ranges, names and daily feeds
DUMMY_ONLY = [
    "MC6001LT", "MC600", "2CHE", "WEEDICIDE", "Colombo", "COLOMBO", "C001", "e.g. 03", "e.g. 04", "e.g. 05",
    "Returns start with 7", "22 Oct 2026", "Enterprise Trading", "Global Consumer", "General Trade",
    "Export Sales", "Modern Trade", "supermarket", "18-month", "cases", "(CASE)",
]  # fmt: skip


def _texts(tables):
    for table, spec in tables.items():
        yield table, "(table)", spec["description"]
        for col, text in spec["columns"].items():
            yield table, col, text


def test_same_tables_and_columns_as_the_sales_model():
    assert list(cm.TABLES) == list(gm.TABLES)
    for table in gm.TABLES:
        assert list(cm.TABLES[table]["columns"]) == list(gm.TABLES[table]["columns"]), table


def test_no_description_describes_only_the_dummy_data():
    found = [(t, c, p) for t, c, text in _texts(cm.TABLES) for p in DUMMY_ONLY if p in text]
    assert not found


def test_every_column_is_described():
    assert all(text.strip() for _, _, text in _texts(cm.TABLES))


def test_the_client_area_gets_its_own_descriptions_and_the_sales_demo_keeps_its_own():
    assert gm.tables_for("client") is cm.TABLES
    assert gm.tables_for("sales") is gm.TABLES and gm.tables_for("inventory") is gm.TABLES
    # the demo's Genie agents were tuned on these: they must not change
    assert "MC6001LT" in gm.TABLES["fact_sales"]["columns"]["product_id"]
    assert gm.TABLES["fact_sales"]["columns"]["product_id"] != cm.TABLES["fact_sales"]["columns"]["product_id"]


def test_a_change_to_an_unknown_column_is_refused():
    import pytest

    with pytest.raises(ValueError, match="unknown columns"):
        cm._apply(gm.TABLES, {"fact_sales": {"columns": {"no_such_column": "x"}}})


def test_client_data_dictionary_doc_is_up_to_date():
    doc = (ROOT / "docs" / "client_gold_model.md").read_text()
    assert doc == cm.data_dictionary(), (
        "run: uv run python -m sales_insights.pipeline.client_model > docs/client_gold_model.md"
    )
