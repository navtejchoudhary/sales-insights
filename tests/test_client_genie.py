"""The "Sales (SAP data)" Genie agent (client area): configuration, benchmarks and privacy (no Spark).

Every ground-truth query also runs on the SAP-shaped client gold in test_sap_pipeline.py.
"""

from __future__ import annotations

import re
import subprocess
from datetime import date

import yaml

from sales_insights.common.config import REPO_ROOT, load_config
from sales_insights.pipeline import client_model as cm
from sales_insights.semantic import answer_key as ak
from sales_insights.semantic import genie_setup as gs
from sales_insights.semantic import metric_views as mvs

AGENT = ak.SPACES["client"]


def _space():
    return yaml.safe_load((AGENT.folder / "space.yaml").read_text())


def test_the_agent_reads_the_client_areas_own_gold():
    assert (AGENT.area, AGENT.own_gold, AGENT.private) == ("client", True, True)
    tables = gs.table_names(load_config("dev"), AGENT)
    assert tables["fact_sales"] == "client_dev.gold.fact_sales"
    assert tables["sales_metrics"] == "client_dev.gold.sales_metrics"
    assert gs.table_names(load_config("dev"))["fact_sales"] == "sales_dev.gold.fact_sales"  # sales unchanged


def test_real_answers_never_go_into_the_repo():
    """The answer key holds real figures: written to git-ignored output/, and its old path is ignored too."""
    assert AGENT.answer_key.parent == REPO_ROOT / "output"
    for path in (AGENT.answer_key, AGENT.folder / "answer_key.csv"):
        ignored = subprocess.run(["git", "check-ignore", "-q", str(path)], cwd=REPO_ROOT)
        assert ignored.returncode == 0, path


def test_benchmarks_are_complete_and_hold_no_real_values():
    qs = ak.load(AGENT.benchmarks)
    assert 12 <= len(qs) <= 20 and len({q.id for q in qs}) == len(qs)
    assert {q.category for q in qs} <= ak.CATEGORIES
    tables = {t: f"t_{t}" for t in mvs.GOLD_TABLES}
    for q in qs:
        assert "${" not in ak.render(q, tables), q.id
        assert len(q.paraphrases) == 2 and q.question not in q.paraphrases, q.id
        text = " ".join([q.question, *q.paraphrases, q.sql])
        assert not re.search(r"\d{6,}", text), q.id  # no customer or document numbers
        assert not re.search(r"'[A-Z]{2,}\d{3,}", text), q.id  # no material codes


def test_space_configuration_follows_genie_best_practice():
    space = _space()
    assert len(space["data"]) <= 6
    assert len(space["instructions"]) < 2800
    assert len(space["sample_questions"]) == 5
    ids = {q.id for q in ak.load(AGENT.benchmarks)}
    examples = {e["from_benchmark"] for e in space["example_queries"]}
    assert examples <= ids
    assert set(space["benchmarks_exclude"]) == examples  # never score Genie on questions it was taught
    for table, cols in space["hidden_columns"].items():
        assert set(cols) < set(cm.TABLES[table]["columns"]), table  # real columns, and never all of them
    assert "is_invoice" in space["instructions"] and "'ZAOR'" not in space["instructions"]


def test_setup_sheet_uses_the_client_tables(tmp_path):
    md_path, csv_path = gs.write(load_config("dev"), tmp_path, as_of=date(2026, 11, 2), agent=AGENT)
    md = md_path.read_text()
    assert md_path.name == "genie_setup_client.md" and csv_path.name == "genie_benchmarks_client.csv"
    assert "`client_dev.gold.sales_metrics`" in md and "sales_dev" not in md and "${" not in md
    qs, taught = ak.load(AGENT.benchmarks), set(_space()["benchmarks_exclude"])
    rows = csv_path.read_text().splitlines()
    assert len(rows) == 1 + 3 * (len(qs) - len(taught))  # every wording of every untaught question
