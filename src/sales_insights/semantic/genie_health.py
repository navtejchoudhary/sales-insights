"""Genie health check: run every benchmark question, record the accuracy, fail when it drops below target.

    run.py genie_health --profile dev --space-id <Agent ID> [--target 90]     (weekly Databricks job)

Why: the benchmark answers are SQL, re-run on every evaluation, so moving dates ("last month") and new data
are handled. What we cannot control is Genie itself (Databricks updates its model, wording drifts). This job
turns "remember to re-run the benchmarks" into an automatic check:
  1. starts a Chat-mode evaluation of every benchmark question (Genie API, same as "Run all benchmarks");
  2. waits for it, reads every assessment (GOOD / BAD / NEEDS_REVIEW);
  3. appends one row to ops.genie_accuracy (the score history);
  4. exits with an error below the target, so the job's failure email tells us within a week.
Runs inside a Databricks job, where the SDK signs in automatically (no token in code or config).
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

DONE_WORDS = {"DONE", "COMPLETED", "SUCCEEDED", "SUCCESS", "FINISHED"}
FAILED_WORDS = {"FAILED", "ERROR", "CANCELLED", "CANCELED"}


@dataclass(frozen=True)
class Score:
    good: int
    total: int
    failed: list[str] = field(default_factory=list)  # questions that were not GOOD

    @property
    def accuracy_pct(self) -> float:
        return round(100 * self.good / self.total, 1) if self.total else 0.0


def score(results: list[dict]) -> Score:
    """Accuracy like the Genie UI: GOOD answers / all answers (NEEDS_REVIEW counts as not good)."""
    good = [r for r in results if r.get("assessment") == "GOOD"]
    failed = sorted(r.get("question", "?") for r in results if r.get("assessment") != "GOOD")
    return Score(good=len(good), total=len(results), failed=failed)


def _statuses(obj: dict) -> set[str]:
    """Every value of a key that looks like a status, anywhere in the response (field names are beta)."""
    found: set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if "status" in k.lower() and isinstance(v, str):
                found.add(v.upper())
            elif isinstance(v, dict | list):
                found |= _statuses(v)
    elif isinstance(obj, list):
        for v in obj:
            found |= _statuses(v)
    return found


def finished(run: dict, results: list[dict], expected: int) -> bool:
    """The evaluation is over when the run says so, or when every expected question has a final status."""
    if _statuses(run) & (DONE_WORDS | FAILED_WORDS):
        return True
    states = [str(r.get("status", "")).upper() for r in results]
    return len(states) >= expected > 0 and all(s in DONE_WORDS | FAILED_WORDS for s in states)


def _d(obj) -> dict:
    return obj.as_dict() if hasattr(obj, "as_dict") else dict(obj or {})


class GenieEvals:
    """Thin wrapper over the Databricks SDK Genie API (kept apart so the logic is tested without Databricks)."""

    def __init__(self, genie, space_id: str):
        self.genie, self.space_id = genie, space_id

    def benchmark_count(self) -> int:
        space = _d(self.genie.get_space(self.space_id, include_serialized_space=True))
        serialized = json.loads(space.get("serialized_space") or "{}")
        return len(serialized.get("benchmarks", {}).get("questions", []))

    def start(self) -> str:
        run = _d(self.genie.genie_create_eval_run(self.space_id))
        return run.get("eval_run_id") or run["id"]

    def run(self, eval_run_id: str) -> dict:
        return _d(self.genie.genie_get_eval_run(self.space_id, eval_run_id))

    def results(self, eval_run_id: str) -> list[dict]:
        out, token = [], None
        while True:
            page = _d(self.genie.genie_list_eval_results(self.space_id, eval_run_id, page_token=token))
            out += page.get("eval_results", [])
            token = page.get("next_page_token")
            if not token:
                return out

    def details(self, eval_run_id: str, result: dict) -> dict:
        d = _d(self.genie.genie_get_eval_result_details(self.space_id, eval_run_id, result["result_id"]))
        return {"question": result.get("question"), "assessment": d.get("assessment")}


def evaluate(evals: GenieEvals, poll_seconds: int = 30, max_wait_seconds: int = 2700, sleep=time.sleep):
    """Start a run, wait for it, return (eval_run_id, Score)."""
    expected = evals.benchmark_count()
    eval_run_id = evals.start()
    waited = 0
    while True:
        results = evals.results(eval_run_id)
        if finished(evals.run(eval_run_id), results, expected):
            break
        if waited >= max_wait_seconds:
            raise TimeoutError(f"evaluation {eval_run_id} not finished after {waited} s")
        sleep(poll_seconds)
        waited += poll_seconds
    return eval_run_id, score([evals.details(eval_run_id, r) for r in results])


def record(spark, lake, eval_run_id: str, s: Score, target: float) -> None:
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("checked_ts", T.TimestampType()),
        T.StructField("eval_run_id", T.StringType()),
        T.StructField("good", T.IntegerType()),
        T.StructField("total", T.IntegerType()),
        T.StructField("accuracy_pct", T.DoubleType()),
        T.StructField("target_pct", T.DoubleType()),
        T.StructField("status", T.StringType()),
        T.StructField("failed_questions", T.StringType()),
    ])  # fmt: skip
    status = "PASS" if s.accuracy_pct >= target else "FAIL"
    row = (datetime.now(UTC), eval_run_id, s.good, s.total, s.accuracy_pct, float(target), status, " | ".join(s.failed))
    lake.append(spark.createDataFrame([row], schema), "ops", "genie_accuracy")


def main(argv: list[str] | None = None) -> None:
    from databricks.sdk import WorkspaceClient

    from sales_insights.common.config import load_config
    from sales_insights.common.lake import Lake
    from sales_insights.common.spark import get_spark

    parser = argparse.ArgumentParser(description="Run the Genie benchmarks and fail below the target accuracy.")
    parser.add_argument("--space-id", required=True, help="Genie agent ID")
    parser.add_argument("--target", type=float, default=90.0, help="minimum accuracy in %% (default 90)")
    args = parser.parse_args(argv)

    eval_run_id, s = evaluate(GenieEvals(WorkspaceClient().genie, args.space_id))
    print(f"  evaluation {eval_run_id}: {s.good}/{s.total} = {s.accuracy_pct}% (target {args.target}%)")
    for q in s.failed:
        print(f"    not good: {q}")
    cfg = load_config()
    spark = get_spark(cfg, app_name="genie_health")
    record(spark, Lake(spark, cfg), eval_run_id, s, args.target)
    if s.accuracy_pct < args.target:
        raise SystemExit(f"Genie accuracy {s.accuracy_pct}% is below the {args.target}% target")


if __name__ == "__main__":
    main()
