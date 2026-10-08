"""Weekly Genie health check: scoring, waiting and the API wrapper, with a fake Genie API (no Databricks)."""

import json

import pytest

from sales_insights.semantic import genie_health as gh


def test_score_counts_only_good_answers():
    s = gh.score([
        {"question": "a?", "assessment": "GOOD"},
        {"question": "b?", "assessment": "BAD"},
        {"question": "c?", "assessment": "NEEDS_REVIEW"},
        {"question": "d?", "assessment": "GOOD"},
    ])  # fmt: skip
    assert (s.good, s.total, s.accuracy_pct, s.failed) == (2, 4, 50.0, ["b?", "c?"])
    assert gh.score([]).accuracy_pct == 0.0


def test_finished_when_the_run_says_so_or_every_question_is_done():
    assert gh.finished({"eval_run_status": "DONE"}, [], expected=72)
    assert not gh.finished({"eval_run_status": "RUNNING"}, [{"status": "DONE"}] * 71, expected=72)
    assert gh.finished({}, [{"status": "DONE"}] * 72, expected=72)
    assert not gh.finished({}, [{"status": "DONE"}] * 71 + [{"status": "RUNNING"}], expected=72)


class FakeGenie:
    """Answers like the SDK: dicts instead of SDK objects. The run finishes on the second poll."""

    def __init__(self, assessments):
        self.assessments, self.polls = assessments, 0

    def get_space(self, space_id, include_serialized_space=False):
        qs = [{"id": str(i)} for i in range(len(self.assessments))]
        return {"serialized_space": json.dumps({"benchmarks": {"questions": qs}})}

    def genie_create_eval_run(self, space_id):
        return {"eval_run_id": "run1"}

    def genie_get_eval_run(self, space_id, eval_run_id):
        self.polls += 1
        return {"eval_run_status": "DONE" if self.polls >= 2 else "RUNNING"}

    def genie_list_eval_results(self, space_id, eval_run_id, page_token=None):
        state = "DONE" if self.polls >= 1 else "RUNNING"  # still answering until the run has been polled once
        rows = [{"result_id": str(i), "question": f"q{i}?", "status": state} for i in range(len(self.assessments))]
        return {"eval_results": rows[:2], "next_page_token": "p2"} if page_token is None else {"eval_results": rows[2:]}

    def genie_get_eval_result_details(self, space_id, eval_run_id, result_id):
        return {"assessment": self.assessments[int(result_id)]}


def test_evaluate_waits_pages_through_results_and_scores():
    fake = FakeGenie(["GOOD", "GOOD", "BAD", "GOOD", "GOOD"])
    slept = []
    run_id, s = gh.evaluate(gh.GenieEvals(fake, "space"), poll_seconds=30, sleep=slept.append)
    assert run_id == "run1" and slept == [30]  # one wait, then finished
    assert (s.good, s.total, s.accuracy_pct, s.failed) == (4, 5, 80.0, ["q2?"])


def test_evaluate_gives_up_after_the_time_limit():
    class Stuck(FakeGenie):
        def genie_get_eval_run(self, space_id, eval_run_id):
            return {"eval_run_status": "RUNNING"}

        def genie_list_eval_results(self, space_id, eval_run_id, page_token=None):
            return {"eval_results": []}

    with pytest.raises(TimeoutError):
        gh.evaluate(gh.GenieEvals(Stuck(["GOOD"]), "space"), poll_seconds=30, max_wait_seconds=60, sleep=lambda s: None)
