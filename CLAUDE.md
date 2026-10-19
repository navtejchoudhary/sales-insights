@AGENTS.md

# Claude Code: how to work in this repo

`AGENTS.md` (imported above) holds the project, the rules and the lessons learned. This file adds only how
Claude works with the human. History: the original execution plan (30 Sep 2026) is superseded by
`AGENTS.md` + `docs/decisions.md`; the project went past it (stock area, 3 agents, CEO page, filters).

## Working with the human (Navtej)

- Before a non-trivial change, give a 3-5 bullet plan and wait for approval. Small changes: just do them.
- One feature branch per task; never commit to `main`. Suggest a commit message after each task.
- Explain in plain words, with references, and say so when you disagree; no yes-man answers.
- If a request conflicts with `AGENTS.md` or `docs/decisions.md`, say so and ask.
- After a change that alters a design choice, add one row to `docs/decisions.md` (date, decision, why, owner).
- Ask before adding or upgrading packages.

## Where Claude may run and what it may do

- **On Navtej's Mac** (Claude Code in the repo): branch, commit, run tests, `bundle deploy` only when asked.
- **In a cloud session without push rights**, deliver changes as patches: commit on a local branch,
  `git format-patch -1 -o <share folder>`, send the file, then delete the local branch. Navtej applies it:
  ```
  git checkout main && git pull && git checkout -b <branch>
  git am ~/Downloads/<patch> && git status --short && uv run pytest
  git push -u origin <branch> && gh pr create --fill && sleep 20 && gh pr checks --watch
  gh pr merge --squash --admin --delete-branch && git checkout main && git pull
  databricks bundle deploy -t dev --profile sales-dev        # when jobs, dashboards or alerts changed
  ```
- Add files to a commit explicitly (`git add <paths>`), never `git add -A`: local runs create files
  (databases, outputs) that must not be committed. Check `git status --short` before committing.
- Never push, never print or store tokens, never put the workspace URL in a file. Screenshots shared
  publicly must have the browser address bar cropped.

## Before you say "done"

- `uv run pytest -m "not spark"` and `uv run ruff check .` pass; for pipeline, metric view or dashboard
  changes the full `uv run pytest` must pass too (CI does not run the Spark tests).
- Regenerated what is generated (`semantic.dashboards`, `pipeline.gold_model`, `semantic.answer_key`).
- Docs that describe the change are updated (`AGENTS.md` status table, the relevant playbook).
