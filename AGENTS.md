# Instructions for all coding agents

These instructions apply to Codex, Cursor, and every agent working in this repository.

## Required Git and pull request workflow

- Read this file, `docs/IMPLEMENTATION_PLAN.md`, `docs/DATA_CONTRACT.md`, and your assigned handoff before editing.
- Work on a task branch based on the latest shared `main`. Never code or commit directly on `main`, and never push directly to `main`.
- Use `codex/pranav-ingestion` for Pranav's work, `codex/karthik-analytics` for Karthik's work, and a separate `codex/<task>` branch for integration or other changes. Inspect existing branches and local changes before switching; preserve other people's work.
- Bootstrap exception: until the shared setup PR is human-merged, workstream branches may start from `origin/codex/starter-bootstrap` so they include the starter and contract. Still target `main` with PRs, disclose the shared dependency, and leave bootstrap merging to a human. After that merge, use updated `origin/main` for new work.
- Commit and push changes only to your task branch. Open a pull request targeting `main`. Open a draft PR while work is incomplete, checks have failed, or required checks remain unrun; update that PR as work proceeds.
- Never merge a pull request, enable auto-merge, or bypass review/branch protections. Leave merging to a human maintainer. Do not force-push or rewrite shared history.
- Include the problem and resulting behavior, scope, validation commands/results, unrun checks and reasons, and interface/dependency changes in the PR description. Do not mark checks as passed unless they ran successfully.
- Finish with the PR URL, branch, commit, test results, and remaining blockers. If your environment cannot push or create a PR, report that explicitly and provide the branch/commit and ready-to-use PR title/body for the human; do not claim a PR exists.

## Ownership and correctness

- Follow file ownership in the handoffs. Coordinate shared contract changes before modifying either side; do not silently change schemas or stage ordering.
- Use the official DuckDB starter and treat the supplied competition `rules.md` as authoritative. Do not invent unavailable rules or modify official expected answers to make tests pass.
- Preserve global event deduplication before validation, exact string semantics, and all specified millisecond boundaries. Retain the fixture and dev-answer comparisons.
- Keep generated datasets, outputs, scratch files, credentials, and local environments out of commits and the image build context.
- The integrator also uses task branches and PRs. Integration work does not grant permission to merge to `main` or publish a competition submission.

These are agent workflow instructions. GitHub-enforced branch protection is a separate repository setting; do not claim it is enabled merely because this file exists.
