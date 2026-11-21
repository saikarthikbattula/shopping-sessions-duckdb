# Development milestones

- Current root-engine refactor: updated Rust source is the default Docker engine;
  project documentation, fixtures and local validation tools remain available.
- `29cf0f5`: removed GitHub Actions workflows at the team's request.
- `27f3849` / PR #47: removed the Python production pipeline and staging adapter.
- `4e0f829` / PR #46: simple-user identity bypass, compact events and CSR processing.
- `4d3547d` / PR #45: integer-key fast path, base36 decoding and guarded deduplication.

Earlier implementation notes and measured results are retained under docs/.
Those historical measurements are not runtime claims for the current engine.
