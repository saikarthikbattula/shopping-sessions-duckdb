# Documentation guide

Use the root [README](../README.md) for current build, run and validation commands.
The production runtime is the portable Rust executable in `native/ingest`.
GitHub Actions workflows were removed at `29cf0f5`; validation and benchmark
scripts remain available for manual execution. Historical CI links record past
runs and do not establish validation of the current revision.

## Current interfaces and validation

- [Implementation plan](IMPLEMENTATION_PLAN.md): remaining acceptance gates.
- [Data contract](DATA_CONTRACT.md): inputs, exact semantics and output schemas.
- [Container design](NATIVE_CONTAINER.md): portable static Rust runtime.
- [Validation record](VALIDATION.md): Rust migration evidence and historical checks.
- [Benchmarking](BENCHMARKING.md): whole-process measurements and exact comparisons.
- [Simple-user fast path](SIMPLE_USER_FASTPATH.md): guarded optimization and evidence.
- [Express Lane candidate](../native/express-lane/README.md): optional evaluation,
  license and provenance; not the production default.

## Historical plans and implementation evidence

These notes preserve design decisions and measured results. Their old entrypoints,
staging interfaces, workflow names and pending tasks may no longer apply.

- Original starter: [starter README](STARTER_README.md).
- Python/DuckDB workstreams: [analytics plan](ANALYTICS_OPTIMIZATION_PLAN.md),
  [Karthik handoff](handoffs/KARTHIK.md), [Pranav handoff](handoffs/PRANAV.md).
- Rust migration: [optimization plan](RUST_OPTIMIZATION_PLAN.md),
  [serial ingestion](RUST_INGESTION.md), [parallel ingestion](PARALLEL_INGESTION.md).
- Performance investigations: [native experiments](NATIVE_EXPERIMENTS.md),
  [CPU study](NATIVE_CPU_STUDY.md), [regression fixes](PERFORMANCE_REGRESSION_FIXES.md),
  [batched CSV parsing](BATCHED_CSV_PARSING.md),
  [cache-efficient events](CACHE_EFFICIENT_EVENTS.md).

Official rules, fixtures, expected answers and generator sources are retained in
place. See [AGENTS.md](../AGENTS.md) for task-branch and human-review requirements.
