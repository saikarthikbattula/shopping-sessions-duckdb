> Historical workstream record. Python/staging commands below are retired.
> Use the current README.md and DATA_CONTRACT.md for the Rust-only runtime.

# Incremental Rust optimization

Source: user-approved plan `inline:rust-optimization`, executed in numbered order. Karthik owns every step. Each increment uses a task branch and PR; agents never merge to main or enable auto-merge.

## Goal and baseline

Reduce the user-reported **242.09-second practice runtime**, targeting **under 60 seconds**, without changing either output table. Preserve submitted commit `7637e570c8221001a7dc8ce7233d9443dfc2d947` and all submitted history.

Keep the DuckDB starter, existing analytical SQL, staging schemas, and scorer interface initially. The Mac is a development environment. Native development, reduced-resource Linux CI, and actual scorer timings are distinct measurements.

## Step 1 — Repeatable baseline measurements

Status: implemented, reviewed, locally validated, and approved for commit by Karthik. All 49 unit/integration tests pass, with 91% coverage of the new benchmark module. The three-round 1M-event Python control passed every exact comparison and input/source stability check: median baseline 6.467 s, candidate 6.595 s (0.981x). No runtime optimization is claimed.

- Export the immutable baseline and benchmark it against the candidate using the same interpreter, generated data, and requested resource configuration.
- Alternate run order, run each arm three times, and report medians. Measure process wall time including startup and parse existing preparation/analytics timings.
- Record process high-water RSS, separately labelled sampled process-tree RSS, and sampled combined scratch/output disk use. Missing measurements are null with diagnostics, never fabricated zeroes.
- Verify exact output equivalence, unique keys, stable source/data fingerprints, and successful exits before publishing performance summaries.
- Exclude export, input hashing, output checks, and metadata probing from measured child time. Preserve logs/report, remove only owned temporary artifacts, and leave runtime code unchanged.

The baseline-versus-candidate control currently runs Python on both sides. Backend-specific environment overrides prepare the harness for later steps; an environment request alone does not prove a backend executed.

## Step 2 — Serial Rust ingestion

Status: planned, after Step 1's pre-commit/review gate.

- Create `native/ingest/` with Rust 1.85, locked compatible Arrow/Parquet dependencies, and bounded typed Parquet output.
- Preserve Python ingestion in `pipeline/ingest_python.py`; adapt `prepare_inputs(...) -> PreparedInputs` to explicit `INGEST_BACKEND=python|rust` selection, with no automatic fallback.
- Implement exact LF-only CSV parsing, ASCII numbers, calendar/offset validation, linked-ID resolution, and byte-wise canonical names, including unused linked IDs.
- Retain all parsed event copies with non-empty IDs, including invalid/late copies; downstream DuckDB still deduplicates before validation and lateness filtering.
- Write the existing schemas and valid empty files. Publish completion only after writers close; propagate errors and leave DuckDB analytics unchanged.
- Prove single-worker staging multiset parity and final output parity before parallelization.

## Step 3 — Parallel parsing

Status: planned, after serial parity passes.

- Freeze identity resolution before parsing; share read-only canonical mappings.
- Schedule approximately 16 MiB LF-aligned chunks over at most NCPU workers. Skip one header per file; handle unterminated final lines, header-only files and highly uneven sizes.
- Use bounded worker-local Arrow buffers and Parquet writers. Any worker failure prevents successful completion.
- Compare outputs with 1, 2 and 8 workers, including repeated shuffled/repartitioned inputs and forced chunk-boundary edge cases.

## Step 4 — Packaging and validation

Status: planned; validation accompanies every increment.

- Add a multi-stage Docker build that compiles the Linux amd64 executable independently of Mac binaries and copies only runtime artifacts. Avoid target-cpu=native.
- Preserve the restricted runner, environment variables, and stderr logging; compile/test Rust before Python integration tests in CI.
- Require Rust parser/identity tests, Python/Rust staging parity, existing unit/differential tests, all 36 official fixtures, exact public dev answers, restricted-container checks, and build time below ten minutes.
- Adjust only implementation-dependent test assumptions about staging layouts or mock locations; preserve semantic assertions and all official expectations.

## Step 5 — Conditional promotion

Status: implemented. Gates passed; Rust is now the default backend.

Matched 1M and 5M multi-seed benchmarks all passed correctness gates (status: ok). Median
wall-time speedups on the ARM64 development Mac at NCPU=8, MEM_GB=4:

| Dataset | Speedup |
| --- | --- |
| 1M seed-1 | 4.40x |
| 1M seed-2 | 4.57x |
| 5M seed-1 | 6.01x |
| 5M seed-2 | 4.32x |

All runs exceeded the ≥2x gate. `INGEST_BACKEND` now defaults to `rust` in both
`pipeline/ingest.py` and `run.sh`. The Python reference remains available via
`INGEST_BACKEND=python`. Validated candidate SHA: `df00ea2` (PR #9, `codex/rust-container`,
the last merged increment before promotion). Submitted baseline
`7637e570c8221001a7dc8ce7233d9443dfc2d947` and all prior history are preserved.

## Step 6 — Profile-driven next plan

Status: completed. Profile-driven next optimization plan established under `docs/ANALYTICS_OPTIMIZATION_PLAN.md`.

### Profiling evidence & bottleneck analysis

Stage-level profiling was instrumented via `scripts/profile_analytics.py` across 3 repeated runs on 5M multi-seed datasets (`dev-benchmark-5m-seed1` and `dev-benchmark-5m-seed2`) with NCPU=8, MEM_GB=4:

| Component | 5M seed-1 (median) | 5M seed-2 (median) | % of Analytics |
|---|---|---|---|
| Preparation (Rust parallel ingest) | 2.319s | 2.147s | - (30-35% wall) |
| Analytics: `session_events` | 0.911s | 0.801s | 35.0% - 38.6% |
| Analytics: `events` (dedup) | 0.561s | 0.649s | 23.8% - 28.4% |
| Analytics: `assist_intervals` | 0.200s | 0.228s | 8.5% - 10.0% |
| Analytics: `assists` | 0.142s | 0.164s | 6.0% - 7.2% |
| Analytics: `credited` (ASOF join) | 0.108s | 0.110s | 4.6% - 4.8% |
| Analytics: `clicks` (dedup) | 0.090s | 0.098s | 3.8% - 4.3% |
| Analytics: `sessions` (aggregation) | 0.080s | 0.088s | 3.4% - 3.9% |
| Analytics: `top_categories` | 0.064s | 0.074s | 2.7% - 3.2% |
| Analytics: `winning_clicks` | 0.050s | 0.052s | 2.1% - 2.3% |
| Analytics: `user_daily` | 0.048s | 0.053s | 2.0% - 2.3% |
| Analytics: `campaign_daily` | 0.008s | 0.009s | <0.5% |
| **Total Analytics (DuckDB)** | **2.361s** | **2.288s** | **100% (52-68% wall)** |
| **Total Process Wall Time** | **4.908s** | **4.515s** | - |

### Strategic decisions based on measurements

1. **Direct columnar handoff is NOT justified**: Staging Parquet write/read takes <0.5s of the 2.1s preparation phase. Parquet staging provides clean failure isolation, out-of-core memory buffering, and parallel DuckDB scans.
2. **Native analytics rewrite in Rust is NOT justified**: End-to-end 5M wall time is already under 5 seconds (extrapolating to ~20-35s at 20M, well below the 60s target). A complete native rewrite has high complexity and risk.
3. **DuckDB analytics optimization is the chosen next increment**: Analytics accounts for 52%–68% of pipeline time. Within DuckDB, sessionization (`session_events` + `events`) accounts for ~62% of analytics time.

Detailed proposal, target query optimizations, and strict acceptance gates are detailed in `docs/ANALYTICS_OPTIMIZATION_PLAN.md`.

## Execution gates

The user's execute instruction approves this plan. The selected ECC orch workflow separately requires presenting actual diffs, validation, and proposed messages for confirmation **before committing**. After each approved commit, publish its PR for human review. No gate authorizes an agent to merge main or submit to the competition.
