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

Status: planned, after correctness and packaging gates.

Benchmark matched 1M and 5M multi-seed/skewed datasets. Make Rust default only if all correctness gates pass and median 5M end-to-end runtime is at least 2x faster than Python within resource limits. Otherwise keep Python default and retain the evidence. Provide a validated candidate commit for practice timing without deleting the baseline or earlier submissions.

## Step 6 — Profile-driven next plan

Status: planned after Rust profiling.

Evaluate direct columnar handoff if intermediate staging dominates; optimize DuckDB if sorting/attribution dominates; consider native analytics only if measurements justify it. Produce one incremental next-change plan with exact-output and benchmark gates before implementing a broader rewrite.

## Execution gates

The user's execute instruction approves this plan. The selected ECC orch workflow separately requires presenting actual diffs, validation, and proposed messages for confirmation **before committing**. After each approved commit, publish its PR for human review. No gate authorizes an agent to merge main or submit to the competition.
