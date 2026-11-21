> Historical project record from before the root-engine replacement.
> Current build/run instructions are in the root README.md.

> Historical workstream record. Python/staging commands below are retired.
> Use the current README.md and DATA_CONTRACT.md for the Rust-only runtime.

# Parallel Rust ingestion

Karthik owns Step 3. The approved implementation is complete locally on
`codex/rust-ingestion-parallel`. This branch starts from shared main and carries
serial Rust commit `75d8003` as `06b1117`; serial PR #7 is a prerequisite. Neither
PR is merged by an agent. Python remains the default, and the shared staged
schemas, PreparedInputs signature and DuckDB analytics remain unchanged.

## Implementation

Resolve all links and campaign membership before parallel parsing. Divide each
event/click input into approximately 16 MiB chunks aligned after LF. Skip exactly
one physical header per file and retain final records without a terminating LF.
Schedule chunks across at most NCPU workers, sharing immutable lookup maps.
Each worker owns its Arrow buffers and Parquet writer, bounded by both row count
and estimated buffered bytes. Process event and click phases successively, with
at most one output part per worker per table, so file count stays bounded.

The internal native completion protocol advances to version 2 with explicit part
names and row counts. The adapter validates exact file inventories, safe names,
ordinary files, schemas, per-part counts and aggregate counts. Version 1 marker
validation is retained. The old serial executable does not accept the new
`--workers` argument; benchmark it through its archived serial adapter rather
than selecting that executable with the parallel adapter.

On error or panic, stop assigning chunks and join all workers before returning
failure. Publish no successful marker after partial failure. The existing adapter
kills/reaps interrupted native processes and cleans only their owned stage.
Keep typed empty outputs and every parsed event copy with a nonempty ID; global
deduplication, validation and lateness filtering still belong to analytics.

## Run and verification scope

Build with `make native-ingest`, then select the backend explicitly:

```sh
INGEST_BACKEND=rust NCPU=8 DATA_DIR=dev OUTPUT_DIR=out-parallel SCRATCH_DIR=scratch .venv/bin/python -m pipeline.pipeline
```

At Karthik's request, implementation-time verification is limited to focused
chunk-boundary, failure/protocol and staging-parity checks. Full fixture/dev-answer
runs, coverage and repeated performance benchmarks are deferred until the
implementation is ready. Deferred checks are not reported as passed. Linux native
container packaging remains Step 4, and multi-seed 1M/5M measurements remain the
Step 5 gate before default promotion.


## Focused implementation results

Rust 1.85.0 release build succeeded. The focused Rust suite passed 14/14 tests,
including chunk/header/unterminated-record boundaries, a record larger than the
production chunk size, worker-count parity, and error/panic cancellation.

`REQUIRE_RUST_INGEST=1 NCPU=2 .venv/bin/python -m unittest discover -s tests -p 'test_ingest*.py' -v`
passed all 31 focused Python ingestion tests with the real native executable.
This includes strict multipart protocol checks and small uneven/shuffled input
staging comparisons at 1, 2 and 8 workers. An inherited test assumed every file
had multiple row groups; it now asserts multiple groups across the staged table,
retaining the per-group batch bound and complete row/multiset checks.

Code and security review found no remaining blocking findings. `git diff --check`
passed. Full analytical integration/official fixtures/dev-answer comparisons,
coverage, repeated performance measurements and native Linux/container validation
have not been rerun for this parallel increment. The serial increment's earlier
results do not establish those parallel acceptance gates. Karthik owns these
remaining checks; Python remains the default until the promotion gates pass.
