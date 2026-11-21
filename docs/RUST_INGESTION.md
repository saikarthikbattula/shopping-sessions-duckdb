> Historical workstream record. Python/staging commands below are retired.
> Use the current README.md and DATA_CONTRACT.md for the Rust-only runtime.

# Serial Rust ingestion: opt-in development backend

Karthik owns this increment. It implements Step 2 of the approved incremental
Rust plan. Python remains the default; the DuckDB starter, analytical SQL,
PreparedInputs signature, and all staged column types/nullability are unchanged.

## Build and run

Install Rust 1.85.0 and the pinned Python development environment, then run:

```sh
make native-ingest
make test-native
INGEST_BACKEND=rust NCPU=2 MEM_GB=2 make fixtures-local
INGEST_BACKEND=rust DATA_DIR=dev OUTPUT_DIR=out-rust-serial SCRATCH_DIR=scratch NCPU=2 MEM_GB=2 .venv/bin/python -m pipeline.pipeline
.venv/bin/python scripts/compare.py out-rust-serial dev-answer
```

`native/ingest/rust-toolchain.toml` pins Rust 1.85.0; Cargo.lock fixes the entire
dependency graph. Direct Arrow/Parquet dependencies are 56.2.0; compatible
transitive patch versions are also locked. `RUST_JOBS` defaults to 2 for this
8 GB development Mac. The helper compiles for the host architecture: never copy
this Mac executable into the Linux amd64 image.

Backend selection is explicit:

- Unset `INGEST_BACKEND`, or `INGEST_BACKEND=python`: use the original Python
  implementation preserved in `pipeline/ingest_python.py`.
- `INGEST_BACKEND=rust`: use `pipeline/native_ingest.py` and the native helper.
- `RUST_INGEST_BIN` optionally overrides the helper path; use an absolute path.
  The default is `native/ingest/target/release/shopping-ingest` under this repo.
- Unknown backends, missing executables, native failures or invalid completion
  protocols raise errors. There is no silent fallback.

`make test-native` requires the real executable and runs all native staging
parity checks. Plain `make test` reports explicit skips for real-native cases
when no helper is built. Set `REQUIRE_RUST_INGEST=1` to turn a missing executable
into a test failure. No Rust tests are claimed passed when skipped.

## Processing and protocol

The Python adapter validates batch_rows and reads the small window manifest
before launch. Rust owns every per-row operation: strict LF-only CSV parsing,
ASCII timestamp/price validation, identity resolution, lookup staging, and
event/click Parquet writing. Valid links are fully resolved before rows are
assigned canonical IDs. Parsing never trims strings or translates CR.

All parsed copies with nonempty event/click IDs survive staging, including
invalid payloads and late events. DuckDB still performs global event-ID
deduplication before filtering validation and lateness. Unknown product IDs
remain valid nonempty products; downstream category lookup supplies `unknown`.

The adapter creates a unique empty staging directory and launches without a shell:

```text
shopping-ingest --data-dir PATH --stage-dir PATH --window-start-ms INT --window-end-ms INT --batch-rows INT
```

The helper creates exactly these files:

```text
events/part-0.parquet
clicks/part-0.parquet
products.parquet
campaigns.parquet
_SUCCESS.json
```

The version-1 success marker is atomically renamed last, only after all Parquet
writers close. It includes window boundaries and row counts for all four tables.
The adapter requires zero exit, empty stdout, ordinary files, exact event/click
directory contents, matching schemas/counts, and a valid marker. Native stderr
is inherited for diagnostics. Invalid ingest_ts exits with code 2 and maps to
the existing ValueError; other native failures propagate. Interrupts kill/reap
the owned child group before removing only that invocation's staging directory.
Successful staged files remain until the existing pipeline scratch cleanup.

Writing is serial and bounded by min(requested batch_rows, 65,536) and an
approximately 16 MiB estimated buffer threshold. Parquet row groups are
explicitly flushed. The limit excludes identity/campaign state, Parquet codec
buffers, transient conversion copies, and the largest physical input line; this
is not a constant-memory guarantee independent of input cardinality or field size.

## Validation and performance gates

Require parser/identity tests, complete staging multiset/schema comparisons,
all existing unit/differential tests, all 36 official fixtures, and exact public
dev-answer equality. Regressions include giant leading zeros, malformed/quoted
fields, CR preservation, unused canonical IDs, long chains, exact boundaries,
header-only files, and input parts named exactly `.csv`. Protocol regressions
reject extra Parquet files/symlinks that analytics would otherwise consume.

The Step 1 benchmark is independently published in PR #6. Until it is merged,
export its harness and compare against the unchanged submitted Python baseline:

```sh
git show origin/codex/baseline-benchmark:scripts/benchmark.py > /private/tmp/shopping-benchmark.py
.venv/bin/python /private/tmp/shopping-benchmark.py dev --candidate-root . --runs 3 --ncpu 2 --mem-gb 2 --report out-benchmark/rust-serial.json --baseline-env INGEST_BACKEND=python --candidate-env INGEST_BACKEND=rust
```

Report all local results as development measurements, not scorer predictions.
Parallel parsing is Step 3. Linux amd64 native packaging/restricted-container
validation is Step 4. Default promotion is Step 5 and still requires exact
correctness and the matched multi-seed 1M/5M performance gate. The current Docker
image has no native executable and continues to run Python by default; selecting
Rust in that image must fail rather than fall back.

## Recorded serial-increment results

Native ARM64 Mac development validation on 2026-11-20, with Rust 1.85.0:

- `make test-native`: 10 Rust tests and 27 focused Python/protocol/parity tests
  passed, with the real native executable required.
- `REQUIRE_RUST_INGEST=1 make test`: all 42 tests passed.
- `INGEST_BACKEND=rust NCPU=2 MEM_GB=2 make fixtures-local`: 36/36 passed.
- `INGEST_BACKEND=rust REQUIRE_RUST_INGEST=1 .venv/bin/python -m unittest tests.test_integration -v`:
  independent raw-CSV differential test passed across its eight seeds and two layouts.
- Final native public-dev run followed by
  `.venv/bin/python scripts/compare.py out-rust-serial dev-answer`: exact match,
  117,038 user/day rows and 27,400 campaign/day rows; zero differing rows or
  duplicate keys in either table.
- Adapter/selector coverage: 95% combined branch/line. Instrumented Rust
  production-file coverage: 95.72% lines and 82.84% regions. Coverage builds were
  excluded from performance measurements.
- Code and security reviews completed; findings about a `.csv` basename and
  unexpected staged Parquet files were reproduced and fixed with regressions.

The isolated benchmark report is generated locally at
`out-benchmark/rust-serial-isolated.json` and is intentionally uncommitted.
Using the exported PR #6 harness and the uninstrumented release executable, it
compared submitted baseline `7637e570c8221001a7dc8ce7233d9443dfc2d947` against
this working tree on identical public seed-1 1M-event/250K-click data. Three runs
per arm alternated baseline/candidate order. All comparisons passed and input/
source fingerprints remained stable. No other pipeline validation ran concurrently.
An earlier exploratory report overlapped a dev comparison and is not used here.

| Median metric | Python baseline | Serial Rust |
| --- | ---: | ---: |
| Process wall time, including startup | 6.436 s | 2.272 s |
| Preparation | 5.306 s | 1.080 s |
| DuckDB analytics | 0.932 s | 1.057 s |
| OS maximum individual-process RSS | 346,832,896 bytes | 316,112,896 bytes |
| Sampled process-tree RSS peak | 342,917,120 bytes | 308,756,480 bytes |
| Sampled scratch plus output peak | 44,922,797 bytes | 44,721,931 bytes |

Median end-to-end speedup was **2.83x**. Disk and tree RSS are sampled, so short
peaks may be missed; individual-process RSS does not sum parent and child memory.
Both arms requested NCPU=2/MEM_GB=2; these configure the application and do not
impose OS resource quotas on the Mac. This result does not establish the 5M,
multi-seed/skew promotion gate or predict the scorer's runtime. Linux native
build, native restricted-container execution, the ten-minute native image build
gate, and official practice timing remain unrun for this increment. Packaging/CI
are Step 4; Karthik owns these remaining gates and default promotion.
