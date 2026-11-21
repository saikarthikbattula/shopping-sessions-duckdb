# Native Linux container packaging

Karthik owns Step 4 on `codex/rust-container`. This branch carries the unmerged
serial and parallel ingestion prerequisites from PRs #7 and #8. Human maintainers
review and merge; agents do not merge or publish competition submissions.

## Image and backend selection

The multi-stage Dockerfile compiles the native helper with Rust 1.85.0 in a
Bookworm builder using the committed Cargo.lock and two build jobs. The existing
Python 3.12 Bookworm runtime receives only the executable at
`/usr/local/bin/shopping-ingest`; Rust/Cargo and the build tree stay in the builder.
`RUST_INGEST_BIN` points the adapter to that executable. No Mac binary is copied,
no platform is pinned in FROM, and `target-cpu=native` is not used. Build with the
existing `make build`, whose default target is linux/amd64.

Python remains the default in this increment. The restricted runner now passes
INGEST_BACKEND explicitly, retaining all existing runtime restrictions:

```sh
make build
INGEST_BACKEND=python scripts/docker_run.sh dev out-python
INGEST_BACKEND=rust scripts/docker_run.sh dev out-rust
.venv/bin/python scripts/check_dev_fingerprints.py out-python
.venv/bin/python scripts/check_dev_fingerprints.py out-rust
```

The scorer does not need an alternate runner or runtime downloads. A later
default promotion must change the application and local runner defaults together
only after its correctness/performance gates pass.

## CI validation

CI installs Rust 1.85.0 before native checks, builds/tests the real helper, runs
the shared unit suite with Python explicitly selected, then exercises the raw-CSV
analytical differential test with Rust selected. Selecting Rust for the entire
unit suite would invalidate an existing Python-writer fault-injection test; native
staging parity is required separately rather than silently skipped.

CI builds one linux/amd64 image under `timeout 600s`, checks its architecture and
size below 8 GB, then runs all 36 official fixtures for each explicit backend
under the existing restricted runner. It generates public seed-1 dev data once
and runs/checks both backends against the published expected fingerprints in
separate output directories. The runner preserves no-network execution, uid
1000, read-only root/data, 64 MiB /tmp, CPU/memory/pids restrictions, and scratch
cleanup. CI uses its existing 2 CPU/2 GB configuration, not scorer-scale quotas.

## Results and remaining gates

Implementation-time checks: `sh -n scripts/docker_run.sh` and `git diff --check`
passed. No native compile, Docker build, fixture/dev run or Linux compatibility
result is claimed by this packaging patch yet. Those checks are configured in CI
and remain pending publication/execution. Local performance measurements are kept
separate from Linux compatibility checks and real practice timings. Karthik owns
remaining validation and promotion; a human maintainer owns merges.
