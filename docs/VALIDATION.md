# Shared setup validation

Status as of November 20, 2026: bootstrap implemented; competition pipeline not implemented.

## Passed

- Installed pinned dependencies in an isolated local Python 3.13 environment on ARM64: DuckDB 1.5.6, PyArrow 18.0.0, and Polars 1.35.2.
- `make test`: three bootstrap integration tests passed. They cover Arrow/Parquet/DuckDB preservation of exact strings, nulls and int64 precision; typed empty staging; scratch configuration; and stage orchestration with synthetic test doubles, DATE outputs, and temporary-directory cleanup.
- Verified all 293 official fixture/harness files are byte-identical to the supplied starter, across all 36 fixture cases. This is an integrity check, not a pipeline correctness result.
- The official checker successfully parsed all 72 expected fixture tables with the pinned Polars version. Local Markdown links were checked, and `git diff --check origin/main` passed. `.gitattributes` preserves official fixture/rules bytes and exempts their intentional whitespace from formatting checks.
- `sh -n run.sh scripts/docker_run.sh` and Makefile dry runs passed.
- `python -m pip check`: no broken requirements. A native module-entrypoint smoke check confirmed a nonzero exit with clear stderr, no Parquet outputs, and cleaned invocation scratch while ingestion is unimplemented.

## Outstanding

- Implement `prepare_inputs` (Pranav) and `run_pipeline` (Karthik). Both currently raise `NotImplementedError`. The real pipeline must fail until those implementations are present.
- Run all 36 official fixtures, public seed-1 dev-answer comparisons, independent differential checks, and performance/resource benchmarks after integration. No correctness or speed claim is made yet.
- Build the official generator with Rust 1.85+ and generate dev data. Generator source is included, but Rust is not installed locally.
- Build and validate the linux/amd64 Docker image under scorer restrictions. Docker is not installed locally; the native tests do not validate the container or Python 3.12 image runtime.
- Obtain `dev-answer.zip` separately for clean-clone dev checks; its contents and generated input data are deliberately excluded from Git.

The PR must remain a draft while the required container checks are unrun. A human may review or merge the bootstrap separately from competition acceptance; merging setup does not certify the unfinished pipeline.
