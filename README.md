# Shopping Sessions — Rust

Production processing is entirely Rust: CSV parsing, identity resolution, global
deduplication, sessionization, attribution, assists and Parquet output. The default
container runs a statically linked Rust executable directly. It contains no
Python interpreter, Python pipeline, DuckDB runtime, or runtime package installer.

Python is used only by local test, fixture, answer-comparison and benchmark tools.
The exact fallback in the Rust analytics module remains available when the
specialized generator fast path's invariants do not hold.

## Layout

- `native/ingest/`: portable production Rust executable and its unit tests.
- `native/express-lane/`: imported MIT-licensed AVX-512 evaluation candidate,
  provenance hashes, license, and optional Dockerfile. Not the default executable.
- `tools/generator/`: official, unmodified Rust dataset generator.
- `fixtures/`: all 36 supplied fixtures and expected answers, unchanged.
- `tests/`: local output/schema checks and small independent brute-force oracles.
- `scripts/`: fixture, dev-answer, performance and restricted-container runners.
- `docs/`: current contracts and performance evidence; old workstream notes are
  explicitly marked historical.

## Build and run

Use Rust 1.85.0 for the portable executable. The AVX-512 candidate pins Rust 1.90.0
and requires a compatible Linux x86_64 CPU; it cannot run on an ARM Mac.

```sh
make native-ingest
DATA_DIR=dev OUTPUT_DIR=out NCPU=8 ./run.sh
# Or invoke Rust directly:
native/ingest/target/release/shopping-ingest --data-dir dev --output-dir out --workers 8
make build                  # linux/amd64 static Rust image
make run-dev DEV=dev OUT=out # scorer-style restricted container run
```

The container starts the binary directly with `/data` and `/output` defaults or
DATA_DIR/OUTPUT_DIR overrides. NCPU overrides automatic cgroup-aware CPU detection.
Output schemas and all millisecond boundaries follow [rules.md](rules.md).
A missing native executable is an error; there is no interpreter fallback.
The retired staging CLI (`--stage-dir`, `--batch-rows`) is no longer supported.

## Local validation

Python 3.12+ and the pinned development packages are only needed for checks:

```sh
make setup
make test                    # Rust unit and native end-to-end boundary tests
make fixtures-local          # all 36 official fixtures
make generator dev           # official seed-1 dev input
make run-local DEV=dev OUT=out
make check-dev               # needs supplied dev-answer/; exact both-table diff
make check-dev-fingerprint   # public seed-1 expected fingerprints
make fixtures                # restricted linux/amd64 container fixtures
```

Generated datasets, outputs, local environments and build artifacts stay outside
Git and the image. Obtain dev-answer.zip from the competition; never alter official
answers to pass a test. Docker is unavailable on the current Mac, so the new image
must pass Linux CI before it is selected for use.

## Performance and candidate evaluation

[Measured simple-user optimization](docs/SIMPLE_USER_FASTPATH.md) reduced CPU on
both 5M seeds. Those native Mac timings do not establish sub-second 20M scorer
performance. [Benchmark instructions](docs/BENCHMARKING.md) describe repeatable
whole-process measurements and exact comparisons.

The imported candidate is evaluated independently on an AVX-512 Linux host.
Its workflow runs fixtures, public dev fingerprints, independent boundary and
randomized tests, and 5M/20M benchmarks on seeds 1 and 2. Stock and worker-spin
variants are compared against the portable Rust pipeline. A tuned variant must
improve wall time by at least 5% and reduce CPU on both seeds before recommendation.
No unmeasured candidate replaces the production executable.

See [the output contract](docs/DATA_CONTRACT.md), [container design](docs/NATIVE_CONTAINER.md),
[implementation plan](docs/IMPLEMENTATION_PLAN.md), and [agent workflow](AGENTS.md).
Work only on task branches and PRs. Human maintainers review and merge; publishing
a GitHub candidate does not submit it to the competition.
