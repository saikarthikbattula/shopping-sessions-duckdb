# Shopping Sessions — Rust

Shopping DB for the Buildathon: exact identity resolution, event deduplication,
sessionization, campaign attribution, assists and daily Parquet aggregates.
Built by **Karthik and Pranav**.

The production engine lives in `src/`. The root Dockerfile builds that engine
and starts `/pipeline` directly in a scratch image. There is one production
engine; documentation, fixtures and local tools are kept alongside it.

## Build and run

The engine requires **Linux x86-64 with Cascade Lake-compatible AVX-512 features**.
Local builds use Rust 1.95.0 or newer. An ARM Mac cannot execute this binary.

```sh
make engine
NCPU=8 target/release/pipeline /absolute/data /absolute/output
# Or use environment variables:
DATA_DIR=/data OUTPUT_DIR=/output NCPU=8 ./run.sh
make build
make run-dev DEV=dev OUT=out
```

The scorer builds the root Dockerfile for linux/amd64. Its command is `/pipeline`;
it does not select an older implementation. Input/output paths default to `/data`
and `/output`. The executable takes positional paths or DATA_DIR/OUTPUT_DIR.
NCPU selects the worker count. Input semantics and output schemas follow rules.md.

## Project files

- `src/`, `Cargo.toml`, `.cargo/`: production Rust engine and compiler settings.
- `TEAM.md`, `CHANGELOG.md`: our team and earlier development milestones.
- `docs/`: engineering notes and performance evidence from earlier commits,
  labeled historical where commands refer to retired implementations.
- `fixtures/`: all 36 official fixtures and expected answers, unchanged.
- `tests/`: local independent boundary, randomized, and output-schema checks.
- `scripts/`: fixture, dev-answer, fingerprint, container, and benchmark helpers.
- `tools/generator/`: the official unmodified Rust data generator.

## Local validation tools

Python is only needed for these checks; it is not in the production image.
Run engine-dependent checks on compatible Linux hardware.

```sh
make setup
make test
make fixtures-local
make dev
make run-local DEV=dev OUT=out
make check-dev               # requires supplied dev-answer/
make check-dev-fingerprint
```

The restricted Docker helper uses BusyBox only for host-side output-permission
cleanup. The production image remains the supplied scratch image.

Generated datasets, expected dev answers, outputs, environments and scratch files
are excluded from Git and the image build context. GitHub Actions workflows remain
removed. Earlier commits are preserved in Git history; current-engine runtime and
20M scorer benchmarks remain to be measured on compatible hardware.
