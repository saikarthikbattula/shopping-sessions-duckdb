# Shopping Sessions — DuckDB

Shopping-event cleaning, identity resolution, session attribution, and daily aggregates, based on the official DuckDB starter.

**Status:** ingestion and analytics are integrated on this branch. All 26 unit/differential tests and 36 official fixtures pass. Both public dev tables match the published answers exactly. Linux amd64 CI also passes the fixtures and public-dev fingerprints in the restricted container. Representative 20M/60M performance at the scorer's 8 vCPUs/24 GB remains unmeasured.

## Development setup

Until the integration PR is human-merged into `main`, clone its branch:

```sh
git clone --branch codex/integration-validation https://github.com/saikarthikbattula/shopping-sessions-duckdb.git
cd shopping-sessions-duckdb
make setup
make test
```

For an existing clone, fetch `origin` and open `codex/integration-validation`. After the integration PR is merged, use an updated `main` instead. Use Python 3.12 or 3.13 for local development. Docker is optional for native tests. The Mac is a development environment; the submission image targets Linux amd64.

The handoffs below record the original assignments to Pranav's Cursor Mobile session and Karthik's Codex session. Both implementations are present; do not rerun the original prompts as if the functions were still stubs. Further work uses new task branches and PRs under `AGENTS.md`.

## Start here

- [Agent instructions: task branches and PRs; human-only merging](AGENTS.md)
- [Implementation plan and integration ownership](docs/IMPLEMENTATION_PLAN.md)
- [Shared ingestion-to-DuckDB contract](docs/DATA_CONTRACT.md)
- [Opt-in serial Rust ingestion](docs/RUST_INGESTION.md)
- [Parallel Rust ingestion](docs/PARALLEL_INGESTION.md)
- [Incremental Rust optimization plan](docs/RUST_OPTIMIZATION_PLAN.md)
- [Repeatable baseline benchmark](docs/BENCHMARKING.md)
- [Pranav: strict parsing and identity resolution](docs/handoffs/PRANAV.md)
- [Karthik: DuckDB analytics and output tables](docs/handoffs/KARTHIK.md)

Pranav owns ingestion and canonical person IDs. Karthik owns sessions, attribution, assists, and daily aggregates. The integrator owns orchestration, Docker, independent correctness checks, and packaging. The shared input contract is unchanged.

Each original handoff includes its agent prompt, ownership boundaries, and coordination report template for reference.

All agents must push only to task branches and open pull requests targeting `main`. Agents must not push to `main`, merge PRs, or enable auto-merge. Incomplete work or unrun required checks belongs in a draft PR. A human maintainer reviews and merges.

## Required competition materials

The official DuckDB starter is imported at the repository root. `rules.md` is authoritative, `fixtures/` and `scripts/` preserve the official checks, and `tools/generator/` contains the unmodified generator source. The original starter README is preserved in `docs/STARTER_README.md`; use this README for the updated module commands. No ZIP downloads are needed to begin either workstream.

Obtain `dev-answer.zip` from the competition and unpack it as `dev-answer/` for final dev comparison; it is deliberately not committed. Generated datasets, answers, outputs, and scratch directories are ignored by Git and excluded from the image build context.

## Development commands

```sh
make test                    # unit, staged differential, and raw-CSV integration tests
NCPU=2 MEM_GB=2 make fixtures-local  # all official fixtures, native development run
make generator               # Rust 1.85+ required
make dev                     # generate seed-1 public dev data
NCPU=2 MEM_GB=2 make run-local # native pipeline run
make check-dev               # needs unpacked dev-answer/
make check-dev-fingerprint   # public seed-1 answer hashes; no answer rows required
make fixtures                # amd64 Docker fixture run; Docker/buildx required
```

The entrypoint is `python -m pipeline.pipeline`. Set `DATA_DIR`, `OUTPUT_DIR`, `SCRATCH_DIR`, `NCPU`, and `MEM_GB` to run it directly. `run.sh` uses the same module inside the Docker image. All runtime diagnostics go to stderr. The official runner enforces no network, read-only root/data, uid 1000, a 64 MB `/tmp`, and process/memory limits.

The Linux CI workflow builds within ten minutes, verifies image architecture/size, and runs the official fixtures and 1M-event public dev set in Docker at 2 CPUs/2 GB. It compares dev output with canonical SHA-256 fingerprints derived directly from the published CSV answers. No answer rows or generated data are committed or copied into the runtime image. These fingerprints are only for public seed 1, never for hidden or other generated datasets.

See [validation status](docs/VALIDATION.md) for checks performed and outstanding gates.

## Acceptance gates

1. Match all 36 official fixtures exactly.
2. Match both published dev tables for seed 1, with 1,000,000 events and 250,000 clicks.
3. Pass additional boundary, randomized differential, and input-order checks.
4. Validate the linux/amd64 image under the scorer's restricted runtime: 8 CPUs, 24 GB RAM, no network, read-only root, and uid 1000.
5. Measure runtime and resource use before selecting a submission commit. The committed repository must remain below 50 MB and contain no generated input data.

The baseline has passed public correctness and reduced-resource Linux container checks. Full-scale throughput, memory/disk limits, and scoring still require representative runs; no competition submission has been made.
