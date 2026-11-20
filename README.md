# Shopping Sessions — DuckDB

Competition planning and implementation handoffs for an exact shopping-event attribution pipeline, based on the official DuckDB starter.

**Status:** shared setup is implemented. The official starter, rules, all 36 fixtures, generator source, shared schemas, and connected workstream entrypoints are present. Three bootstrap integration checks pass. Pranav's ingestion and Karthik's analytics are explicit stubs; this is not yet a working competition submission.

## Pull and start your agent

Until the setup PR is human-merged into `main`, clone its branch:

```sh
git clone --branch codex/starter-bootstrap https://github.com/saikarthikbattula/shopping-sessions-duckdb.git
cd shopping-sessions-duckdb
make setup
make test
```

For an existing clone, fetch `origin` and open `codex/starter-bootstrap` to read the latest handoff. After the setup PR is merged, use an updated `main` instead. Use Python 3.12 or 3.13 for local development. Docker is optional for native workstream tests.

Then open the appropriate handoff below and paste its prompt into your agent. The prompt creates your own task branch from the shared setup; do not code on the setup branch or `main`. Pranav uses Cursor Mobile and Karthik uses Codex. If the mobile agent cannot execute commands, it must report which validations need another environment.

## Start here

- [Agent instructions: task branches and PRs; human-only merging](AGENTS.md)
- [Implementation plan and integration ownership](docs/IMPLEMENTATION_PLAN.md)
- [Shared ingestion-to-DuckDB contract](docs/DATA_CONTRACT.md)
- [Pranav: strict parsing and identity resolution](docs/handoffs/PRANAV.md)
- [Karthik: DuckDB analytics and output tables](docs/handoffs/KARTHIK.md)

Pranav owns ingestion and canonical person IDs. Karthik owns sessions, attribution, assists, and daily aggregates. The integrator owns starter setup, orchestration, Docker, independent correctness checks, and packaging. The handoff documents describe proposed ownership; they have not been delivered through an external messaging service.

Each handoff includes a ready-to-paste agent prompt: Pranav's is written for Cursor Mobile, and Karthik's for Codex. They include branch names, file boundaries, missing-prerequisite handling, acceptance checks, and a report template to relay between agents.

All agents must push only to task branches and open pull requests targeting `main`. Agents must not push to `main`, merge PRs, or enable auto-merge. Incomplete work or unrun required checks belongs in a draft PR. A human maintainer reviews and merges.

## Required competition materials

The official DuckDB starter is imported at the repository root. `rules.md` is authoritative, `fixtures/` and `scripts/` preserve the official checks, and `tools/generator/` contains the unmodified generator source. The original starter README is preserved in `docs/STARTER_README.md`; use this README for the updated module commands. No ZIP downloads are needed to begin either workstream.

Obtain `dev-answer.zip` from the competition and unpack it as `dev-answer/` for final dev comparison; it is deliberately not committed. Generated datasets, answers, outputs, and scratch directories are ignored by Git and excluded from the image build context.

## Development commands

```sh
make test                    # shared setup checks; does not prove pipeline correctness
make fixtures-local          # official fixtures; expected to fail until both workstreams are implemented
make generator               # Rust 1.85+ required
make dev                     # generate seed-1 public dev data
make run-local               # native pipeline run after implementation
make check-dev               # needs unpacked dev-answer/
make fixtures                # amd64 Docker fixture run; Docker/buildx required
```

The entrypoint is `python -m pipeline.pipeline`. Set `DATA_DIR`, `OUTPUT_DIR`, `SCRATCH_DIR`, `NCPU`, and `MEM_GB` to run it directly. `run.sh` uses the same module inside the Docker image. All runtime diagnostics go to stderr; stage stubs deliberately exit with an error rather than writing misleading empty outputs.

See [validation status](docs/VALIDATION.md) for checks performed and outstanding gates.

## Acceptance gates

1. Match all 36 official fixtures exactly.
2. Match both published dev tables for seed 1, with 1,000,000 events and 250,000 clicks.
3. Pass additional boundary, randomized differential, and input-order checks.
4. Validate the linux/amd64 image under the scorer's restricted runtime: 8 CPUs, 24 GB RAM, no network, read-only root, and uid 1000.
5. Measure runtime and resource use before selecting a submission commit. The committed repository must remain below 50 MB and contain no generated input data.

The Dockerfile is present, but the Docker image has not been built or checked on this machine. This repository is not yet a competition submission.
