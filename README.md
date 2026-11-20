# Shopping Sessions — DuckDB

Competition planning and implementation handoffs for an exact shopping-event attribution pipeline, based on the official DuckDB starter.

**Status:** planning documents only. The starter has not been imported, the pipeline is not implemented, and no fixture, dev-answer, or Docker validation has run.

## Start here

- [Implementation plan and integration ownership](docs/IMPLEMENTATION_PLAN.md)
- [Shared ingestion-to-DuckDB contract](docs/DATA_CONTRACT.md)
- [Pranav: strict parsing and identity resolution](docs/handoffs/PRANAV.md)
- [Karthik: DuckDB analytics and output tables](docs/handoffs/KARTHIK.md)

Pranav owns ingestion and canonical person IDs. Karthik owns sessions, attribution, assists, and daily aggregates. The integrator owns starter setup, orchestration, Docker, independent correctness checks, and packaging. The handoff documents describe proposed ownership; they have not been delivered through an external messaging service.

Each handoff includes a ready-to-paste agent prompt: Pranav's is written for Cursor Mobile, and Karthik's for Codex. They include branch names, file boundaries, missing-prerequisite handling, acceptance checks, and a report template to relay between agents.

## Required competition materials

Obtain `starter-duckdb.zip`, `rules.md`, `generator.zip`, and `dev-answer.zip` from the competition. The supplied rules are authoritative. Import the starter into the repository root before implementing; preserve its fixture and comparison tools. Keep generated datasets and dev answers outside the committed submission.

## Acceptance gates

1. Match all 36 official fixtures exactly.
2. Match both published dev tables for seed 1, with 1,000,000 events and 250,000 clicks.
3. Pass additional boundary, randomized differential, and input-order checks.
4. Validate the linux/amd64 image under the scorer's restricted runtime: 8 CPUs, 24 GB RAM, no network, read-only root, and uid 1000.
5. Measure runtime and resource use before selecting a submission commit. The committed repository must remain below 50 MB and contain no generated input data.

The Dockerfile and runnable pipeline will be added when the starter is imported. This repository is not yet a competition submission.
