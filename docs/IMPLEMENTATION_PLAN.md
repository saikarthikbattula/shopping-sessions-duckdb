# DuckDB competition implementation plan

Status: ready for implementation. This workspace currently contains planning documents only; the starter has not been imported and no pipeline or checks have run.

## Goal and source of truth

Start from the competition's `starter-duckdb.zip`. Implement the exact behavior in its supplied `rules.md`, then optimize end-to-end runtime under 8 CPUs and 24 GB RAM. Obtain `generator.zip` and `dev-answer.zip` from the competition as well. These materials are setup inputs, never runtime dependencies.

Retain the starter Docker entrypoint, pinned DuckDB version, runner, fixtures, and comparison tools. Generated data, dev answers, scratch files, and outputs stay out of Git and the image build context. The committed submission must be below 50 MB; the image must be linux/amd64 and below 8 GB.

## Ownership and integration order

| Owner | Deliverable | Handoff |
| --- | --- | --- |
| Pranav | Strict CSV/timestamp parsing, union-find identity resolution, bounded typed staging | [Pranav](handoffs/PRANAV.md) |
| Karthik | DuckDB event/click processing, sessions, attribution, assists, both output tables | [Karthik](handoffs/KARTHIK.md) |
| Integrator (Codex) | Starter import, shared contract module, orchestration, dependencies, Docker, independent oracle, full validation and submission candidate | [Shared contract](DATA_CONTRACT.md) |

1. Integrator imports the starter and rules, creates the contract module and an isolated environment, and pins the Arrow dependency. Preserve the supplied fixtures and harness.
2. Pranav and Karthik can implement independently against the shared contract. Karthik uses synthetic staged tables until ingestion is ready. Keep edits within each owner's files.
3. Integrator wires `prepare_inputs` to `run_pipeline` in the starter entrypoint and runs all exact comparisons. Resolve correctness failures before tuning.
4. Measure stages and resource usage across seeds and increasing sizes. Keep only optimizations that preserve exact results.
5. Build and check the amd64 image under scorer flags, commit the validated candidate, and provide reproducible commands and evidence. Public repository creation and track submission remain separate handoff actions.

## Processing design

- Python streams strict parsed input into bounded Arrow/Parquet batches under scratch. Union-find resolves every valid link, including unused IDs, before event/click person assignment.
- DuckDB deduplicates events globally on earliest ingestion before validation and lateness filtering. Deduplicate and validate clicks separately.
- Sessionize by canonical person and millisecond timestamp; only gaps greater than 1,800,000 split. All events belong to the session start's UTC date.
- Aggregate category purchase revenue per person/day, resolving ties by UTF-8 byte order. Unknown products use `unknown`; no purchases produces null.
- Collapse credit candidates at equal person/timestamp to smallest click ID, as-of join to session starts, then enforce the inclusive seven-day bound.
- Compute assists from per-person/campaign merged inclusive click intervals, using a running maximum endpoint to handle nested intervals. Join sessions against those intervals without expanding every qualifying click. Count each campaign once per session.
- Aggregate credit and assists separately and combine keys, preserving assisted-only rows. Write the exact required Parquet columns and DATE types, including typed empty outputs.

## Validation and performance gates

- All 36 supplied fixtures match, followed by seed 1 with 1,000,000 events and 250,000 clicks against the published dev answers.
- Add focused tests for parser syntax, CR/spaces, ASCII numbers, overflow and leading zeros, leap dates, earliest invalid duplicates, long link chains, unused minimum IDs, exact time boundaries, nested assist intervals, and byte-order ties.
- Integrator builds a small independent reference implementation and runs randomized differential and input-order/repartitioning checks. Validate unique output keys, schemas, session/revenue conservation, and assisted-only rows.
- Log stage timings to stderr; track peak memory and scratch use. Honor environment variables, reserve Python/Arrow headroom, and release intermediates. Output plus scratch must remain below 50 GB. Runtime has no network and root is read-only.
- Use this ARM64 Mac for native development. Docker and the generator's Rust toolchain still need setup. Validate linux/amd64 separately; local/emulated timings do not predict scorer timings.
- Exercise multiple seeds and 20M/60M-event runs where local resources permit. Record any unrun validation explicitly. No paid cloud resources are authorized or assumed. Sub-60-second practice runtime is an objective, not a prediction.

## Handoff completion rule

Each owner returns changed files, commands run, results, known limitations, and any contract issue. A document being written does not mean work has been assigned through an external service, started, tested, or completed.
