# Pranav handoff: strict ingestion and identity resolution

Status: prepared; not sent externally and implementation has not started.

## Objective and ownership

Implement the input side of the official DuckDB starter solution. Read [the implementation plan](../IMPLEMENTATION_PLAN.md) and [the shared data contract](../DATA_CONTRACT.md) first, then the supplied `rules.md`.

Own `pipeline/ingest.py`, `pipeline/parsing.py`, `pipeline/identity.py`, and `tests/test_ingest.py`. Integrator supplies `pipeline/contracts.py` and imports the starter. Do not edit Karthik's analytics, the starter entrypoint, Docker, or dependencies without coordination. Request missing dependencies through the integrator.

## Required behavior

- Parse bytes/UTF-8 with LF as the only line terminator. Preserve CR, tabs, spaces, and case. Accept quotes only around a whole field, reject wrong field counts and stray quotes, and treat quoted empty fields as null. Skip one header per file, including header-only files.
- Parse exact F1/F2/F3 timestamps with ASCII digits, valid Gregorian dates, strict offset ranges, and integer milliseconds. Compare very long F3 numbers safely; leading zeros must not cause a valid timestamp to be rejected. Apply manifest windows as defined in the contract.
- Resolve valid links transitively with union-find, including cycles, repetitions, self-links, long chains, shared devices, and IDs never used in events. Use union by size/path compression while tracking a separate byte-wise minimum name.
- Retain all parsed event copies with non-empty event IDs, even when invalid or late. Set typed validation fields, never choose a duplicate or filter an invalid winner here. Never filter lateness here.
- Prepare clicks and lookups according to the shared schema. Preserve arbitrary strings, not just generator-shaped IDs. Emit bounded Arrow batches and typed Parquet, including empty tables.
- Keep input files streaming and release batch buffers promptly. Do not accumulate event/click dictionaries for the whole dataset. Identity state is the intentional persistent in-memory structure.

## Acceptance evidence

- Focused tests for malformed quoting/field counts, nulls, LF versus CRLF, whitespace, ASCII versus Unicode digits, overflow/leading zeros, impossible/leap dates, offsets, and exact timestamp window boundaries.
- Identity tests for transitive chains hundreds of links long, unused minimum IDs, Unicode byte ordering, duplicate/self links, empty sides, and disconnected components.
- A staging test proving that an invalid earliest event and a valid later duplicate both survive ingestion for downstream deduplication.
- An integration smoke test exposing the agreed schemas to DuckDB with empty and non-empty inputs. Full fixture/dev comparisons belong to integration with Karthik's output.

## Return handoff

Provide changed files, exact test commands/results, preparation timing and memory observations if measured, and unresolved limitations. Flag contract changes explicitly. Do not claim full pipeline correctness from ingestion-only tests. Do not publish, submit, or send external messages as part of this work.
