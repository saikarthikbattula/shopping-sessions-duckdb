# Pranav handoff: strict ingestion and identity resolution

Status: prepared; not sent externally and implementation has not started.

## Using this with Cursor Mobile

Open this repository in a Cursor agent session with repository access, then paste the prompt below. If the mobile session cannot run commands, have it prepare the code changes and explicitly report which checks still need a terminal-capable environment. Karthik uses Codex for the analytical side. Relay coordination notes between the two sessions; neither agent can assume it can see the other's unpushed edits.

```text
Implement Pranav's ingestion workstream in:
https://github.com/saikarthikbattula/shopping-sessions-duckdb

First inspect the current branch, git status, and applicable AGENTS.md instructions.
Read README.md, docs/IMPLEMENTATION_PLAN.md, docs/DATA_CONTRACT.md, and
docs/handoffs/PRANAV.md. Read the competition rules.md when available.
Treat the data contract as the interface shared with Karthik's Codex agent.

Create branch codex/pranav-ingestion from the latest shared main. Preserve
existing work; if the branch already exists, inspect and continue it.
Never commit or push directly to main. Push only to your task branch and
open a PR targeting main; use a draft while incomplete or required checks
are unrun/failing. Never merge the PR or enable auto-merge. A human
maintainer reviews and merges. Follow root AGENTS.md throughout.
Implement strict parsing, timestamp validation, identity union-find, and
bounded Arrow/Parquet staging. Own only pipeline/ingest.py,
pipeline/parsing.py, pipeline/identity.py, and tests/test_ingest.py.
Implement prepare_inputs with exactly the signature and schemas in the
shared contract. Never deduplicate events or discard invalid event copies
before the analytical stage. Apply all acceptance checks in this handoff.

The repository may initially contain planning documents only. Inspect first.
If the official starter, rules, dependencies, or pipeline/contracts.py are
missing, report the specific missing prerequisite to Pranav for relay to
the integrator. Continue independent parser and union-find work that can
be verified from the available specification; do not invent missing
competition rules or silently take over integrator-owned files. Do not
claim complete ingestion validation until the prerequisites are present.

Do not edit analytics, shared contract, Docker, runner, or dependencies
without coordinating a concrete change with the integrator. Do not alter
official fixture expectations, hard-code generated IDs, commit datasets,
publish a competition submission, or force-push. Do not assume access to
another agent or external messaging service.

Run meaningful focused tests if your environment permits. Finish with:
branch and commit if committed; changed files; exact commands and results;
PR URL (or explicitly state why a PR could not be created);
checks not run and why; contract compatibility; known limitations; and a
short coordination note that Pranav can paste to Karthik/integrator.
State the next action and owner for each unresolved dependency. Do not
report planned work as implemented or unrun tests as passed.
```

### Coordination message to send back

Copy the agent's final report to Karthik with this structure:

```text
Pranav / ingestion
Branch + commit:
PR URL + draft/ready status:
Files changed:
Contract: unchanged / proposed change with reason
Tests run + results:
Tests not run + reason:
Ready for integration: yes / no
Blockers and next action + owner:
```

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
