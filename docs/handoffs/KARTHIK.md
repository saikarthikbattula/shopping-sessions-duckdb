# Karthik handoff: DuckDB analytics and output tables

Status: prepared; not sent externally and implementation has not started.

## Objective and ownership

Implement the analytical side of the official DuckDB starter solution. Read [the implementation plan](../IMPLEMENTATION_PLAN.md) and [the shared data contract](../DATA_CONTRACT.md) first, then the supplied `rules.md`.

Own `pipeline/analytics.py`, optional `pipeline/sql/`, and `tests/test_analytics.py`. Use the integrator-provided connection and contract. Do not edit Pranav's ingestion modules, the entrypoint, Docker, or dependencies without coordination. Begin with synthetic staged Parquet matching the contract so work can proceed independently.

## Required behavior

- Read every staged part. Deduplicate events globally on event ID using minimum ingestion time; equal-time copies are identical. Only then filter `payload_valid` and lateness. Keep exactly 24 hours late and negative ingestion delays. Deduplicate clicks on click ID before filtering their validity.
- Sessionize by canonical person with integer millisecond ordering. Use an explicit row window when accumulating boundary markers so equal timestamps share one session. Split only for gaps over 1,800,000 ms. Derive UTC day from session start; carry it onto every event and purchase in that session.
- Aggregate user/day sessions, events, revenue, and category purchase revenue. Resolve top-category ties by byte order; return null when there are no purchases. Unknown products use `unknown`.
- Credit each session with the most recent eligible click. At equal timestamps, smallest click ID wins by byte order. Collapse those ties before an as-of join, then enforce the inclusive 604,800,000 ms bound. Missing eligible clicks produce organic credit and channel.
- Count each assisting campaign once per session, including the winning campaign. Merge each person/campaign's inclusive click eligibility intervals using a running maximum prior end, handling nested intervals. Join session starts to merged intervals. Preserve exact endpoints and avoid the raw session-to-click expansion.
- Aggregate credited and assisted results separately and combine their keys. Include assisted-only campaign/day rows with zero credited metrics. Organic assists are always zero. Derive purchasing sessions from purchase existence, not revenue heuristics.
- Write both exact output schemas as Parquet with DATE days and integer totals; produce schema-correct empty tables. Use default byte-order string semantics and no locale collation; verify ties with non-ASCII test data.
- Release intermediate tables when no longer needed and expose useful stage timings to the integrator. Keep spills under the configured scratch location. Do not set an independent memory/thread budget.

## Acceptance evidence

- Synthetic staged-input tests for invalid earliest duplicates, cross-person duplicate copies, exact lateness/session/attribution boundaries, equal timestamps, midnight-crossing sessions, unknown products, category and click-ID ties, and typed empty outputs.
- Assist tests for repeated campaign clicks, nested/overlapping/touching intervals, expired clicks, future clicks, multiple campaigns, and assisted-only rows.
- Unique output keys and matching total sessions/revenue across the two tables. Verify organic assists are zero and summed assists cover all non-organic credited sessions.
- Once ingestion is integrated, all 36 official fixtures and both public dev tables must match exactly. A passing synthetic test set alone does not satisfy this gate.

## Return handoff

Provide changed files, exact commands/results, query timing and memory observations if measured, and unresolved limitations. Flag contract changes explicitly. Leave Docker/full-image checks and submission packaging to the integrator. Do not publish, submit, or send external messages as part of this work.
