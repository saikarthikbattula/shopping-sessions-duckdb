# Native ingestion helper

Build with Rust 1.85.0 and the committed Cargo lockfile:

```
cargo build --release --locked --manifest-path native/ingest/Cargo.toml
```

The executable accepts `--data-dir`, `--stage-dir`, `--window-start-ms`,
`--window-end-ms`, `--batch-rows`, and optional `--workers` (positive integer,
serial by default). The Python adapter validates the manifest and passes its
validated NCPU worker limit. The helper receives an existing empty staging
directory and never downloads data or writes input files.

Links and campaign membership are resolved serially, then immutable canonical
string maps are shared by workers. Events and clicks run as successive phases.
Input files are divided into approximately 16 MiB descriptors whose endpoints
fall after LF. Planning reads only headers and boundary suffixes rather than
scanning all records twice. The first physical header is skipped exactly once,
and final records without LF are retained. A record larger than a chunk is
processed once as a whole record. Strict UTF-8, CR preservation, timestamp/price
parsing, and retention of invalid/late copies are unchanged.

An atomic job index distributes chunks across at most the requested workers,
with fewer workers when there are fewer chunks. Each worker has one Arrow/Parquet
sink and at most one output part per table. Batches flush at the lesser of the
requested row count and 65,536 rows, or about 16 MiB estimated buffered column
bytes. Writer row groups flush explicitly. Identity/campaign state, the largest
physical line, temporary Arrow conversion copies and codec buffers are additional
memory costs; process memory is not capped by these estimates. Empty tables have
at least one typed Parquet file. No new Rust dependencies or unsafe code are used.

The version-2 `_SUCCESS.json` marker includes total row counts and exact
`parts.events`/`parts.clicks` lists of objects containing `name` and `rows`.
Lookups retain their fixed filenames. The marker is atomically renamed last,
only after every worker joins and every writer closes successfully. Workers
cancel further job allocation on an error or panic; all launched workers are
joined before returning failure. Partial files may remain for adapter cleanup.
No success marker is published after failure.

The program writes no stdout. Errors use stderr and exit 1, invalid ingestion
timestamps exit 2, and CLI errors exit 64. Run focused Rust tests with
`cargo test --locked --manifest-path native/ingest/Cargo.toml`. Full staging and
final-output parity remain integration gates. Python stays the default backend.
