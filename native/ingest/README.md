# Serial ingestion helper

Build with Rust 1.85.0 and the committed Cargo lockfile:

```
cargo build --release --locked --manifest-path native/ingest/Cargo.toml
```

The executable accepts `--data-dir`, `--stage-dir`, `--window-start-ms`,
`--window-end-ms`, and `--batch-rows`. The Python adapter validates the manifest
and supplies an existing empty staging directory. This program does not read the
manifest. It stages every parsed event/click copy with a nonempty ID, preserving
invalid payloads and late events for downstream deduplication.

All parsing and output are serial, with LF-only buffered input and strict UTF-8.
Links are resolved before staging with union by size and path compression, then
frozen into canonical string lookup. Output uses Snappy Parquet with exact shared
Arrow schemas, including empty files. Each batch/row group has at most the lesser
of the requested row count and 65,536 rows. A batch also flushes after its estimated
string/column buffers reach 16 MiB. Explicit writer flushing bounds accumulated
row groups. This is an estimate, not a hard process memory cap: identity and
campaign membership state, the largest physical input line/row, temporary Arrow
conversion buffers, allocator overhead, and Parquet encoder memory are additional.
One table writer is active at a time.

The program writes no stdout. Errors use stderr and exit 1, invalid ingestion
timestamps exit 2, and CLI errors exit 64. `_SUCCESS.json` is atomically renamed
into place only after all four Parquet writers close successfully. Partial output
may remain on failure; the adapter owns cleanup and never accepts it without the
success marker.

Run focused Rust tests with `cargo test --locked --manifest-path
native/ingest/Cargo.toml`. Full native/Python parity and analytics validation are
owned by the integration harness.
