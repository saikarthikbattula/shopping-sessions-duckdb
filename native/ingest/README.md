# Production Rust pipeline

This executable performs the complete CSV-to-Parquet computation. The Python
pipeline, adapter, and intermediate Parquet staging protocol have been retired.

```sh
cargo build --release --locked --manifest-path native/ingest/Cargo.toml
cargo test --locked --manifest-path native/ingest/Cargo.toml
native/ingest/target/release/shopping-ingest --data-dir dev --output-dir out --workers 8
```

It also accepts positional DATA_DIR OUTPUT_DIR arguments for the official fixture
runner. With no arguments, DATA_DIR and OUTPUT_DIR environment variables default
to /data and /output. NCPU is a positive explicit worker count; otherwise Linux
cgroup v2/v1 quotas and available parallelism determine the count, capped at 16.
Optional --window-start-ms and --window-end-ms must be supplied together and
satisfy start < end; ordinarily manifest.json supplies the window.

Unknown, duplicate and retired staging options fail with exit 64. Operational
errors exit 1, and invalid guaranteed ingestion timestamps exit 2. Diagnostics
use stderr; successful runs write only the required Parquet output tables.

The generator-specialized pipeline has checked packing/ID invariants and falls
back to the exact Rust implementation. No Python or DuckDB runtime is involved.
The independent scalar timestamp parser is compiled only for unit tests.

See ../../docs/SIMPLE_USER_FASTPATH.md for identity, packing and CSR details.
