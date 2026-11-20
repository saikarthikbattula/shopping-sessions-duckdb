# Shared ingestion-to-DuckDB contract

Status: agreed planning interface; code is to be created during starter integration. The supplied rules remain authoritative.

## Modules and calls

Integrator owns `pipeline/contracts.py` with frozen `PreparedInputs` and these fields:

- `events_dir: Path`, `clicks_dir: Path`: staged Parquet directories.
- `products_path: Path`, `campaigns_path: Path`: staged lookup Parquet files.
- `window_start_ms: int`, `window_end_ms: int`: manifest boundaries.

Pranav implements `prepare_inputs(data_dir: Path, scratch_dir: Path, *, batch_rows: int = 65536) -> PreparedInputs` in `pipeline/ingest.py`. Helpers belong in `pipeline/parsing.py` and `pipeline/identity.py`.

Karthik implements `run_pipeline(con: duckdb.DuckDBPyConnection, prepared: PreparedInputs, output_dir: Path) -> None` in `pipeline/analytics.py`; SQL may live under `pipeline/sql/`.

Integrator owns the existing `pipeline/pipeline.py`: environment/configuration, DuckDB connection, ingestion call, analytical call, timings, and failure exit. It owns Dockerfile, run.sh, Makefile, requirements, and harness integration. Neither owner creates a connection or executes the pipeline on module import.

## Staged schemas

String fields are Arrow UTF-8; milliseconds and prices are signed int64; `payload_valid` is a non-null Boolean. Nullability below is explicit. Numeric staging never uses floats.

| Table | Columns |
| --- | --- |
| events | `event_id: string NOT NULL`, `person_id: string nullable`, `ingest_ms: int64 NOT NULL`, `ts_ms: int64 nullable`, `event_type: string nullable`, `product_id: string nullable`, `price_cents: int64 nullable`, `payload_valid: bool NOT NULL` |
| clicks | `click_id: string NOT NULL`, `person_id: string nullable`, `campaign_id: string nullable`, `ts_ms: int64 nullable`, `payload_valid: bool NOT NULL` |
| products | `product_id: string NOT NULL`, `category: string NOT NULL` |
| campaigns | `campaign_id: string NOT NULL`, `channel: string NOT NULL` |

Events: drop only malformed lines and empty event IDs before staging. Keep every other copy, including invalid and late ones. `payload_valid` means non-empty user, valid in-window timestamp, exact allowed type, non-empty product, and valid purchase price where applicable. Lateness is deliberately excluded and applied by Karthik after deduplication. Invalid/out-of-window timestamps become null. `price_cents` is null for non-purchases and invalid prices. For valid purchases it is the parsed integer. Ingestion times are always valid by the input rules.

Clicks: drop only malformed lines and empty click IDs before staging. Keep remaining copies; `payload_valid` means non-empty user, valid in-window timestamp, and known campaign. Invalid/out-of-window timestamps become null. Karthik deduplicates before filtering.

Canonical person names derive from all valid links, independent of event/click validation. Null users remain null. Unlinked IDs map to themselves. Do not normalize strings or replace IDs with numeric suffixes. UTF-8 byte order governs person names and every specified tie.

## Staging and operational behavior

- Write batches to a dedicated subdirectory of the provided scratch directory. Read every input `*.csv`, skip exactly its first physical header line, and process header-like data rows normally.
- Use typed Parquet even for empty inputs: each staged directory must contain at least one readable file with the agreed schema.
- No ordering guarantee across or within staged files. Do not partition events by user before global event-ID deduplication.
- Use the same strict CSV dialect for lookups. They are guaranteed clean and unique; stage their strings unchanged.
- Do not delete staged files until the analytical consumer finishes. Integrator manages successful-run cleanup and shared memory budgeting.
- Malformed input rows are skipped as specified; operational failures raise exceptions and must not produce a success exit. No runtime downloads or stdout data dumps.

Contract changes must be coordinated before either owner changes schema or stage ordering. Document proposals in the handoff results; do not silently adapt one side.
