> Historical project record from before the root-engine replacement.
> Current build/run instructions are in the root README.md.

# Rust input/output contract

The user-requested Rust-only migration retires PreparedInputs, the Python
prepare_inputs/run_pipeline interface, the Python/native staging adapter, and
intermediate events/clicks Parquet staging. Their historical definitions remain
available at Git revision 4e0f829. No production consumer uses those interfaces.

The official rules.md and final output schemas are unchanged. The production
interface is native/ingest's complete CSV-to-Parquet executable, documented in
its README. It reads every CSV in events/, clicks/ and links/, plus products.csv,
campaigns.csv and manifest.json. Input directories remain read-only.

Identity resolution uses all valid links before event/click assignment, including
unused minimum IDs and transitive shared-device components. Preserve exact UTF-8
string semantics. Deduplicate events globally by smallest ingestion time before
validation and lateness filtering. Preserve every specified millisecond boundary.

## Outputs

`user_daily/`: user_id (string), day (Parquet DATE), sessions (integer), events
(integer), revenue_cents (integer), top_category (nullable string).

`campaign_daily/`: campaign_id (string), channel (string), day (Parquet DATE),
sessions (integer), purchasing_sessions (integer), revenue_cents (integer),
assisted_sessions (integer).

Output folders contain one or more directly nested Parquet files, including typed
empty outputs. Row order and file count are irrelevant. Never create duplicate
logical keys or extra columns. Tests/schemas.py is a local validation definition,
not a production dependency or an alternative pipeline.

The specialized generator path and exact fallback are both Rust. Fast-path
assumptions must be verified; unsupported records select the exact computation.
Changing the implementation cannot change the official expected outputs.
