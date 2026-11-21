> Historical project record from before the root-engine replacement.
> Current build/run instructions are in the root README.md.

# Simple-user identity and compact sessionization

This increment starts from `origin/main` at `4d3547d4f98344f45a882a71527645d66bc6212f`.
The exact pipeline, official fixtures, published dev answers, and output contract
remain the correctness references. Sub-second 20M scorer performance is not yet
established by local development measurements.

## Identity proof and routing

`analytics/simple_identity.rs` derives an immutable, directly indexed user table
from the existing full link union-find's frozen result. Each entry records its
exact canonical name, person shard, connected alias mask, and complex flag.
Both sides of cross-user components are complex. Multiple disconnected trees,
large alias sets, uppercase canonical names, kiosks, and unrecognized encodings
use the existing exact identity tables.

A decoded generator user number is not itself proof of identity: link dirt can
leave aliases disconnected. Only aliases in the verified component mask use the
common user token. An absent alias 0 is its own person; other absent aliases use
exact resolution. Canonical names include all valid links, including unused IDs.
The direct table has at most 2^20 entries. The hash lookup remains available for
IDs outside its envelope. Simple tokens survive parsing and global event-ID
deduplication, and analytics uses a numeric user-to-local-person table so it does
not hash the same canonical name for every event again.

## Checked 16-byte records

`analytics/fast_records.rs` stores both parsed events and surviving events in
`FastEvent { w0: u64, w1: u64 }`:

- `w0`: relative timestamp in milliseconds (31), price cents (24), category (8),
  valid bit (1). Zero price encodes a non-purchase; validated purchases are positive.
- `w1`: person token (21), relative ingestion seconds (21), local event ID (22).
  Person tokens are either a simple user index or an index into that parse worker's
  exact exceptional-identity side array. Ingestion's origin is one day before the
  window, preserving early ingestion and the generator's late-ingestion range.

The requested 16-bit local event-ID field cannot cover the requested 5M workload
(5M / 64 exceeds 65,536); 22 bits avoid truncating those IDs. Eight category bits
preserve the existing local record representation. Every field has a checked
range; fractional relative ingestion seconds or values outside the envelope
return an error and invoke the exact pipeline. Relative event timestamps retain
all milliseconds. Invalid winners remain in global deduplication and cannot be
rescued by later valid copies. After deduplication, dead event-ID bits identify
the parse worker owning an exceptional person's side entry.

The session sort also uses a 16-byte `LocalEvent`, preserving the established
category and purchase representation.

## Counting scatter and click CSR

`analytics/click_index.rs` builds click offsets with counts and a prefix sum,
scatters by canonical component's local numeric person, then sorts each row by
timestamp and exact click-ID bytes. Attribution reads a person's range through
two offset accesses. Shared-device and split-alias components retain their exact
person identity. Initial `FastClick` buffers remain solely for global click-ID
deduplication; attribution consumes the CSR rows.

Events likewise use counting scatter by numeric person, then sort only that
person's timestamps with a single integer key. A worker reuses its event scratch
buffer between shards. Each person remains owned by one analytics shard; the
index requires no cross-worker synchronization. Existing inclusive credit and
assist boundaries and the strictly-greater-than-30-minute session rule remain.

## Other changes and cleanup

Key recovery polls an atomic stop flag every 256 candidates instead of acquiring
a shared mutex for every candidate. The result remains protected by the mutex and
is read only after all search threads have joined.

Scrambled event/click IDs must have canonical lowercase or whole-ID uppercase
encodings within 40 bits. A spelling-namespace bit keeps `eABC`-style mixed-case
variants on the exact fallback, and keeps valid lowercase IDs distinct from their
whole-ID uppercase counterparts. The decoded ordinal is checked before narrowing
to u32; unsupported encodings select the exact pipeline. Direct numeric product/campaign caches reject leading-zero
alternate spellings and integer overflow so exact map lookup handles them.

The CPU benchmark helper now uses macOS `wait4` resource measurements, including
key recovery and all worker threads. Linux retains GNU time measurements. Stage
CPU logs do not include all process CPU and are not used as whole-process totals.

Three retired experimental patch files were removed. Their evidence remains in
`NATIVE_EXPERIMENTS.md`, with reproduction commands using Git history. Generated
`gen_*` folders are excluded from Git and the Docker context. Existing local
handoff work and validation inputs are preserved.

## Validation

All commands below passed on the final release executable:

```sh
cargo test --manifest-path native/ingest/Cargo.toml
cargo build --release --locked --manifest-path native/ingest/Cargo.toml
.venv/bin/python scripts/run_fixtures.py fixtures -- native/ingest/target/release/shopping-ingest
native/ingest/target/release/shopping-ingest --data-dir dev --output-dir target/dev_test --workers 8
.venv/bin/python scripts/compare.py dev-answer target/dev_test
.venv/bin/python -m unittest discover -s tests -v
```

- Native unit tests: 47 passed.
- Official fixtures: 36/36 passed.
- Python tests: 77 passed.
- Published dev tables: user_daily 117,038 rows and campaign_daily 27,400 rows;
  zero differences in both directions and zero duplicate keys.

### 5M seed-1 check

Median whole-process wall: **1.962858s → 1.102507s**.
Median whole-process CPU: **6.932829s → 4.198250s**
(**39.44% reduction**). Median peak RSS: 767,508,480 → 520,421,376 bytes.
Parsed event copies: 5,357,964.

```sh
.venv/bin/python scripts/benchmark_cpu_candidate.py /private/tmp/shopping-simple-baseline native/ingest/target/release/shopping-ingest dev-benchmark-5m-seed1 target/simple-seed1-verified.json --workers 8 --require-fast-path
```

### 5M seed-2 check

Median whole-process wall: **2.129464s → 0.932623s**.
Median whole-process CPU: **7.220097s → 4.190221s**
(**41.96% reduction**). Median peak RSS: 751,190,016 → 532,316,160 bytes.
Parsed event copies: 5,315,568.

```sh
.venv/bin/python scripts/benchmark_cpu_candidate.py /private/tmp/shopping-simple-baseline native/ingest/target/release/shopping-ingest dev-benchmark-5m-seed2 target/simple-seed2-verified.json --workers 8 --require-fast-path
```

Each seed uses three alternating baseline/candidate rounds, eight workers, no
opt-in profiling, and whole-process user plus system CPU. Both tables match by
DuckDB EXCEPT ALL in both directions after every round. `--require-fast-path`
rejects a candidate that silently falls back. Reports include executable hashes
and reject executable changes during measurement. Baseline is the release build
of 4d3547d; the same existing generated inputs are used by both arms.

Linux amd64 Docker checks remain unrun locally because Docker is unavailable.
The 20M scorer target remains unverified; these are native Mac development
measurements, not scorer timings. Both seeds reduce CPU; sub-second wall time
is not consistently established even for 5M on this host.

Final benchmark executable SHA-256 values:

- baseline: `cb73238330d7fc6a1107b8e5228eeb6a39ec254c8266e56ceb71851fc40fb722`
- candidate: `54a81298bfbe142a559bcf713c597668327e3eec9f2335f53c4ebe6f513ed5ba`
