# Antigravity handoff — native pipeline performance

Prepared November 21, 2026. Karthik owns the optimization work.

## Resume point and latest scorer result

Repository: https://github.com/saikarthikbattula/shopping-sessions-duckdb

Current verified production main:
`6fd327f2a6d813d251857f3a057bb87a09c2c83a`, the human merge of PR #23.
Its tree matches the validated candidate at
`6b2998f36a3834d96112ad415ec7d0e8bba41d33`.

Karthik reports **5.12 seconds** after this merge. The previous reported score
was **4.78 seconds** with PR #19. The latest run is about **7.1% slower**.
The working assumption is that the 5.12-second submission used the production
SHA above; confirm the submission record if it becomes available.

The two scores span PRs #20, #21 and #23. We have no isolated scorer timing for
aaad475 immediately before PR #23, so this comparison cannot identify which
change caused the regression or distinguish it from run variation. The scorer's
full dataset size, bytes and stage logs remain unavailable.

**Goal: 0.59 seconds or lower with exact outputs.** Treat the local-to-server
performance mismatch as the immediate investigation. The current source is a
fully native Rust pipeline, with the Python/DuckDB implementation retained as a
reference. Earlier ingestion-only optimization documents describe historical work.

## Workflow and ownership

- Read root AGENTS.md, rules.md, docs/IMPLEMENTATION_PLAN.md,
  docs/DATA_CONTRACT.md and this handoff before editing. The original Pranav/
  Karthik split is historical; Karthik now owns the performance work throughout.
- Fetch origin and inspect git status/worktrees. Start a new codex/<task> branch
  from latest origin/main. Preserve other agents' work and existing branches.
- Commit/push only to task branches. Open PRs targeting main, using drafts for
  incomplete work or pending/failed required checks. Human maintainer merges.
- Never force-push, enable auto-merge, change expected answers, commit datasets,
  or submit a competition entry without explicit user authorization.
- Keep verification focused during implementation. Run the final correctness and
  restricted-container gates before calling a candidate validated.

## Commit and experiment map

| Reference | Meaning |
| --- | --- |
| 1e3b144 | PR #19 native implementation and CPU counters; original Antigravity handoff head |
| 63a4fc2 | PR #19 merge; reported scorer result 4.78s |
| ff03ee5 / PR #20 | RawEvent storage reduced from 80 to 64 bytes, exact wide timestamp fallback, full-key sort fallback for wide windows |
| ad246dc / PR #21 | Opt-in worker profiler, native benchmark harness, hot-person oracle, three archived experiment patches |
| aaad475 | Production merge through PR #21, including PR #20 |
| cf76272 / PR #23 | Word-at-a-time table hashing and cached canonical identity/routing |
| 6b2998f | PR #23 candidate integrated with aaad475 |
| 6fd327f | PR #23 merge; current production and assumed 5.12s submission |
| 33b888948690329d8a03ab51bd8327cabee76ab7 / PR #22 | Isolated diagnostic probe, intentional exit 42; keep out of main |

PR links use https://github.com/saikarthikbattula/shopping-sessions-duckdb/pull/NUMBER.

### What PR #23 changed

All implementation changes are in native/ingest/src/analytics.rs.

- FastHasher processes eight-byte words and a padded tail, incorporates length,
  and applies final mixing. HashMap/HashSet continue comparing complete keys.
  Changing this shared hasher also changes person-shard distribution and other
  table layouts, so its effects extend beyond the parsing instruction count.
- FrozenIdentity values now hold a ResolvedPerson containing the exact CompactId
  and cached u8 person shard. Events/clicks copy these directly for linked IDs,
  avoiding repeated canonical hashing and reconstruction.
- Added borrowed-key lookup tests across word boundaries and date parsing
  regressions. A multi-date timestamp cache was tried and discarded after slower
  measurements. Production retains the original single-entry date cache.
- The 128-bit event-ID fingerprint function was preserved.

### Local evidence that preceded the 5.12s score

Mac ARM64 development measurements used eight workers, existing generated inputs,
profiling disabled, and exact comparisons of both output tables. Initial PR #19
comparisons showed gains on both 5M seeds, including a confirmation run.

After integrating current main aaad475, two alternating three-run batches were
performed per seed. These medians include all six runs per executable:

| 5M seed | aaad475 process wall | Candidate process wall | Wall reduction | Baseline summed CPU | Candidate summed CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.6725s | 0.6194s | 7.9% | 3539.1ms | 3268.3ms |
| 2 | 0.6618s | 0.6204s | 6.2% | 3464.7ms | 3220.3ms |

Event-ingestion CPU fell 1351.4→1252.1ms and 1318.7→1231.8ms. Median peak RSS
increased about 52–58MB on 5M inputs. Individual batches varied materially,
including gains below 5%; all runs were retained in the combined figures.
These local improvements failed to establish a scorer improvement.

Local reports, if retained on this Mac:
`out-benchmark/ingestion-current-main*.json`.
Other trials use date-cache-*, word-hash-* and ingestion-final-* report names.
Reports contain executable/input hashes, wall time, CPU, peak RSS and stderr.

### Earlier experiments that were rejected

Read docs/NATIVE_EXPERIMENTS.md. Independent source patches against 63a4fc2
are in docs/experiments; use git apply --recount on a clean matching checkout.

- Incremental attribution counts/dense active campaign set lowered CPU but had
  inconsistent wall improvements across the two 5M seeds.
- Dynamic largest-first shard scheduling improved only about 1.6–1.9%.
- Winner-reference deduplication initially looked faster, then regressed on one
  seed in confirmation.

These runtime variants are archived patches, not active algorithms. A custom
dedup table and direct Arrow/Parquet streaming were not promoted.

## Server evidence obtained with the deliberate failure probe

The user submitted the diagnostic branch based on PR #19. A successful native
run emitted its timing logs and then intentionally returned 42, allowing public
validation stderr to be displayed. This is the **public 1.07M-event dev set**,
not the full dataset producing the 4.78s/5.12s scores.

| Stage | Wall ms | Summed CPU ms | Reported CPU share |
| --- | ---: | ---: | ---: |
| Identity | 18.217 | 16.410 | 1.2% |
| Events parse/identity lookup/routing | 164.469 | 593.444 | 42.9% |
| Events dedup | 31.603 | 213.449 | 15.4% |
| Click processing | 101.681 | 168.209 | 12.2% |
| Session analytics/sort/Parquet | 61.614 | 385.305 | 27.9% |
| Campaign write | 6.078 | 6.077 | 0.4% |

Total 1,071,554 parsed events, wall 296.476ms, reported CPU 1382.895ms,
1.291 CPU microseconds/event. Identity count 60,397. Click processing overlaps
event work; do not add all wall times. Reported CPU divided by pipeline wall is
about 4.7 core-equivalents, which mixes sequential phases and parallel execution.

Main's entrypoint already exits successfully. The exit-42 probe is isolated.
The user specifically asked to leave the normal entrypoint alone. Any further
intentional-failure probe should use its own branch and explicit user approval.

## Available profiling and reproduction

Build using Rust 1.85 and the committed Cargo.lock:

```sh
cargo build --release --locked --jobs 2 --manifest-path native/ingest/Cargo.toml
NCPU=8 NATIVE_PROFILE=1 DATA_DIR="$PWD/dev-benchmark-5m-seed1" OUTPUT_DIR="$PWD/out-profile" SCRATCH_DIR="$PWD/scratch" RUST_INGEST_BIN="$PWD/native/ingest/target/release/shopping-ingest" ./run.sh
```

NATIVE_PROFILE=1 adds analytics worker completion, sorting, attribution, combined
Arrow/Parquet construction/write time, row count and record-size logs. The stage
timings and summed thread CPU are emitted ordinarily. Finer event-ingestion
substage timings are still missing.

RawEvent is now 64 bytes. The earlier profiler measured RawPersonEvent at 48 and
LocalEvent at **24 bytes**, despite an existing comment claiming 16. Measure actual
sizes after any layout change. Sorting, attribution and writing are still
combined in the ordinary stage summary.

For a matched local comparison, independently build immutable baseline and
candidate executables and use a new report path each time:

```sh
.venv/bin/python scripts/benchmark_native.py /absolute/baseline /absolute/candidate dev-benchmark-5m-seed1 out-benchmark/new-comparison.json
```

The current harness uses macOS time -l and refuses Linux. For Linux performance
work, add a Linux-compatible measurement path or use native perf/time tools on
an existing authorized machine. GitHub's current 2CPU/2GB job provides correctness
and compatibility evidence. The scorer supplies 8 pinned vCPUs, 24GB, linux/amd64,
Intel Cascade Lake or newer, no runtime network, read-only root/data, uid1000,
64MB /tmp and writable /output plus /scratch with a combined 50GB limit.
The image/build limits are 8GB and ten minutes.

Existing local datasets are dev, dev-benchmark-1m-seed2,
dev-benchmark-5m-seed1 and dev-benchmark-5m-seed2. Public CSV answers are in
dev-answer. They are ignored and must stay outside commits/image context.

If this Mac still has the isolated tools, cargo/rustc live under
/private/tmp/shopping-rust/bin with CARGO_HOME=/private/tmp/shopping-cargo.
Use the repository .venv. Temporary paths and saved executables may disappear;
rebuild references from Git when unavailable.

## Verified gates and limits

The integrated PR #23 commit passed:

- 21 Rust release tests, including exact long IDs, compact timestamps and word
  hash borrowed-key lookup.
- 70 Python tests, including the hot-person scanning oracle with 5,000 clicks,
  400 sessions, and one/eight workers.
- All 36 fixtures and both direct public dev comparisons locally.
- Linux native build/tests, staging parity, raw differential checks, both backends'
  36 fixtures and public dev fingerprints under the restricted container.
- Image architecture/size and the ten-minute image build cap.

Successful CI:
https://github.com/saikarthikbattula/shopping-sessions-duckdb/actions/runs/44881553913

Exact strings, global earliest-ingestion event deduplication before validation,
invalid/late copies, timestamp boundaries, click byte-order ties, distinct assists,
session-start dates and typed empty outputs must all remain correct.

Known inherited limitation: event dedup uses a 128-bit fingerprint as its key
without comparing full event-ID bytes. Statistical collision arguments do not
establish exact equality. A change to this area must preserve full-key correctness.
CompactId's long-ID implementation also retains process-lifetime leaked storage;
cached linked identities reduce repeated construction but do not replace that
storage model.

## Immediate next work

1. Confirm the 5.12s submission SHA when possible. Preserve both scored references.
2. Compare PR #19, aaad475 and 6fd327f on identical Linux amd64 data/resources if
   existing hardware permits. Isolate PR #23's shared hasher change from cached
   identity/routing. Include worker/shard distribution and peak memory, since
   the hasher changes routing as well as hash cost. Avoid inferring an isolated
   regression from the two reported scores.
3. Split event ingestion measurements into decoding/timestamps, ID lookup/routing,
   event-ID hashing and buffering with opt-in coarse or sampled instrumentation.
   Disable profiling for acceptance timing. Avoid clock calls on every row.
4. Choose one measured change. Preserve exact output comparisons and alternate
   execution order. Use medians of at least three runs on both 5M seeds, plus
   focused skew cases. The existing local acceptance target is at least 5%
   end-to-end gain on both seeds; CPU-only reductions require further evaluation.
5. Before offering a submission candidate, run the final correctness and Linux
   container gates. Report the exact SHA, PR, measurements, memory tradeoffs,
   unrun checks and limitations. Karthik performs the scorer test.

Do not automatically revert PR #23 or assume a new rewrite will meet 0.59s.
The immediate outcome is an evidence-backed explanation and one independently
measured improvement or revert proposal for human review.

## Prompt to resume

Continue native pipeline optimization from this handoff. First inspect current
main, git status and applicable AGENTS.md instructions. Read this document and
rules.md. Karthik reports 5.12 seconds following PR #23 versus 4.78 seconds after
PR #19, with a target of 0.59 seconds or lower. Investigate the local-to-server
performance mismatch and isolate hashing/routing costs before selecting the next
change. Use task branches and PRs, preserve exact semantics, and leave merges
and competition submissions to Karthik. Return a concrete tested result with
its commit/PR and clearly labelled local, Linux and scorer evidence.
