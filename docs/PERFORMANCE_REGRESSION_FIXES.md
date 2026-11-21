# Native performance regression investigation

Reference: production commit `f5f40eb6e1f19e4ecf7763eb4b07e36e555eff93`.
The latest reported scorer result is 5.52 seconds; the preceding reported
results were 5.12 seconds and 4.78 seconds. Those observations alone do not
establish which change caused a regression, because scorer stage timings
and input size are unavailable.

## Changes

- Limit each chunk's underlying reader to its assigned bytes **before**
  buffering. A 512 KiB buffer previously could fetch beyond the chunk end.
  This becomes more costly as chunks shrink. LF-aligned boundaries and
  unterminated final records retain their existing handling.
- Remove the parsing identity caches keyed by first byte, last byte, and
  length. A 100,000-line sample of the existing 5M seed-1 events gave only
  1,579 hits in 99,821 eligible lookups (1.58%). Cache misses still performed
  the canonical map lookup and copied the ID into the cache. Analytics uses
  a single last-person cache instead of the same collision-prone slot array.
- Schedule click deduplication, event deduplication, and analytics shards
  through a shared largest-first queue. Each shard is claimed once; each
  person still belongs to one analytics shard. Worker-local allocations
  remain reusable across jobs.

No schemas, ingestion ordering, attribution rules, timestamp parsing SIMD,
or runner exit behavior change. These fixes address observed implementation
costs; their impact on scorer runtime requires a pinned practice submission.

## Validation and measurement

Local Apple-silicon validation: 25 Rust tests, 70 Python tests, 36 official
fixtures, public dev fingerprints, and direct comparisons of both published
dev-answer tables pass. New Rust regressions verify that buffered reads
cannot cross the byte limit and that 1/2/8 workers claim every shard once.

The `native-performance` CI job builds the reference and candidate separately
with Rust 1.85, then benchmarks 1M/5M inputs with seeds 1 and 2. It uses two
workers on a Linux x86 host and reports three alternating runs' median process
wall time, summed worker CPU, peak RSS, and exact output equality. Profiling
is disabled. Summaries appear in the GitHub Actions job summary. These host
measurements are separate from restricted-container correctness and scorer
timing; two workers do not reproduce the scorer's eight-vCPU allocation.

The portable comparison command is:

```sh
.venv/bin/python scripts/benchmark_native.py BASELINE_BINARY CANDIDATE_BINARY DATA_DIR NEW_REPORT.json --workers 2
```

Keep this PR draft until Linux correctness/container checks finish. Review
both 5M seeds before retaining the combined performance changes; improvements
below 5% are inconclusive. Karthik reviews/merges and owns the next pinned
scorer submission. No improvement toward 0.59 seconds is claimed yet.
