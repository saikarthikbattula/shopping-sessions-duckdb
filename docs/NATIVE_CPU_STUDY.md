# Native CPU study at bb16596

Baseline: `bb16596f59e601fc3b231e4608ace144a7949e42`.
[Initial Linux measurement run](https://github.com/saikarthikbattula/shopping-sessions-duckdb/actions/runs/44924694223).
Rust 1.85 release, optimization level 3, fat LTO, line-table debug symbols.
Two workers on a GitHub Linux x86 host; these are not scorer measurements.

## Ablations

Generated seed 1: 5,000,000 requested events and 1,250,000 clicks. The baseline
retains 5,357,964 parsed event copies before deduplication/validation. All CPU
per-event numbers below use that same denominator, including the discard arm.
CPU is whole-process user + system, not the pipeline's partial worker sum.
Each row uses three alternating baseline/ablation pairs. Instrumentation is
disabled in measured runs. Each ablation is built in an independent temporary
copy of the pinned baseline and deliberately produces incorrect outputs.

| Ablation | CPU µs/copy baseline → ablation | Wall seconds baseline → ablation | Peak RSS MiB baseline → ablation | Whole CPU reduction |
| --- | --- | --- | --- | --- |
| Constant category | 0.745 → 0.726 | 1.85 → 1.78 | 1100.8 → 1118.7 | 2.5% |
| Raw user, no canonical lookup | 0.734 → 0.635 | 1.77 → 1.59 | 1041.4 → 1097.5 | 13.5% |
| No event deduplication | 0.734 → 0.670 | 1.83 → 1.62 | 1107.6 → 1118.3 | 8.7% |
| Event CSV scan/split/discard only | 0.737 → 0.095 | 1.79 → 0.26 | 1041.7 → 4.6 | 87.1% |

The raw-user arm reduced event parsing CPU from 1686.9 to 1236.4 ms and click
CPU from 629.9 to 515.4 ms. Analytics increased from 790.4 to 864.0 ms as
identity groups changed. Constant categories reduced event parsing from
1787.0 to 1662.5 ms. Removing event deduplication reduced that stage from
576.2 to 194.6 ms; routing/filtering remains. These changes alter downstream
work, so their reductions are not additive or exact exclusive function costs.
The discard arm reads event files only and runs CSV field parsing; it skips
timestamps, identities, clicks, deduplication, analytics, and output.

Baseline system CPU is approximately 0.59–0.63 seconds (15–16% of total), with
roughly 281,000–301,000 minor faults and zero major faults. The discard arm
uses 0.02 system seconds and 731 minor faults. Raw scanning alone does not
explain the full process cost, but these results do not prove cache misses
are the dominant hardware stall.

Both `perf stat` and `perf record` were attempted. The runner refused hardware
events because `perf_event_paranoid=4` (stat exit 1, record exit 255). IPC,
hardware cache misses, and a sampled function split are **unavailable**.
No memory-bound conclusion is inferred without those counters.

## Selected experiment

Identity lookup is the largest measured individual lookup ablation. Test a
contiguous immutable table for aliases and canonical IDs of up to 15 bytes:
one hash, bounded-load linear probing, exact complete-key comparison, and
canonical bytes plus person shard in the same 33-byte cell. Longer IDs or
short aliases whose canonical ID is long retain the original exact map
fallback. Unlinked users retain their full identity. No sorting, deduplication,
timestamp, attribution, output-schema, or scorer-interface changes are made.
This experiment is retained only if whole-process CPU per event decreases.

## Reproduction and gates

```sh
python3 scripts/profile_native_cpu.py DATA_5M NEW_SCRATCH --workers 2
.venv/bin/python scripts/benchmark_cpu_candidate.py BASELINE CANDIDATE DATA NEW_REPORT.json --workers 2
```

The profiling workflow compares the correct candidate against the pinned
baseline on generated seed-1 1M, 5M, and 10M datasets. Every one of three
interleaved pairs must match both Parquet tables using DuckDB `EXCEPT ALL` in
both directions. The ordinary validation job retains the 36 official
fixtures, long-ID regressions, differential tests, public dev checks, and
restricted-container checks. Candidate performance results are pending.
Eight-vCPU timing and the ≤0.65-second target remain unmeasured.
