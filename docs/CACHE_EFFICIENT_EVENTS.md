# Cache-efficient native records

Baseline: `023349d4990b88ecd2238e93364804b976131f6f` (shared main).
This experiment implements general compact-record and partitioning techniques
independently. No Express Lane source code is incorporated.

## Representation experiment

`CompactId` uses 16 bytes instead of 32. IDs of up to 15 bytes are inline;
longer IDs retain the complete immutable allocation and its exact length.
Rust allocations are bounded by `isize::MAX`, leaving the length's high bit
available to distinguish the external representation. No ID syntax, numeric
suffix, maximum input length, or generator relationship is assumed.

This preserves the existing process-lifetime allocation ownership for long
IDs, extending that path to lengths 16–31. Workloads dominated by those lengths
may allocate more and must be measured separately. Equality, ordering, hashing,
and output all inspect the complete original bytes.

| Record | Baseline bytes | Candidate bytes |
| --- | ---: | ---: |
| CompactId | 32 | 16 |
| RawEvent | 64 | 48 |
| RawPersonEvent | 48 | 32 |
| RawClick | 80 | 48 |
| RawPersonClick | 80 | 48 |
| LocalEvent | 16 | 16 |

The event deduplication key, earliest-ingestion selection, validity and lateness
ordering, identity resolution, timestamp precision, attribution rules, output
schemas, and scorer entrypoint are unchanged. The inherited event-ID hash
collision limitation is not addressed by this representation experiment.

## Event partition experiment

Person and click partitions remain at 64. Event-ID partitions increase to 512,
with an initial table capacity of 16,384 entries instead of 131,072. Each
parsing worker's total initial event-vector reservation stays at 1,048,576
records (512 × 2,048 instead of 64 × 16,384).

Routing now mixes both halves of the existing event-ID hash using our existing
`FastHasher::finish`. The deduplication key itself is unchanged. Previously,
low FNV bits depended too heavily on particular ID bytes. A 50,000-copy sample
of the existing seed-1 five-million-event dataset occupied 33 of 64 partitions,
with 3,433 copies in the largest. The new routing occupied all 512 partitions,
with 126 copies in the largest. This sample measures distribution, not runtime.
The regression test varies interior bytes while holding boundary bytes fixed.

## Validation and measurement

The compact-ID-only experiment used three alternating pairs per seed, eight
workers, Rust 1.85 release with the same settings for both executables. Both
tables matched exactly in all six pairs. ARM64 development measurements:

| 5M dataset | Baseline → compact wall seconds | Baseline → compact peak RSS MiB |
| --- | ---: | ---: |
| Seed 1 | 4.507 → 4.564 | 907.3 → 743.6 |
| Seed 2 | 5.353 → 3.775 | 800.1 → 729.1 |

Compact IDs alone reduced memory and logged worker CPU on both seeds but did
not consistently reduce wall time.

The combined compact-ID and partition candidate passed all exact comparisons
in three new alternating pairs per seed:

| 5M dataset | Baseline → candidate wall seconds | Wall reduction | Baseline → candidate peak RSS MiB |
| --- | ---: | ---: | ---: |
| Seed 1 | 2.045 → 1.841 | 10.0% | 1010.2 → 636.5 |
| Seed 2 | 0.914 → 0.720 | 21.2% | 984.4 → 685.5 |

Both one-million-event seeds also passed three alternating pairs: wall time
fell from 0.243 to 0.203 seconds on seed 1 (16.4%) and from 0.222 to 0.162
seconds on seed 2 (27.1%). All 24 combined-candidate table comparisons across
the four datasets matched exactly.

Host conditions varied between experiment series; compare arms within each
paired series rather than timings across series. The logged CPU sum covers
selected workers, not whole-process CPU. It rose on seed 1 and fell on seed 2;
the consistent improvements above are wall time and peak RSS.

Measured executables, complete stderr, input and executable SHA-256 hashes,
and individual runs are retained in the ignored local directory
`out-benchmark/cache-events/`. Reproduce a paired comparison with:

```sh
.venv/bin/python scripts/benchmark_native.py BASELINE_BINARY CANDIDATE_BINARY DATA NEW_REPORT.json --workers 8
```

Each candidate must pass Rust and Python regression tests, the 36
official fixtures, published development answers, and exact baseline/candidate
table comparisons on both existing five-million-event seeds. Native ARM64
development timings do not establish a scorer or Linux x86 speedup.

Local checks: all 36 official fixtures passed; the candidate matched both
published public-dev fingerprints and the unpacked dev answers. The direct
native oracle test passed four random cases, original and opaque Unicode IDs,
one/eight workers, and two/seven input shards (16 comparisons). The original
74-test Python suite passed, followed by both native oracle tests and the full
75-test suite after adding the direct comparison. The final suite was run with
a fixed source snapshot because benchmark tests deliberately reject source
changes during a comparison. Standalone Rust ID-boundary and partition-distribution
checks passed. Clean Linux validation is pending. A local full Rust test build was interrupted by
compiler-cache cleanup and is not reported as passed.

The sub-one-second scorer target remains unmeasured. No competition submission
is made by this experiment.
