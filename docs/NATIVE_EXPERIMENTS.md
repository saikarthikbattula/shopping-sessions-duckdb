> Historical project record from before the root-engine replacement.
> Current build/run instructions are in the root README.md.

# Native pipeline experiments

Karthik owns this work. The scorer reference is PR #19 at 4.78 seconds, with a
0.59-second target. The retired attribution, balance, and references patches apply to
63a4fc2, the merge of PR #19. They remain available in Git history for
review and reproduction, rather than in the current source tree. On a clean
experiment branch at that baseline, retrieve one with
`git show 4d3547d:docs/experiments/NAME.patch | git apply --recount`.
All three patches passed this apply check against that reference source.

## Results and selection

Each comparison uses three alternating runs per executable, eight workers, the
same existing generated input, and exact comparisons of both output tables.
Process wall time includes startup. Summed thread CPU comes from native logs.
Peak RSS comes from macOS time -l. These are native Mac development measurements.

| Experiment | 5M seed 1 wall reduction | 5M seed 2 wall reduction | Decision |
| --- | ---: | ---: | --- |
| Incremental attribution window | -9.9% | 6.6% | Below threshold on seed 1 |
| Dynamic largest-first shard queue | 1.9% | 1.6% | Below threshold on both |
| Winner references, initial run | 6.2% | 31.6% | Required confirmation |
| Winner references, confirmation with opt-in profiling disabled | 6.4% | -15.1% | Inconsistent across seeds |

Negative reductions indicate slower execution. Every listed comparison matched
both tables exactly. The reference confirmation also matched both 1M seeds,
with wall reductions of 5.8% and 3.6%. None consistently met the required
5% wall reduction on both 5M seeds, so the production algorithms retain the
PR #19 implementation. No scorer improvement is established by these results.

Attribution used reusable campaign counts and a dense active set with indexed
removal. Sorted equal-time groups supplied the winning click without rescanning
ties. Its focused timestamp/expiry/reset regression and all Rust tests passed.
Dynamic scheduling used atomic queues sorted by shard row count, preserving whole
person shards. Winner references used Rust borrows into immutable parsed buffers
until dedup workers joined. The existing u128 event-ID hash keys were preserved.
Full-byte hash-collision verification remains an inherited correctness limitation.
The custom hash-table experiment is deferred.

## Reproduce comparisons

Build a separate clean reference checkout at 1e3b144 with Rust 1.85 and the
committed lockfile. Build a patched task checkout with the same settings.
Run the following for each existing 1M/5M seed directory, using a new report path:

```sh
.venv/bin/python scripts/benchmark_native.py /absolute/reference/shopping-ingest /absolute/candidate/shopping-ingest dev-benchmark-5m-seed1 out-benchmark/experiment.json
```

The harness disables NATIVE_PROFILE, checks executable hashes and input content
stability, requires exact table comparisons, and records wall time, CPU, peak RSS,
input bytes and executable hashes. Reports, outputs and generated datasets stay
outside Git. macOS time -l is required by this development harness.

## Profile the next change

Run the native executable with NATIVE_PROFILE=1 and the ordinary data/output
arguments. Each analytics worker reports completion wall time, sorting time,
attribution time, combined Parquet construction/writing time, row count, and
intermediate record sizes. The maximum worker completion time identifies the
stage's slowest worker. Existing stage logs provide event counts and summed CPU.
Input bytes are recorded by the benchmark harness.

The development profile measured RawEvent at 80 bytes, RawPersonEvent at 48,
and LocalEvent at 24. The existing source comment describing LocalEvent as
16 bytes differs from its actual layout. Compacting that representation is a
specific follow-up experiment. The focused hot-person regression exercises
5,000 clicks and 400 sessions against an independent scanning oracle with
one and eight workers, including equal-time winners, repeated campaigns and
expiry. It passed on the attribution and dynamic-scheduling executables.

Use these counters to investigate heavy-person attribution and memory traffic
before expanding the architecture. The 0.59-second scorer goal remains open.
