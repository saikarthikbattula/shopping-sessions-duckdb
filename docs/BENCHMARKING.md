# Repeatable baseline measurements

This native-development benchmark compares the current checkout with submitted
commit `7637e570c8221001a7dc8ce7233d9443dfc2d947`. It does not change the pipeline,
choose its default backend, commit code, build containers, or submit a score.
Karthik owns this workstream.

## Run a comparison

Use the pinned development environment and an existing immutable dataset:

```sh
make setup
make benchmark DEV=dev BENCH_REPORT=out-benchmark/python-control.json
```

The default is three runs per arm at `NCPU=2`, `MEM_GB=2`. The command alternates
the order: baseline/candidate, candidate/baseline, baseline/candidate. Every run
has fresh output and scratch directories. Both arms use the same Python
interpreter and installed dependency versions.

For explicit options or future backend selection:

```sh
.venv/bin/python scripts/benchmark.py dev \
  --baseline-ref 7637e570c8221001a7dc8ce7233d9443dfc2d947 \
  --candidate-root . --runs 3 --ncpu 2 --mem-gb 2 \
  --sample-interval 0.1 --report out-benchmark/rust-candidate.json \
  --baseline-env INGEST_BACKEND=python \
  --candidate-env INGEST_BACKEND=rust \
  --candidate-env RUST_INGEST_BIN=/absolute/path/to/shopping-ingest
```

The Rust selector above becomes useful only when the later ingestion adapter is
implemented. Environment values are recorded as **requested configuration**, not
proof the selected code honored them. Step 1 itself adds no Rust backend. A
comparison of the unchanged Python pipeline against itself is a control run,
not an optimization result.

Only `INGEST_BACKEND` and `RUST_INGEST_BIN` overrides are allowed. Ambient values
of those variables are removed unless explicitly requested. Resource settings,
data/output/scratch paths, HOME, TMPDIR, and Python import paths are controlled
by the benchmark and cannot be overridden per arm. Arbitrary inherited
environment values or credentials are not written into the report.

## Correctness gates

The baseline is exported using `git archive` into a benchmark-owned temporary
directory. No branch, worktree, baseline commit, or submitted history is changed.
Unsafe archive entries, including links, special files, and escaping paths, are
rejected. The candidate runs from its current checkout; source fingerprints
include tracked and untracked, non-ignored files, excluding benchmark artifacts
and the separately fingerprinted input dataset.

Every output, including the first baseline output itself, is validated with the
**archived baseline's** exact comparison script. Comparison and hashing are
outside measured process time. A candidate cannot weaken the checker by editing
its own `scripts/compare.py`. The benchmark requires both Parquet output
directories and rejects root-level answer CSVs that could shadow Parquet in the
comparison script.

Nonzero exits, missing/inconsistent stage timings, output differences, duplicate
keys, changed inputs, or changed candidate source invalidate the comparison.
The JSON failure report contains no median/speedup summary. An `ok` result means
equivalence to the reference baseline; it does not independently establish hidden
data correctness or a competition score.

## Report and artifacts

The report records:

- Resolved baseline/candidate commits, candidate dirty state and source fingerprint.
- Dataset content fingerprint, file count and bytes before/after execution.
- Interpreter, package versions, OS, architecture, CPU count and shared settings.
- Each run's preparation/analytics/pipeline timings and startup-inclusive wall time.
- OS high-water RSS, sampled process-tree RSS, sampled scratch/output use, and diagnostics.
- Per-arm medians and baseline/candidate wall-time speedup only after all gates pass.

Choose a new report filename for every experiment. Existing reports or the
associated `<report-stem>.logs` directory are never overwritten. Per-run stdout,
stderr and comparison logs remain alongside the report. Only the randomly named
benchmark-owned temporary directory is removed after execution. Unrelated files
are preserved. SIGINT/SIGTERM stop the owned child process group before cleanup.

Artifacts cannot be placed inside the input tree. Input root aliases are
resolved, but symlinks inside consumed input files/directories or candidate
source files are unsupported and rejected. The input directory cannot contain
the candidate checkout. Default `out-benchmark/` artifacts are already ignored
by Git and excluded from Docker context; do not commit custom-path artifacts.

## Interpret measurements accurately

- Native execution does **not** enforce CPU, memory, filesystem or network
  restrictions. `NCPU` and `MEM_GB` configure the application, not OS quotas.
- `max_individual_rss_bytes` uses `wait4`: macOS reports bytes; Linux reports KiB
  and is normalized to bytes. This is the largest individual-process high-water
  RSS reported by the OS, including reaped descendants, **not** simultaneous
  combined process-tree memory.
- `sampled_tree_rss_peak_bytes` sums live descendants. It can double-count shared
  pages and miss peaks between samples. An unavailable sampler produces null and
  a diagnostic, never a fabricated zero.
- `sampled_disk_peak_bytes` measures logical file sizes under the run's output
  and scratch roots. It may miss short-lived spills and does not prove a disk quota.
- Filesystem caches are uncontrolled. Alternating order reduces ordering bias but
  does not establish cold-cache performance. Keep resource settings, inputs and
  machine conditions comparable.
- Mac development, ordinary Linux CI, and scorer results must remain separate.
  These numbers do not predict timings on the scorer's Xeon/8-vCPU/24-GB setup.

The eventual promotion gate remains exact correctness plus matched multi-seed
1M/5M comparisons demonstrating the planned improvement; a fast but incorrect
candidate never receives a speedup summary.

## Initial control validation

The first control used the official seed-1 1M-event/250K-click dataset on the
ARM64 development Mac with Python 3.13, NCPU=2 and MEM_GB=2. Three runs per arm
passed all exact output comparisons and before/after input/source fingerprints.

| Median measurement | Archived baseline | Unchanged Python candidate |
| --- | --- | --- |
| Process wall time | 6.467 s | 6.595 s |
| Preparation | 5.359 s | 5.607 s |
| Analytics | 0.883 s | 0.848 s |
| Individual-process high-water RSS | 343,015,424 bytes | 339,574,784 bytes |
| Sampled scratch/output logical bytes | 44,922,797 | 44,922,797 |

Wall speedup was 0.981x, a reference-versus-reference control result rather than
an optimization. The local report is `out-benchmark/python-control-20261120.json`
with sibling logs; artifacts are ignored and are not part of the submitted image.
All 49 unit/integration tests passed, including 23 benchmark tests; the benchmark
module reached 91% combined branch/line coverage. CI and scorer timings for this
benchmark-only increment are not claimed.
