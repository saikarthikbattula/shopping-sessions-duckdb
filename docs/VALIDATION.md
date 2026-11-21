> Historical project record from before the root-engine replacement.
> Current build/run instructions are in the root README.md.

# Rust-only migration status

Local portable-runtime checks after removing the Python production pipeline:
40 Rust unit tests, 24 local validation tests, and all 36 official fixtures pass.
Both published dev tables have zero differences and zero duplicate keys. Python
is only used for local checks; the production image contains only the static Rust
binary and BusyBox utilities. The former staging protocol/tests are retired;
analytical boundary and randomized checks now execute the native binary directly.

The imported MIT-licensed AVX-512 candidate is isolated and unmeasured locally:
this ARM Mac cannot execute it. Its Linux workflow gates fixtures, public dev,
independent differential tests, and 5M/20M two-seed measurements before selection.
Static-musl image construction and restricted container checks are also pending
Linux CI because Docker is unavailable here. Do not treat earlier Python-backed
container checks below as validation of the new image or imported candidate.

---

# Integrated pipeline validation

Status as of November 20, 2026: public correctness and reduced-resource container compatibility pass on the integration branch. Full-size scorer performance is not yet established.

## Source and contract

- Analytics: Karthik commit `92e2fe3`, merged into main through PR #3.
- Ingestion: Pranav commit `8336d0d`, combined on `codex/integration-validation` without merging PR #4 to main.
- Integration changes add checks, CI, and documentation; the two implementations, shared signature/schemas, runtime dependencies, official fixtures and expected tables are unchanged.
- The official generator built with an isolated Rust 1.85 toolchain. Cargo refreshes a stale reference-package entry in the supplied lock during build; the imported lock is preserved unchanged in Git.

## Correctness evidence

- `make test`: 26 tests pass locally on Python 3.13 and in Linux CI on Python 3.12. Includes 12 staged differential seeds, each reversed and repartitioned, plus eight raw-CSV differential cases, each requoted/shuffled/repartitioned into two layouts. Raw cases construct expected semantics independently of production parsing and identity resolution, then use a brute-force analytical oracle.
- `NCPU=2 MEM_GB=2 make fixtures-local`: **36/36 official fixtures pass** through the ordinary module entrypoint. The 293 official fixture/harness files remain byte-identical to the starter.
- Generated the official seed-1 dev set with 1,000,000 events and 250,000 clicks. `make check-dev`: **MATCH**, including unique keys and every field in both tables. User daily: **117,038 rows**. Campaign daily: **27,400 rows**. No missing, extra, or differing rows; no duplicate keys.
- `make check-dev-fingerprint`: matches canonical SHA-256 fingerprints derived directly from the published answer CSVs. Only hashes/row counts are committed. A focused test verifies reordered/partitioned output matches and altered revenue fails. This check is solely for the public seed-1 dev set and is excluded from the runtime image.

## Linux container evidence

[Successful Linux validation run](https://github.com/saikarthikbattula/shopping-sessions-duckdb/actions/runs/44841621029) tested integration code at `a8a57d0`:

- Ubuntu Linux, Python 3.12, native `linux/amd64` Docker image.
- Image build about 12 seconds, inside the enforced 600-second build cap. Image size **328,054,508 bytes**, below 8 GB.
- Official fixture runner: **36/36 pass** with no runtime network, read-only root/data, uid 1000, a 64 MB tmpfs, dropped capabilities, no new privileges, 4,096 PID limit, and no swap beyond the memory limit.
- Generated seed-1 dev data runs successfully in the same restricted image; both table fingerprints match the published answers.
- CI uses **2 CPUs / 2 GB**, explicitly reduced test limits. It proves compatibility and public correctness under those limits; it does not reproduce the scorer's Xeon CPU, 8 vCPUs, 24 GB, hidden data or final sizes. Docker launcher timing is not a scorer measurement.

The workflow runs on PRs/main and is available for manual dispatch. GitHub Actions logs provide evidence for each revision; no automatic merge or submission is configured.

## Development profile

Native ARM64 Mac, Python 3.13, `NCPU=2`, `MEM_GB=2` (application configuration, not OS CPU/memory quotas), public seed-1 dev data:

| Measurement | Result |
| --- | --- |
| Process wall time including startup | 7.73 s |
| Pipeline preparation | 5.813 s |
| Pipeline analytics | 0.962 s |
| Pipeline total excluding startup | 6.775 s |
| Maximum resident set size | 340,525,056 bytes (about 325 MiB) |
| Sampled scratch plus output peak in separate profiled run | 46,079,185 bytes |

Preparation is about 86% of measured pipeline time. A separate cProfile run points to timestamp parsing, row parsing and per-row staging as the main Python costs. The profiled run is slower because of instrumentation; its timing is not used as the baseline above. Disk sampling at 100 ms may miss short-lived peaks. These are development measurements, not predictions for the scorer.

Reproduce native checks with Rust 1.85+ on PATH and `dev-answer.zip` unpacked as `dev-answer/`:

```sh
make setup test
NCPU=2 MEM_GB=2 make fixtures-local
make generator dev
NCPU=2 MEM_GB=2 make run-local
make check-dev
make check-dev-fingerprint
```

For CPU profiling, run the module under `python -m cProfile -o PROFILE_PATH -m pipeline.pipeline` with the same data/output/scratch environment. Runtime stage timing diagnostics go to stderr.

## Remaining gates and owners

- **Performance workstream / integrator:** profile representative 20M/60M datasets on 8 vCPUs and 24 GB, including all seeds/skew, container-start-to-exit time, peak memory and scratch/output disk. The 50 GB combined disk cap and final 180-second limit have not been proven at full size.
- **Performance workstream / integrator:** optimize preparation if full-size timings require it, preserving every exact comparison. No speed claim follows from a correct public dev run.
- **Human maintainer:** review and merge the ingestion/integration PRs. The integration PR stays draft pending remaining performance gates; a human can choose to merge the correctness baseline independently of final scoring readiness.
- **Team:** select a validated commit and submit it on the competition track. No competition submission has been made.

Generated inputs, expected answer rows, output files, profiling artifacts, compiled binaries and virtual environments remain excluded from Git and the runtime image.
