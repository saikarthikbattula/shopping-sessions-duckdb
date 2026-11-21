> Historical project record from before the root-engine replacement.
> Current build/run instructions are in the root README.md.

# Repeatable Rust measurements

Use an immutable existing dataset and two already built executables. Python is a
local measurement/checking dependency; the measured process is always Rust.

```sh
make setup native-ingest
.venv/bin/python scripts/benchmark_cpu_candidate.py /absolute/baseline native/ingest/target/release/shopping-ingest dev-benchmark-5m-seed1 out-benchmark/seed1.json --workers 8 --require-fast-path
```

The CPU harness alternates three rounds, records whole-process user+system CPU,
wall time, peak RSS and executable hashes, and compares both output tables with
DuckDB EXCEPT ALL in both directions. macOS wait4 and Linux GNU time include key
recovery and every worker. Stage CPU sums omit work and cannot substitute for
whole-process CPU. Reject silent fallback when measuring the specialized path.

The original Python/staging benchmark and DuckDB stage profiler are retired.
Their source and past reports remain available in Git history at 4e0f829.

## Imported AVX-512 candidate

On compatible Linux x86_64 hardware, build from native/express-lane so Cargo loads
its CPU configuration, then run:

```sh
.venv/bin/python scripts/benchmark_express_lane.py native/ingest/target/release/shopping-ingest native/express-lane/target/release/pipeline /absolute/data out-express/report.json --workers 8
```

The candidate harness compares trusted Rust, stock upstream settings, XL_SPIN=0,
and XL_SPIN=50 in rotating order. Every round must match both tables exactly.
It records fallback status, executable hashes and whole-process wait4 resources.
The Linux workflow runs this on 5M and 20M seeds 1/2 after fixtures, public-dev and
independent differential gates. A tuned variant must improve wall by at least 5%
and reduce CPU against both stock and trusted Rust on both seeds to be recommended.
No recommendation is a scorer result or permission to submit a competition entry.
