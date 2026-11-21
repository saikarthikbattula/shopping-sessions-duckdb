# One-off performance iteration

Baseline: cb70c668a91cbc720b4afe01cf1a8626804cf10b, the submitted root engine.
This branch contains measurement/validation tools only; production code is unchanged.
The workflow exists only on codex/performance-lab, not main, and the repo remains private.

Primary metric: whole-process wall seconds, including startup, input reading,
writing and teardown. Each variant must match both tables exactly, reduce median
wall by at least 5% on both seeds and sizes, reduce whole-process CPU, and stay
inside 24 GiB RSS before promotion is considered. Reconfirm the winner as a source
change against the baseline; runtime-knob experiments alone do not change defaults.

Budget: two standard GitHub runner attempts initially, no paid runner provisioning,
four one-knob variants plus baseline, three rotating rounds, 5M/20M seeds 1/2,
180-second per-process timeouts, 30-minute per-job limit. A runner without the
required AVX-512 features records an explicit unsupported result and does not
execute the binary or produce a timing claim. CI hardware is not scorer hardware.

Hard gates before timing: Rust unit tests, all 36 official fixtures, independent
raw CSV/boundary tests, opaque and >40-bit ID ranges, shuffled/repartitioned links,
shifted date windows, and public dev fingerprints. Generator-specific assumptions
are not evidence of eligibility; organizer clarification remains separate.

Variants: XL_SPIN=0, XL_SPIN=50, XL_ENDREL=0, XL_S3=0. Everything else is fixed.
Artifacts retain compiler/container/binary hashes, input hashes, CPU details,
whole-process CPU/RSS/fault counts, stage profile, output comparisons, failures
and timeouts. An interrupted/incomplete ledger is not a passed benchmark.
