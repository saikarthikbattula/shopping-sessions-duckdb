# Batched native CSV parsing

Baseline: shared main `0c5093ddc36073c3fecf089b7c6dfe24c082371b`, the merged
compact-ID and event-partition optimization. This increment independently
implements buffer-wide structural scanning; it incorporates no external source.

## Processing change

The previous reader searches for each LF, then scans that row again for commas
and quotes, then validates quoted fields. The new scanner classifies comma,
quote and LF bytes across a buffer and emits already separated fields directly
to the existing event/click processing callbacks.

AVX2 hosts classify 32 bytes per block, selected at runtime once per buffer.
Other hosts use exact eight-byte equality masks with a scalar tail. Loads stay
inside the supplied slice. Quote counts and field boundaries establish that
quotes occur only around an entire field; no normalization or CR stripping is
performed. Malformed rows and empty IDs retain their existing admission rules.

The bounded file reader still reads at most the planned chunk. A row spanning
buffers is assembled and checked by the existing line parser. The same fallback
handles an unterminated final physical line. Header skipping, input discovery,
thread counts, identity resolution, deduplication, timestamps, attribution,
output schemas and the scorer entrypoint are unchanged.

The original line reader remains available for the existing scratch-only CPU
ablation control. Product, campaign and identity lookup parsing is unchanged.

## Validation

The standalone module and reader harness passed eight tests: exact byte masks,
quote/null/CR rules, malformed and incomplete rows, block boundaries, 1,000
random structural-byte cases, every buffer capacity from 1 to 65 bytes, rows
longer than 512 KiB, bounded reads, cancellation and callback/read errors.
The x86 module cross-compiles with warnings denied. The full native release
build and all 37 Rust tests passed. All 75 Python regression tests, the 36
official fixtures, both published dev fingerprints, and exact comparisons with
the unpacked dev answers passed. The direct native oracle includes opaque
Unicode IDs and one/eight workers.

The scalar mask tests include adjacent bytes equal to a delimiter XOR 1. The
usual subtract-one zero-byte trick can falsely classify such neighbours because
of carries; the implementation uses exact independent seven-bit additions.

## Measurement

Use the existing `scripts/benchmark_native.py` harness against the immutable
baseline executable, with three alternating pairs and exact output comparisons
on both 1M and 5M seeds. Keep compilation outside measured runs. Local reports,
executables and scratch harnesses stay in ignored `out-benchmark/batched-parsing/`.

Initial eight-worker ARM64 pairs matched both tables exactly on both 5M seeds.
Seed 1 wall medians were 0.530 seconds baseline and 0.582 candidate (9.7% slower);
seed 2 was 0.504 and 0.429 (14.9% faster). Logged worker CPU fell on both, but
seed 1 had substantial variation in downstream analytics time. Confirmation,
smaller-dataset comparisons and native Linux x86 measurements determine whether
to retain the change. A second seed-1 series measured 0.525 to 0.502 seconds;
including all six seed-1 pairs gives medians of 0.527 to 0.511 (3.2% faster).
The 1M seed-1 series measured 0.161 to 0.150 seconds (6.8% faster).
The 1M seed-2 series measured 0.127 to 0.117 seconds (7.6% faster). All 30 table
comparisons, including the confirmation series, matched exactly. Native Linux
x86 performance validation is still required before promotion.

The actual scorer's sub-one-second target and 20M/60M behavior remain unmeasured.
