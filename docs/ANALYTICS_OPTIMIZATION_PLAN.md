> Historical project record from before the root-engine replacement.
> Current build/run instructions are in the root README.md.

> Historical workstream record. Python/staging commands below are retired.
> Use the current README.md and DATA_CONTRACT.md for the Rust-only runtime.

# DuckDB Analytics Optimization Plan

**Owner:** Karthik<br>
**Source:** Profiling evidence from Step 6 (`inline:rust-optimization#step-6`)<br>
**Prerequisites:** Rust ingestion default (PR #10 merged into `main`)

---

## 1. Goal and Context

With the parallel Rust ingestion engine promoted to default, data preparation time dropped by **10–14x** (from ~28–36s down to ~2.1–2.5s on 5M datasets).

As a result, **DuckDB analytics is now the dominant pipeline component**, taking **52% to 68% of total wall-clock time**.

| Metric | Baseline (Python default) | Current (Rust default) | Shift |
|---|---|---|---|
| Preparation (5M) | 27.8s – 36.1s | 2.1s – 2.5s | 10x – 14x faster |
| Analytics (5M) | 2.5s – 4.6s | 2.3s – 6.5s | **Dominates total time (52%–68%)** |
| Total Wall Time (5M) | 30.5s – 41.5s | 4.5s – 9.6s | 4.3x – 6.0x overall speedup |

---

## 2. Profile Breakdown of Analytics Stages

Using `scripts/profile_analytics.py`, we measured per-stage execution times over 3 repeated runs on `dev-benchmark-5m-seed1` and `dev-benchmark-5m-seed2` (`NCPU=8`, `MEM_GB=4`):

```
┌───────────────────────────────────────┬────────────┬────────────┐
│ Stage                                 │ 5M seed-1  │ 5M seed-2  │
├───────────────────────────────────────┼────────────┼────────────┤
│ session_events (boundary window)      │   0.911s   │   0.801s   │ (35% - 39%)
│ events (deduplication & validation)   │   0.561s   │   0.649s   │ (24% - 28%)
│ assist_intervals (island merging)     │   0.200s   │   0.228s   │ ( 8% - 10%)
│ assists (interval range join)         │   0.142s   │   0.164s   │ ( 6% -  7%)
│ credited (ASOF join)                  │   0.108s   │   0.110s   │ ( 4% -  5%)
│ clicks (deduplication)                │   0.090s   │   0.098s   │ ( 4% -  4%)
│ sessions (session aggregation)        │   0.080s   │   0.088s   │ ( 3% -  4%)
│ top_categories (QUALIFY & ranking)    │   0.064s   │   0.074s   │ ( 3% -  3%)
│ winning_clicks                        │   0.050s   │   0.052s   │ ( 2% -  2%)
│ user_daily                            │   0.048s   │   0.053s   │ ( 2% -  2%)
│ campaign_daily                        │   0.008s   │   0.009s   │ (<0.5%)     │
└───────────────────────────────────────┴────────────┴────────────┘
```

### Key Architectural Findings:
1. **Direct Columnar Handoff is NOT justified**: Staging Parquet files takes under 0.5s of the 2.1s preparation phase. Parquet files provide clean cross-language decoupling, streaming memory bounds, and native parallel scans in DuckDB.
2. **Native Analytics in Rust is NOT justified**: At 5M events, the pipeline completes in ~4.5–5.0 seconds. Linear extrapolation to 20M events yields ~18–35 seconds, comfortably achieving the competition's under-60s target. A full SQL rewrite in Rust introduces substantial risk and complexity without an empirical performance justification.
3. **Sessionization is the clear bottleneck**: The combination of `events` (deduplication by `event_id`) and `session_events` (window functions partitioned by `person_id` ordered by `ts_ms, event_id`) accounts for **~62% of analytics time**.

---

## 3. Proposed Next Change: Targeted Sessionization & Analytics Optimization

### Primary Targets for Next Increment:

1. **Pre-filter & Column Pruning in `session_events`**:
   - `session_events` currently keeps all columns (`event_type`, `product_id`, `price_cents`) across millions of rows for both purchase and non-purchase events.
   - For `top_categories`, only purchase events are ever consulted. Pruning non-purchase event payloads earlier or splitting purchase events reduces the volume of data sorted and joined in `top_categories`.

2. **Deduplication and Window Partition Optimization**:
   - In `events`: `PARTITION BY event_id ORDER BY ingest_ms` performs a global hash/sort over 5M rows.
   - If events are already mostly ordered or if filtering early can be applied, DuckDB can eliminate unnecessary spill.

3. **Click Interval Optimization (`assist_intervals`)**:
   - `assist_intervals` uses a 3-CTE island-merging algorithm (`distinct_times`, `previous_ends`, `islands`) with two window passes (`max() OVER (...)` and `sum() OVER (...)`).
   - We will evaluate collapsing or indexing the click interval generation to reduce the 0.22s cost.

4. **DuckDB Configuration Tuning**:
   - Test `SET preserve_insertion_order = false` (already enabled).
   - Evaluate memory limit allocation: `mem_gb * 0.75` for DuckDB vs OS buffering on scorer's 8 vCPUs / 24 GB setup.

---

## 4. Acceptance Gates

Every change in this workstream must satisfy the following strict gates:

1. **Exact Correctness Gate**:
   - All 69 unit, differential, and parity tests must pass (`make test`).
   - All 36 official edge-case fixtures (`make fixtures-local` or `make fixtures`) must match byte-for-byte.
   - Output comparison against official published dev answers (`scripts/check_dev_fingerprints.py` and `scripts/compare.py`) must pass with zero diffs.
2. **Benchmark Gate**:
   - Matched 3-round benchmark on 5M seed-1 and seed-2 datasets.
   - Analytics phase duration must decrease without regressing preparation time or increasing peak RSS beyond resource limits (`MEM_GB=2` or `4`).
3. **Repository & PR Governance**:
   - Karthik owns all analytics work.
   - Work strictly on task branch `codex/karthik-analytics-optimization` targeting `main`.
   - Open a draft PR while in progress; no agent may merge to `main` or enable auto-merge. Human maintainer reviews and merges.
