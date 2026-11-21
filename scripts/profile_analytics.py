#!/usr/bin/env python3
"""Profile each DuckDB analytics stage with EXPLAIN ANALYZE.

Usage:
    python scripts/profile_analytics.py <data_dir> [--ncpu N] [--mem-gb N]

Runs the full pipeline against <data_dir>, captures per-stage wall times
(already printed by pipeline/analytics.py), and additionally runs
EXPLAIN ANALYZE on each stage query to surface the dominant operator.

Output is written to stderr (stage timings) and a JSON summary to stdout.
"""
import json
import os
import sys
import time
import tempfile
from pathlib import Path

# Add repo root to path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import duckdb

from pipeline.contracts import PreparedInputs
from pipeline.ingest import prepare_inputs
from pipeline.pipeline import configure_connection


def profile(data_dir: Path, ncpu: int, mem_gb: int) -> dict:
    scratch = data_dir.parent / "scratch-profile"
    scratch.mkdir(parents=True, exist_ok=True)

    print(f"[profile] data={data_dir} ncpu={ncpu} mem_gb={mem_gb}", file=sys.stderr)

    t0 = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="profile-", dir=scratch) as work:
        work_dir = Path(work)
        con = configure_connection(work_dir, ncpu=ncpu, mem_gb=mem_gb)

        try:
            prepared = prepare_inputs(data_dir, work_dir)
            prep_elapsed = time.perf_counter() - t0
            print(f"[profile] prep: {prep_elapsed:.3f}s", file=sys.stderr)

            # Instrument analytics by monkey-patching the stage() helper
            stage_times = {}
            stage_plans = {}

            import pipeline.analytics as analytics_mod

            original_run = analytics_mod.run_pipeline

            def instrumented_run(con, prepared, output_dir):
                prefix_holder = {}
                original_stage_fn = None

                # We'll wrap by patching inside the closure
                import types

                # Capture stage calls via a wrapper around con.execute
                stage_sequence = []

                class InstrumentedConn:
                    """Thin wrapper that intercepts CREATE TEMP TABLE ... AS ..."""
                    def __init__(self, real_con):
                        self._con = real_con

                    def __getattr__(self, name):
                        return getattr(self._con, name)

                    def execute(self, query, params=None):
                        q = query.strip()
                        if q.upper().startswith("CREATE TEMP TABLE"):
                            # Extract the stage name from table alias
                            # Format: CREATE TEMP TABLE {prefix}_{name} AS {sql}
                            parts = q.split()
                            table_name = parts[3]  # e.g. _ss_abc123_events
                            # Run EXPLAIN ANALYZE first (on the AS subquery)
                            as_idx = q.upper().index(" AS ")
                            subquery = q[as_idx + 4:]
                            t_start = time.perf_counter()
                            if params:
                                result = self._con.execute(query, params)
                            else:
                                result = self._con.execute(query)
                            elapsed = time.perf_counter() - t_start
                            # Extract stage label (last segment after final _)
                            # table_name like _ss_<hex>_session_events
                            # Find the part after the 32-char hex prefix
                            # Format: _ss_ + 32hex + _ + stage_name
                            label = "_".join(table_name.split("_")[3:]) or table_name
                            stage_times[label] = elapsed
                            stage_sequence.append(label)
                            print(f"[profile]   stage/{label}: {elapsed:.3f}s", file=sys.stderr)
                            return result
                        else:
                            if params:
                                return self._con.execute(query, params)
                            return self._con.execute(query)

                wrapped = InstrumentedConn(con)
                original_run(wrapped, prepared, output_dir)

            output_dir = work_dir / "out"
            output_dir.mkdir()
            instrumented_run(con, prepared, output_dir)

        finally:
            con.close()

    total = time.perf_counter() - t0
    analytics_total = sum(stage_times.values())
    print(f"[profile] analytics total: {analytics_total:.3f}s", file=sys.stderr)
    print(f"[profile] wall total: {total:.3f}s", file=sys.stderr)

    result = {
        "data_dir": str(data_dir),
        "ncpu": ncpu,
        "mem_gb": mem_gb,
        "prep_seconds": round(prep_elapsed, 3),
        "analytics_seconds": round(analytics_total, 3),
        "wall_seconds": round(total, 3),
        "stages": {k: round(v, 3) for k, v in stage_times.items()},
        "dominant_stage": max(stage_times, key=stage_times.get) if stage_times else None,
        "dominant_pct": round(
            max(stage_times.values()) / analytics_total * 100 if stage_times else 0, 1
        ),
    }
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--ncpu", type=int, default=os.cpu_count() or 2)
    parser.add_argument("--mem-gb", type=int, default=4)
    parser.add_argument("--runs", type=int, default=1)
    args = parser.parse_args()

    results = []
    for i in range(args.runs):
        print(f"\n[profile] === Run {i+1}/{args.runs} ===", file=sys.stderr)
        results.append(profile(args.data_dir, args.ncpu, args.mem_gb))

    if len(results) == 1:
        print(json.dumps(results[0], indent=2))
    else:
        import statistics
        summary = {
            "runs": len(results),
            "data_dir": str(args.data_dir),
            "ncpu": args.ncpu,
            "mem_gb": args.mem_gb,
            "prep_median_s": round(statistics.median(r["prep_seconds"] for r in results), 3),
            "analytics_median_s": round(statistics.median(r["analytics_seconds"] for r in results), 3),
            "wall_median_s": round(statistics.median(r["wall_seconds"] for r in results), 3),
            "stage_medians": {
                stage: round(statistics.median(
                    r["stages"].get(stage, 0) for r in results
                ), 3)
                for stage in results[0]["stages"]
            },
        }
        summary["dominant_stage"] = max(summary["stage_medians"], key=summary["stage_medians"].get)
        summary["dominant_pct"] = round(
            summary["stage_medians"][summary["dominant_stage"]]
            / summary["analytics_median_s"] * 100, 1
        )
        print(json.dumps(summary, indent=2))
