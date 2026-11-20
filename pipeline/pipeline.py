#!/usr/bin/env python3
"""Scorer entrypoint. Run locally with python -m pipeline.pipeline."""

import os
import sys
import tempfile
import time
from pathlib import Path

import duckdb

from pipeline.analytics import run_pipeline
from pipeline.ingest import prepare_inputs


def configure_connection(
    scratch_dir: Path, *, ncpu: int, mem_gb: int
) -> duckdb.DuckDBPyConnection:
    """Reserve Python/Arrow headroom; put DuckDB spills on writable disk."""
    if ncpu < 1 or mem_gb < 1:
        raise ValueError("NCPU and MEM_GB must be positive integers")
    temp_dir = scratch_dir / "duckdb_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"SET threads = {ncpu}")
        con.execute(f"SET memory_limit = '{int(mem_gb * 0.75 * 1024)}MiB'")
        con.execute("SET temp_directory = ?", [str(temp_dir)])
        con.execute("SET preserve_insertion_order = false")
    except Exception:
        con.close()
        raise
    return con


def main() -> None:
    data = Path(os.environ.get("DATA_DIR", "/data"))
    output = Path(os.environ.get("OUTPUT_DIR", "/output"))
    scratch = Path(os.environ.get("SCRATCH_DIR", "/scratch"))
    ncpu = int(os.environ.get("NCPU", os.cpu_count() or 1))
    mem_gb = int(os.environ.get("MEM_GB", "8"))
    scratch.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="shopping-sessions-", dir=scratch) as work:
        work_dir = Path(work)
        con = configure_connection(work_dir, ncpu=ncpu, mem_gb=mem_gb)
        try:
            prepared = prepare_inputs(data, work_dir)
            prepared_at = time.perf_counter()
            print(f"prepare_inputs: {prepared_at - started:.3f}s", file=sys.stderr)
            run_pipeline(con, prepared, output)
            print(
                f"analytics: {time.perf_counter() - prepared_at:.3f}s; "
                f"total: {time.perf_counter() - started:.3f}s",
                file=sys.stderr,
            )
        finally:
            con.close()


if __name__ == "__main__":
    main()
