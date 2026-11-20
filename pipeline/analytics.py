"""Karthik's analytical entrypoint; implementation belongs on his task branch."""

from pathlib import Path

import duckdb

from pipeline.contracts import PreparedInputs


def run_pipeline(
    con: duckdb.DuckDBPyConnection, prepared: PreparedInputs, output_dir: Path
) -> None:
    """Produce the two daily tables according to rules.md."""
    raise NotImplementedError(
        "Analytics is not implemented. See docs/handoffs/KARTHIK.md."
    )
