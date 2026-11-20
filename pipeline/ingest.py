"""Pranav's ingestion entrypoint; implementation belongs on his task branch."""

from pathlib import Path

from pipeline.contracts import PreparedInputs


def prepare_inputs(
    data_dir: Path, scratch_dir: Path, *, batch_rows: int = 65536
) -> PreparedInputs:
    """Parse and stage inputs according to docs/DATA_CONTRACT.md."""
    raise NotImplementedError(
        "Ingestion is not implemented. See docs/handoffs/PRANAV.md."
    )
