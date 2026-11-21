"""Select an explicit ingestion backend while preserving the public contract."""
import os
from pathlib import Path

from pipeline import ingest_python, native_ingest
from pipeline.contracts import PreparedInputs


def prepare_inputs(
    data_dir: Path, scratch_dir: Path, *, batch_rows: int = 65536
) -> PreparedInputs:
    backend = os.environ.get('INGEST_BACKEND', 'rust')
    if backend == 'python':
        return ingest_python.prepare_inputs(data_dir, scratch_dir, batch_rows=batch_rows)
    if backend == 'rust':
        return native_ingest.prepare_inputs(data_dir, scratch_dir, batch_rows=batch_rows)
    raise ValueError('INGEST_BACKEND must be python or rust')
