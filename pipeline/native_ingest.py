"""Launch native staging and validate its committed, typed output protocol."""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile

import pyarrow.parquet as pq

from pipeline.contracts import (
    CAMPAIGNS_SCHEMA, CLICKS_SCHEMA, EVENTS_SCHEMA, PRODUCTS_SCHEMA, PreparedInputs,
)
from pipeline.parsing import parse_timestamp

_ROOT = Path(__file__).resolve().parents[1]


def _ordinary(path: Path, *, directory=False):
    if path.is_symlink() or not (path.is_dir() if directory else path.is_file()):
        raise RuntimeError(f'native ingestion output is not an ordinary {"directory" if directory else "file"}: {path}')


def _validate(stage, start, end):
    marker = stage / '_SUCCESS.json'
    _ordinary(marker)
    try:
        value = json.loads(marker.read_text(encoding='utf-8'))
        expected = {'protocol_version': 1, 'window_start_ms': start, 'window_end_ms': end}
        if not isinstance(value, dict):
            raise ValueError('marker must be an object')
        for key, number in expected.items():
            if type(value.get(key)) is not int or value[key] != number:
                raise ValueError(f'invalid {key}')
        counts = value['rows']
        if not isinstance(counts, dict) or set(counts) != {'events', 'clicks', 'products', 'campaigns'}:
            raise ValueError('invalid row-count tables')
        for name, path, schema in [
            ('events', stage / 'events/part-0.parquet', EVENTS_SCHEMA),
            ('clicks', stage / 'clicks/part-0.parquet', CLICKS_SCHEMA),
            ('products', stage / 'products.parquet', PRODUCTS_SCHEMA),
            ('campaigns', stage / 'campaigns.parquet', CAMPAIGNS_SCHEMA),
        ]:
            count = counts[name]
            if type(count) is not int or count < 0:
                raise ValueError(f'invalid {name} row count')
            if name in ('events', 'clicks'):
                _ordinary(path.parent, directory=True)
                if set(path.parent.iterdir()) != {path}:
                    raise ValueError(f'{name} contains unexpected native output files')
            _ordinary(path)
            with pq.ParquetFile(path) as parquet:
                if parquet.schema_arrow != schema or parquet.metadata.num_rows != count:
                    raise ValueError(f'{name} schema or row count mismatch')
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise RuntimeError(f'invalid native ingestion success protocol: {error}') from error


def prepare_inputs(
    data_dir: Path, scratch_dir: Path, *, batch_rows: int = 65536
) -> PreparedInputs:
    if isinstance(batch_rows, bool) or not isinstance(batch_rows, int) or batch_rows <= 0:
        raise ValueError('batch_rows must be a positive integer')
    manifest = json.loads((data_dir / 'manifest.json').read_text(encoding='utf-8'))
    start = parse_timestamp(manifest['window_start'])
    end = parse_timestamp(manifest['window_end'])
    if start is None or end is None or start >= end:
        raise ValueError('manifest must specify a valid increasing timestamp window')
    binary = os.environ.get('RUST_INGEST_BIN', str(_ROOT / 'native/ingest/target/release/shopping-ingest'))
    scratch_dir.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix='ingestion-', dir=scratch_dir))
    try:
        process = subprocess.Popen([
            binary, '--data-dir', str(data_dir), '--stage-dir', str(stage),
            '--window-start-ms', str(start), '--window-end-ms', str(end),
            '--batch-rows', str(min(batch_rows, 65536)),
        ], stdout=subprocess.PIPE, start_new_session=True)
        try:
            stdout, _ = process.communicate()
        except BaseException:
            # The session has its own process group: terminate all native work
            # and reap the child before deleting files it may still be writing.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            finally:
                process.communicate()
            raise
        if process.returncode == 2:
            raise ValueError('ingest_ts violates the guaranteed valid timestamp input')
        if process.returncode != 0:
            raise RuntimeError(f'native ingestion exited with status {process.returncode}')
        if stdout:
            raise RuntimeError('native ingestion emitted unexpected stdout')
        _validate(stage, start, end)
        return PreparedInputs(stage / 'events', stage / 'clicks', stage / 'products.parquet', stage / 'campaigns.parquet', start, end)
    except BaseException:
        shutil.rmtree(stage)
        raise
