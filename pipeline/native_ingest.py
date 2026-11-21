"""Launch native staging and validate its committed, typed output protocol."""
import json
import os
from pathlib import Path
import re
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
        expected = {'window_start_ms': start, 'window_end_ms': end}
        if not isinstance(value, dict):
            raise ValueError('marker must be an object')
        version = value.get('protocol_version')
        if type(version) is not int or version not in (1, 2):
            raise ValueError('invalid protocol_version')
        for key, number in expected.items():
            if type(value.get(key)) is not int or value[key] != number:
                raise ValueError(f'invalid {key}')
        counts = value['rows']
        if not isinstance(counts, dict) or set(counts) != {'events', 'clicks', 'products', 'campaigns'}:
            raise ValueError('invalid row-count tables')
        inventories = value.get('parts')
        if version == 2 and (not isinstance(inventories, dict) or set(inventories) != {'events', 'clicks'}):
            raise ValueError('invalid part inventory tables')
        for name, schema in [
            ('events', EVENTS_SCHEMA), ('clicks', CLICKS_SCHEMA),
            ('products', PRODUCTS_SCHEMA), ('campaigns', CAMPAIGNS_SCHEMA),
        ]:
            count = counts[name]
            if type(count) is not int or count < 0:
                raise ValueError(f'invalid {name} row count')
            if name in ('events', 'clicks'):
                directory = stage / name
                _ordinary(directory, directory=True)
                entries = inventories[name] if version == 2 else [{'name': 'part-0.parquet', 'rows': count}]
                if not isinstance(entries, list) or not entries:
                    raise ValueError(f'{name} needs a nonempty part inventory')
                files = {}
                for entry in entries:
                    if not isinstance(entry, dict) or set(entry) != {'name', 'rows'}:
                        raise ValueError(f'invalid {name} part entry')
                    filename, rows = entry['name'], entry['rows']
                    if not isinstance(filename, str) or re.fullmatch(r'part-[0-9]+\.parquet', filename) is None:
                        raise ValueError(f'invalid {name} part filename')
                    if filename in files or type(rows) is not int or rows < 0:
                        raise ValueError(f'invalid {name} part count or duplicate filename')
                    files[filename] = rows
                if sum(files.values()) != count:
                    raise ValueError(f'{name} aggregate row count mismatch')
                paths = [(directory / filename, rows) for filename, rows in files.items()]
                if set(directory.iterdir()) != {path for path, _ in paths}:
                    raise ValueError(f'{name} contains unexpected native output files')
            else:
                paths = [(stage / f'{name}.parquet', count)]
            for path, rows in paths:
                _ordinary(path)
                with pq.ParquetFile(path) as parquet:
                    if parquet.schema_arrow != schema or parquet.metadata.num_rows != rows:
                        raise ValueError(f'{name} schema or row count mismatch')
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise RuntimeError(f'invalid native ingestion success protocol: {error}') from error


def prepare_inputs(
    data_dir: Path, scratch_dir: Path, *, batch_rows: int = 65536
) -> PreparedInputs:
    if isinstance(batch_rows, bool) or not isinstance(batch_rows, int) or batch_rows <= 0:
        raise ValueError('batch_rows must be a positive integer')
    ncpu = os.environ.get('NCPU', str(os.cpu_count() or 1))
    if re.fullmatch(r'[0-9]+', ncpu) is None or int(ncpu) <= 0:
        raise ValueError('NCPU must be a positive integer')
    workers = int(ncpu)
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
            '--workers', str(workers),
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
