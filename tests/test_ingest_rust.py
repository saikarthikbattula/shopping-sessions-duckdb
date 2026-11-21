"""Native protocol failures and real-binary staging parity."""
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import ingest, ingest_python, native_ingest
from pipeline.contracts import EVENTS_SCHEMA, CLICKS_SCHEMA, PRODUCTS_SCHEMA, CAMPAIGNS_SCHEMA
import test_ingest


class NativeProtocolTests(unittest.TestCase):
    setUp = test_ingest.StagingTests.setUp
    write = test_ingest.StagingTests.write
    read = test_ingest.StagingTests.read
    check_duckdb = test_ingest.StagingTests.check_duckdb
    def run_native(self, effect=None, code=0, stdout=b''):
        process = unittest.mock.Mock()
        process.returncode = code
        def communicate():
            if effect:
                effect(self.argv)
            return stdout, None
        process.communicate.side_effect = communicate
        def launch(argv, **kwargs):
            self.argv = argv
            self.assertTrue(kwargs['start_new_session'])
            self.assertEqual(kwargs['stdout'], subprocess.PIPE)
            self.assertNotIn('shell', kwargs)
            return process
        return patch('pipeline.native_ingest.subprocess.Popen', side_effect=launch), process

    def success(self, argv):
        stage = Path(argv[argv.index('--stage-dir') + 1])
        start = int(argv[argv.index('--window-start-ms') + 1])
        end = int(argv[argv.index('--window-end-ms') + 1])
        for name, schema in [('events/part-0.parquet', EVENTS_SCHEMA), ('clicks/part-0.parquet', CLICKS_SCHEMA), ('products.parquet', PRODUCTS_SCHEMA), ('campaigns.parquet', CAMPAIGNS_SCHEMA)]:
            path = stage / name
            path.parent.mkdir(exist_ok=True)
            pq.write_table(pa.Table.from_batches([], schema=schema), path)
        (stage / '_SUCCESS.json').write_text(json.dumps({'protocol_version': 1, 'window_start_ms': start, 'window_end_ms': end, 'rows': dict.fromkeys(['events', 'clicks', 'products', 'campaigns'], 0)}))

    def test_selector(self):
        sentinel = object()
        for backend, module in [('python', ingest_python), ('rust', native_ingest)]:
            with patch.dict(os.environ, {'INGEST_BACKEND': backend}), patch.object(module, 'prepare_inputs', return_value=sentinel) as prepare:
                self.assertIs(ingest.prepare_inputs(self.data, self.scratch, batch_rows=7), sentinel)
                prepare.assert_called_once_with(self.data, self.scratch, batch_rows=7)
        with patch.dict(os.environ, {'INGEST_BACKEND': 'auto'}), self.assertRaises(ValueError):
            ingest.prepare_inputs(self.data, self.scratch)
        with patch.dict(os.environ, {}, clear=True), patch.object(ingest_python, 'prepare_inputs', return_value=sentinel):
            self.assertIs(ingest.prepare_inputs(self.data, self.scratch), sentinel)

    def test_validation_precedes_launch_and_staging(self):
        with patch('pipeline.native_ingest.subprocess.Popen') as launch:
            for value in (False, True, 0, -1, 1.5, '1', None):
                with self.assertRaisesRegex(ValueError, '^batch_rows must be a positive integer$'):
                    native_ingest.prepare_inputs(self.data, self.scratch, batch_rows=value)
            self.write('manifest.json', '{"window_start":"bad","window_end":"bad"}')
            with self.assertRaisesRegex(ValueError, '^manifest must specify a valid increasing timestamp window$'):
                native_ingest.prepare_inputs(self.data, self.scratch)
            launch.assert_not_called()
            self.assertFalse(self.scratch.exists())

    def test_success_and_huge_batch(self):
        launcher, process = self.run_native(self.success)
        with launcher, patch.dict(os.environ, {'RUST_INGEST_BIN': '/path with spaces/it\'s-native'}):
            result = native_ingest.prepare_inputs(self.data, self.scratch, batch_rows=10**100)
        self.assertEqual(self.argv[0], '/path with spaces/it\'s-native')
        self.assertEqual(self.argv[-1], '65536')
        self.assertTrue(result.events_dir.exists())
        self.check_duckdb(result)

    def success_v2(self, argv):
        self.success(argv)
        stage = Path(argv[argv.index('--stage-dir') + 1])
        marker = stage / '_SUCCESS.json'
        obj = json.loads(marker.read_text())
        obj['protocol_version'] = 2
        obj['parts'] = {}
        for name, schema in [('events', EVENTS_SCHEMA), ('clicks', CLICKS_SCHEMA)]:
            pq.write_table(pa.Table.from_batches([], schema=schema), stage / name / 'part-1.parquet')
            obj['parts'][name] = [{'name': f'part-{i}.parquet', 'rows': 0} for i in range(2)]
        marker.write_text(json.dumps(obj))

    def test_parallel_inventory_and_worker_argument(self):
        launcher, _ = self.run_native(self.success_v2)
        with launcher, patch.dict(os.environ, {'NCPU': '2'}):
            result = native_ingest.prepare_inputs(self.data, self.scratch)
        self.assertEqual(self.argv[self.argv.index('--workers') + 1], '2')
        self.assertEqual(len(list(result.events_dir.glob('*.parquet'))), 2)
        self.check_duckdb(result)

    def test_invalid_parallel_inventory_is_rejected(self):
        def corrupt(kind):
            def effect(argv):
                self.success_v2(argv)
                stage = Path(argv[argv.index('--stage-dir') + 1])
                marker = stage / '_SUCCESS.json'
                obj = json.loads(marker.read_text())
                part = obj['parts']['events'][0]
                if kind == 'duplicate': obj['parts']['events'][1] = part.copy()
                elif kind == 'traversal': part['name'] = '../products.parquet'
                elif kind == 'absolute': part['name'] = '/tmp/part-0.parquet'
                elif kind == 'separator': part['name'] = 'x/part-0.parquet'
                elif kind == 'backslash': part['name'] = 'x\\part-0.parquet'
                elif kind == 'unicode': part['name'] = 'part-٠.parquet'
                elif kind == 'bool': part['rows'] = True
                elif kind == 'negative': part['rows'] = -1
                elif kind == 'count': part['rows'] = 1
                elif kind == 'aggregate': obj['rows']['events'] = 1
                elif kind == 'empty': obj['parts']['events'] = []
                elif kind == 'missing': del obj['parts']['events']
                elif kind == 'extra-table': obj['parts']['other'] = []
                elif kind == 'not-list': obj['parts']['events'] = {}
                elif kind == 'not-object': obj['parts']['events'][0] = 'part-0.parquet'
                elif kind == 'extra-file':
                    pq.write_table(pa.Table.from_batches([], schema=EVENTS_SCHEMA), stage / 'events/part-2.parquet')
                elif kind == 'symlink':
                    path = stage / 'events/part-1.parquet'
                    path.unlink()
                    path.symlink_to(stage / 'events/part-0.parquet')
                elif kind == 'schema':
                    pq.write_table(pa.table({'wrong': []}), stage / 'events/part-1.parquet')
                marker.write_text(json.dumps(obj))
            return effect
        for kind in ['duplicate', 'traversal', 'absolute', 'separator', 'backslash', 'unicode',
                     'bool', 'negative', 'count', 'aggregate', 'empty', 'missing', 'extra-table',
                     'not-list', 'not-object', 'extra-file', 'symlink', 'schema']:
            with self.subTest(kind=kind):
                launcher, _ = self.run_native(corrupt(kind))
                with launcher, self.assertRaises(RuntimeError):
                    native_ingest.prepare_inputs(self.data, self.scratch)
                self.assertEqual(list(self.scratch.iterdir()), [])

    def test_worker_limit_is_validated_before_launch(self):
        with patch('pipeline.native_ingest.subprocess.Popen') as launch:
            for value in ['0', '-1', '', '1.5', ' 2', '+2', '٢']:
                with self.subTest(value=value), patch.dict(os.environ, {'NCPU': value}):
                    with self.assertRaisesRegex(ValueError, '^NCPU must be a positive integer$'):
                        native_ingest.prepare_inputs(self.data, self.scratch)
            launch.assert_not_called()
            self.assertFalse(self.scratch.exists())

    def test_failed_exit_cleans_only_owned_stage(self):
        self.scratch.mkdir()
        sibling = self.scratch / 'ingestion-existing'
        sibling.mkdir()
        for code, exception in [(2, ValueError), (1, RuntimeError), (-9, RuntimeError)]:
            launcher, _ = self.run_native(code=code)
            with launcher, self.assertRaises(exception) as error:
                native_ingest.prepare_inputs(self.data, self.scratch)
            if code == 2:
                self.assertEqual(str(error.exception), 'ingest_ts violates the guaranteed valid timestamp input')
            self.assertEqual(list(self.scratch.iterdir()), [sibling])

    def test_invalid_success_protocol_cleanup(self):
        def corrupt(kind):
            def effect(argv):
                self.success(argv)
                stage = Path(argv[argv.index('--stage-dir') + 1])
                marker = stage / '_SUCCESS.json'
                obj = json.loads(marker.read_text())
                if kind == 'missing': marker.unlink()
                elif kind == 'json': marker.write_text('bad')
                elif kind == 'symlink':
                    path = stage / 'products.parquet'
                    path.unlink()
                    path.symlink_to(stage / 'campaigns.parquet')
                elif kind == 'directory-symlink':
                    (stage / 'events/part-0.parquet').unlink()
                    (stage / 'events').rmdir()
                    (stage / 'events').symlink_to(stage / 'clicks', target_is_directory=True)
                elif kind == 'extra-part':
                    pq.write_table(pa.Table.from_batches([], schema=EVENTS_SCHEMA), stage / 'events/extra.parquet')
                elif kind == 'extra-symlink':
                    (stage / 'events/extra.parquet').symlink_to(stage / 'events/part-0.parquet')
                elif kind == 'schema': pq.write_table(pa.table({'wrong': []}), stage / 'products.parquet')
                else:
                    if kind == 'version': obj['protocol_version'] = True
                    if kind == 'window': obj['window_end_ms'] += 1
                    if kind == 'count': obj['rows']['events'] = 1
                    if kind == 'bool-count': obj['rows']['events'] = False
                    if kind == 'negative': obj['rows']['events'] = -1
                    marker.write_text(json.dumps(obj))
            return effect
        for kind in ['missing', 'json', 'symlink', 'directory-symlink', 'extra-part', 'extra-symlink', 'schema', 'version', 'window', 'count', 'bool-count', 'negative']:
            with self.subTest(kind=kind):
                launcher, _ = self.run_native(corrupt(kind))
                with launcher, self.assertRaises(RuntimeError):
                    native_ingest.prepare_inputs(self.data, self.scratch)
                self.assertEqual(list(self.scratch.iterdir()), [])

    def test_stdout_and_launch_failure(self):
        launcher, _ = self.run_native(self.success, stdout=b'noisy')
        with launcher, self.assertRaises(RuntimeError):
            native_ingest.prepare_inputs(self.data, self.scratch)
        self.assertEqual(list(self.scratch.iterdir()), [])
        with patch('pipeline.native_ingest.subprocess.Popen', side_effect=FileNotFoundError('missing')), self.assertRaises(FileNotFoundError):
            native_ingest.prepare_inputs(self.data, self.scratch)
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_real_fake_executable_exit_and_protocol_failures(self):
        binary = self.root / "fake native's executable"
        for script, exception in [('raise SystemExit(2)', ValueError),
                                  ('raise SystemExit(7)', RuntimeError),
                                  ('pass', RuntimeError),
                                  ("print('unexpected stdout')", RuntimeError)]:
            binary.write_text(f'#!{sys.executable}\n{script}\n')
            binary.chmod(0o700)
            with self.subTest(script=script), patch.dict(os.environ, {'RUST_INGEST_BIN': str(binary)}):
                with self.assertRaises(exception):
                    native_ingest.prepare_inputs(self.data, self.scratch)
                self.assertEqual(list(self.scratch.iterdir()), [])

    def test_interrupt_kills_group_and_reaps_before_cleanup(self):
        launcher, process = self.run_native()
        process.pid = 424242
        process.communicate.side_effect = [KeyboardInterrupt(), (b'', None)]
        with launcher, patch('pipeline.native_ingest.os.killpg') as kill, self.assertRaises(KeyboardInterrupt):
            native_ingest.prepare_inputs(self.data, self.scratch)
        kill.assert_called_once()
        self.assertEqual(process.communicate.call_count, 2)
        self.assertEqual(list(self.scratch.iterdir()), [])


class RustParityTests(test_ingest.StagingTests):
    def test_parallel_workers_preserve_uneven_and_shuffled_parts(self):
        self.write('links/part.csv', 'header\nu,z\nz,A unused\n')
        events = [f'e{i},u,1790812800000,1790812800000,purchase,p,250' for i in range(30)]
        # Include invalid copies for downstream dedup, CR in exact strings,
        # unterminated records and header-only/empty files of uneven sizes.
        events += ['dup,u,bad,1790812800000,view,p,',
                   'dup,z,1790812800000,1790812800001,view,p,',
                   'é, u\r,1790812800000,1790812800000,cart,p,']
        chunks = ['header\n' + '\n'.join(events[:1]),
                  'header\n' + '\n'.join(events[1:]), 'unterminated header', '']
        for index, text in enumerate(chunks):
            self.write(f'events/part-{index}.csv', text)
        self.write('clicks/part-a.csv', 'header\nk,z,c,1790812800000')
        self.write('clicks/part-b.csv', 'header\nother,u,c,1790812800001\n')
        for workers in (1, 2, 8):
            with self.subTest(workers=workers), patch.dict(os.environ, {'NCPU': str(workers)}):
                result = self.compare(self.data, self.scratch, batch_rows=3)
                marker = json.loads((result.events_dir.parent / '_SUCCESS.json').read_text())
                self.assertEqual(marker['protocol_version'], 2)
                self.assertLessEqual(len(marker['parts']['events']), workers)
                self.assertLessEqual(len(marker['parts']['clicks']), workers)
        for index, text in enumerate(reversed(chunks)):
            self.write(f'events/part-{index}.csv', text)
        with patch.dict(os.environ, {'NCPU': '2'}):
            self.compare(self.data, self.scratch, batch_rows=3)

    def test_native_cli_rejects_invalid_arguments_and_nonempty_stage(self):
        binary = os.environ.get('RUST_INGEST_BIN', str(Path(__file__).resolve().parents[1] / 'native/ingest/target/release/shopping-ingest'))
        stage = self.root / 'direct-stage'
        stage.mkdir()
        arguments = ['--data-dir', str(self.data), '--stage-dir', str(stage),
                     '--window-start-ms', '1790812800000', '--window-end-ms', '1792022400000',
                     '--batch-rows', '2']
        for invalid in [[], ['--unknown', 'x'], ['--data-dir'],
                        arguments + ['--batch-rows', '2'],
                        arguments + ['--workers', '0'],
                        arguments + ['--workers', '-1'],
                        arguments[:-1] + ['0'], arguments[:-1] + ['-1'],
                        arguments[:5] + ['not-an-integer'] + arguments[6:]]:
            with self.subTest(arguments=invalid):
                result = subprocess.run([binary, *invalid], capture_output=True)
                self.assertEqual(result.returncode, 64, result.stderr)
                self.assertEqual(result.stdout, b'')
                self.assertFalse((stage / '_SUCCESS.json').exists())
        sentinel = stage / 'keep.txt'
        sentinel.write_text('keep')
        result = subprocess.run([binary, *arguments], capture_output=True)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(result.stdout, b'')
        self.assertEqual(sentinel.read_text(), 'keep')
        self.assertFalse((stage / '_SUCCESS.json').exists())

    def test_dot_csv_basename_is_read_in_every_input_folder(self):
        self.write('links/.csv', 'a,b\nu,z\nz,A unused\n')
        self.write('events/.csv', 'header\ne,u,2026-10-01T00:00:00Z,2026-10-01T00:00:00Z,purchase,p,250\n')
        self.write('clicks/.csv', 'header\nk,z,c,2026-10-01T00:00:00Z\n')
        result = self.compare(self.data, self.scratch, batch_rows=1)
        self.assertEqual(self.read(result.events_dir)[0]['person_id'], 'A unused')
        self.assertEqual(self.read(result.clicks_dir)[0]['person_id'], 'A unused')

    def setUp(self):
        super().setUp()
        binary = Path(os.environ.get('RUST_INGEST_BIN', Path(__file__).resolve().parents[1] / 'native/ingest/target/release/shopping-ingest'))
        if not binary.is_file():
            if os.environ.get('REQUIRE_RUST_INGEST') == '1':
                self.fail(f'Required Rust ingestion binary missing: {binary}')
            self.skipTest('Rust ingestion binary not built; set REQUIRE_RUST_INGEST=1 to require it')
        # Exercise all existing staging cases against real native output and compare
        # every field, duplicate multiplicity, schema and nullability to Python.
        self.selector = patch('test_ingest.prepare_inputs', side_effect=self.compare)
        self.selector.start()
        self.addCleanup(self.selector.stop)

    def compare(self, data, scratch, *, batch_rows=65536):
        python = ingest_python.prepare_inputs(data, scratch, batch_rows=batch_rows)
        rust = native_ingest.prepare_inputs(data, scratch, batch_rows=batch_rows)
        self.assertEqual((python.window_start_ms, python.window_end_ms), (rust.window_start_ms, rust.window_end_ms))
        for field in ['events_dir', 'clicks_dir', 'products_path', 'campaigns_path']:
            left, right = pq.read_table(getattr(python, field)), pq.read_table(getattr(rust, field))
            self.assertEqual(left.schema, right.schema)
            self.assertCountEqual(left.to_pylist(), right.to_pylist())
            path = getattr(rust, field)
            files = list(path.glob('*.parquet')) if path.is_dir() else [path]
            for file in files:
                metadata = pq.read_metadata(file)
                for group in range(metadata.num_row_groups):
                    self.assertLessEqual(metadata.row_group(group).num_rows, min(batch_rows, 65536))
        return rust

    def test_bad_configuration_and_operational_errors_raise(self):
        # Python writer fault injection is specific to Python, not native.
        for value in [0, -1, True, 1.5]:
            with self.assertRaises(ValueError):
                native_ingest.prepare_inputs(self.data, self.scratch, batch_rows=value)

    def test_unicode_chains_offsets_calendar_and_paths(self):
        renamed = self.root / "data with space and 'quote"
        self.data.rename(renamed)
        self.data = renamed
        self.write('links/part.csv', 'header\n' + ''.join(f'user{i},user{i+1}\n' for i in range(900)) + 'user900, A unused\né,中\nz,é\n u\r,z\n')
        self.write('events/part.csv', 'header\n' + ''.join(f'e{i},{user},{ts},2026-10-01T00:00:00Z,purchase,p,{price}\n' for i, (user, ts, price) in enumerate([
            ('user900', '2026-09-30T20:00:00-04:00', '1'), ('中', '2026-10-01T02:00:00+02:00', '000250'),
            (' u\r', '2027-02-29T00:00:00Z', '٢٥٠'), ('é', '0000-01-01T00:00:00Z', '1'),
            ('中', '2028-02-29T00:00:00Z', '1'), ('u', '2026-10-01T00:00:00+24:00', '1'),
        ])))
        self.compare(self.data, self.scratch, batch_rows=1)
