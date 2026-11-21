"""Repeatable benchmark behavior, measurement semantics, and failure gates."""

import contextlib
import json
import io
import os
import signal
import shutil
import sys
import tarfile
import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.contracts import CAMPAIGN_DAILY_SCHEMA, USER_DAILY_SCHEMA
from scripts import benchmark


REPO = Path(__file__).resolve().parents[1]


def write_outputs(folder, *, revenue=0, duplicate=False):
    user = dict(user_id='u1', day=date(2026, 10, 3), sessions=1, events=1,
                revenue_cents=revenue, top_category=None if not revenue else 'books')
    campaign = dict(campaign_id='organic', channel='organic', day=date(2026, 10, 3),
                    sessions=1, purchasing_sessions=int(bool(revenue)),
                    revenue_cents=revenue, assisted_sessions=0)
    for name, schema, rows in (
        ('user_daily', USER_DAILY_SCHEMA, [user, user] if duplicate else [user]),
        ('campaign_daily', CAMPAIGN_DAILY_SCHEMA, [campaign]),
    ):
        (folder / name).mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), folder / name / 'part-0.parquet')


class BenchmarkUnitTests(unittest.TestCase):
    def test_comparator_is_killed_and_reaped_on_parent_termination(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first, same = root / 'first', root / 'same'
            write_outputs(first)
            write_outputs(same)
            process = MagicMock(pid=987)
            process.__enter__.return_value = process
            process.communicate.side_effect = benchmark.BenchmarkInterrupted(signal.SIGTERM)
            process.wait.side_effect = [benchmark.BenchmarkInterrupted(signal.SIGTERM), 0]
            with (patch.object(benchmark.subprocess, 'Popen', return_value=process),
                  patch.object(benchmark.os, 'killpg') as kill,
                  self.assertRaises(benchmark.BenchmarkInterrupted)):
                benchmark.compare_outputs(same, first, REPO, sys.executable, root / 'compare.log')
            kill.assert_called_once_with(987, signal.SIGKILL)
            self.assertEqual(process.wait.call_count, 2)

    def test_alternating_order_and_medians(self):
        self.assertEqual(benchmark.alternating_order(3), [
            (1, 'baseline'), (1, 'candidate'), (2, 'candidate'),
            (2, 'baseline'), (3, 'baseline'), (3, 'candidate'),
        ])
        runs = []
        for arm, values in [('baseline', [9, 3, 6]), ('candidate', [1, 4, 2])]:
            for value in values:
                runs.append(dict(arm=arm, wall_seconds=value, preparation_seconds=value / 2,
                                 analytics_seconds=value / 4, pipeline_seconds=value * .75,
                                 max_individual_rss_bytes=10, sampled_tree_rss_peak_bytes=None,
                                 sampled_disk_peak_bytes=20))
        summary = benchmark.summarize(runs)
        self.assertEqual(summary['baseline']['wall_seconds'], 6)
        self.assertEqual(summary['candidate']['wall_seconds'], 2)
        self.assertEqual(summary['wall_speedup'], 3)
        self.assertIsNone(summary['candidate']['sampled_tree_rss_peak_bytes'])

    def test_timing_parser_rejects_missing_ambiguous_or_impossible_values(self):
        valid = 'prepare_inputs: 1.125s\nanalytics/events: 0.1s\nanalytics: 0.875s; total: 2.000s\n'
        self.assertEqual(benchmark.parse_timings(valid), dict(
            preparation_seconds=1.125, analytics_seconds=.875, pipeline_seconds=2.0))
        for text in ['', 'prepare_inputs: nan', valid + valid,
                     'prepare_inputs: 2.0s\nanalytics: 2.0s; total: 1.0s\n']:
            with self.subTest(text=text), self.assertRaises(benchmark.BenchmarkError):
                benchmark.parse_timings(text)

    def test_memory_units_and_descendants(self):
        self.assertEqual(benchmark.normalize_maxrss(12, 'darwin'), 12)
        self.assertEqual(benchmark.normalize_maxrss(12, 'linux'), 12288)
        rows = {1: (0, 100), 2: (1, 200), 3: (2, 300), 4: (0, 999)}
        self.assertEqual(benchmark.tree_rss(1, rows), 600)
        self.assertIsNone(benchmark.tree_rss(9, rows))

    def test_native_process_parsers_measure_only_the_selected_tree(self):
        with (patch.object(benchmark.sys, 'platform', 'darwin'),
              patch.object(benchmark.subprocess, 'run', return_value=SimpleNamespace(stdout='1 0 2\n2 1 3\n8 0 9\n'))):
            self.assertEqual(benchmark.sample_process_tree(1), 5 * 1024)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for pid, parent, pages in [(10, 0, 3), (11, 10, 2), (99, 0, 8)]:
                folder = root / str(pid)
                folder.mkdir()
                (folder / 'stat').write_text(f'{pid} (name with spaces) S {parent} 0')
                (folder / 'statm').write_text(f'100 {pages}')
            with (patch.object(benchmark.sys, 'platform', 'linux'),
                  patch.object(benchmark, 'Path', return_value=root),
                  patch.object(benchmark.os, 'sysconf', return_value=4096)):
                self.assertEqual(benchmark.sample_process_tree(10), 5 * 4096)

    def test_backend_overrides_are_allowlisted(self):
        self.assertEqual(benchmark.parse_override('INGEST_BACKEND=rust'), ('INGEST_BACKEND', 'rust'))
        self.assertEqual(benchmark.parse_override("RUST_INGEST_BIN=/tmp/with space's/binary"),
                         ('RUST_INGEST_BIN', "/tmp/with space's/binary"))
        for value in ['NCPU=8', 'DATA_DIR=/tmp', 'HOME=/tmp', 'PYTHONPATH=/tmp',
                      'API_TOKEN=secret', 'ARBITRARY=value', 'invalid', 'INGEST_BACKEND=']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                benchmark.parse_override(value)

    def test_input_fingerprint_tracks_consumed_files_not_unrelated_notes(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / 'data'
            shutil.copytree(REPO / 'fixtures/E1/input', data)
            first = benchmark.input_fingerprint(data)
            (data / 'note.txt').write_text('not consumed')
            self.assertEqual(first, benchmark.input_fingerprint(data))
            (data / 'events/new.csv').write_text('header\n')
            self.assertNotEqual(first['sha256'], benchmark.input_fingerprint(data)['sha256'])

    def test_sample_failures_are_null_with_diagnostics(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sampler = benchmark.ResourceSampler(123, [root], .01)
            with patch.object(benchmark, 'sample_process_tree', side_effect=OSError('unavailable')):
                sampler.sample()
                sampler.sample()
            self.assertIsNone(sampler.tree_peak)
            self.assertTrue(sampler.warnings)
            self.assertEqual(sampler.disk_peak, 0)

    def test_unsafe_archives_and_input_symlinks_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            destination = root / 'export'
            destination.mkdir()
            for name, kind in [('../escaped', tarfile.REGTYPE), ('/absolute', tarfile.REGTYPE),
                               ('link', tarfile.SYMTYPE), ('hardlink', tarfile.LNKTYPE),
                               ('fifo', tarfile.FIFOTYPE)]:
                buffer = io.BytesIO()
                with tarfile.open(fileobj=buffer, mode='w') as archive:
                    member = tarfile.TarInfo(name)
                    member.type = kind
                    member.linkname = '/outside'
                    archive.addfile(member)
                with (self.subTest(name=name), patch.object(benchmark, '_git', return_value=buffer.getvalue()),
                      self.assertRaises(benchmark.BenchmarkError)):
                    benchmark.export_baseline(root, 'commit', destination)
            self.assertFalse((root / 'escaped').exists())
            data = root / 'data'
            shutil.copytree(REPO / 'fixtures/E1/input', data)
            (data / 'events/symlink.csv').symlink_to(data / 'events/part-00000.csv')
            with self.assertRaises(benchmark.BenchmarkError):
                benchmark.input_fingerprint(data)

    def test_sampler_disk_failure_and_signal_handler(self):
        sampler = benchmark.ResourceSampler(123, [], .1)
        with (patch.object(benchmark, 'sample_process_tree', return_value=None),
              patch.object(benchmark, 'disk_bytes', side_effect=OSError('unavailable'))):
            sampler.sample()
        self.assertIsNone(sampler.disk_peak)
        self.assertTrue(sampler.warnings)
        with self.assertRaises(benchmark.BenchmarkInterrupted) as raised:
            benchmark._interrupt(signal.SIGTERM, None)
        self.assertEqual(raised.exception.signum, signal.SIGTERM)

    def test_child_is_killed_when_resource_sampler_cannot_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / 'logs'
            logs.mkdir()
            process = SimpleNamespace(pid=123, returncode=None)
            with (patch.object(benchmark.subprocess, 'Popen', return_value=process),
                  patch.object(benchmark.ResourceSampler, 'start', side_effect=RuntimeError('thread failed')),
                  patch.object(benchmark.os, 'killpg') as kill,
                  patch.object(benchmark.os, 'wait4', return_value=(123, 9, None)),
                  self.assertRaisesRegex(benchmark.BenchmarkError, 'thread failed')):
                benchmark.run_invocation(root=REPO, data_dir=root, run_dir=root / 'run', logs_dir=logs,
                                         run_name='failure', ncpu=1, mem_gb=1, env_overrides={},
                                         sample_interval=.1, python=sys.executable)
            kill.assert_called_once_with(123, signal.SIGKILL)

    def test_invalid_arguments_and_duplicate_overrides(self):
        for arguments in [['--runs', '0'], ['--sample-interval', 'nan'], ['--candidate-env', 'TOKEN=x']]:
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                benchmark.build_parser().parse_args(['data', *arguments])
        with self.assertRaises(benchmark.BenchmarkError):
            benchmark._overrides([('INGEST_BACKEND', 'python'), ('INGEST_BACKEND', 'rust')])

    def test_comparison_rejects_duplicates_missing_tables_and_changed_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first, same, duplicate, different = [root / name for name in ('first', 'same', 'dup', 'different')]
            write_outputs(first)
            write_outputs(same)
            write_outputs(duplicate, duplicate=True)
            write_outputs(different, revenue=1)
            benchmark.compare_outputs(same, first, REPO, sys.executable, root / 'same.log')
            for folder in [duplicate, different, root / 'missing']:
                with self.subTest(folder=folder), self.assertRaises(benchmark.BenchmarkError):
                    benchmark.compare_outputs(folder, first, REPO, sys.executable, root / f'{folder.name}.log')


class BenchmarkOrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / "data with space's"
        shutil.copytree(REPO / 'fixtures/E1/input', self.data)

    def args(self, *extra):
        return benchmark.build_parser().parse_args([
            str(self.data), '--candidate-root', str(REPO), '--runs', '1', '--ncpu', '1',
            '--mem-gb', '1', '--report', str(self.root / 'results/report.json'), *extra,
        ])

    def fake_run(self, **kwargs):
        folder = kwargs['run_dir']
        self.assertFalse(folder.exists())
        folder.mkdir()
        (folder / 'scratch').mkdir()
        write_outputs(folder / 'output')
        return dict(exit_code=0, wall_seconds=1.0, preparation_seconds=.2,
                    analytics_seconds=.3, pipeline_seconds=.5,
                    max_individual_rss_bytes=100, sampled_tree_rss_peak_bytes=None,
                    sampled_disk_peak_bytes=300, sampling_warnings=['RSS sampler unavailable'])

    def test_run_rounds_use_fresh_dirs_and_preserve_unrelated_files(self):
        sentinel = self.root / 'keep.txt'
        sentinel.write_text('keep')
        args = self.args('--runs', '3')
        with patch.object(benchmark, 'run_invocation', side_effect=self.fake_run) as run:
            report = benchmark.run_benchmark(args)
        self.assertEqual(report['status'], 'ok')
        self.assertEqual([item['arm'] for item in report['runs']],
                         ['baseline', 'candidate', 'candidate', 'baseline', 'baseline', 'candidate'])
        self.assertEqual(report['summary']['wall_speedup'], 1.0)
        self.assertEqual(len({call.kwargs['run_dir'] for call in run.call_args_list}), 6)
        self.assertEqual(sentinel.read_text(), 'keep')
        self.assertEqual(json.loads(args.report.read_text())['status'], 'ok')
        self.assertFalse(any(args.report.parent.glob('.benchmark-*')))

    def test_nonzero_exit_and_missing_timings_never_get_speedup_summary(self):
        for error in ('child', 'timings'):
            with self.subTest(error=error):
                args = self.args('--report', str(self.root / f'{error}/report.json'))
                def failed(**kwargs):
                    result = self.fake_run(**kwargs)
                    if error == 'child':
                        result['exit_code'] = 3
                    else:
                        result.pop('pipeline_seconds')
                    return result
                with patch.object(benchmark, 'run_invocation', side_effect=failed):
                    report = benchmark.run_benchmark(args)
                self.assertEqual(report['status'], 'failed')
                self.assertNotIn('summary', report)

    def test_output_mismatch_invalidates_benchmark(self):
        args = self.args()
        def changed(**kwargs):
            result = self.fake_run(**kwargs)
            if kwargs['run_name'].endswith('candidate'):
                path = kwargs['run_dir'] / 'output/user_daily/part-0.parquet'
                table = pq.read_table(path).to_pylist()
                table[0]['events'] += 1
                pq.write_table(pa.Table.from_pylist(table, schema=USER_DAILY_SCHEMA), path)
            return result
        with patch.object(benchmark, 'run_invocation', side_effect=changed):
            report = benchmark.run_benchmark(args)
        self.assertEqual(report['status'], 'failed')
        self.assertNotIn('summary', report)

    def test_input_mutation_invalidates_benchmark(self):
        args = self.args()
        def changed(**kwargs):
            result = self.fake_run(**kwargs)
            if kwargs['run_name'].endswith('candidate'):
                with (self.data / 'events/part-00000.csv').open('ab') as target:
                    target.write(b'\n')
            return result
        with patch.object(benchmark, 'run_invocation', side_effect=changed):
            report = benchmark.run_benchmark(args)
        self.assertEqual(report['status'], 'failed')
        self.assertIn('input', report['error'].lower())
        self.assertNotIn('summary', report)

    def test_candidate_source_mutation_invalidates_benchmark(self):
        args = self.args()
        with (patch.object(benchmark, 'run_invocation', side_effect=self.fake_run),
              patch.object(benchmark, 'source_fingerprint', side_effect=[{'sha256': 'before'}, {'sha256': 'after'}])):
            report = benchmark.run_benchmark(args)
        self.assertEqual(report['status'], 'failed')
        self.assertIn('source', report['error'].lower())

    def test_thread_start_failure_writes_failed_report(self):
        args = self.args()
        with patch.object(benchmark.ResourceSampler, 'start', side_effect=RuntimeError('thread failed')):
            report = benchmark.run_benchmark(args)
        self.assertEqual(report['status'], 'failed')
        self.assertIn('thread failed', report['error'])
        self.assertNotIn('summary', report)
        self.assertEqual(json.loads(args.report.read_text())['status'], 'failed')

    def test_artifacts_inside_data_and_existing_paths_are_rejected(self):
        for report in [self.data / 'report.json', self.root / 'existing.json', self.root / 'logs-case.json']:
            if report.name == 'existing.json':
                report.write_text('keep')
            if report.name == 'logs-case.json':
                (self.root / 'logs-case.logs').mkdir()
            args = self.args('--report', str(report))
            with self.subTest(report=report), self.assertRaises(benchmark.BenchmarkError):
                benchmark.run_benchmark(args)
        self.assertEqual((self.root / 'existing.json').read_text(), 'keep')
        args = self.args()
        args.data_dir = REPO
        with self.assertRaises(benchmark.BenchmarkError):
            benchmark.run_benchmark(args)

    def test_main_restores_signal_handler_on_preflight_failure_and_termination(self):
        previous = signal.getsignal(signal.SIGTERM)
        for error, expected in [(benchmark.BenchmarkError('bad input'), 1),
                                (benchmark.BenchmarkInterrupted(signal.SIGTERM), 143),
                                (KeyboardInterrupt(), 130)]:
            with (self.subTest(error=type(error).__name__),
                  patch.object(benchmark, 'run_benchmark', side_effect=error),
                  contextlib.redirect_stderr(io.StringIO())):
                self.assertEqual(benchmark.main([str(self.data)]), expected)
            self.assertEqual(signal.getsignal(signal.SIGTERM), previous)

    def test_real_tiny_fixture_against_archived_submission(self):
        args = self.args('--report', str(self.root / "real result's/report.json"))
        report = benchmark.run_benchmark(args)
        self.assertEqual(report['status'], 'ok', report.get('error'))
        self.assertEqual(len(report['runs']), 2)
        self.assertTrue(all(item['correctness'] == 'passed' for item in report['runs']))
        self.assertEqual(report['baseline']['commit'], benchmark.DEFAULT_BASELINE)
        self.assertEqual(report['measurement_context'], 'native-development')
        self.assertTrue(all(item['max_individual_rss_bytes'] > 0 for item in report['runs']))
        self.assertTrue(all(item['preparation_seconds'] >= 0 for item in report['runs']))

    def test_main_returns_nonzero_for_failed_report(self):
        args = [str(self.data), '--report', str(self.root / 'main.json')]
        with patch.object(benchmark, 'run_benchmark', return_value={'status': 'failed', 'error': 'test'}):
            self.assertEqual(benchmark.main(args), 1)


if __name__ == '__main__':
    unittest.main()
