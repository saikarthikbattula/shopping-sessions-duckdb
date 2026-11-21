"""CPU study uses isolated transformations and whole-process measurements."""
import importlib.util
from pathlib import Path
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.benchmark_cpu_candidate import compare_outputs, require_fast_path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('profile_native_cpu', ROOT / 'scripts/profile_native_cpu.py')
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


class CpuProfileTests(unittest.TestCase):
    def test_duckdb_comparison_checks_duplicates_and_both_tables(self):
        with tempfile.TemporaryDirectory() as temporary:
            left, right = (Path(temporary) / arm for arm in ('left', 'right'))
            for root in (left, right):
                for table in ('user_daily', 'campaign_daily'):
                    (root / table).mkdir(parents=True)
                    pq.write_table(pa.table({'id': ['a', 'a', 'b']}), root / table / 'part.parquet')
            compare_outputs(left, right)
            pq.write_table(pa.table({'id': ['a', 'b', 'b']}), right / 'campaign_daily/part.parquet')
            with self.assertRaisesRegex(RuntimeError, 'campaign_daily: 2 differing rows'):
                compare_outputs(left, right)

    def test_resource_measurement_uses_user_plus_system(self):
        result = SimpleNamespace(returncode=0, stderr='total_events: 1071554\ncpu_sum: 999ms\n'
                                 'CPU_MEASURE 1.21 0.32 0.81 1234 2 876\n')
        with patch.object(profile.sys, 'platform', 'linux'), patch.object(profile.subprocess, 'run', return_value=result):
            measured = profile.measure(Path('binary'), Path('data'), Path('out'), 2)
        self.assertAlmostEqual(measured['cpu_seconds'], 1.53)
        self.assertEqual(measured['system_seconds'], 0.32)
        self.assertEqual(measured['peak_rss_bytes'], 1234 * 1024)
        self.assertEqual(measured['parsed_event_copies'], 1071554)

    def test_fast_path_benchmark_rejects_silent_fallback(self):
        require_fast_path({'stderr': '[fast_path] activated\n[simple_identity] indexed_users: 10'})
        for log in ('[fast_path] activated', '[simple_identity] indexed_users: 10\nfalling back to exact pipeline'):
            with self.assertRaisesRegex(RuntimeError, 'did not complete'):
                require_fast_path({'stderr': log})

    def test_macos_wait4_records_exact_child_resources(self):
        child = SimpleNamespace(pid=123, returncode=None)
        usage = SimpleNamespace(ru_utime=1.25, ru_stime=0.5, ru_maxrss=123456,
                                ru_majflt=2, ru_minflt=300)
        def launch(*args, **kwargs):
            kwargs['stderr'].write('total_events: 5000000\n')
            kwargs['stderr'].flush()
            return child
        with patch.object(profile.sys, 'platform', 'darwin'), \
             patch.object(profile.subprocess, 'Popen', side_effect=launch), \
             patch.object(profile.os, 'wait4', return_value=(123, 0, usage)) as wait:
            measured = profile.measure(Path('binary'), Path('data'), Path('out'), 8)
        wait.assert_called_once_with(123, 0)
        self.assertEqual(child.returncode, 0)
        self.assertEqual(measured['cpu_seconds'], 1.75)
        self.assertEqual(measured['peak_rss_bytes'], 123456)
        self.assertEqual(measured['parsed_event_copies'], 5000000)

    def test_failed_process_cannot_be_reported_as_a_measurement(self):
        with patch.object(profile.sys, 'platform', 'linux'), patch.object(profile.subprocess, 'run', return_value=SimpleNamespace(returncode=1, stderr='failed')):
            with self.assertRaisesRegex(RuntimeError, 'failed'):
                profile.measure(Path('binary'), Path('data'), Path('out'), 2)

    def test_scratch_transformations_are_separate_and_fail_on_anchor_drift(self):
        source = (ROOT / 'native/ingest/src/analytics.rs').read_text()
        baseline = subprocess.run(['git', '-C', str(ROOT), 'show', f'{profile.BASELINE}:native/ingest/src/analytics.rs'],
                                  capture_output=True, text=True)
        sources = {'current': source}
        if baseline.returncode == 0:
            sources['baseline'] = baseline.stdout
        for name, text in sources.items():
            for variant in profile.VARIANTS:
                with self.subTest(source=name, variant=variant):
                    self.assertNotEqual(profile.ablate(text, variant), text)
        self.assertEqual((ROOT / 'native/ingest/src/analytics.rs').read_text(), source)
        with self.assertRaises(ValueError):
            profile.ablate('', 'raw_user')
        with self.assertRaises(ValueError):
            profile.replace_once('twice twice', 'twice', 'replacement')


if __name__ == '__main__':
    unittest.main()
