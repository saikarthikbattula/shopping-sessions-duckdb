"""CPU study uses isolated transformations and whole-process measurements."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('profile_native_cpu', ROOT / 'scripts/profile_native_cpu.py')
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


class CpuProfileTests(unittest.TestCase):
    def test_resource_measurement_uses_user_plus_system(self):
        result = SimpleNamespace(returncode=0, stderr='total_events: 1071554\ncpu_sum: 999ms\n'
                                 'CPU_MEASURE 1.21 0.32 0.81 1234 2 876\n')
        with patch.object(profile.subprocess, 'run', return_value=result):
            measured = profile.measure(Path('binary'), Path('data'), Path('out'), 2)
        self.assertAlmostEqual(measured['cpu_seconds'], 1.53)
        self.assertEqual(measured['system_seconds'], 0.32)
        self.assertEqual(measured['peak_rss_bytes'], 1234 * 1024)
        self.assertEqual(measured['parsed_event_copies'], 1071554)

    def test_failed_process_cannot_be_reported_as_a_measurement(self):
        with patch.object(profile.subprocess, 'run', return_value=SimpleNamespace(returncode=1, stderr='failed')):
            with self.assertRaisesRegex(RuntimeError, 'failed'):
                profile.measure(Path('binary'), Path('data'), Path('out'), 2)

    def test_scratch_transformations_are_separate_and_fail_on_anchor_drift(self):
        source = (ROOT / 'native/ingest/src/analytics.rs').read_text()
        for variant in profile.VARIANTS:
            with self.subTest(variant=variant):
                changed = profile.ablate(source, variant)
                self.assertNotEqual(changed, source)
                self.assertEqual((ROOT / 'native/ingest/src/analytics.rs').read_text(), source)
        with self.assertRaises(ValueError):
            profile.ablate('', 'raw_user')
        with self.assertRaises(ValueError):
            profile.replace_once('twice twice', 'twice', 'replacement')


if __name__ == '__main__':
    unittest.main()
