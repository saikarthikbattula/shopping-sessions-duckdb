"""The CI fingerprint check must be order-independent and reject differences."""

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.run_fixtures import SCHEMAS, load_expected
from tests.schemas import USER_DAILY_SCHEMA, CAMPAIGN_DAILY_SCHEMA


class DevFingerprintTests(unittest.TestCase):
    def test_matches_reordered_fixture_rows_and_rejects_changed_revenue(self):
        scripts = Path(__file__).resolve().parents[1] / 'scripts'
        with patch.dict(sys.modules, run_fixtures=__import__('scripts.run_fixtures', fromlist=[''])):
            spec = importlib.util.spec_from_file_location('dev_fingerprints', scripts / 'check_dev_fingerprints.py')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        expected = Path(__file__).resolve().parents[1] / 'fixtures/E14/expected'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / 'expected.json'
            manifest.write_text(json.dumps(dict(tables=module.fingerprints(expected))))
            for name, schema in SCHEMAS.items():
                frame = load_expected(expected / f'{name}.csv', schema)
                (root / name).mkdir()
                rows = frame.reverse().to_dicts()
                # Convert accepted Int128 values to Parquet integers, as production does.
                for i, row in enumerate(rows):
                    arrow_schema = USER_DAILY_SCHEMA if name == 'user_daily' else CAMPAIGN_DAILY_SCHEMA
                    pq.write_table(pa.Table.from_pylist([row], schema=arrow_schema), root / name / f'part-{i}.parquet')
            command = [sys.executable, str(scripts / 'check_dev_fingerprints.py'), str(root), '--expected', str(manifest)]
            matched = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(matched.returncode, 0, matched.stderr)
            path = root / 'user_daily/part-0.parquet'
            rows = pq.read_table(path).to_pylist()
            rows[0]['revenue_cents'] += 1
            pq.write_table(pa.Table.from_pylist(rows, schema=USER_DAILY_SCHEMA), path)
            different = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(different.returncode, 0)
            self.assertIn('fingerprints differ', different.stderr)


if __name__ == '__main__':
    unittest.main()
