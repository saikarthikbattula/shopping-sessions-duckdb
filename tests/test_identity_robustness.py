"""Unseen ID spelling/range and date-window checks against independent expected rows."""
import copy
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

import pyarrow.parquet as pq

from tests.native_support import native_binary, run_native
from tests.test_analytics import BASE, DAY, canonical
from tests.test_integration import raw_case, utc_text, write_case

ID_COLUMNS = {'events': {'event': (0,), 'user': (1,)},
              'clicks': {'click': (0,), 'user': (1,)},
              'links': {'user': (0, 1)}}


def base36(value):
    digits = '0123456789abcdefghijklmnopqrstuvwxyz'
    out = ''
    while value:
        value, remainder = divmod(value, 36)
        out = digits[remainder] + out
    return out or '0'


def renamed(records, mode):
    values = {kind: set() for kind in ('event', 'click', 'user')}
    for table, kinds in ID_COLUMNS.items():
        for row in records[table]:
            if isinstance(row, str): continue
            for kind, columns in kinds.items():
                values[kind].update(row[i] for i in columns if row[i])
    mappings = {}
    for kind, names in values.items():
        ordered = sorted(names, key=lambda n: n.encode())
        mappings[kind] = {name: ('opaque-λ-' + name if mode == 'opaque' else
                                {'event': 'e', 'click': 'k', 'user': 'u'}[kind] + base36((1 << 55) + i))
                          for i, name in enumerate(ordered)}
    result = {}
    for table, rows in records.items():
        result[table] = []
        for row in rows:
            if isinstance(row, str): result[table].append(row); continue
            row = list(row)
            for kind, columns in ID_COLUMNS[table].items():
                for i in columns:
                    if row[i]: row[i] = mappings[kind][row[i]]
            result[table].append(tuple(row))
    return result, mappings['user']


def shift_timestamp(text, days):
    if text is None: return None
    if text and all('0' <= c <= '9' for c in text):
        value = int(text)
        return str(value + days * DAY) if value <= (1 << 63) - 1 else text
    if not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-][0-9]{2}:[0-9]{2})', text):
        return text
    try:
        value = datetime.fromisoformat(text.replace('Z', '+00:00')) + timedelta(days=days)
    except ValueError:
        return text
    return value.isoformat(timespec='seconds').replace('+00:00', 'Z')


class IdentityRobustnessTests(unittest.TestCase):
    def test_opaque_and_large_ids_and_shifted_windows(self):
        for seed in (5, 17):
            original, expected = raw_case(seed)
            for mode, days in (('opaque', 0), ('large', 0), ('opaque', 500)):
                with self.subTest(seed=seed, mode=mode, days=days), tempfile.TemporaryDirectory() as tmp:
                    records, user_names = renamed(original, mode)
                    if days:
                        for table, columns in (('events', (2, 3)), ('clicks', (3,))):
                            records[table] = [tuple(shift_timestamp(v, days) if i in columns else v
                                                    for i, v in enumerate(row)) if not isinstance(row, str) else row
                                              for row in records[table]]
                    root = Path(tmp); data = root / 'data'
                    write_case(data, records, seed + 100, parts=7)
                    if days:
                        import json
                        (data / 'manifest.json').write_text(json.dumps({
                            'window_start': utc_text(BASE + days * DAY),
                            'window_end': utc_text(BASE + (days + 14) * DAY)}))
                    run_native(data, root / 'actual', workers=2)
                    for table, rows, key in (('user_daily', expected[0], 'user_id'),
                                              ('campaign_daily', expected[1], 'campaign_id')):
                        wanted = [dict(row, day=row['day'] + timedelta(days=days),
                                       **({'user_id': user_names[row['user_id']]} if key == 'user_id' else {}))
                                  for row in rows]
                        self.assertEqual(canonical(pq.read_table(root / 'actual' / table).to_pylist(), key),
                                         canonical(wanted, key))
                    env = dict(os.environ, NCPU='2')
                    generic = subprocess.run([str(native_binary()), '--generic', str(data), str(root / 'generic')],
                                             env=env, capture_output=True, text=True, timeout=60)
                    self.assertEqual(generic.returncode, 0, generic.stderr)
                    for table, key in (('user_daily', 'user_id'), ('campaign_daily', 'campaign_id')):
                        self.assertEqual(canonical(pq.read_table(root / 'generic' / table).to_pylist(), key),
                                         canonical(pq.read_table(root / 'actual' / table).to_pylist(), key))
