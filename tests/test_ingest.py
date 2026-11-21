"""Ingestion acceptance checks, independent of downstream analytical filtering."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq

from pipeline.contracts import CAMPAIGNS_SCHEMA, CLICKS_SCHEMA, EVENTS_SCHEMA, PRODUCTS_SCHEMA
from pipeline.identity import IdentityMap
from pipeline.ingest import prepare_inputs
from pipeline.parsing import parse_line, parse_timestamp, parse_unsigned, read_rows, window_timestamp


class ParsingTests(unittest.TestCase):
    def test_strict_fields_nulls_and_whitespace(self):
        self.assertEqual(parse_line('" u1",u1 ,"",\t\r\n', 4), (" u1", "u1 ", None, "\t\r"))
        for line in ('', '\n', 'a,b,c', '"a,b', 'a"x,b', '"a" ,b', '"a""b",c', '"a,b",c'):
            with self.subTest(line=line):
                self.assertIsNone(parse_line(line, 2))
        self.assertEqual(parse_line('"a","b"', 2), ("a", "b"))
        self.assertIsNone(parse_line('a,"b"\r\n', 2))

    def test_lf_only_and_exactly_one_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "part.csv"
            path.write_bytes(b'ignored\rheader\nfirst,last\r\nfirst,last\na\rb,c\n"",x')
            self.assertEqual(list(read_rows(path, 2)), [
                ("first", "last\r"), ("first", "last"), ("a\rb", "c"), (None, "x"),
            ])

    def test_ascii_prices_and_unbounded_leading_zeros(self):
        for value in (None, '', '٢٥٠', '２５０', '+250', '-1', '250.0', '1e2', '1\r', ' 1', '10000001', '9' * 10000):
            with self.subTest(value=str(value)[:20]):
                self.assertIsNone(parse_unsigned(value, 10_000_000))
        self.assertEqual(parse_unsigned('0' * 10000 + '250', 10_000_000), 250)
        self.assertEqual(parse_unsigned('10000000', 10_000_000), 10_000_000)
        self.assertEqual(parse_unsigned('000', 10_000_000), 0)

    def test_timestamp_forms_dates_and_offsets(self):
        expected = parse_timestamp('2026-10-03T23:58:10Z')
        self.assertEqual(expected, 1791071890000)
        for value in ('2026-10-03T19:58:10-04:00', '2026-10-04T01:58:10+02:00',
                      '2026-10-03T23:58:10-00:00', '0' * 10000 + str(expected)):
            self.assertEqual(parse_timestamp(value), expected)
        self.assertEqual(parse_timestamp('2028-02-29T00:00:00Z'), 1835395200000)
        self.assertEqual(parse_timestamp('1970-01-01T00:00:00+23:59'), -86340000)
        self.assertEqual(parse_timestamp('86400000'), 86400000)
        self.assertEqual(parse_timestamp(str(2**63 - 1)), 2**63 - 1)
        for value in ('2027-02-29T00:00:00Z', '2028-02-30T00:00:00Z', '2026-13-01T00:00:00Z',
                      '2026-01-00T00:00:00Z', '0000-01-01T00:00:00Z',
                      '2026-10-03T24:00:00Z', '2026-10-03T23:60:00Z', '2026-10-03T23:59:60Z',
                      '2026-10-03T00:00:00+24:00', '2026-10-03T00:00:00+05:60',
                      '2026-10-03T00:00:00+0200', '2026-10-03T00:00:00',
                      '2026-10-03t00:00:00Z', '2026-10-03T00:00:00z',
                      '2026-10-03 00:00:00Z', '2026-10-03T00:00:00.001Z',
                      '2026-10-03T00:00:00Z\r', '٢٥٠', '２５０', '-5', '1.0', '1e12',
                      '9' * 10000, str(2**63)):
            with self.subTest(value=value[:40]):
                self.assertIsNone(parse_timestamp(value))

    def test_millisecond_window_boundaries(self):
        start, end = 1790812800000, 1792022400000
        for low in (start, start - 604800000):
            for ts, expected in ((low - 1, None), (low, low), (end - 1, end - 1), (end, None)):
                self.assertEqual(window_timestamp(str(ts), low, end), expected)


class IdentityTests(unittest.TestCase):
    def test_chains_shared_devices_cycles_unused_minimum_and_exact_ids(self):
        identity = IdentityMap()
        for index in range(900, 0, -1):
            identity.union(f'user{index}', f'user{index - 1}')
        identity.union('user500', 'Unused minimum')
        identity.union('device', 'user900')
        identity.union('device', 'other person')
        identity.union('user0', 'user900')
        identity.union('user900', 'user0')
        identity.union('user0', 'user0')
        identity.union(None, 'AAA')
        identity.union('AAA', None)
        identity.union('', 'AAA')
        for user in ('user0', 'user899', 'other person', 'device'):
            self.assertEqual(identity.person(user), 'Unused minimum')
        self.assertEqual(identity.person('AAA'), 'AAA')
        self.assertEqual(identity.person('User0'), 'User0')
        self.assertIsNone(identity.person(None))
        identity.union('é', '中')
        identity.union('中', 'z')
        self.assertEqual(identity.person('é'), 'z')
        identity.union(' separate\r', 'separate\t')
        self.assertEqual(identity.person('separate\t'), ' separate\r')


class StagingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data, self.scratch = self.root / 'data', self.root / 'scratch'
        self.data.mkdir()
        for directory in ('events', 'clicks', 'links'):
            (self.data / directory).mkdir()
            self.write(f'{directory}/header-only.csv', 'header\n')
        self.write('manifest.json', json.dumps({
            'window_start': '2026-10-01T00:00:00Z', 'window_end': '2026-10-15T00:00:00Z',
        }))
        self.write('products.csv', 'product_id,category\np,category\n" p"," category\t"\n')
        self.write('campaigns.csv', 'campaign_id,channel\nc,channel\n" c","channel\r"\n')

    def write(self, name, text):
        (self.data / name).write_bytes(text.encode('utf-8'))

    def read(self, path):
        return pq.read_table(path).to_pylist()

    def check_duckdb(self, prepared):
        with duckdb.connect() as con:
            for path, schema in ((prepared.events_dir, EVENTS_SCHEMA), (prepared.clicks_dir, CLICKS_SCHEMA),
                                 (prepared.products_path, PRODUCTS_SCHEMA), (prepared.campaigns_path, CAMPAIGNS_SCHEMA)):
                files = list(path.glob('*.parquet')) if path.is_dir() else [path]
                for file in files:
                    self.assertEqual(pq.read_schema(file), schema)
                rows = con.execute('SELECT * FROM read_parquet(?)', [[str(file) for file in files]]).fetchall()
                self.assertEqual(len(rows), len(self.read(path)))

    def test_all_copies_and_invalid_payloads_survive_bounded_staging(self):
        self.write('links/part.csv', 'a,b\nu,z\nz,A unused\n,AAA\n"broken,b\n')
        self.write('events/part.csv', 'header\n'
                   'duplicate,u,bad,2026-10-01T00:00:00Z,purchase,p,250\n'
                   'late,u,2026-10-01T00:00:00Z,2026-10-03T00:00:00Z,view,p,garbage\n'
                   ',u,bad,2026-10-01T00:00:00Z,view,p,\n'
                   'malformed,row\n'
                   'cr,u,2026-10-01T00:00:00Z,2026-10-01T00:00:00Z,purchase,p,250\r\n'
                   'zero,u,2026-10-01T00:00:00Z,2026-10-01T00:00:00Z,purchase,p,0\n'
                   'empty-user,,2026-10-01T00:00:00Z,2026-10-01T00:00:00Z,view,p,\n'
                   'type,u,2026-10-01T00:00:00Z,2026-10-01T00:00:00Z,Purchase,p,2\n'
                   'product,u,2026-10-01T00:00:00Z,2026-10-01T00:00:00Z,cart,,\n')
        self.write('events/another.csv', 'header\n'
                   'duplicate,z,2026-10-01T00:00:00Z,2026-10-01T00:00:01Z,purchase,p,000250\n'
                   'unknown,Unlinked,2026-10-01T00:00:00Z,2026-10-01T00:00:00Z,cart,missing,garbage\n')
        start, end = 1790812800000, 1792022400000
        self.write('clicks/part.csv', 'header\n' + ''.join(
            f'{name},u,{campaign},{ts}\n' for name, campaign, ts in (
                ('dup', 'c', start), ('dup', 'c', start), ('old', 'c', start - 604800000),
                ('too-old', 'c', start - 604800001), ('last', 'c', end - 1), ('end', 'c', end),
                ('unknown', 'C', start), ('cr', 'c', str(start) + '\r'),
            )
        ) + ',u,c,1\nmalformed\nempty,,c,1790812800000\n')
        prepared = prepare_inputs(self.data, self.scratch, batch_rows=2)
        events = self.read(prepared.events_dir)
        self.assertEqual(len(events), 9)
        duplicates = sorted((row for row in events if row['event_id'] == 'duplicate'), key=lambda row: row['ingest_ms'])
        self.assertEqual([row['payload_valid'] for row in duplicates], [False, True])
        self.assertEqual([row['person_id'] for row in duplicates], ['A unused', 'A unused'])
        self.assertIsNone(duplicates[0]['ts_ms'])
        self.assertEqual(duplicates[1]['price_cents'], 250)
        by_id = {row['event_id']: row for row in events}
        self.assertTrue(by_id['late']['payload_valid'])
        self.assertIsNone(by_id['late']['price_cents'])
        self.assertTrue(by_id['unknown']['payload_valid'])
        self.assertEqual(by_id['unknown']['person_id'], 'Unlinked')
        self.assertIsNone(by_id['empty-user']['person_id'])
        for key in ('cr', 'zero', 'empty-user', 'type', 'product'):
            self.assertFalse(by_id[key]['payload_valid'])
        clicks = self.read(prepared.clicks_dir)
        self.assertEqual(len(clicks), 9)
        self.assertEqual(sum(row['payload_valid'] for row in clicks), 4)
        self.assertEqual(sum(row['click_id'] == 'dup' for row in clicks), 2)
        self.assertEqual(self.read(prepared.products_path)[1], {'product_id': ' p', 'category': ' category\t'})
        self.assertEqual(self.read(prepared.campaigns_path)[1], {'campaign_id': ' c', 'channel': 'channel\r'})
        for path in (prepared.events_dir, prepared.clicks_dir):
            row_groups = 0
            for file in path.glob('*.parquet'):
                metadata = pq.ParquetFile(file).metadata
                row_groups += metadata.num_row_groups
                self.assertTrue(all(metadata.row_group(i).num_rows <= 2 for i in range(metadata.num_row_groups)))
            self.assertGreater(row_groups, 1)
        self.check_duckdb(prepared)

    def test_empty_tables_and_repeated_calls_have_independent_paths(self):
        self.write('products.csv', 'header\n')
        self.write('campaigns.csv', 'header\n')
        first = prepare_inputs(self.data, self.scratch, batch_rows=1)
        second = prepare_inputs(self.data, self.scratch, batch_rows=1)
        self.assertNotEqual(first.events_dir, second.events_dir)
        self.assertTrue(first.events_dir.exists())
        self.assertEqual(self.read(first.events_dir), [])
        self.assertEqual(self.read(first.clicks_dir), [])
        self.check_duckdb(first)

    def test_event_windows_prices_and_lateness_are_staged_exactly(self):
        start, end = 1790812800000, 1792022400000
        cases = [
            ('start', str(start), 'view', 'not a price', True, None),
            ('before', str(start - 1), 'view', '', False, None),
            ('last', str(end - 1), 'view', '', True, None),
            ('end', str(end), 'view', '', False, None),
            ('huge-ts', '9' * 10000, 'view', '', False, None),
            ('zero-prefix', '0' * 10000 + str(start), 'purchase', '0' * 10000 + '1', True, 1),
            ('max-price', str(start), 'purchase', '10000000', True, 10000000),
            ('over-price', str(start), 'purchase', '10000001', False, None),
            ('unicode-price', str(start), 'purchase', '٢٥٠', False, None),
        ]
        self.write('events/part.csv', 'header\n' + ''.join(
            f'{name},u,{ts},2026-10-02T00:00:00Z,{kind},p,{price}\n'
            for name, ts, kind, price, valid, cents in cases
        ) + 'late,u,1790812800000,2026-10-02T00:00:01Z,cart,p,\n'
          + 'early-ingest,u,1790812800000,2026-09-30T00:00:00Z,cart,p,\n')
        prepared = prepare_inputs(self.data, self.scratch, batch_rows=1)
        events = {row['event_id']: row for row in self.read(prepared.events_dir)}
        for name, ts, kind, price, valid, cents in cases:
            self.assertEqual(events[name]['payload_valid'], valid, name)
            self.assertEqual(events[name]['price_cents'], cents, name)
        self.assertEqual(events['start']['ingest_ms'] - events['start']['ts_ms'], 86400000)
        self.assertTrue(events['late']['payload_valid'])
        self.assertTrue(events['early-ingest']['payload_valid'])
        self.assertIsNone(events['before']['ts_ms'])
        self.assertIsNone(events['end']['ts_ms'])

    def test_header_like_data_rows_are_not_skipped(self):
        self.write('links/part.csv', 'a,b\na,b\n')
        self.write('clicks/part.csv', 'click_id,user_id,campaign_id,ts\nclick_id,user_id,campaign_id,ts\n')
        prepared = prepare_inputs(self.data, self.scratch)
        self.assertEqual(self.read(prepared.clicks_dir)[0]['click_id'], 'click_id')

    def test_bad_configuration_and_operational_errors_raise(self):
        for batch_rows in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                prepare_inputs(self.data, self.scratch, batch_rows=batch_rows)
        with patch('pipeline.ingest_python.pq.ParquetWriter', side_effect=OSError('disk full')):
            with self.assertRaisesRegex(OSError, 'disk full'):
                prepare_inputs(self.data, self.scratch)
        self.write('manifest.json', '{"window_start":"bad","window_end":"bad"}')
        with self.assertRaises(ValueError):
            prepare_inputs(self.data, self.scratch)


if __name__ == '__main__':
    unittest.main()
