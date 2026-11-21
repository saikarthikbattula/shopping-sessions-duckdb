"""Raw CSV differential checks spanning ingestion and analytical processing."""

import contextlib
import io
import json
import os
import random
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq

from pipeline.pipeline import main
from tests.test_analytics import BASE, CAMPAIGNS, DAY, GAP, PRODUCTS, WEEK, canonical, reference


def utc_text(milliseconds):
    return (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=milliseconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def timestamp_text(milliseconds, rng):
    if milliseconds % 1000 or rng.randrange(3) == 0:
        return "0" * rng.randrange(40) + str(milliseconds)
    if rng.randrange(2):
        return utc_text(milliseconds)
    offset = rng.choice([-240, 330, 1439, 0])
    local = datetime(1970, 1, 1) + timedelta(milliseconds=milliseconds, minutes=offset)
    sign = "-" if offset < 0 else "+"
    hours, minutes = divmod(abs(offset), 60)
    return local.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{hours:02}:{minutes:02}"


def raw_case(seed):
    """Construct CSV fields and known semantic rows together, without parsing.

    Canonical IDs come from constructed components, not production union-find;
    the expected tables use the independent brute-force analytical reference.
    """
    rng = random.Random(seed)
    components = [
        ["\tunused", "u1", "é", "z", "shared-device"] + [f"chain-{i}" for i in range(180)],
        [" U", "U", "u "], ["solo"], ["different", "different-device"],
    ]
    names = {alias: min(group, key=lambda value: value.encode()) for group in components for alias in group}
    aliases = [alias for group in components for alias in group if alias != "\tunused"]
    links = []
    for group in components:
        shuffled = list(group)
        rng.shuffle(shuffled)
        links.extend(tuple(shuffled[i:i + 2]) for i in range(len(shuffled) - 1))
    links.extend([("u1", "u1"), ("u1", "shared-device"), ("shared-device", "u1"),
                  ("user_id_a", "user_id_b"), (None, "solo"), ("solo", None),
                  'u1,solo,extra', 'u1,so"lo', ''])
    events, clicks, expected_events, expected_clicks = [], [], [], []
    for i in range(120):
        instant = BASE + rng.choice([-1, 0, 1, GAP, GAP + 1, DAY - 1, DAY, WEEK, 14 * DAY - 1, 14 * DAY])
        text = timestamp_text(instant, rng)
        valid_ts = BASE <= instant < BASE + 14 * DAY
        if rng.randrange(10) == 0:
            text, valid_ts = rng.choice([None, '2026-02-30T00:00:00Z', '1.79e12', '٢٥٠', '9' * 50]), False
        user = rng.choice(aliases + [None])
        kind = rng.choice(['view', 'cart', 'purchase', 'purchase', 'Purchase', 'cart '])
        product = rng.choice(['p1', 'p2', 'missing', None])
        price, cents = rng.choice([('1', 1), ('500', 500), ('0250', 250), ('10000000', 10000000),
                                   ('0', None), ('-1', None), ('1.0', None), ('10000001', None), ('２５０', None), (None, None)])
        ingest = ((instant + rng.choice([-DAY, 0, DAY, DAY + 1000])) // 1000) * 1000
        identifier = f"e{i}"
        fields = (identifier, user, text, utc_text(ingest), kind, product, price)
        row = dict(event_id=identifier, person_id=names.get(user), ingest_ms=ingest,
                   ts_ms=instant if valid_ts else None, event_type=kind, product_id=product,
                   price_cents=cents if kind == 'purchase' else None,
                   payload_valid=(user is not None and valid_ts and product is not None
                                  and kind in ('view', 'cart', 'purchase')
                                  and (kind != 'purchase' or cents is not None)))
        events.append(fields)
        expected_events.append(row)
        if rng.randrange(4) == 0:
            # A later copy can change user; the original must still win globally.
            later = dict(row, person_id=names['solo'], ingest_ms=ingest + 2000)
            later['payload_valid'] = bool(valid_ts and product is not None
                                         and kind in ('view', 'cart', 'purchase')
                                         and (kind != 'purchase' or cents is not None))
            events.append((identifier, 'solo', text, utc_text(ingest + 2000), kind, product, price))
            expected_events.append(later)
    # Invalid earliest copy suppresses a later valid copy, across partitions.
    events.extend([('suppressed', 'solo', 'bad', utc_text(BASE), 'view', 'p1', None),
                   ('suppressed', 'solo', str(BASE), utc_text(BASE + 1000), 'view', 'p1', None),
                   (None, 'solo', str(BASE), utc_text(BASE), 'view', 'p1', None),
                   ('cr-view', 'solo', str(BASE), utc_text(BASE), 'view', 'p1', 'ignored\r'),
                   ('cr-purchase', 'solo', str(BASE), utc_text(BASE), 'purchase', 'p1', '1000\r'),
                   'bad,too,few', 'bad"id,solo,bad,bad,view,p1,1', ''])
    expected_events.extend([
        dict(event_id='suppressed', person_id='solo', ingest_ms=BASE, ts_ms=None,
             event_type='view', product_id='p1', price_cents=None, payload_valid=False),
        dict(event_id='suppressed', person_id='solo', ingest_ms=BASE + 1000, ts_ms=BASE,
             event_type='view', product_id='p1', price_cents=None, payload_valid=True),
        dict(event_id='cr-view', person_id='solo', ingest_ms=BASE, ts_ms=BASE,
             event_type='view', product_id='p1', price_cents=None, payload_valid=True),
        dict(event_id='cr-purchase', person_id='solo', ingest_ms=BASE, ts_ms=BASE,
             event_type='purchase', product_id='p1', price_cents=None, payload_valid=False),
    ])
    for i in range(80):
        instant = BASE + rng.choice([-WEEK - 1, -WEEK, -1, 0, 1, DAY, WEEK, 14 * DAY - 1, 14 * DAY])
        text = timestamp_text(instant, rng)
        valid_ts = BASE - WEEK <= instant < BASE + 14 * DAY
        if rng.randrange(10) == 0:
            text, valid_ts = '2026-10-01T00:00:00Z\r', False
        user = rng.choice(aliases + [None])
        campaign = rng.choice(['c1', 'c2', 'c3', ' c1', None])
        fields = (f'k{i}', user, campaign, text)
        row = dict(click_id=f'k{i}', person_id=names.get(user), campaign_id=campaign,
                   ts_ms=instant if valid_ts else None,
                   payload_valid=(user is not None and valid_ts and campaign in CAMPAIGNS))
        clicks.append(fields)
        expected_clicks.append(row)
        if rng.randrange(4) == 0:
            clicks.append(fields)
            expected_clicks.append(dict(row))
    clicks.extend(['bad,too,many,fields,here', 'bad"id,solo,c1,0', (None, 'solo', 'c1', str(BASE)), ''])
    return dict(events=events, clicks=clicks, links=links), reference(expected_events, expected_clicks, PRODUCTS, CAMPAIGNS)


def write_case(data, records, seed, parts):
    rng = random.Random(seed)

    def line(fields):
        if isinstance(fields, str):
            return fields + '\n'
        encoded = []
        for value in fields:
            value = '' if value is None else value
            encoded.append('"' + value + '"' if rng.random() < 0.4 else value)
        return ','.join(encoded) + '\n'

    headers = {'events': 'event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents',
               'clicks': 'click_id,user_id,campaign_id,ts', 'links': 'user_id_a,user_id_b'}
    for name, rows in records.items():
        folder = data / name
        folder.mkdir(parents=True)
        shuffled = list(rows)
        rng.shuffle(shuffled)
        for part in range(parts + 1):  # Include a header-only file in every folder.
            with (folder / f'part-{part}.csv').open('w', encoding='utf-8', newline='') as target:
                target.write(headers[name] + '\n')
                if part < parts:
                    target.writelines(line(fields) for fields in shuffled[part::parts])
    (data / 'products.csv').write_text('product_id,category\n' + ''.join(f'{key},{value}\n' for key, value in PRODUCTS.items()))
    (data / 'campaigns.csv').write_text('campaign_id,channel\n' + ''.join(f'{key},{value}\n' for key, value in CAMPAIGNS.items()))
    (data / 'manifest.json').write_text(json.dumps(dict(window_start=utc_text(BASE), window_end=utc_text(BASE + 14 * DAY))))


class RawIntegrationTests(unittest.TestCase):
    def test_raw_csv_differential_and_repartitioning(self):
        for seed in range(8):
            records, expected = raw_case(seed)
            for parts in (2, 7):
                with self.subTest(seed=seed, parts=parts), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    data, output = root / 'data', root / 'output'
                    write_case(data, records, seed + parts, parts)
                    stdout = io.StringIO()
                    with (patch.dict(os.environ, dict(DATA_DIR=str(data), OUTPUT_DIR=str(output),
                                                     SCRATCH_DIR=str(root / 'scratch'), NCPU='2', MEM_GB='2')),
                          contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(stdout)):
                        main()
                    self.assertEqual(stdout.getvalue(), '')
                    actual = tuple(canonical(pq.read_table(output / name).to_pylist(), key)
                                   for name, key in [('user_daily', 'user_id'), ('campaign_daily', 'campaign_id')])
                    self.assertEqual(actual, expected)
                    self.assertEqual(list((root / 'scratch').iterdir()), [])


if __name__ == '__main__':
    unittest.main()
