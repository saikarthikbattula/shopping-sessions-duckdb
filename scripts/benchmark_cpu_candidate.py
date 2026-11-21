#!/usr/bin/env python3
"""Interleaved whole-process CPU benchmark with DuckDB multiset equality gates."""
import argparse
import hashlib
import platform
import json
from pathlib import Path
import statistics
import tempfile

import duckdb

if __package__:
    from .profile_native_cpu import measure
else:
    from profile_native_cpu import measure


def compare_outputs(left, right):
    with duckdb.connect() as connection:
        connection.execute("SET memory_limit = '512MB'")
        connection.execute('SET threads = 2')
        for table in ('user_daily', 'campaign_daily'):
            paths = [str(root / table / '**/*.parquet') for root in (left, right)]
            differences = connection.execute('''
                WITH a AS (SELECT * FROM read_parquet(?)),
                     b AS (SELECT * FROM read_parquet(?))
                SELECT count(*) FROM (
                    (SELECT * FROM a EXCEPT ALL SELECT * FROM b)
                    UNION ALL
                    (SELECT * FROM b EXCEPT ALL SELECT * FROM a)
                )''', paths).fetchone()[0]
            if differences:
                raise RuntimeError(f'{table}: {differences} differing rows (EXCEPT ALL both ways)')


def require_fast_path(result):
    log = result['stderr']
    if '[simple_identity]' not in log or 'falling back' in log:
        raise RuntimeError('Candidate did not complete the simple-user fast path')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('baseline', type=Path)
    parser.add_argument('candidate', type=Path)
    parser.add_argument('data', type=Path)
    parser.add_argument('report', type=Path)
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--require-fast-path', action='store_true',
                        help='Reject candidate runs that silently use the exact fallback')
    args = parser.parse_args()
    if args.workers < 1:
        parser.error('workers must be positive')
    hashes = {arm: hashlib.sha256(getattr(args, arm).read_bytes()).hexdigest()
              for arm in ('baseline', 'candidate')}
    runs = []
    with tempfile.TemporaryDirectory(prefix='cpu-candidate-') as temporary:
        root = Path(temporary)
        for round_number in range(3):
            order = ('baseline', 'candidate') if round_number % 2 == 0 else ('candidate', 'baseline')
            for arm in order:
                result = measure(getattr(args, arm).resolve(), args.data.resolve(), root / f'{round_number}-{arm}', args.workers)
                if arm == 'candidate' and args.require_fast_path:
                    require_fast_path(result)
                runs.append(dict(arm=arm, round=round_number, **result))
            compare_outputs(root / f'{round_number}-baseline', root / f'{round_number}-candidate')
    counts = {run['parsed_event_copies'] for run in runs}
    if len(counts) != 1 or not next(iter(counts)):
        raise RuntimeError(f'Baseline/candidate input event counts differ: {counts}')
    count = next(iter(counts))
    summary = {arm: {key: statistics.median(r[key] for r in runs if r['arm'] == arm)
                    for key in ('user_seconds', 'system_seconds', 'cpu_seconds', 'wall_seconds',
                                'peak_rss_bytes', 'minor_faults', 'major_faults')}
               for arm in ('baseline', 'candidate')}
    for row in summary.values():
        row['cpu_us_per_event_copy'] = row['cpu_seconds'] * 1e6 / count
    summary['cpu_reduction_percent'] = 100 * (1 - summary['candidate']['cpu_seconds'] / summary['baseline']['cpu_seconds'])
    for arm, digest in hashes.items():
        if hashlib.sha256(getattr(args, arm).read_bytes()).hexdigest() != digest:
            raise RuntimeError(f'{arm} executable changed during measurement')
    report = dict(binary_sha256=hashes, platform=platform.platform(),
                  require_fast_path=args.require_fast_path, workers=args.workers, parsed_event_copies=count, profiling=False,
                  exact_outputs=True, summary=summary, runs=runs)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    with args.report.open('x') as stream:
        json.dump(report, stream, indent=2)
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
