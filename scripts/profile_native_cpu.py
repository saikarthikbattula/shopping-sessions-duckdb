#!/usr/bin/env python3
"""Linux CPU ablations. Wrong-output executables exist only in scratch copies."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import tempfile

BASELINE = 'bb16596f59e601fc3b231e4608ace144a7949e42'
VARIANTS = ('constant_category', 'raw_user', 'no_dedup', 'parse_discard')


def replace_once(source, old, new):
    if source.count(old) != 1:
        raise ValueError(f'Expected one source anchor: {old[:100]!r}')
    return source.replace(old, new, 1)


def ablate(source, variant):
    if variant == 'constant_category':
        start = source.index('                            let category_idx = if is_valid {')
        end = source.index('                            let (eid_hash, shard_idx)', start)
        return source[:start] + '                            let category_idx = unknown_idx;\n\n' + source[end:]
    if variant == 'raw_user':
        # Event and click rows both resolve identity through IdentityBatch::push.
        source = replace_once(source,
            '''                ids.prefetch(hash);
                row.set_person(CompactId::from_bytes(user), 0);
                (hash, true)''',
            '''                row.set_person(CompactId::from_bytes(user), get_shard_bytes(user) as u8);
                (hash, false)''')
        return replace_once(source,
            'let (person, shard) = ids.get_compact_with_shard(user);',
            'let (person, shard) = (CompactId::from_bytes(user), get_shard_bytes(user) as u8);')
    if variant == 'parse_discard':
        return replace_once(source,
            '    let workers_count = workers_count.clamp(1, 16);',
            '''    let workers_count = workers_count.clamp(1, 16);
    let chunks = plan_chunks(&list_csv_files(&data_dir.join("events"))?, 2 * 1024 * 1024)?;
    let next = AtomicUsize::new(0);
    let cancelled = AtomicBool::new(false);
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for _ in 0..workers_count {
            let chunks = &chunks;
            let next = &next;
            let cancelled = &cancelled;
            handles.push(scope.spawn(move || -> Result<()> {
                let avx2 = {
                    #[cfg(target_arch = "x86_64")]
                    { is_x86_feature_detected!("avx2") }
                    #[cfg(not(target_arch = "x86_64"))]
                    { false }
                };
                while let Some(chunk) = chunks.get(next.fetch_add(1, Ordering::Relaxed)) {
                    for_each_chunk_line(chunk, cancelled, |line| {
                        let mut fields = [None; 7];
                        std::hint::black_box(parse_csv_line_fast(line, &mut fields, avx2));
                        std::hint::black_box(fields);
                        Ok(())
                    })?;
                }
                Ok(())
            }));
        }
        for handle in handles { handle.join().unwrap()?; }
        Ok(())
    })?;
    return Ok(());
''')
    if variant == 'no_dedup':
        section = source.index('// 6. Lock-Free Parallel Deduplication of Events')
        start = source.index('                    while let Some(shard_idx) = jobs.next() {', section)
        end = source.index('                    let cpu_ns = thread_cpu_time_ns()', start)
        replacement = '''                    while let Some(shard_idx) = jobs.next() {
                        for w_shards in worker_event_shards {
                            for ev in &w_shards.shards[shard_idx] {
                                if ev.flags & VALID != 0 {
                                    let (ingest_ms, ts_ms) = w_shards.decode_times(ev, window_start);
                                    if ts_ms != i64::MIN && ingest_ms.saturating_sub(ts_ms) <= 86_400_000 {
                                        local_person_shards[ev.p_shard as usize].push(RawPersonEvent {
                                            person: ev.person, ts_ms, price_cents: ev.price_cents,
                                            cat_idx: ev.cat_idx, is_purchase: ev.flags & PURCHASE != 0,
                                        });
                                    }
                                }
                            }
                        }
                    }
'''
        result = source[:start] + replacement + source[end:]
        # Capacity is tunable; still require exactly one complete declaration.
        declaration = (r'                    let mut dedup_map: HashMap<u128, DedupWinner, FastBuildHasher> =\n'
                       r'                        HashMap::with_capacity_and_hasher\(\d+, FastBuildHasher\);')
        result, count = re.subn(declaration, '', result)
        if count != 1:
            raise ValueError('Expected one event dedup map declaration')
        return result
    raise ValueError(variant)


def build(repo, scratch, variants):
    # Share dependency artifacts; copy each linked executable before the next build.
    target = scratch / 'target'
    for variant in ('baseline', *variants):
        root = scratch / variant
        root.mkdir()
        archive = subprocess.check_output(['git', '-C', str(repo), 'archive', BASELINE, 'native/ingest'])
        subprocess.run(['tar', '-x', '-C', str(root)], input=archive, check=True)
        crate = root / 'native/ingest'
        manifest = crate / 'Cargo.toml'
        manifest.write_text(manifest.read_text().replace('opt-level = 3', 'opt-level = 3\ndebug = 1')
                            .replace('strip = true', 'strip = false'))
        if variant != 'baseline':
            source = crate / 'src/analytics.rs'
            source.write_text(ablate(source.read_text(), variant))
        subprocess.run(['cargo', 'build', '--release', '--locked', '--jobs', '2',
                        '--manifest-path', str(manifest), '--target-dir', str(target)], check=True)
        shutil.copy2(target / 'release/shopping-ingest', root / 'binary')


def measure(binary, data, output, workers):
    env = os.environ.copy()
    env.pop('NATIVE_PROFILE', None)
    process = subprocess.run(['/usr/bin/time', '-f', 'CPU_MEASURE %U %S %e %M %F %R',
        str(binary), '--data-dir', str(data), '--output-dir', str(output), '--workers', str(workers)],
        capture_output=True, text=True, env=env)
    if process.returncode:
        raise RuntimeError(process.stderr)
    match = re.search(r'CPU_MEASURE ([\d.]+) ([\d.]+) ([\d.]+) (\d+) (\d+) (\d+)', process.stderr)
    if not match:
        raise RuntimeError('Missing whole-process resource measurements')
    user, system, wall, rss, major, minor = map(float, match.groups())
    count = re.search(r'total_events: (\d+)', process.stderr)
    return dict(user_seconds=user, system_seconds=system, cpu_seconds=user + system,
                wall_seconds=wall, peak_rss_bytes=int(rss * 1024),
                major_faults=int(major), minor_faults=int(minor),
                parsed_event_copies=int(count[1]) if count else None, stderr=process.stderr)


def perf(binary, data, root, workers, sudo=False):
    tool = shutil.which('perf')
    if not tool:
        return dict(available=False, reason='perf not installed; IPC and function split unavailable')
    command = [str(binary), '--data-dir', str(data), '--output-dir', str(root / 'perf-output'),
               '--workers', str(workers)]
    prefix = ['sudo', '-n', tool] if sudo else [tool]
    stat = subprocess.run([*prefix, 'stat', '-e', 'cycles,instructions,cache-misses,page-faults',
                           '--', *command], capture_output=True, text=True)
    (root / 'perf-stat.txt').write_text(stat.stderr)
    record = subprocess.run([*prefix, 'record', '-F', '999', '--call-graph', 'dwarf',
        '-o', str(root / 'perf.data'), '--', *command], capture_output=True, text=True)
    (root / 'perf-record.txt').write_text(record.stderr)
    report = ''
    if record.returncode == 0:
        result = subprocess.run([*prefix, 'report', '--stdio', '--no-children', '-i', str(root / 'perf.data')],
                                capture_output=True, text=True)
        report = result.stdout
        (root / 'perf-report.txt').write_text(report)
    instructions = re.search(r'([\d,.]+)\s+instructions', stat.stderr)
    cycles = re.search(r'([\d,.]+)\s+cycles', stat.stderr)
    ipc = (float(instructions[1].replace(',', '')) / float(cycles[1].replace(',', ''))
           if instructions and cycles else None)
    return dict(available=True, sudo=sudo, stat_exit=stat.returncode, record_exit=record.returncode,
                ipc=ipc, stat=stat.stderr, record=record.stderr, function_report=report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('data', type=Path)
    parser.add_argument('work', type=Path)
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--reuse-build', action='store_true')
    parser.add_argument('--sudo-perf', action='store_true', help='Use passwordless sudo for read-only perf on CI')
    args = parser.parse_args()
    if args.workers < 1:
        parser.error('workers must be positive')
    repo = Path(__file__).resolve().parents[1]
    root, data = args.work.resolve(), args.data.resolve()
    root.mkdir(parents=True, exist_ok=True)
    if not args.reuse_build:
        build(repo, root, VARIANTS)
    results = {}
    for variant in VARIANTS:
        runs = []
        for round_number in range(3):
            order = ('baseline', variant) if round_number % 2 == 0 else (variant, 'baseline')
            for arm in order:
                with tempfile.TemporaryDirectory(prefix='cpu-ablation-', dir=root) as output:
                    result = measure(root / arm / 'binary', data, Path(output), args.workers)
                runs.append(dict(arm=arm, round=round_number, **result))
        counts = {r['parsed_event_copies'] for r in runs if r['arm'] == 'baseline'}
        if len(counts) != 1 or not next(iter(counts)):
            raise RuntimeError(f'Unstable baseline event counts: {counts}')
        count = next(iter(counts))
        summary = {arm: {key: statistics.median(r[key] for r in runs if r['arm'] == arm)
                        for key in ('cpu_seconds', 'user_seconds', 'system_seconds', 'wall_seconds',
                                    'peak_rss_bytes', 'major_faults', 'minor_faults')}
                   for arm in ('baseline', variant)}
        for row in summary.values():
            row['cpu_us_per_event_copy'] = row['cpu_seconds'] * 1e6 / count
        results[variant] = dict(parsed_event_copies=count, runs=runs, summary=summary,
                               outputs_valid=False)
        print(json.dumps({variant: summary}, indent=2), flush=True)
    report = dict(baseline=BASELINE, workers=args.workers, profiling_during_measurement=False,
                  ablations=results, perf=perf(root / 'baseline/binary', data, root, args.workers, args.sudo_perf),
                  binaries={v: hashlib.sha256((root / v / 'binary').read_bytes()).hexdigest()
                            for v in ('baseline', *VARIANTS)})
    (root / 'report.json').write_text(json.dumps(report, indent=2))
    print(json.dumps({'perf': report['perf']}, indent=2), flush=True)


if __name__ == '__main__':
    main()
