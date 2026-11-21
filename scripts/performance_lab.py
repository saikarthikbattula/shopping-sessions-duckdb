#!/usr/bin/env python3
"""Bounded Linux benchmark: unchanged baseline versus one runtime knob per variant."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import tempfile
import time
import threading

if __package__:
    from .benchmark_express_lane import compare_outputs
else:
    from benchmark_express_lane import compare_outputs

VARIANTS = {'baseline': {}, 'spin0': {'XL_SPIN': '0'}, 'spin50': {'XL_SPIN': '50'},
            'endrel0': {'XL_ENDREL': '0'}, 's30': {'XL_S3': '0'}}


def fingerprint(root):
    digest = hashlib.sha256()
    total = 0
    for path in sorted(root.rglob('*')):
        if path.is_file():
            digest.update(str(path.relative_to(root)).encode())
            with path.open('rb') as f:
                while chunk := f.read(1024 * 1024): digest.update(chunk); total += len(chunk)
    return digest.hexdigest(), total


def measure(binary, data, output, workers, overrides):
    env = {k: v for k, v in os.environ.items() if not k.startswith('XL_') and k != 'V1ND_LOG'}
    env['NCPU'] = str(workers); env.update(overrides)
    with tempfile.TemporaryFile(mode='w+') as log:
        started = time.perf_counter()
        child = subprocess.Popen([str(binary), str(data), str(output)], env=env,
                                 stdout=subprocess.DEVNULL, stderr=log)
        timed_out = threading.Event()
        def stop():
            timed_out.set()
            try: child.kill()
            except ProcessLookupError: pass
        timer = threading.Timer(180, stop); timer.start()
        try:
            _, status, usage = os.wait4(child.pid, 0)
            child.returncode = os.waitstatus_to_exitcode(status)
        except BaseException:
            child.kill(); child.wait()
            raise
        finally:
            timer.cancel()
        wall = time.perf_counter() - started
        log.seek(0); stderr = log.read()
    return dict(exit_code=child.returncode, timed_out=timed_out.is_set(), wall_seconds=wall,
                cpu_seconds=usage.ru_utime + usage.ru_stime,
                peak_rss_bytes=usage.ru_maxrss * 1024,
                major_faults=usage.ru_majflt, minor_faults=usage.ru_minflt,
                fallback=bool(re.search(r'FALLBACK|\[generic\]|\[v1nd\]', stderr)), stderr=stderr)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('binary', type=Path); p.add_argument('data', type=Path); p.add_argument('report', type=Path)
    p.add_argument('--workers', type=int, default=2); p.add_argument('--rounds', type=int, default=3)
    p.add_argument('--case', default='unspecified'); args = p.parse_args()
    if platform.system() != 'Linux' or args.workers < 1 or args.rounds < 3:
        p.error('Linux, positive workers, and at least 3 rounds required')
    binary, data = args.binary.resolve(), args.data.resolve()
    sha = hashlib.sha256(binary.read_bytes()).hexdigest()
    data_sha, size = fingerprint(data)
    report = dict(case=args.case, baseline_commit=os.environ.get('BASELINE_COMMIT'),
                  platform=platform.platform(), cpuinfo=Path('/proc/cpuinfo').read_text(),
                  workers=args.workers, rounds=args.rounds, binary_sha256=sha,
                  input_sha256=data_sha, input_bytes=size, variants=VARIANTS, runs=[],
                  status='running', exact_outputs=False)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    if args.report.exists(): p.error('Report already exists')
    try:
        with tempfile.TemporaryDirectory(prefix='performance-lab-') as tmp:
            root = Path(tmp)
            profile = measure(binary, data, root / 'profile', args.workers, {'XL_TIMING': '1'})
            report['profile'] = profile
            args.report.write_text(json.dumps(report, indent=2))
            if profile['exit_code']: raise RuntimeError('Baseline profile failed')
            for round_number in range(args.rounds):
                names = list(VARIANTS)
                names = names[round_number % len(names):] + names[:round_number % len(names)]
                for name in names:
                    result = measure(binary, data, root / f'{round_number}-{name}', args.workers, VARIANTS[name])
                    report['runs'].append(dict(variant=name, round=round_number, **result))
                    args.report.write_text(json.dumps(report, indent=2))
                    if result['exit_code']: raise RuntimeError(f'{name} failed: {result["stderr"]}')
                for name in names:
                    if name != 'baseline': compare_outputs(root / f'{round_number}-baseline', root / f'{round_number}-{name}')
        if hashlib.sha256(binary.read_bytes()).hexdigest() != sha: raise RuntimeError('Binary changed')
        if fingerprint(data)[0] != data_sha: raise RuntimeError('Input changed')
        report['summary'] = {name: {metric: statistics.median(r[metric] for r in report['runs'] if r['variant'] == name)
                                   for metric in ('wall_seconds', 'cpu_seconds', 'peak_rss_bytes')}
                             for name in VARIANTS}
        report['exact_outputs'] = True; report['status'] = 'passed'
        print(json.dumps(report['summary'], indent=2))
    except BaseException as error:
        report['status'] = 'failed'; report['error'] = str(error)
        raise
    finally:
        args.report.write_text(json.dumps(report, indent=2))


if __name__ == '__main__': main()
