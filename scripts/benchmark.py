#!/usr/bin/env python3
"""Compare a candidate checkout with an immutable submitted baseline, natively.

This is a development benchmark, not a replica of scorer hardware or quotas.
Extraction, hashing, output comparisons, and metadata collection are untimed.
"""

import argparse
import hashlib
import importlib.metadata
import io
import json
import math
import os
import platform
import re
import signal
import statistics
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from pathlib import Path


DEFAULT_BASELINE = '7637e570c8221001a7dc8ce7233d9443dfc2d947'
REPO = Path(__file__).resolve().parents[1]
ALLOWED_ENV = {'INGEST_BACKEND', 'RUST_INGEST_BIN'}
METRICS = ('wall_seconds', 'preparation_seconds', 'analytics_seconds', 'pipeline_seconds',
           'max_individual_rss_bytes', 'sampled_tree_rss_peak_bytes', 'sampled_disk_peak_bytes')


class BenchmarkError(Exception):
    """The requested comparison cannot produce a trustworthy performance result."""


class BenchmarkInterrupted(KeyboardInterrupt):
    def __init__(self, signum):
        self.signum = signum


def _interrupt(signum, _frame):
    raise BenchmarkInterrupted(signum)


def alternating_order(rounds):
    return [(round_number, arm) for round_number in range(1, rounds + 1)
            for arm in (('baseline', 'candidate') if round_number % 2 else ('candidate', 'baseline'))]


def parse_timings(stderr):
    number = r'([0-9]+(?:\.[0-9]+)?)'
    preparation = re.findall(r'^prepare_inputs: ' + number + r's$', stderr, re.MULTILINE)
    final = re.findall(r'^analytics: ' + number + r's; total: ' + number + r's$', stderr, re.MULTILINE)
    if len(preparation) != 1 or len(final) != 1:
        raise BenchmarkError('Missing or ambiguous pipeline stage timings')
    values = [float(preparation[0]), float(final[0][0]), float(final[0][1])]
    if not all(math.isfinite(value) for value in values) or abs(sum(values[:2]) - values[2]) > .005:
        raise BenchmarkError('Inconsistent pipeline stage timings')
    return dict(zip(('preparation_seconds', 'analytics_seconds', 'pipeline_seconds'), values))


def normalize_maxrss(value, system):
    if system == 'darwin':
        return int(value)
    if system.startswith('linux'):
        return int(value) * 1024
    raise BenchmarkError('Native RSS measurement supports only macOS and Linux')


def tree_rss(root_pid, processes):
    if root_pid not in processes:
        return None
    selected = {root_pid}
    while True:
        descendants = {pid for pid, (parent, _) in processes.items() if parent in selected}
        expanded = selected | descendants
        if expanded == selected:
            return sum(processes[pid][1] for pid in selected)
        selected = expanded


def sample_process_tree(root_pid):
    processes = {}
    if sys.platform.startswith('linux'):
        page_bytes = os.sysconf('SC_PAGE_SIZE')
        for entry in Path('/proc').iterdir():
            if not entry.name.isdigit():
                continue
            try:
                fields = (entry / 'stat').read_text().rsplit(')', 1)[1].split()
                rss = int((entry / 'statm').read_text().split()[1]) * page_bytes
                processes[int(entry.name)] = (int(fields[1]), rss)
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                continue
    else:
        result = subprocess.run(['ps', '-axo', 'pid=,ppid=,rss='], check=True,
                                capture_output=True, text=True, timeout=2)
        for line in result.stdout.splitlines():
            pid, parent, kib = map(int, line.split())
            processes[pid] = (parent, kib * 1024)
    return tree_rss(root_pid, processes)


def disk_bytes(folders):
    total = 0
    for folder in folders:
        for path in folder.rglob('*'):
            try:
                if not path.is_symlink() and path.is_file():
                    total += path.stat().st_size
            except FileNotFoundError:
                pass  # A pipeline can remove its temporary files while sampled.
    return total


class ResourceSampler:
    def __init__(self, pid, folders, interval):
        self.pid, self.folders, self.interval = pid, folders, interval
        self.tree_peak = self.disk_peak = None
        self.warnings = []
        self._rss_available = self._disk_available = True
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def sample(self):
        if self._rss_available:
            try:
                value = sample_process_tree(self.pid)
                if value is not None:
                    self.tree_peak = max(self.tree_peak or 0, value)
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                self._rss_available = False
                self.tree_peak = None
                self.warnings.append(f'Process-tree RSS unavailable: {type(error).__name__}')
        if self._disk_available:
            try:
                self.disk_peak = max(self.disk_peak or 0, disk_bytes(self.folders))
            except OSError as error:
                self._disk_available = False
                self.disk_peak = None
                self.warnings.append(f'Disk sampling unavailable: {type(error).__name__}')

    def _loop(self):
        while not self._stop.is_set():
            self.sample()
            self._stop.wait(self.interval)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join()
        # Include final persistent output without pretending to catch every spill.
        if self._disk_available:
            try:
                self.disk_peak = max(self.disk_peak or 0, disk_bytes(self.folders))
            except OSError:
                self.disk_peak = None
                self.warnings.append('Final disk sample unavailable')
        if self.tree_peak is None and self._rss_available:
            self.warnings.append('No process-tree RSS sample captured before child exit')


def parse_override(value):
    key, separator, content = value.partition('=')
    if not separator or key not in ALLOWED_ENV or not content:
        raise ValueError('Only nonempty INGEST_BACKEND and RUST_INGEST_BIN overrides are allowed')
    return key, content


def _arg_override(value):
    try:
        return parse_override(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def _positive_int(value):
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError('Must be a positive integer')
    return result


def _positive_float(value):
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise argparse.ArgumentTypeError('Must be positive and finite')
    return result


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('data_dir', type=Path)
    parser.add_argument('--baseline-ref', default=DEFAULT_BASELINE)
    parser.add_argument('--candidate-root', type=Path, default=REPO)
    parser.add_argument('--runs', type=_positive_int, default=3)
    parser.add_argument('--ncpu', type=_positive_int, default=2)
    parser.add_argument('--mem-gb', type=_positive_int, default=2)
    parser.add_argument('--report', type=Path, default=Path('out-benchmark/report.json'))
    parser.add_argument('--sample-interval', type=_positive_float, default=.1)
    parser.add_argument('--baseline-env', type=_arg_override, action='append', default=[])
    parser.add_argument('--candidate-env', type=_arg_override, action='append', default=[])
    return parser


def _git(root, *arguments):
    return subprocess.check_output(['git', '-C', str(root), *arguments])


def _fingerprint(root, paths):
    digest = hashlib.sha256()
    size = 0
    for path in sorted(paths):
        if path.is_symlink():
            raise BenchmarkError(f'Symlink inputs/source are unsupported: {path}')
        digest.update(path.relative_to(root).as_posix().encode() + b'\0')
        with path.open('rb') as source:
            while block := source.read(1024 * 1024):
                digest.update(block)
                size += len(block)
        digest.update(b'\0')
    return dict(sha256=digest.hexdigest(), files=len(paths), bytes=size)


def input_fingerprint(data):
    paths = [data / name for name in ('manifest.json', 'products.csv', 'campaigns.csv')]
    for name in ('events', 'clicks', 'links'):
        folder = data / name
        if folder.is_symlink() or not folder.is_dir():
            raise BenchmarkError(f'Expected an ordinary input directory: {folder}')
        paths.extend(folder.glob('*.csv'))
    return _fingerprint(data, paths)


def source_fingerprint(root, excluded):
    names = _git(root, 'ls-files', '-z', '--cached', '--others', '--exclude-standard').decode().split('\0')
    paths = []
    for name in set(names) - {''}:
        path = root / name
        if any(path.is_relative_to(item) for item in excluded):
            continue
        if path.is_file() or path.is_symlink():
            paths.append(path)
    return _fingerprint(root, paths)


def export_baseline(root, commit, destination):
    blob = _git(root, 'archive', '--format=tar', commit)
    with tarfile.open(fileobj=io.BytesIO(blob)) as archive:
        for member in archive.getmembers():
            path = destination / member.name
            if not (member.isfile() or member.isdir()) or not path.resolve().is_relative_to(destination):
                raise BenchmarkError('Baseline archive contains an unsafe path or file type')
        archive.extractall(destination, filter='data')


def run_invocation(*, root, data_dir, run_dir, logs_dir, run_name, ncpu, mem_gb,
                   env_overrides, sample_interval, python):
    run_dir.mkdir()
    output, scratch = run_dir / 'output', run_dir / 'scratch'
    output.mkdir()
    scratch.mkdir()
    environment = dict(os.environ)
    for key in ALLOWED_ENV:
        environment.pop(key, None)
    environment.update(env_overrides)
    environment.update(DATA_DIR=str(data_dir), OUTPUT_DIR=str(output), SCRATCH_DIR=str(scratch),
                       NCPU=str(ncpu), MEM_GB=str(mem_gb), HOME=str(scratch), TMPDIR=str(scratch),
                       PYTHONPATH=str(root), PYTHONNOUSERSITE='1')
    stdout_path, stderr_path = [logs_dir / f'{run_name}.{stream}.log' for stream in ('stdout', 'stderr')]
    with stdout_path.open('x') as stdout, stderr_path.open('x') as stderr:
        started = time.perf_counter()
        process = subprocess.Popen([python, '-m', 'pipeline.pipeline'], cwd=root, env=environment,
                                   stdout=stdout, stderr=stderr, start_new_session=True)
        sampler = ResourceSampler(process.pid, [output, scratch], sample_interval)
        sampler_started = False
        try:
            try:
                sampler.start()
            except RuntimeError as error:
                raise BenchmarkError(f'Resource sampler startup failed: {error}') from error
            sampler_started = True
            _, status, usage = os.wait4(process.pid, 0)
            wall = time.perf_counter() - started
            process.returncode = os.waitstatus_to_exitcode(status)
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            _, status, _ = os.wait4(process.pid, 0)
            process.returncode = os.waitstatus_to_exitcode(status)
            raise
        finally:
            if sampler_started:
                sampler.stop()
    result = dict(exit_code=process.returncode, wall_seconds=wall,
                  max_individual_rss_bytes=normalize_maxrss(usage.ru_maxrss, sys.platform),
                  sampled_tree_rss_peak_bytes=sampler.tree_peak, sampled_disk_peak_bytes=sampler.disk_peak,
                  sampling_warnings=sampler.warnings, stdout_log=str(stdout_path), stderr_log=str(stderr_path))
    try:
        result.update(parse_timings(stderr_path.read_text(errors='replace')))
    except BenchmarkError as error:
        result['timings_error'] = str(error)
    return result


def compare_outputs(actual, expected, root, python, log_path):
    for folder in (actual, expected):
        for table in ('user_daily', 'campaign_daily'):
            directory = folder / table
            if (folder / f'{table}.csv').exists() or directory.is_symlink() or not directory.is_dir():
                raise BenchmarkError(f'Expected Parquet output directory: {directory}')
    with log_path.open('x') as log:
        process = subprocess.Popen(
            [python, str(root / 'scripts/compare.py'), str(actual), str(expected)],
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
        try:
            returncode = process.wait()
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise
    if returncode:
        raise BenchmarkError(f'Output comparison failed; see {log_path}')


def summarize(runs):
    summary = {}
    for arm in ('baseline', 'candidate'):
        selected = [run for run in runs if run['arm'] == arm]
        summary[arm] = {}
        for name in METRICS:
            values = [run.get(name) for run in selected]
            summary[arm][name] = statistics.median(values) if values and all(value is not None for value in values) else None
    summary['wall_speedup'] = summary['baseline']['wall_seconds'] / summary['candidate']['wall_seconds']
    return summary


def _overrides(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise BenchmarkError(f'Duplicate requested environment override: {key}')
        result[key] = value
    return result


def run_benchmark(args):
    data, candidate, report_path = args.data_dir.resolve(), args.candidate_root.resolve(), args.report.resolve()
    logs = report_path.parent / (report_path.stem + '.logs')
    if candidate.is_relative_to(data):
        raise BenchmarkError('Input directory must not contain the candidate source checkout')
    if args.report.is_symlink():
        raise BenchmarkError('Refusing a symlink report path')
    if report_path.is_relative_to(data) or logs.is_relative_to(data):
        raise BenchmarkError('Benchmark artifacts must be outside the input directory')
    if report_path.exists() or logs.exists():
        raise BenchmarkError('Refusing to overwrite existing report or log directory')
    if not hasattr(os, 'wait4') or sys.platform not in ('darwin', 'linux'):
        raise BenchmarkError('Native benchmark requires macOS or Linux with wait4')
    overrides = dict(baseline=_overrides(args.baseline_env), candidate=_overrides(args.candidate_env))
    baseline_commit = _git(candidate, 'rev-parse', '--verify', '--end-of-options', args.baseline_ref + '^{commit}').decode().strip()
    candidate_commit = _git(candidate, 'rev-parse', 'HEAD').decode().strip()
    candidate_dirty = bool(_git(candidate, 'status', '--porcelain').strip())
    host = dict(system=platform.system(), architecture=platform.machine(), logical_cpus=os.cpu_count(),
                python=sys.executable, python_version=platform.python_version(),
                dependencies={name: importlib.metadata.version(name) for name in ('duckdb', 'pyarrow', 'polars')})
    before_input = input_fingerprint(data)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    logs.mkdir()
    try:
        report_file = report_path.open('x')
    except BaseException:
        logs.rmdir()  # Only the empty directory this invocation just created.
        raise
    report = dict(schema_version=1, status='running', measurement_context='native-development',
                  baseline=dict(commit=baseline_commit, requested_environment=overrides['baseline']),
                  candidate=dict(commit=candidate_commit, requested_environment=overrides['candidate'],
                                 dirty=candidate_dirty), host=host,
                  configuration=dict(ncpu=args.ncpu, mem_gb=args.mem_gb, runs_per_arm=args.runs,
                                     sample_interval_seconds=args.sample_interval),
                  input=before_input, runs=[],
                  limitations=[
                      'Application thread/memory settings are not OS-enforced quotas.',
                      'Native development measurements are not scorer timings; filesystem caches are uncontrolled.',
                      'wait4 RSS is maximum individual-process high-water RSS, not simultaneous combined tree memory.',
                      'Sampled summed tree RSS can double-count shared pages and miss short peaks.',
                      'Sampled logical scratch/output bytes can miss short-lived files and are not a disk-quota proof.',
                      'Requested environment is configuration, not proof a backend honored it.',
                  ])
    try:
        with tempfile.TemporaryDirectory(prefix='.benchmark-', dir=report_path.parent) as temporary:
            work = Path(temporary)
            excluded = [data, report_path, logs, work]
            before_source = source_fingerprint(candidate, excluded)
            report['candidate']['source'] = before_source
            baseline = work / 'baseline-source'
            baseline.mkdir()
            export_baseline(candidate, baseline_commit, baseline)
            reference = None
            for round_number, arm in alternating_order(args.runs):
                run_name = f'round-{round_number}-{arm}'
                run_dir = work / run_name
                metrics = run_invocation(root=baseline if arm == 'baseline' else candidate,
                                         data_dir=data, run_dir=run_dir, logs_dir=logs, run_name=run_name,
                                         ncpu=args.ncpu, mem_gb=args.mem_gb, env_overrides=overrides[arm],
                                         sample_interval=args.sample_interval, python=sys.executable)
                metrics.update(arm=arm, round=round_number)
                report['runs'].append(metrics)
                if metrics['exit_code']:
                    raise BenchmarkError(f'{run_name} exited with code {metrics["exit_code"]}')
                if not all(name in metrics for name in ('preparation_seconds', 'analytics_seconds', 'pipeline_seconds')):
                    raise BenchmarkError(f'{run_name}: {metrics.get("timings_error", "missing timings")}')
                reference = reference or run_dir / 'output'
                # A candidate cannot weaken its own correctness checker.
                compare_outputs(run_dir / 'output', reference, baseline, sys.executable, logs / f'{run_name}.compare.log')
                metrics['correctness'] = 'passed'
            after_input = input_fingerprint(data)
            report['input_after'] = after_input
            if after_input != before_input:
                raise BenchmarkError('Input content changed during the benchmark')
            after_source = source_fingerprint(candidate, excluded)
            report['candidate']['source_after'] = after_source
            if after_source != before_source:
                raise BenchmarkError('Candidate source changed during the benchmark')
            report['summary'] = summarize(report['runs'])
            report['status'] = 'ok'
    except (BenchmarkError, OSError, ValueError, subprocess.SubprocessError) as error:
        report['status'] = 'failed'
        report['error'] = str(error)
        report.pop('summary', None)
    except KeyboardInterrupt:
        report['status'] = 'failed'
        report['error'] = 'Benchmark interrupted'
        raise
    finally:
        with report_file:
            json.dump(report, report_file, indent=2)
            report_file.write('\n')
    return report


def main(argv=None):
    args = build_parser().parse_args(argv)
    previous_handler = signal.signal(signal.SIGTERM, _interrupt)
    try:
        report = run_benchmark(args)
    except (BenchmarkError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f'Benchmark failed: {error}', file=sys.stderr)
        return 1
    except BenchmarkInterrupted as error:
        print('Benchmark terminated; owned child processes were stopped', file=sys.stderr)
        return 128 + error.signum
    except KeyboardInterrupt:
        print('Benchmark interrupted; owned child processes were stopped', file=sys.stderr)
        return 130
    finally:
        signal.signal(signal.SIGTERM, previous_handler)
    print(json.dumps(dict(status=report['status'], report=str(args.report.resolve()),
                          summary=report.get('summary'), error=report.get('error')), indent=2))
    return 0 if report['status'] == 'ok' else 1


if __name__ == '__main__':
    raise SystemExit(main())
