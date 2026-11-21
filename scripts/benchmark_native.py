#!/usr/bin/env python3
"""Compare two native executables on identical data in alternating order."""
import argparse
import json
from pathlib import Path
import re
import statistics
import subprocess
import tempfile
import time
import hashlib
import sys
import os
import platform


def input_fingerprint(data):
    digest = hashlib.sha256()
    for path in sorted(data.rglob("*")):
        if path.is_file():
            digest.update(str(path.relative_to(data)).encode())
            digest.update(str(path.stat().st_size).encode())
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("data", type=Path)
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    if sys.platform != "darwin":
        parser.error("This harness uses macOS time -l for peak RSS")
    environment = os.environ.copy()
    environment.pop("NATIVE_PROFILE", None)
    fingerprint = input_fingerprint(args.data)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    with args.report.open("x") as report, tempfile.TemporaryDirectory(prefix="native-benchmark-") as work:
        root = Path(work)
        results = []
        hashes = {arm: hashlib.sha256(binary.read_bytes()).hexdigest()
                  for arm, binary in (("baseline", args.baseline), ("candidate", args.candidate))}
        for round_number in range(3):
            order = ("baseline", "candidate") if round_number != 1 else ("candidate", "baseline")
            for arm in order:
                binary = getattr(args, arm).resolve()
                output = root / f"{round_number}-{arm}"
                started = time.perf_counter()
                process = subprocess.run(
                    ["/usr/bin/time", "-l", str(binary), "--data-dir", str(args.data.resolve()),
                     "--output-dir", str(output), "--workers", "8"],
                    capture_output=True, text=True, check=True, env=environment)
                wall = time.perf_counter() - started
                cpu = re.search(r"cpu_sum: ([0-9.]+)ms", process.stderr)
                rss = re.search(r"([0-9]+)\s+maximum resident set size", process.stderr)
                results.append(dict(arm=arm, round=round_number, wall_seconds=wall,
                                    cpu_ms=float(cpu[1]), rss_bytes=int(rss[1]), stderr=process.stderr))
            subprocess.run([sys.executable, "scripts/compare.py",
                            str(root / f"{round_number}-baseline"),
                            str(root / f"{round_number}-candidate")], check=True)
        summary = {arm: {metric: statistics.median(r[metric] for r in results if r["arm"] == arm)
                         for metric in ("wall_seconds", "cpu_ms", "rss_bytes")}
                   for arm in ("baseline", "candidate")}
        summary["speedup"] = summary["baseline"]["wall_seconds"] / summary["candidate"]["wall_seconds"]
        for arm in ("baseline", "candidate"):
            if hashlib.sha256(getattr(args, arm).read_bytes()).hexdigest() != hashes[arm]:
                raise RuntimeError("Benchmark executable changed during measurement")
        if input_fingerprint(args.data) != fingerprint:
            raise RuntimeError("Benchmark inputs changed during measurement")
        json.dump(dict(data=str(args.data.resolve()), workers=8, binary_sha256=hashes,
                       input_sha256=fingerprint,
                       platform=platform.platform(), profiling=False,
                       input_bytes=sum(p.stat().st_size for p in args.data.rglob("*") if p.is_file()),
                       runs=results, summary=summary), report, indent=2)
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
