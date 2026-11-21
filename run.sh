#!/bin/sh
# Entry point. Runs with no arguments; exit 0 means "done, check my output".
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
# Read-only root filesystem: everything that wants a temp or home dir gets /scratch.
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
# Rust is the default backend; override with INGEST_BACKEND=python to use the Python reference.
: "${INGEST_BACKEND:=rust}"
export INGEST_BACKEND

detect_cpus() {
    # 1. cgroup v2
    if [ -f /sys/fs/cgroup/cpu.max ]; then
        quota=$(cut -d" " -f1 /sys/fs/cgroup/cpu.max 2>/dev/null || true)
        period=$(cut -d" " -f2 /sys/fs/cgroup/cpu.max 2>/dev/null || true)
        if [ "$quota" != "max" ] && [ -n "$quota" ] && [ -n "$period" ] && [ "$period" -gt 0 ] 2>/dev/null; then
            cpus=$(( (quota + period - 1) / period ))
            if [ "$cpus" -gt 0 ]; then
                echo "$cpus"
                return
            fi
        fi
    fi
    # 2. cgroup v1
    if [ -f /sys/fs/cgroup/cpu/cpu.cfs_quota_us ] && [ -f /sys/fs/cgroup/cpu/cpu.cfs_period_us ]; then
        quota=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us 2>/dev/null || true)
        period=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us 2>/dev/null || true)
        if [ "$quota" -gt 0 ] && [ "$period" -gt 0 ] 2>/dev/null; then
            cpus=$(( (quota + period - 1) / period ))
            if [ "$cpus" -gt 0 ]; then
                echo "$cpus"
                return
            fi
        fi
    fi
    # 3. host nproc, capped at 16 to prevent thread explosion under cgroup throttling
    raw=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 8)
    if [ "$raw" -gt 16 ]; then
        echo 16
    elif [ "$raw" -gt 0 ]; then
        echo "$raw"
    else
        echo 8
    fi
}

NCPU="${NCPU:-$(detect_cpus)}"
NATIVE_BIN="${RUST_INGEST_BIN:-/usr/local/bin/shopping-ingest}"

if [ "$INGEST_BACKEND" = "rust" ] && [ -x "$NATIVE_BIN" ]; then
    exec "$NATIVE_BIN" --data-dir "$DATA_DIR" --output-dir "$OUTPUT_DIR" --workers "$NCPU"
fi

PYTHON="${PYTHON:-python3}"
if [ -d ".venv/bin" ]; then
    PYTHON=".venv/bin/python"
fi
exec "$PYTHON" -m pipeline.pipeline
