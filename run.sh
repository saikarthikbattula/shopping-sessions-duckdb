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

NCPU="${NCPU:-$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 8)}"
NATIVE_BIN="${RUST_INGEST_BIN:-/usr/local/bin/shopping-ingest}"

if [ "$INGEST_BACKEND" = "rust" ] && [ -x "$NATIVE_BIN" ]; then
    exec "$NATIVE_BIN" --data-dir "$DATA_DIR" --output-dir "$OUTPUT_DIR" --workers "$NCPU"
fi

PYTHON="${PYTHON:-python3}"
if [ -d ".venv/bin" ]; then
    PYTHON=".venv/bin/python"
fi
exec "$PYTHON" -m pipeline.pipeline
