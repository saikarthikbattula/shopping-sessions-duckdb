#!/bin/sh
# Local convenience wrapper; the container executes the Rust binary directly.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
NATIVE_BIN="${RUST_INGEST_BIN:-$ROOT/native/ingest/target/release/shopping-ingest}"
if [ ! -x "$NATIVE_BIN" ]; then
    echo "Rust executable missing: $NATIVE_BIN. Run make native-ingest." >&2
    exit 127
fi
exec "$NATIVE_BIN" "$@"
