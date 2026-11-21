#!/bin/sh
# Local wrapper; Docker starts the same production engine directly as /pipeline.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
NATIVE_BIN="${RUST_INGEST_BIN:-$ROOT/target/release/pipeline}"
if [ ! -x "$NATIVE_BIN" ]; then
    echo "Rust executable missing: $NATIVE_BIN. Run make engine on compatible Linux x86-64 hardware." >&2
    exit 127
fi
exec "$NATIVE_BIN" "$@"
