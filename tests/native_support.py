"""Local test runner for the production Rust executable."""
import os
from pathlib import Path
import subprocess


def native_binary():
    binary = Path(os.environ.get('RUST_INGEST_BIN', 'target/release/pipeline')).resolve()
    if not binary.is_file():
        raise FileNotFoundError(f'Build the Rust executable first: make native-ingest ({binary})')
    return binary


def run_native(data, output, *, workers=2):
    env = dict(os.environ, NCPU=str(workers))
    if env.get('NATIVE_CLI_STYLE', 'positional') == 'positional':
        command = [str(native_binary()), str(data), str(output)]
    else:
        command = [str(native_binary()), '--data-dir', str(data), '--output-dir', str(output), '--workers', str(workers)]
    result = subprocess.run(command, env=env, capture_output=True, text=True)
    if result.returncode:
        raise AssertionError(f'Rust pipeline exited {result.returncode}: {result.stderr}')
    if result.stdout:
        raise AssertionError(f'Unexpected runtime stdout: {result.stdout}')
    return result
