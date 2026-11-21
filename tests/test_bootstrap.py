"""Rust-only entrypoint, CLI and output contract checks."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import pyarrow.parquet as pq
from tests.native_support import native_binary, run_native
from tests.schemas import USER_DAILY_SCHEMA, CAMPAIGN_DAILY_SCHEMA

ROOT = Path(__file__).resolve().parents[1]


class RuntimeTests(unittest.TestCase):
    def test_no_argument_entrypoint_matches_explicit_rust_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data = ROOT/'fixtures/E1/data'
            if not data.exists(): data = ROOT/'fixtures/E1/input'
            run_native(data,root/'explicit')
            env = dict(os.environ,DATA_DIR=str(data),OUTPUT_DIR=str(root/'environment'),
                       NCPU='2',RUST_INGEST_BIN=str(native_binary()),PYTHON='/nonexistent',
                       INGEST_BACKEND='python')
            result=subprocess.run([str(ROOT/'run.sh')],env=env,capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertEqual(result.stdout,'')
            for table,schema in [('user_daily',USER_DAILY_SCHEMA),('campaign_daily',CAMPAIGN_DAILY_SCHEMA)]:
                actual=pq.read_table(root/'environment'/table)
                self.assertEqual(actual.schema.remove_metadata(),schema)
                def rows(path): return sorted(pq.read_table(path/table).to_pylist(),key=lambda r: repr(sorted(r.items())))
                self.assertEqual(rows(root/'explicit'),rows(root/'environment'))

    def test_missing_executable_fails_without_python_fallback(self):
        env=dict(os.environ,RUST_INGEST_BIN='/nonexistent/shopping-ingest')
        result=subprocess.run([str(ROOT/'run.sh')],env=env,capture_output=True,text=True)
        self.assertEqual(result.returncode,127)
        self.assertIn('Rust executable missing',result.stderr)

    def test_invalid_cpu_and_retired_staging_arguments_fail(self):
        for args,env in [([],dict(os.environ,NCPU='0')),(['--stage-dir','out'],os.environ),
                         (['--workers','bad'],os.environ)]:
            result=subprocess.run([str(native_binary()),*args],env=env,capture_output=True,text=True)
            self.assertEqual(result.returncode,64,result.stderr)
