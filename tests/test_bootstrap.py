"""Integration checks for shared plumbing, not competition-correctness tests."""

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.contracts import (
    CAMPAIGN_DAILY_SCHEMA,
    CAMPAIGNS_SCHEMA,
    CLICKS_SCHEMA,
    EVENTS_SCHEMA,
    PRODUCTS_SCHEMA,
    USER_DAILY_SCHEMA,
    PreparedInputs,
)
from pipeline.pipeline import configure_connection, main


class BootstrapIntegrationTests(unittest.TestCase):
    def test_staged_parquet_preserves_exact_strings_nulls_and_int64(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_path = root / "events.parquet"
            large_ms = 2**53 + 1
            table = pa.Table.from_pylist([
                {
                    "event_id": "e\r", "person_id": " é\t ",
                    "ingest_ms": large_ms, "ts_ms": None,
                    "event_type": "purchase", "product_id": "p1",
                    "price_cents": None, "payload_valid": False,
                },
                {
                    "event_id": "e2", "person_id": None,
                    "ingest_ms": large_ms + 1, "ts_ms": large_ms,
                    "event_type": "view", "product_id": " p1",
                    "price_cents": None, "payload_valid": False,
                },
            ], schema=EVENTS_SCHEMA)
            pq.write_table(table, event_path)
            con = configure_connection(root / "scratch'quoted", ncpu=2, mem_gb=2)
            try:
                rows = con.execute(
                    "SELECT event_id, person_id, ingest_ms, ts_ms, price_cents, "
                    "payload_valid FROM read_parquet(?) ORDER BY ingest_ms",
                    [str(event_path)],
                ).fetchall()
                self.assertEqual(rows, [
                    ("e\r", " é\t ", large_ms, None, None, False),
                    ("e2", None, large_ms + 1, large_ms, None, False),
                ])
                self.assertEqual(con.execute("SELECT current_setting('threads')").fetchone()[0], 2)
                self.assertEqual(
                    con.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0], False
                )
                spill = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
                self.assertEqual(Path(spill), root / "scratch'quoted" / "duckdb_tmp")
            finally:
                con.close()

    def test_empty_staged_schemas_are_readable_by_duckdb(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            con = configure_connection(root, ncpu=1, mem_gb=1)
            try:
                for name, schema in (
                    ("events", EVENTS_SCHEMA), ("clicks", CLICKS_SCHEMA),
                    ("products", PRODUCTS_SCHEMA), ("campaigns", CAMPAIGNS_SCHEMA),
                ):
                    with self.subTest(name=name):
                        path = root / f"{name}.parquet"
                        pq.write_table(pa.Table.from_batches([], schema=schema), path)
                        self.assertEqual(pq.read_schema(path), schema)
                        self.assertEqual(
                            con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0], 0
                        )
            finally:
                con.close()

    def test_orchestrator_connects_stages_and_cleans_only_its_work_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data, output, scratch = (root / part for part in ("data", "output", "scratch"))
            scratch.mkdir()
            sentinel = scratch / "keep.txt"
            sentinel.write_text("unrelated scratch file")
            seen = {}

            def fake_prepare(data_dir, work_dir):
                self.assertEqual(data_dir, data)
                seen["work"] = work_dir
                return PreparedInputs(
                    work_dir / "events", work_dir / "clicks",
                    work_dir / "products.parquet", work_dir / "campaigns.parquet",
                    0, 1,
                )

            def fake_analytics(con, prepared, output_dir):
                self.assertEqual(con.execute("SELECT 42").fetchone()[0], 42)
                self.assertEqual(prepared.events_dir, seen["work"] / "events")
                self.assertEqual(output_dir, output)
                for name, schema in (
                    ("user_daily", USER_DAILY_SCHEMA),
                    ("campaign_daily", CAMPAIGN_DAILY_SCHEMA),
                ):
                    folder = output_dir / name
                    folder.mkdir(parents=True)
                    pq.write_table(pa.Table.from_batches([], schema=schema), folder / "part-0.parquet")

            stdout, stderr = io.StringIO(), io.StringIO()
            with (
                patch.dict(os.environ, {
                    "DATA_DIR": str(data), "OUTPUT_DIR": str(output),
                    "SCRATCH_DIR": str(scratch), "NCPU": "1", "MEM_GB": "1",
                }),
                patch("pipeline.pipeline.prepare_inputs", side_effect=fake_prepare),
                patch("pipeline.pipeline.run_pipeline", side_effect=fake_analytics),
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
                main()
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("total:", stderr.getvalue())
            self.assertFalse(seen["work"].exists())
            self.assertTrue(sentinel.exists())
            for name in ("user_daily", "campaign_daily"):
                schema = pq.read_schema(output / name / "part-0.parquet")
                self.assertEqual(schema.field("day").type, pa.date32())


if __name__ == "__main__":
    unittest.main()
