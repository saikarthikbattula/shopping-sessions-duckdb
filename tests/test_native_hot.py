"""Independent attribution checks for a person with many clicks and sessions."""
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import pyarrow.parquet as pq


class NativeHotPersonTests(unittest.TestCase):
    def test_many_overlapping_clicks_and_sessions(self):
        binary = Path(os.environ.get("RUST_INGEST_BIN", "native/ingest/target/release/shopping-ingest")).resolve()
        if not binary.is_file():
            if os.environ.get("REQUIRE_RUST_INGEST") == "1":
                self.fail("Required native executable is missing")
            self.skipTest("Native executable has not been built")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / "data"
            for name in ("events", "clicks", "links"):
                (data / name).mkdir(parents=True)
            (data / "manifest.json").write_text(json.dumps({
                "window_start": "1970-01-01T00:00:00Z",
                "window_end": "1970-01-15T00:00:00Z"}))
            (data / "products.csv").write_text("product_id,category\np1,category\n")
            (data / "campaigns.csv").write_text("campaign_id,channel\n" +
                "".join(f"c{i},channel{i}\n" for i in range(3)))
            clicks = [(i // 2 * 60000, f"click{i:08}", f"c{i % 3}") for i in range(5000)]
            (data / "clicks/part.csv").write_text("click_id,user_id,campaign_id,ts\n" +
                "".join(f"{cid},person,{campaign},{ts}\n" for ts, cid, campaign in clicks))
            starts = [i * 1860000 for i in range(400)]
            epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
            (data / "events/part.csv").write_text("event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n" +
                "".join(f"event{i},person,{ts},{(epoch + timedelta(milliseconds=ts)).strftime('%Y-%m-%dT%H:%M:%SZ')},view,p1,\n"
                        for i, ts in enumerate(starts)))
            sessions, assists = Counter(), Counter()
            for start in starts:
                day = (epoch + timedelta(milliseconds=start)).date()
                eligible = [click for click in clicks if start - 604800000 <= click[0] <= start]
                winner = min(eligible, key=lambda click: (-click[0], click[1]))[2] if eligible else "organic"
                sessions[winner, day] += 1
                for campaign in {click[2] for click in eligible}:
                    assists[campaign, day] += 1
            for workers in (1, 8):
                output = root / f"output-{workers}"
                subprocess.run([str(binary), "--data-dir", str(data), "--output-dir", str(output),
                                "--workers", str(workers)], check=True, capture_output=True)
                rows = pq.read_table(output / "campaign_daily").to_pylist()
                actual = {(row["campaign_id"], row["day"]): row for row in rows}
                self.assertEqual(set(actual), set(sessions) | set(assists))
                for key, row in actual.items():
                    self.assertEqual(row["sessions"], sessions[key])
                    self.assertEqual(row["assisted_sessions"], assists[key])
                    self.assertEqual(row["revenue_cents"], 0)
                    self.assertEqual(row["purchasing_sessions"], 0)
                users = pq.read_table(output / "user_daily").to_pylist()
                self.assertEqual(sum(row["events"] for row in users), 400)
                self.assertEqual(sum(row["sessions"] for row in users), 400)
