"""Analytical checks over staged inputs, independent of CSV ingestion."""

import contextlib
import io
import random
import tempfile
import unittest
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.analytics import run_pipeline
from pipeline.contracts import (
    CAMPAIGN_DAILY_SCHEMA, CAMPAIGNS_SCHEMA, CLICKS_SCHEMA, EVENTS_SCHEMA,
    PRODUCTS_SCHEMA, USER_DAILY_SCHEMA, PreparedInputs,
)


DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000
BASE = int(datetime(2026, 10, 1, tzinfo=timezone.utc).timestamp()) * 1000
PRODUCTS = {"p1": "books", "p2": "shoes"}
CAMPAIGNS = {"c1": "search", "c2": "social", "c3": "email"}


def event(event_id, person, ts, kind="view", price=None, product="p1", *, ingest=None, valid=True):
    return dict(event_id=event_id, person_id=person, ts_ms=ts,
                ingest_ms=ts if ingest is None else ingest, event_type=kind,
                product_id=product, price_cents=price, payload_valid=valid)


def click(click_id, person, campaign, ts, *, valid=True):
    return dict(click_id=click_id, person_id=person, campaign_id=campaign,
                ts_ms=ts, payload_valid=valid)


def canonical(rows, id_column):
    return sorted(rows, key=lambda row: (row[id_column].encode("utf-8"), row["day"]))


def reference(events, clicks, products, campaigns):
    """Small direct Python oracle: session lists and brute-force click lookup.

    This deliberately does not use window functions, interval union, or as-of
    joins, so randomized checks exercise a different algorithm from production.
    Parsing, linking and validation are already represented by the contract.
    """
    copies = {}
    for row in events:
        old = copies.get(row["event_id"])
        if old is None or row["ingest_ms"] < old["ingest_ms"]:
            copies[row["event_id"]] = row
    people = defaultdict(list)
    for row in copies.values():
        if row["payload_valid"] and row["ingest_ms"] - row["ts_ms"] <= DAY:
            people[row["person_id"]].append(row)
    click_copies = {}
    for row in clicks:
        click_copies.setdefault(row["click_id"], row)
    valid_clicks = [row for row in click_copies.values() if row["payload_valid"]]

    users, campaign_days = {}, {}
    categories = defaultdict(lambda: defaultdict(int))

    def campaign_row(campaign, day):
        return campaign_days.setdefault((campaign, day), dict(
            campaign_id=campaign, channel="organic" if campaign == "organic" else campaigns[campaign],
            day=day, sessions=0, purchasing_sessions=0, revenue_cents=0, assisted_sessions=0,
        ))

    for person, rows in people.items():
        sessions = []
        previous = None
        for row in sorted(rows, key=lambda row: row["ts_ms"]):
            if previous is None or row["ts_ms"] - previous > GAP:
                sessions.append([])
            sessions[-1].append(row)
            previous = row["ts_ms"]
        for session in sessions:
            start = session[0]["ts_ms"]
            day = (datetime(1970, 1, 1) + timedelta(milliseconds=start)).date()
            purchases = [row for row in session if row["event_type"] == "purchase"]
            revenue = sum(row["price_cents"] for row in purchases)
            key = person, day
            user = users.setdefault(key, dict(user_id=person, day=day, sessions=0,
                                             events=0, revenue_cents=0, top_category=None))
            user["sessions"] += 1
            user["events"] += len(session)
            user["revenue_cents"] += revenue
            for row in purchases:
                categories[key][products.get(row["product_id"], "unknown")] += row["price_cents"]
            eligible = [row for row in valid_clicks if row["person_id"] == person
                        and start - WEEK <= row["ts_ms"] <= start]
            winner = min(eligible, key=lambda row: (-row["ts_ms"], row["click_id"].encode())) if eligible else None
            credited = campaign_row(winner["campaign_id"] if winner else "organic", day)
            credited["sessions"] += 1
            credited["purchasing_sessions"] += bool(purchases)
            credited["revenue_cents"] += revenue
            for campaign in {row["campaign_id"] for row in eligible}:
                campaign_row(campaign, day)["assisted_sessions"] += 1
    for key, totals in categories.items():
        users[key]["top_category"] = min(totals, key=lambda category: (-totals[category], category.encode()))
    return canonical(list(users.values()), "user_id"), canonical(list(campaign_days.values()), "campaign_id")


class AnalyticsTests(unittest.TestCase):
    def stage(self, root, events, clicks, products, campaigns, *, parts=3):
        stage = root / "staged' input"
        for name, schema, rows in (
            ("events", EVENTS_SCHEMA, events), ("clicks", CLICKS_SCHEMA, clicks),
        ):
            folder = stage / name
            folder.mkdir(parents=True)
            for part in range(parts):
                pq.write_table(pa.Table.from_pylist(rows[part::parts], schema=schema),
                               folder / f"part-{part}.parquet")
        product_path, campaign_path = stage / "products.parquet", stage / "campaigns.parquet"
        pq.write_table(pa.Table.from_pylist([
            dict(product_id=key, category=value) for key, value in products.items()
        ], schema=PRODUCTS_SCHEMA), product_path)
        pq.write_table(pa.Table.from_pylist([
            dict(campaign_id=key, channel=value) for key, value in campaigns.items()
        ], schema=CAMPAIGNS_SCHEMA), campaign_path)
        return PreparedInputs(stage / "events", stage / "clicks", product_path,
                              campaign_path, BASE, BASE + 14 * DAY)

    def execute(self, events, clicks=(), *, products=None, campaigns=None, parts=3):
        products = PRODUCTS if products is None else products
        campaigns = CAMPAIGNS if campaigns is None else campaigns
        with tempfile.TemporaryDirectory() as tmp, duckdb.connect() as con:
            root = Path(tmp)
            prepared = self.stage(root, list(events), list(clicks), products, campaigns, parts=parts)
            # Neither the algorithm nor DATE conversion may depend on this setting.
            con.execute("SET TimeZone = 'Pacific/Honolulu'")
            con.execute("SET threads = 2")
            with contextlib.redirect_stderr(io.StringIO()):
                run_pipeline(con, prepared, root / "output' tables")
            results = []
            for name, schema, key in (
                ("user_daily", USER_DAILY_SCHEMA, "user_id"),
                ("campaign_daily", CAMPAIGN_DAILY_SCHEMA, "campaign_id"),
            ):
                table = pq.read_table(root / "output' tables" / name)
                self.assertEqual(set(table.column_names), set(schema.names))
                for field in schema:
                    self.assertEqual(table.schema.field(field.name).type, field.type)
                rows = table.to_pylist()
                self.assertEqual(len(rows), len({(row[key], row["day"]) for row in rows}))
                results.append(canonical(rows, key))
            users, campaign_days = results
            self.assertEqual(sum(row["sessions"] for row in users), sum(row["sessions"] for row in campaign_days))
            self.assertEqual(sum(row["revenue_cents"] for row in users), sum(row["revenue_cents"] for row in campaign_days))
            self.assertTrue(all(row["assisted_sessions"] == 0 for row in campaign_days if row["campaign_id"] == "organic"))
            self.assertGreaterEqual(sum(row["assisted_sessions"] for row in campaign_days),
                                    sum(row["sessions"] for row in campaign_days if row["campaign_id"] != "organic"))
            self.assertEqual(con.execute("SHOW TABLES").fetchall(), [])
        expected = reference(events, clicks, products, campaigns)
        self.assertEqual(tuple(results), expected)
        return tuple(results)

    def test_empty_events_clicks_and_lookups(self):
        self.assertEqual(self.execute([], products={}, campaigns={}), ([], []))
        self.execute([], [click("k", "u", "c1", BASE)])
        self.execute([event("invalid", None, BASE, valid=False)])

    def test_global_dedup_precedes_validation_and_lateness(self):
        rows = [
            event("bad-first", "u", BASE, ingest=BASE + 2),
            event("move-user", "later-user", BASE + 1, ingest=BASE + 10),
            event("bad-first", None, None, ingest=BASE, valid=False),
            event("move-user", "u", BASE, "purchase", 700, ingest=BASE + 1),
            event("exactly-late", "u", BASE + 2, ingest=BASE + 2 + DAY),
            event("too-late", "u", BASE + 3, ingest=BASE + 3 + DAY + 1),
            event("negative-delay", "u", BASE + 4, ingest=BASE - DAY),
        ]
        rows.append(dict(rows[3]))  # Equal-ingestion copies are identical.
        users, _ = self.execute(rows)
        self.assertEqual([(r["user_id"], r["events"], r["revenue_cents"]) for r in users], [("u", 3, 700)])

    def test_equal_timestamps_and_exact_session_gap(self):
        rows = [event("start", "u", BASE), event("z", "u", BASE + GAP + 1),
                event("a", "u", BASE + GAP + 1, "purchase", 10),
                event("b", "u", BASE + 2 * GAP + 1),
                event("other", "u", BASE + 3 * GAP + 2),
                event("initial-tie", "u", BASE)]
        users, _ = self.execute(rows)
        self.assertEqual((users[0]["sessions"], users[0]["events"]), (3, 6))
        for seed in range(4):
            random.Random(seed).shuffle(rows)
            self.execute(rows, parts=seed + 1)

    def test_midnight_categories_unknown_and_no_purchase_null(self):
        start = BASE + DAY - 10 * 60 * 1000
        rows = [event("a", " u\r", start),
                event("b", " u\r", start + 20 * 60 * 1000, "purchase", 500, "p1"),
                event("c", " u\r", start + 2 * 60 * 60 * 1000, "purchase", 500, "p2"),
                event("d", "u", BASE, "purchase", 501, "missing"),
                event("e", "u", BASE, "purchase", 500, "p1"),
                event("f", "U", BASE)]
        users, _ = self.execute(rows)
        by_key = {(r["user_id"], r["day"]): r for r in users}
        self.assertEqual(by_key[(" u\r", date(2026, 10, 1))]["revenue_cents"], 500)
        self.assertEqual(by_key[(" u\r", date(2026, 10, 2))]["top_category"], "shoes")
        self.assertEqual(by_key[("u", date(2026, 10, 1))]["top_category"], "unknown")
        self.assertIsNone(by_key[("U", date(2026, 10, 1))]["top_category"])

    def test_day_category_ties_use_byte_order_and_totals_are_int64(self):
        products = {"p1": "é", "p2": "z"}
        rows = [event("a", "u", BASE, "purchase", 500, "p1"),
                event("b", "u", BASE + GAP + 1, "purchase", 500, "p2")]
        users, _ = self.execute(rows, products=products)
        self.assertEqual(users[0]["top_category"], "z")
        self.assertEqual(users[0]["sessions"], 2)
        big = [event(f"big-{i}", "u", BASE, "purchase", 10_000_000) for i in range(300)]
        users, campaigns = self.execute(big)
        self.assertEqual(users[0]["revenue_cents"], 3_000_000_000)
        self.assertEqual(campaigns[0]["purchasing_sessions"], 1)

    def test_click_boundaries_ties_and_dedup(self):
        people = ["exact-week", "expired", "exact-start", "future", "tie", "invalid"]
        rows = [event(person, person, BASE, "purchase", 100) for person in people]
        clicks = [click("w", people[0], "c1", BASE - WEEK),
                  click("x", people[1], "c1", BASE - WEEK - 1, valid=False),
                  click("s", people[2], "c2", BASE),
                  click("f", people[3], "c2", BASE + 1),
                  click("é", people[4], "c1", BASE - 1),
                  click("z", people[4], "c2", BASE - 1),
                  click("bad", people[5], None, None, valid=False)]
        clicks.extend([dict(clicks[0]), dict(clicks[4]), dict(clicks[5])])
        _, campaigns = self.execute(rows, clicks)
        totals = {row["campaign_id"]: row for row in campaigns}
        self.assertEqual(totals["c2"]["sessions"], 2)
        self.assertEqual(totals["c1"]["assisted_sessions"], 2)
        self.assertEqual(totals["organic"]["sessions"], 3)
        # A valid click can expire for a later session even while in click window.
        self.execute([event("late", "u", BASE + WEEK + 1)], [click("old", "u", "c1", BASE)])

    def test_assist_interval_union_boundaries_and_assisted_only_rows(self):
        rows = [event("s0", "u", BASE, "purchase", 100),
                event("s1", "u", BASE + DAY),
                event("s2", "u", BASE + WEEK),
                event("s3", "u", BASE + WEEK + 1)]
        clicks = [click("a", "u", "c1", BASE - WEEK),
                  click("b", "u", "c1", BASE),
                  click("c", "u", "c1", BASE + WEEK + 2),
                  click("d", "u", "c2", BASE),
                  click("e", "u", "c2", BASE + 1),
                  click("f", "u", "c3", BASE - 1)]
        clicks.extend(click(f"repeat-{i}", "u", "c1", BASE) for i in range(20))
        _, campaigns = self.execute(rows, clicks)
        assisted_only = next(row for row in campaigns if row["campaign_id"] == "c3")
        self.assertEqual((assisted_only["sessions"], assisted_only["revenue_cents"],
                          assisted_only["assisted_sessions"]), (0, 0, 1))
        first_c1 = next(row for row in campaigns if row["campaign_id"] == "c1" and row["day"] == date(2026, 10, 1))
        self.assertEqual(first_c1["assisted_sessions"], 1)

    def test_click_mid_session_credits_only_later_sessions(self):
        rows = [event("a", "canonical", BASE), event("b", "canonical", BASE + 20 * 60 * 1000),
                event("c", "canonical", BASE + 2 * GAP)]
        clicks = [click("mid", "canonical", "c1", BASE + 10 * 60 * 1000)]
        _, campaigns = self.execute(rows, clicks)
        self.assertEqual({row["campaign_id"]: row["sessions"] for row in campaigns}, {"c1": 1, "organic": 1})

    def test_randomized_against_independent_reference_and_repartitioning(self):
        for seed in range(12):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                rows, clicks = [], []
                for i in range(100):
                    ts = BASE + rng.choice([0, GAP, GAP + 1, DAY - 1000, DAY, WEEK, WEEK + 1]) + rng.choice([0, 1, 60000])
                    purchase = rng.random() < 0.35
                    row = event(f"e{i}", rng.choice(["u", "U", " u", "é", "z"]), ts,
                                "purchase" if purchase else rng.choice(["view", "cart"]),
                                rng.choice([1, 500, 1000]) if purchase else None,
                                rng.choice(["p1", "p2", "unknown"]),
                                ingest=ts + rng.choice([-1, 0, DAY, DAY + 1]), valid=rng.random() > 0.1)
                    rows.append(row)
                    if rng.random() < 0.25:
                        later = dict(row, ingest_ms=row["ingest_ms"] + 1, person_id="later-user")
                        rows.append(later)
                for i in range(70):
                    row = click(f"k{i}", rng.choice(["u", "U", " u", "é", "z"]),
                                rng.choice(list(CAMPAIGNS)),
                                BASE + rng.choice([-WEEK, -1, 0, 1, DAY, WEEK, WEEK + 1]),
                                valid=rng.random() > 0.1)
                    clicks.append(row)
                    if rng.random() < 0.3:
                        clicks.append(dict(row))
                rng.shuffle(rows)
                rng.shuffle(clicks)
                first = self.execute(rows, clicks)
                rows.reverse()
                clicks.reverse()
                self.assertEqual(first, self.execute(rows, clicks, parts=5))

    def test_repeated_calls_and_failures_preserve_caller_tables(self):
        with tempfile.TemporaryDirectory() as tmp, duckdb.connect() as con:
            root = Path(tmp)
            prepared = self.stage(root, [event("a", "u", BASE)], [], PRODUCTS, CAMPAIGNS)
            con.execute("CREATE TEMP TABLE caller_state AS SELECT 42 AS value")
            for i in range(2):
                with contextlib.redirect_stderr(io.StringIO()):
                    run_pipeline(con, prepared, root / f"out-{i}")
                self.assertEqual(con.execute("SHOW TABLES").fetchall(), [("caller_state",)])
            blocked_output = root / "not-a-directory"
            blocked_output.write_text("keep")
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(OSError):
                run_pipeline(con, prepared, blocked_output)
            self.assertEqual(con.execute("SHOW TABLES").fetchall(), [("caller_state",)])
            self.assertEqual(con.execute("SELECT * FROM caller_state").fetchall(), [(42,)])
            missing_clicks = PreparedInputs(
                prepared.events_dir, root / "missing-clicks", prepared.products_path,
                prepared.campaigns_path, prepared.window_start_ms, prepared.window_end_ms,
            )
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(duckdb.IOException):
                run_pipeline(con, missing_clicks, root / "never-written")
            self.assertEqual(con.execute("SHOW TABLES").fetchall(), [("caller_state",)])
            self.assertFalse((root / "never-written").exists())


if __name__ == "__main__":
    unittest.main()
