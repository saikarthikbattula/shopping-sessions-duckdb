"""Local brute-force correctness oracle and Rust end-to-end boundary checks."""

import contextlib
import io
import random
import tempfile
import unittest
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq

from tests.schemas import USER_DAILY_SCHEMA, CAMPAIGN_DAILY_SCHEMA
from tests.native_support import run_native
import csv
import json



DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000
BASE = int(datetime(2026, 10, 1, tzinfo=timezone.utc).timestamp()) * 1000
PRODUCTS = {"p1": "books", "p2": "shoes"}
CAMPAIGNS = {"c1": "search", "c2": "social", "c3": "email"}


def event(event_id, person, ts, kind="view", price=None, product="p1", *, ingest=None, valid=True):
    return dict(event_id=event_id, person_id=person, ts_ms=ts,
                ingest_ms=((ts if ingest is None else ingest) // 1000) * 1000, event_type=kind,
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
    def execute(self, events, clicks=(), *, products=None, campaigns=None, parts=3):
        products = PRODUCTS if products is None else products
        campaigns = CAMPAIGNS if campaigns is None else campaigns
        events, clicks = list(events), list(clicks)
        def ingest_text(ms):
            return (datetime(1970,1,1,tzinfo=timezone.utc)+timedelta(milliseconds=ms)).strftime('%Y-%m-%dT%H:%M:%SZ')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name, header, rows in (
                ('events',['event_id','user_id','ts','ingest_ts','event_type','product_id','price_cents'],
                 [[r['event_id'],r['person_id'] if r['payload_valid'] else None,r['ts_ms'],
                   ingest_text(r['ingest_ms']),r['event_type'],r['product_id'],r['price_cents']] for r in events]),
                ('clicks',['click_id','user_id','campaign_id','ts'],
                 [[r['click_id'],r['person_id'] if r['payload_valid'] else None,r['campaign_id'],r['ts_ms']] for r in clicks]),
            ):
                (root/name).mkdir()
                for part in range(parts):
                    with (root/name/f'{part}.csv').open('w',newline='') as f:
                        writer=csv.writer(f,lineterminator='\n'); writer.writerow(header); writer.writerows(rows[part::parts])
            for name, header, rows in [('products',['product_id','category'],products.items()),
                                       ('campaigns',['campaign_id','channel'],campaigns.items())]:
                with (root/f'{name}.csv').open('w',newline='') as f:
                    writer=csv.writer(f,lineterminator='\n'); writer.writerow(header); writer.writerows(rows)
            (root/'manifest.json').write_text(json.dumps({'window_start':ingest_text(BASE),'window_end':ingest_text(BASE+14*DAY)}))
            output=root/'output'
            run_native(root,output,workers=2)
            results=[]
            for name,schema,key in [('user_daily',USER_DAILY_SCHEMA,'user_id'),('campaign_daily',CAMPAIGN_DAILY_SCHEMA,'campaign_id')]:
                table=pq.read_table(output/name)
                self.assertEqual(set(table.column_names),set(schema.names))
                for field in schema: self.assertEqual(table.schema.field(field.name).type,field.type)
                rows=table.to_pylist()
                self.assertEqual(len(rows),len({(r[key],r['day']) for r in rows}))
                results.append(canonical(rows,key))
            users,campaign_days=results
            self.assertEqual(sum(r['sessions'] for r in users),sum(r['sessions'] for r in campaign_days))
            self.assertEqual(sum(r['revenue_cents'] for r in users),sum(r['revenue_cents'] for r in campaign_days))
            self.assertTrue(all(r['assisted_sessions']==0 for r in campaign_days if r['campaign_id']=='organic'))
            self.assertGreaterEqual(sum(r['assisted_sessions'] for r in campaign_days),
                                    sum(r['sessions'] for r in campaign_days if r['campaign_id']!='organic'))
        self.assertEqual(tuple(results),reference(events,clicks,products,campaigns))
        return tuple(results)

    def test_empty_events_clicks_and_lookups(self):
        self.assertEqual(self.execute([], products={}, campaigns={}), ([], []))
        self.execute([], [click("k", "u", "c1", BASE)])
        self.execute([event("invalid", None, BASE, valid=False)])

    def test_global_dedup_precedes_validation_and_lateness(self):
        rows = [
            event("bad-first", "u", BASE, ingest=BASE + 2000),
            event("move-user", "later-user", BASE + 1, ingest=BASE + 10000),
            event("bad-first", None, None, ingest=BASE, valid=False),
            event("move-user", "u", BASE, "purchase", 700, ingest=BASE + 1000),
            event("exactly-late", "u", BASE, ingest=BASE + DAY),
            event("too-late", "u", BASE + 999, ingest=BASE + DAY + 1000),
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
                        later = dict(row, ingest_ms=row["ingest_ms"] + 1000, person_id="later-user")
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



if __name__ == "__main__":
    unittest.main()
