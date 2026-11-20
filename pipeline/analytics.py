"""DuckDB analytics over the validated staging contract.

Event validation flags are intentionally consumed only after event-ID dedup.
All time calculations use integer milliseconds or timezone-free timestamps.
"""

import sys
import time
from pathlib import Path
from uuid import uuid4

import duckdb

from pipeline.contracts import PreparedInputs


def run_pipeline(
    con: duckdb.DuckDBPyConnection, prepared: PreparedInputs, output_dir: Path
) -> None:
    """Write both daily tables without changing the caller's connection settings.

    Temporary table names are invocation-specific. Only this invocation's
    tables are removed, including when a query or output write fails.
    """
    prefix = "_ss_" + uuid4().hex
    tables: dict[str, str] = {}

    def stage(name: str, query: str, parameters=()) -> str:
        table = f"{prefix}_{name}"
        started = time.perf_counter()
        con.execute(f"CREATE TEMP TABLE {table} AS {query}", parameters)
        tables[name] = table
        print(f"analytics/{name}: {time.perf_counter() - started:.3f}s", file=sys.stderr)
        return table

    def release(*names: str) -> None:
        for name in names:
            con.execute(f"DROP TABLE {tables.pop(name)}")

    try:
        events = stage("events", """
            SELECT event_id, person_id, ts_ms, event_type, product_id, price_cents
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY event_id ORDER BY ingest_ms
                ) AS copy_number
                FROM read_parquet(?)
            )
            WHERE copy_number = 1 AND payload_valid
              AND ingest_ms <= ts_ms + 86400000
        """, [str(prepared.events_dir / "*.parquet")])

        # Event IDs are unique here. The same total ordering in both windows
        # places the sole boundary marker before all its timestamp peers.
        session_events = stage("session_events", f"""
            SELECT person_id, ts_ms, event_type, product_id, price_cents,
                   sum(CASE WHEN previous_ms IS NULL OR ts_ms - previous_ms > 1800000
                            THEN 1 ELSE 0 END) OVER (
                       PARTITION BY person_id ORDER BY ts_ms, event_id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   )::BIGINT AS session_no
            FROM (
                SELECT *, lag(ts_ms) OVER (
                    PARTITION BY person_id ORDER BY ts_ms, event_id
                ) AS previous_ms
                FROM {events}
            )
        """)
        release("events")

        sessions = stage("sessions", f"""
            SELECT person_id, session_no, min(ts_ms) AS start_ms,
                   CAST(epoch_ms(min(ts_ms)) AS DATE) AS day,
                   count(*) AS events,
                   count(*) FILTER (WHERE event_type = 'purchase') AS purchase_count,
                   sum(CASE WHEN event_type = 'purchase' THEN price_cents ELSE 0 END)::BIGINT
                       AS revenue_cents
            FROM {session_events}
            GROUP BY person_id, session_no
        """)

        top_categories = stage("top_categories", f"""
            SELECT person_id, day, category AS top_category
            FROM (
                SELECT e.person_id, s.day, coalesce(p.category, 'unknown') AS category,
                       sum(e.price_cents) AS revenue
                FROM {session_events} e
                JOIN {sessions} s USING (person_id, session_no)
                LEFT JOIN read_parquet(?) p USING (product_id)
                WHERE e.event_type = 'purchase'
                GROUP BY e.person_id, s.day, coalesce(p.category, 'unknown')
            )
            QUALIFY row_number() OVER (
                PARTITION BY person_id, day ORDER BY revenue DESC, encode(category)
            ) = 1
        """, [str(prepared.products_path)])
        release("session_events")

        user_daily = stage("user_daily", f"""
            SELECT d.person_id AS user_id, d.day, d.sessions, d.events,
                   d.revenue_cents, t.top_category
            FROM (
                SELECT person_id, day, count(*) AS sessions,
                       sum(events)::BIGINT AS events,
                       sum(revenue_cents)::BIGINT AS revenue_cents
                FROM {sessions} GROUP BY person_id, day
            ) d
            LEFT JOIN {top_categories} t USING (person_id, day)
        """)
        release("top_categories")

        clicks = stage("clicks", """
            SELECT click_id, person_id, campaign_id, ts_ms
            FROM (
                SELECT *, row_number() OVER (PARTITION BY click_id) AS copy_number
                FROM read_parquet(?)
            )
            WHERE copy_number = 1 AND payload_valid
        """, [str(prepared.clicks_dir / "*.parquet")])

        winning_clicks = stage("winning_clicks", f"""
            SELECT person_id, ts_ms, campaign_id
            FROM {clicks}
            QUALIFY row_number() OVER (
                PARTITION BY person_id, ts_ms ORDER BY encode(click_id)
            ) = 1
        """)

        credited = stage("credited", f"""
            SELECT campaign_id, day, count(*) AS sessions,
                   count(*) FILTER (WHERE purchase_count > 0) AS purchasing_sessions,
                   sum(revenue_cents)::BIGINT AS revenue_cents
            FROM (
                SELECT s.day, s.purchase_count, s.revenue_cents,
                       CASE WHEN c.ts_ms >= s.start_ms - 604800000
                            THEN c.campaign_id ELSE 'organic' END AS campaign_id
                FROM {sessions} s
                ASOF LEFT JOIN {winning_clicks} c
                  ON s.person_id = c.person_id AND s.start_ms >= c.ts_ms
            )
            GROUP BY campaign_id, day
        """)
        release("winning_clicks")

        # Union inclusive click-eligibility intervals before joining sessions.
        # Each session can then match at most one interval per campaign. This
        # avoids a potentially enormous raw click/session range join for whales.
        intervals = stage("assist_intervals", f"""
            WITH distinct_times AS (
                SELECT DISTINCT person_id, campaign_id, ts_ms FROM {clicks}
            ), previous_ends AS (
                SELECT *, max(ts_ms + 604800000) OVER (
                    PARTITION BY person_id, campaign_id ORDER BY ts_ms
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) AS previous_end
                FROM distinct_times
            ), islands AS (
                SELECT *, sum(CASE WHEN previous_end IS NULL OR ts_ms > previous_end
                                   THEN 1 ELSE 0 END) OVER (
                    PARTITION BY person_id, campaign_id ORDER BY ts_ms
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS island
                FROM previous_ends
            )
            SELECT person_id, campaign_id, min(ts_ms) AS first_ms,
                   max(ts_ms + 604800000) AS last_ms
            FROM islands GROUP BY person_id, campaign_id, island
        """)
        release("clicks")

        assists = stage("assists", f"""
            SELECT i.campaign_id, s.day, count(*) AS assisted_sessions
            FROM {sessions} s JOIN {intervals} i
              ON s.person_id = i.person_id
             AND s.start_ms >= i.first_ms AND s.start_ms <= i.last_ms
            GROUP BY i.campaign_id, s.day
        """)
        release("assist_intervals", "sessions")

        campaign_daily = stage("campaign_daily", f"""
            SELECT coalesce(c.campaign_id, a.campaign_id) AS campaign_id,
                   CASE WHEN coalesce(c.campaign_id, a.campaign_id) = 'organic'
                        THEN 'organic' ELSE lookup.channel END AS channel,
                   coalesce(c.day, a.day) AS day,
                   coalesce(c.sessions, 0)::BIGINT AS sessions,
                   coalesce(c.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
                   coalesce(c.revenue_cents, 0)::BIGINT AS revenue_cents,
                   coalesce(a.assisted_sessions, 0)::BIGINT AS assisted_sessions
            FROM {credited} c FULL OUTER JOIN {assists} a USING (campaign_id, day)
            LEFT JOIN read_parquet(?) lookup
              ON lookup.campaign_id = coalesce(c.campaign_id, a.campaign_id)
        """, [str(prepared.campaigns_path)])
        release("credited", "assists")

        for name, table in (("user_daily", user_daily), ("campaign_daily", campaign_daily)):
            folder = output_dir / name
            folder.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"COPY {table} TO ? (FORMAT PARQUET, COMPRESSION SNAPPY)",
                [str(folder / "part-0.parquet")],
            )
    finally:
        for table in reversed(list(tables.values())):
            con.execute(f"DROP TABLE IF EXISTS {table}")
