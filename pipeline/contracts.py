"""Shared interface; coordinate schema changes between both workstreams."""

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa


@dataclass(frozen=True)
class PreparedInputs:
    events_dir: Path
    clicks_dir: Path
    products_path: Path
    campaigns_path: Path
    window_start_ms: int
    window_end_ms: int


EVENTS_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("person_id", pa.string()),
    pa.field("ingest_ms", pa.int64(), nullable=False),
    pa.field("ts_ms", pa.int64()),
    pa.field("event_type", pa.string()),
    pa.field("product_id", pa.string()),
    pa.field("price_cents", pa.int64()),
    pa.field("payload_valid", pa.bool_(), nullable=False),
])
CLICKS_SCHEMA = pa.schema([
    pa.field("click_id", pa.string(), nullable=False),
    pa.field("person_id", pa.string()),
    pa.field("campaign_id", pa.string()),
    pa.field("ts_ms", pa.int64()),
    pa.field("payload_valid", pa.bool_(), nullable=False),
])
PRODUCTS_SCHEMA = pa.schema([
    pa.field("product_id", pa.string(), nullable=False),
    pa.field("category", pa.string(), nullable=False),
])
CAMPAIGNS_SCHEMA = pa.schema([
    pa.field("campaign_id", pa.string(), nullable=False),
    pa.field("channel", pa.string(), nullable=False),
])
USER_DAILY_SCHEMA = pa.schema([
    pa.field("user_id", pa.string(), nullable=False),
    pa.field("day", pa.date32(), nullable=False),
    pa.field("sessions", pa.int64(), nullable=False),
    pa.field("events", pa.int64(), nullable=False),
    pa.field("revenue_cents", pa.int64(), nullable=False),
    pa.field("top_category", pa.string()),
])
CAMPAIGN_DAILY_SCHEMA = pa.schema([
    pa.field("campaign_id", pa.string(), nullable=False),
    pa.field("channel", pa.string(), nullable=False),
    pa.field("day", pa.date32(), nullable=False),
    pa.field("sessions", pa.int64(), nullable=False),
    pa.field("purchasing_sessions", pa.int64(), nullable=False),
    pa.field("revenue_cents", pa.int64(), nullable=False),
    pa.field("assisted_sessions", pa.int64(), nullable=False),
])
