"""Output schemas used only by local correctness checks."""
import pyarrow as pa

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
