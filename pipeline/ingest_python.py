"""Stream strict inputs into typed staging without deduplication or lateness filtering."""

import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.contracts import (
    CAMPAIGNS_SCHEMA, CLICKS_SCHEMA, EVENTS_SCHEMA, PRODUCTS_SCHEMA, PreparedInputs,
)
from pipeline.identity import IdentityMap
from pipeline.parsing import parse_timestamp, parse_unsigned, read_rows, window_timestamp


def _write_rows(path, schema, rows, batch_rows):
    """One file per table, bounded row groups, and a schema even with no rows."""
    with pq.ParquetWriter(path, schema) as writer:
        columns = [[] for _ in schema]
        count = 0
        for row in rows:
            for column, value in zip(columns, row):
                column.append(value)
            count += 1
            if count == batch_rows:
                writer.write_batch(pa.RecordBatch.from_arrays(
                    [pa.array(column, type=field.type) for column, field in zip(columns, schema)],
                    schema=schema,
                ))
                columns = [[] for _ in schema]
                count = 0
        if count:
            writer.write_batch(pa.RecordBatch.from_arrays(
                [pa.array(column, type=field.type) for column, field in zip(columns, schema)],
                schema=schema,
            ))


def _parts(data_dir, name, field_count):
    for path in sorted((data_dir / name).glob("*.csv")):
        yield from read_rows(path, field_count)


def prepare_inputs(
    data_dir: Path, scratch_dir: Path, *, batch_rows: int = 65536
) -> PreparedInputs:
    """Parse and stage inputs according to docs/DATA_CONTRACT.md.

    Only identity components and campaign membership persist across batches.
    Files remain available until the caller removes its scratch directory.
    """
    if isinstance(batch_rows, bool) or not isinstance(batch_rows, int) or batch_rows <= 0:
        raise ValueError("batch_rows must be a positive integer")
    manifest = json.loads((data_dir / "manifest.json").read_text(encoding="utf-8"))
    start = parse_timestamp(manifest["window_start"])
    end = parse_timestamp(manifest["window_end"])
    if start is None or end is None or start >= end:
        raise ValueError("manifest must specify a valid increasing timestamp window")
    identities = IdentityMap()
    for first, second in _parts(data_dir, "links", 2):
        identities.union(first, second)

    scratch_dir.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix="ingestion-", dir=scratch_dir))
    events_dir, clicks_dir = stage / "events", stage / "clicks"
    events_dir.mkdir()
    clicks_dir.mkdir()
    products_path, campaigns_path = stage / "products.parquet", stage / "campaigns.parquet"
    _write_rows(products_path, PRODUCTS_SCHEMA, read_rows(data_dir / "products.csv", 2), batch_rows)
    campaigns = set()

    def campaign_rows():
        for row in read_rows(data_dir / "campaigns.csv", 2):
            campaigns.add(row[0])
            yield row

    _write_rows(campaigns_path, CAMPAIGNS_SCHEMA, campaign_rows(), batch_rows)

    def event_rows():
        for event_id, user, ts, ingest, kind, product, price in _parts(data_dir, "events", 7):
            if event_id is None:
                continue
            timestamp = window_timestamp(ts, start, end)
            ingest_ms = parse_timestamp(ingest)
            if ingest_ms is None:
                raise ValueError("ingest_ts violates the guaranteed valid timestamp input")
            cents = parse_unsigned(price, 10_000_000) if kind == "purchase" else None
            if cents == 0:
                cents = None
            valid = (
                user is not None and timestamp is not None and product is not None
                and kind in ("view", "cart", "purchase")
                and (kind != "purchase" or cents is not None)
            )
            yield (event_id, identities.person(user), ingest_ms, timestamp, kind, product, cents, valid)

    def click_rows():
        for click_id, user, campaign, ts in _parts(data_dir, "clicks", 4):
            if click_id is None:
                continue
            timestamp = window_timestamp(ts, start - 604_800_000, end)
            valid = user is not None and timestamp is not None and campaign in campaigns
            yield (click_id, identities.person(user), campaign, timestamp, valid)

    _write_rows(events_dir / "part-0.parquet", EVENTS_SCHEMA, event_rows(), batch_rows)
    _write_rows(clicks_dir / "part-0.parquet", CLICKS_SCHEMA, click_rows(), batch_rows)
    return PreparedInputs(
        events_dir, clicks_dir, products_path, campaigns_path, start, end
    )
