"""The competition's LF-only CSV dialect and integer timestamp parsing."""

import re
from datetime import date
from pathlib import Path
from typing import Iterator


_TIMESTAMP = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T"
    r"([0-9]{2}):([0-9]{2}):([0-9]{2})(Z|[+-][0-9]{2}:[0-9]{2})"
)
_EPOCH_ORDINAL = date(1970, 1, 1).toordinal()


def parse_line(line: str, field_count: int) -> tuple[str | None, ...] | None:
    """Remove one LF, never CR; return None for malformed physical lines."""
    if line.endswith("\n"):
        line = line[:-1]
    if not line:
        return None
    fields = line.split(",")
    if len(fields) != field_count:
        return None
    for index, field in enumerate(fields):
        if '"' in field:
            if len(field) < 2 or field[0] != '"' or field[-1] != '"':
                return None
            field = field[1:-1]
            if '"' in field:
                return None
        fields[index] = field or None
    return tuple(fields)


def read_rows(path: Path, field_count: int) -> Iterator[tuple[str | None, ...]]:
    """Stream bytes so Python's universal-newline handling cannot change IDs."""
    with path.open("rb") as source:
        next(source, None)  # Exactly one physical header, regardless of contents.
        for raw in source:
            row = parse_line(raw.decode("utf-8"), field_count)
            if row is not None:
                yield row


def parse_unsigned(value: str | None, maximum: int) -> int | None:
    """Bound ASCII decimal input before int(), including arbitrarily many zeros."""
    if not value or not value.isascii() or not value.isdecimal():
        return None
    significant = value.lstrip("0") or "0"
    bound = str(maximum)
    if len(significant) > len(bound) or (
        len(significant) == len(bound) and significant > bound
    ):
        return None
    return int(significant)


def parse_timestamp(value: str | None) -> int | None:
    """Parse F1/F2/F3 without floats; unrepresentable F3 values cannot be staged."""
    if not value:
        return None
    if value.isascii() and value.isdecimal():
        return parse_unsigned(value, 2**63 - 1)
    match = _TIMESTAMP.fullmatch(value)
    if match is None:
        return None
    year, month, day, hour, minute, second = map(int, match.groups()[:6])
    if hour > 23 or minute > 59 or second > 59:
        return None
    try:
        days = date(year, month, day).toordinal() - _EPOCH_ORDINAL
    except ValueError:
        return None
    zone = match[7]
    offset = 0
    if zone != "Z":
        zone_hour, zone_minute = int(zone[1:3]), int(zone[4:6])
        if zone_hour > 23 or zone_minute > 59:
            return None
        offset = (zone_hour * 60 + zone_minute) * (1 if zone[0] == "+" else -1)
    return ((days * 24 + hour) * 3600 + minute * 60 + second - offset * 60) * 1000


def window_timestamp(value: str | None, start_ms: int, end_ms: int) -> int | None:
    timestamp = parse_timestamp(value)
    return timestamp if timestamp is not None and start_ms <= timestamp < end_ms else None
