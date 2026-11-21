#!/usr/bin/env python3
"""Check only the public seed-1 dev output without committing answer rows.

The manifest is derived from the published dev-answer CSVs, not pipeline output.
This script and its hashes are development checks; neither is in the runtime image.
"""

import argparse
import hashlib
import json
from pathlib import Path

from run_fixtures import SCHEMAS, load_actual, load_expected


def fingerprints(folder: Path) -> dict:
    result = {}
    for table, schema in SCHEMAS.items():
        csv = folder / f"{table}.csv"
        frame = load_expected(csv, schema) if csv.is_file() else load_actual(folder / table, schema)
        keys = [key for key in ("user_id", "campaign_id", "day") if key in schema]
        if frame.n_unique(subset=keys) != frame.height:
            raise ValueError(f"{table}: duplicate output keys")
        digest = hashlib.sha256()
        digest.update((json.dumps(list(schema), separators=(",", ":")) + "\n").encode())
        for row in frame.select(list(schema)).sort(keys).iter_rows():
            encoded = json.dumps(row, ensure_ascii=False, separators=(",", ":"),
                                 default=lambda value: value.isoformat())
            digest.update((encoded + "\n").encode("utf-8"))
        result[table] = dict(rows=frame.height, sha256=digest.hexdigest())
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    parser.add_argument("--expected", type=Path, default=Path(__file__).resolve().parents[1] / "tests/dev_expected_fingerprints.json")
    args = parser.parse_args()
    expected = json.loads(args.expected.read_text())["tables"]
    actual = fingerprints(args.folder)
    print(json.dumps(actual, indent=2))
    if actual != expected:
        raise SystemExit("Public seed-1 dev fingerprints differ from the published answers")
    print("MATCH: public seed-1 dev fingerprints")


if __name__ == "__main__":
    main()
