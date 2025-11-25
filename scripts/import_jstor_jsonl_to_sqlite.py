"""
Import JSTOR JSONL metadata into a SQLite database with a single flat table.

Usage:
  python scripts/import_jstor_jsonl_to_sqlite.py \
      --input data_import/data/jstor_metadata_2025-11-24.jsonl \
      --output data_import/data/jstor_metadata_2025-11-24.sqlite \
      [--table jstor]

Details:
- Flattens nested dictionaries by joining keys with underscores.
- Keeps lists as JSON strings.
- Creates indexes for fast lookup by DOIs and journal name when present.
"""

import argparse
import sqlite3
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import orjson as json

from taxonomy import getinput


def flatten_record(obj: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in obj.items():
        col = f"{prefix}_{key}" if prefix else key
        col = sanitize_column(col)
        if isinstance(value, dict):
            out.update(flatten_record(value, col))
        elif isinstance(value, list):
            out[col] = json.dumps(value)
        else:
            out[col] = value
    return out


def columns_from_record(obj: dict[str, Any]) -> set[str]:
    cols: set[str] = set()
    for key, value in obj.items():
        col = sanitize_column(key)
        if isinstance(value, dict):
            subcols = columns_from_record(value)
            for subcol in subcols:
                cols.add(f"{col}_{subcol}")
        else:
            cols.add(col)
    return cols


def sanitize_column(name: str) -> str:
    # SQLite-friendly: letters, digits, underscore only
    return name.replace(" ", "_").replace("-", "_").replace(".", "_").replace("/", "_")


def read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def determine_columns(records: Iterable[dict[str, Any]]) -> list[str]:
    columns: set[str] = set()
    for rec in getinput.print_every_n(records, label="records", n=10000):
        flat = columns_from_record(rec)
        new_columns = flat - columns
        if new_columns:
            print(
                f"Found {len(new_columns)} new columns: {new_columns}, total now {len(columns) + len(new_columns)}"
            )
            columns.update(new_columns)
    return sorted(columns)


def create_table(conn: sqlite3.Connection, table: str, columns: list[str]) -> None:
    cols_sql = ", ".join([f'"{c}" TEXT' for c in columns])
    conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    conn.execute(f'CREATE TABLE "{table}" ({cols_sql})')


def insert_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    records: Iterable[dict[str, Any]],
    batch_size: int = 1000,
) -> None:
    placeholders = ", ".join(["?"] * len(columns))
    cols_sql = ", ".join([f'"{c}"' for c in columns])
    sql = f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders})'

    def row_values(row: dict[str, Any]) -> list[Any]:
        return [row.get(c) for c in columns]

    batch: list[list[Any]] = []
    with conn:
        for rec in getinput.print_every_n(records, label="records", n=10000):
            flat = flatten_record(rec)
            batch.append(row_values(flat))
            if len(batch) >= batch_size:
                conn.executemany(sql, batch)
                batch.clear()
        if batch:
            conn.executemany(sql, batch)


def create_indexes(conn: sqlite3.Connection, table: str, columns: list[str]) -> None:
    # DOI indexes: JSTOR DOI and any other *doi* columns
    if "ithaka_doi" in columns:
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{table}_ithaka_doi ON "{table}"(ithaka_doi)'
        )
    for col in columns:
        if col == "ithaka_doi":
            continue
        if "doi" in col.lower():
            conn.execute(
                f'CREATE INDEX IF NOT EXISTS idx_{table}_{col}_doi ON "{table}"("{col}")'
            )

    # Journal name: commonly is_part_of; also index journal_code if present
    if "is_part_of" in columns:
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{table}_is_part_of ON "{table}"(is_part_of)'
        )
    if "identifiers_journal_code" in columns:
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{table}_journal_code ON "{table}"(identifiers_journal_code)'
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        type=Path,
        default=Path("data_import/data/jstor_metadata_2025-11-24.jsonl"),
        help="Path to JSTOR JSONL dump",
    )
    ap.add_argument(
        "--output",
        type=Path,
        required=False,
        help="Output SQLite DB path (default: same name with .sqlite)",
    )
    ap.add_argument(
        "--table", default="jstor", help="SQLite table name (default: jstor)"
    )
    args = ap.parse_args()

    input_path: Path = args.input
    output_path: Path = args.output or input_path.with_suffix(".sqlite")
    table: str = args.table

    print(f"Reading: {input_path}")
    # First pass: determine columns
    columns = determine_columns(read_jsonl(input_path))
    print(f"Columns: {len(columns)}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(output_path)
    try:
        create_table(conn, table, columns)
        # Second pass: stream inserts
        insert_rows(conn, table, columns, read_jsonl(input_path))
        create_indexes(conn, table, columns)
        conn.commit()
    finally:
        conn.close()
    print(f"Wrote SQLite DB: {output_path}")


if __name__ == "__main__":
    main()
