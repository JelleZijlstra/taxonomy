"""Build the local indexed GeoNames database from ``allCountries.txt``.

The input schema is documented at https://download.geonames.org/export/dump/readme.txt.
The output is a derived artifact and should not be committed to the repository.
"""

import argparse
import datetime
import sqlite3
from collections.abc import Iterable, Iterator
from pathlib import Path

from taxonomy.apis.geonames import SCHEMA_VERSION
from taxonomy.config import get_options

FIELD_COUNT = 19
INSERT_SQL = """
INSERT INTO geonames VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
"""


def _optional_int(value: str) -> int | None:
    return int(value) if value else None


def parse_row(line: str, *, line_number: int) -> tuple[object, ...]:
    fields = line.rstrip("\n").split("\t")
    if len(fields) != FIELD_COUNT:
        raise ValueError(
            f"line {line_number}: expected {FIELD_COUNT} tab-separated fields, "
            f"found {len(fields)}"
        )
    return (
        int(fields[0]),
        fields[1],
        fields[2],
        fields[3],
        float(fields[4]),
        float(fields[5]),
        fields[6],
        fields[7],
        fields[8],
        fields[9],
        fields[10],
        fields[11],
        fields[12],
        fields[13],
        int(fields[14]),
        _optional_int(fields[15]),
        _optional_int(fields[16]),
        fields[17],
        fields[18],
    )


def iter_rows(path: Path) -> Iterator[tuple[object, ...]]:
    with path.open(encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            yield parse_row(line, line_number=line_number)


def _batched(
    rows: Iterable[tuple[object, ...]], batch_size: int
) -> Iterator[list[tuple[object, ...]]]:
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript("""
        CREATE TABLE geonames (
            geoname_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            ascii_name TEXT NOT NULL,
            alternate_names TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            feature_class TEXT NOT NULL,
            feature_code TEXT NOT NULL,
            country_code TEXT NOT NULL,
            alternate_country_codes TEXT NOT NULL,
            admin1_code TEXT NOT NULL,
            admin2_code TEXT NOT NULL,
            admin3_code TEXT NOT NULL,
            admin4_code TEXT NOT NULL,
            population INTEGER NOT NULL,
            elevation INTEGER,
            dem INTEGER,
            timezone TEXT NOT NULL,
            modification_date TEXT NOT NULL
        ) STRICT;

        CREATE TABLE metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) STRICT;
        """)


def _create_indexes(connection: sqlite3.Connection) -> None:
    print("Creating ordinary indexes...")
    connection.executescript("""
        CREATE INDEX geonames_country_admin1
            ON geonames(country_code, admin1_code);
        CREATE INDEX geonames_country_admin2
            ON geonames(country_code, admin1_code, admin2_code);
        CREATE INDEX geonames_country_feature
            ON geonames(country_code, feature_class, feature_code);
        """)

    print("Creating full-text name index...")
    connection.execute("""
        CREATE VIRTUAL TABLE geonames_fts USING fts5(
            name,
            ascii_name,
            alternate_names,
            content='geonames',
            content_rowid='geoname_id',
            tokenize='unicode61 remove_diacritics 2'
        )
        """)
    connection.execute("INSERT INTO geonames_fts(geonames_fts) VALUES ('rebuild')")

    print("Creating spatial index...")
    connection.execute("""
        CREATE VIRTUAL TABLE geonames_rtree USING rtree(
            geoname_id,
            min_latitude,
            max_latitude,
            min_longitude,
            max_longitude
        )
        """)
    connection.execute("""
        INSERT INTO geonames_rtree
        SELECT geoname_id, latitude, latitude, longitude, longitude
        FROM geonames
        """)


def build_database(
    input_path: Path,
    output_path: Path,
    *,
    batch_size: int = 50_000,
    progress_every: int = 500_000,
) -> int:
    """Build ``output_path`` atomically and return the number of imported rows."""
    input_path = input_path.expanduser().resolve()
    output_path = output_path.expanduser().resolve()
    if not input_path.is_file():
        raise FileNotFoundError(input_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(f".{output_path.name}.tmp")
    temporary_path.unlink(missing_ok=True)

    count = 0
    connection = sqlite3.connect(temporary_path)
    try:
        connection.executescript("""
            PRAGMA journal_mode = OFF;
            PRAGMA synchronous = OFF;
            PRAGMA locking_mode = EXCLUSIVE;
            PRAGMA temp_store = MEMORY;
            PRAGMA cache_size = -262144;
            """)
        _create_schema(connection)
        print(f"Importing {input_path}...")
        with connection:
            for batch in _batched(iter_rows(input_path), batch_size):
                connection.executemany(INSERT_SQL, batch)
                count += len(batch)
                if progress_every and count % progress_every < len(batch):
                    print(f"Imported {count:,} rows")

        _create_indexes(connection)
        imported_at = datetime.datetime.now(datetime.UTC).isoformat()
        metadata = {
            "schema_version": str(SCHEMA_VERSION),
            "source_path": str(input_path),
            "source_size": str(input_path.stat().st_size),
            "source_mtime_ns": str(input_path.stat().st_mtime_ns),
            "row_count": str(count),
            "imported_at": imported_at,
        }
        connection.executemany(
            "INSERT INTO metadata(key, value) VALUES (?, ?)", metadata.items()
        )
        connection.execute("ANALYZE")
        connection.execute("PRAGMA optimize")
        result = connection.execute("PRAGMA quick_check").fetchone()
        if result is None or result[0] != "ok":
            raise RuntimeError(f"SQLite quick_check failed: {result}")
        connection.commit()
    except BaseException:
        connection.close()
        temporary_path.unlink(missing_ok=True)
        raise
    else:
        connection.close()

    temporary_path.replace(output_path)
    print(f"Wrote {count:,} rows to {output_path}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("~/data/geonames/allCountries.txt"),
        help="GeoNames allCountries.txt path",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="SQLite output path (defaults to geonames_db_filename)",
    )
    parser.add_argument("--batch-size", type=int, default=50_000)
    args = parser.parse_args()
    output_path = args.output or get_options().geonames_db_filename
    if output_path == Path():
        parser.error("configure geonames_db_filename or pass --output")
    build_database(args.input, output_path, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
