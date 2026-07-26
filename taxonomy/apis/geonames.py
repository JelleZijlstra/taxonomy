"""Query the local SQLite copy of the GeoNames gazetteer."""

from __future__ import annotations

import functools
import math
import sqlite3
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypeAlias

from taxonomy import coordinates
from taxonomy.config import get_options
from taxonomy.db import coordinate_lint, helpers

SCHEMA_VERSION = 1
MatchKind: TypeAlias = Literal["name", "ascii_name", "alternate_name"]


@dataclass(frozen=True, slots=True)
class GeoNamesRecord:
    geoname_id: int
    name: str
    ascii_name: str
    alternate_names: str
    latitude: float
    longitude: float
    feature_class: str
    feature_code: str
    country_code: str
    alternate_country_codes: str
    admin1_code: str
    admin2_code: str
    admin3_code: str
    admin4_code: str
    population: int
    elevation: int | None
    dem: int | None
    timezone: str
    modification_date: str

    @property
    def point(self) -> coordinates.Point:
        return coordinates.Point(self.longitude, self.latitude)

    def names(self) -> tuple[str, ...]:
        output = [self.name]
        if self.ascii_name and self.ascii_name not in output:
            output.append(self.ascii_name)
        output.extend(
            name
            for name in self.alternate_names.split(",")
            if name and name not in output
        )
        return tuple(output)


@dataclass(frozen=True, slots=True)
class GeoNamesMatch:
    record: GeoNamesRecord
    matched_name: str
    match_kind: MatchKind


@dataclass(frozen=True, slots=True)
class NearbyGeoNamesRecord:
    record: GeoNamesRecord
    distance_km: float


@functools.cache
def _get_connection(path: Path) -> sqlite3.Connection:
    if path == Path():
        raise RuntimeError("geonames_db_filename is not configured")
    if not path.is_file():
        raise RuntimeError(f"GeoNames database does not exist: {path}")
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    schema_version = connection.execute(
        "SELECT value FROM metadata WHERE key = 'schema_version'"
    ).fetchone()
    if schema_version is None or int(schema_version[0]) != SCHEMA_VERSION:
        connection.close()
        actual = None if schema_version is None else schema_version[0]
        raise RuntimeError(
            f"unsupported GeoNames database schema {actual!r}; "
            f"expected {SCHEMA_VERSION}"
        )
    return connection


def _get_database_path(db_path: Path | None) -> Path:
    if db_path is not None:
        return db_path.expanduser()
    return get_options().geonames_db_filename


def _query(
    sql: str, args: tuple[object, ...], *, db_path: Path | None = None
) -> list[sqlite3.Row]:
    connection = _get_connection(_get_database_path(db_path))
    return list(connection.execute(sql, args))


def _record_from_row(row: sqlite3.Row) -> GeoNamesRecord:
    return GeoNamesRecord(
        **{field: row[field] for field in GeoNamesRecord.__dataclass_fields__}
    )


def get_by_id(geoname_id: int, *, db_path: Path | None = None) -> GeoNamesRecord | None:
    rows = _query(
        "SELECT * FROM geonames WHERE geoname_id = ?", (geoname_id,), db_path=db_path
    )
    if not rows:
        return None
    return _record_from_row(rows[0])


def get_administrative_hierarchy(
    record: GeoNamesRecord, *, db_path: Path | None = None
) -> list[GeoNamesRecord]:
    """Return the ADM1-ADM4 records identified by a feature's admin codes."""
    path = _get_database_path(db_path)
    return list(
        _get_administrative_hierarchy(
            path,
            record.country_code,
            record.admin1_code,
            record.admin2_code,
            record.admin3_code,
            record.admin4_code,
        )
    )


@functools.cache
def _get_administrative_hierarchy(
    path: Path,
    country_code: str,
    admin1_code: str,
    admin2_code: str,
    admin3_code: str,
    admin4_code: str,
) -> tuple[GeoNamesRecord, ...]:
    conditions = []
    args: list[object] = [country_code]
    codes = (admin1_code, admin2_code, admin3_code, admin4_code)
    for level, code in enumerate(codes, start=1):
        if not code:
            break
        conditions.append(
            "(feature_code = ? AND "
            + " AND ".join(f"admin{index}_code = ?" for index in range(1, level + 1))
            + ")"
        )
        args.extend((f"ADM{level}", *codes[:level]))
    if not conditions or not country_code:
        return ()
    rows = _query(
        f"""
        SELECT * FROM geonames
        WHERE country_code = ? AND ({' OR '.join(conditions)})
        ORDER BY feature_code, population DESC, geoname_id
        """,
        tuple(args),
        db_path=path,
    )
    return tuple(_record_from_row(row) for row in rows)


def _fts_phrase(text: str) -> str:
    return f'"{text.replace(chr(34), chr(34) * 2)}"'


def _matching_name(record: GeoNamesRecord, query: str) -> tuple[str, MatchKind] | None:
    normalized_query = helpers.simplify_string(query, clean_words=False)
    if helpers.simplify_string(record.name, clean_words=False) == normalized_query:
        return record.name, "name"
    if (
        record.ascii_name
        and helpers.simplify_string(record.ascii_name, clean_words=False)
        == normalized_query
    ):
        return record.ascii_name, "ascii_name"
    for name in record.alternate_names.split(","):
        if (
            name
            and helpers.simplify_string(name, clean_words=False) == normalized_query
        ):
            return name, "alternate_name"
    return None


def search_name(
    name: str,
    *,
    country_code: str | None = None,
    admin1_code: str | None = None,
    feature_classes: Collection[str] = (),
    feature_codes: Collection[str] = (),
    limit: int = 20,
    db_path: Path | None = None,
) -> list[GeoNamesMatch]:
    """Return exact normalized matches to a primary, ASCII, or alternate name."""
    if not name.strip() or limit < 1:
        return []
    clauses = ["geonames_fts MATCH ?"]
    args: list[object] = [_fts_phrase(name)]
    if country_code is not None:
        clauses.append("g.country_code = ?")
        args.append(country_code.upper())
    if admin1_code is not None:
        clauses.append("g.admin1_code = ?")
        args.append(admin1_code)
    for column, values in (
        ("feature_class", tuple(feature_classes)),
        ("feature_code", tuple(feature_codes)),
    ):
        if values:
            placeholders = ", ".join("?" for _ in values)
            clauses.append(f"g.{column} IN ({placeholders})")
            args.extend(values)
    rows = _query(
        f"""
        SELECT g.*
        FROM geonames_fts
        JOIN geonames AS g ON g.geoname_id = geonames_fts.rowid
        WHERE {' AND '.join(clauses)}
        ORDER BY g.population DESC, g.geoname_id
        """,
        tuple(args),
        db_path=db_path,
    )
    matches: list[GeoNamesMatch] = []
    for row in rows:
        record = _record_from_row(row)
        matching = _matching_name(record, name)
        if matching is None:
            continue
        matched_name, match_kind = matching
        matches.append(GeoNamesMatch(record, matched_name, match_kind))
        if len(matches) >= limit:
            break
    return matches


def _longitude_ranges(
    longitude: float, delta: float
) -> tuple[tuple[float, float], ...]:
    if delta >= 180:
        return ((-180, 180),)
    minimum = longitude - delta
    maximum = longitude + delta
    if minimum < -180:
        return ((minimum + 360, 180), (-180, maximum))
    if maximum > 180:
        return ((minimum, 180), (-180, maximum - 360))
    return ((minimum, maximum),)


def find_nearby(
    latitude: float,
    longitude: float,
    *,
    radius_km: float = 10,
    country_code: str | None = None,
    feature_classes: Collection[str] = (),
    feature_codes: Collection[str] = (),
    limit: int = 20,
    db_path: Path | None = None,
) -> list[NearbyGeoNamesRecord]:
    """Return GeoNames features within ``radius_km``, nearest first."""
    if not -90 <= latitude <= 90:
        raise ValueError(f"invalid latitude: {latitude}")
    if not -180 <= longitude <= 180:
        raise ValueError(f"invalid longitude: {longitude}")
    if radius_km <= 0:
        raise ValueError("radius_km must be positive")
    if limit < 1:
        return []

    latitude_delta = min(90, radius_km / 110.574)
    longitude_scale = 111.320 * abs(math.cos(math.radians(latitude)))
    longitude_delta = 180 if longitude_scale < 1e-9 else radius_km / longitude_scale
    longitude_clauses = []
    args: list[object] = [
        max(-90, latitude - latitude_delta),
        min(90, latitude + latitude_delta),
    ]
    for minimum, maximum in _longitude_ranges(longitude, longitude_delta):
        longitude_clauses.append("(r.max_longitude >= ? AND r.min_longitude <= ?)")
        args.extend((minimum, maximum))

    clauses = [
        "r.max_latitude >= ?",
        "r.min_latitude <= ?",
        f"({' OR '.join(longitude_clauses)})",
    ]
    if country_code is not None:
        clauses.append("g.country_code = ?")
        args.append(country_code.upper())
    for column, values in (
        ("feature_class", tuple(feature_classes)),
        ("feature_code", tuple(feature_codes)),
    ):
        if values:
            placeholders = ", ".join("?" for _ in values)
            clauses.append(f"g.{column} IN ({placeholders})")
            args.extend(values)

    rows = _query(
        f"""
        SELECT g.*
        FROM geonames_rtree AS r
        JOIN geonames AS g ON g.geoname_id = r.geoname_id
        WHERE {' AND '.join(clauses)}
        """,
        tuple(args),
        db_path=db_path,
    )
    origin = coordinates.Point(longitude, latitude)
    nearby = []
    for row in rows:
        record = _record_from_row(row)
        distance = coordinate_lint.distance_km(origin, record.point)
        if distance <= radius_km:
            nearby.append(NearbyGeoNamesRecord(record, distance))
    nearby.sort(key=lambda item: (item.distance_km, item.record.geoname_id))
    return nearby[:limit]
