from __future__ import annotations

import datetime
import math
import re
from collections.abc import Iterable

from taxonomy import coordinates
from taxonomy.db import coordinate_lint, helpers, models
from taxonomy.db.constants import AltitudeUnit, OccurrenceBasis
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.location import Location, LocationStatus
from taxonomy.db.models.taxon import Taxon

from .model import OccurrenceRecord, OccurrenceRecordTag

COORDINATE_LOCATION_TOLERANCE_KM = 5

_SIGNED_DECIMAL_COORDINATES = re.compile(
    r"^\s*(?P<latitude>[+-]?\d+(?:\.\d+)?)\s*[,;/]\s*"
    r"(?P<longitude>[+-]?\d+(?:\.\d+)?)\s*$"
)
_ELEVATION = re.compile(
    r"^\s*(?:elev(?:ation)?\.?|alt(?:itude)?\.?)?\s*"
    r"(?P<elevation>-?\d[\d,]*(?:\.\d+)?(?:\s*[-–]\s*\d[\d,]*(?:\.\d+)?)?)"
    r"\s*(?P<unit>m|met(?:er|re)s?|ft|feet|foot)\.?\s*$",
    re.IGNORECASE,
)
_ISO_DATE = re.compile(r"^(?P<year>\d{4})(?:-(?P<month>\d{2})(?:-(?P<day>\d{2}))?)?$")


def get_inferred_taxon(record: OccurrenceRecord) -> Taxon | None:
    mapped_name = record.classification_entry.mapped_name
    if mapped_name is None:
        return None
    return mapped_name.taxon.resolve_redirect()


def _get_location_by_name(name: str) -> Location | None:
    candidates = list(Location.select().filter(Location.name == name))
    if len(candidates) != 1:
        return None
    candidate = candidates[0]
    if candidate.deleted is LocationStatus.alias:
        return candidate.parent
    if candidate.deleted is LocationStatus.valid:
        return candidate
    return None


def get_inferred_location(record: OccurrenceRecord) -> Location | None:
    hint_names = [
        tag.name
        for tag in record.get_tags(record.tags, OccurrenceRecordTag.LocationHint)
    ]
    hinted_candidates = {
        candidate
        for name in hint_names
        if (candidate := _get_location_by_name(name)) is not None
    }
    if len(hinted_candidates) == 1:
        return next(iter(hinted_candidates))
    if hint_names:
        return None
    if candidate := _get_location_by_name(record.locality_text):
        return candidate
    try:
        region = models.Region.get(models.Region.name == record.locality_text)
        return region.get_location()
    except models.Region.DoesNotExist, models.Location.DoesNotExist:
        return None


def check_taxon(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    inferred = get_inferred_taxon(record)
    if record.taxon is None:
        if inferred is None:
            yield f"{record}: cannot infer taxon from classification entry [missing_taxon]"
        else:
            message = f"{record}: taxon should be {inferred} [missing_taxon]"
            if cfg.autofix:
                print(message)
                record.taxon = inferred
            else:
                yield message
        return
    if inferred is None or record.taxon == inferred:
        return
    if record.has_tag(OccurrenceRecordTag.TaxonomicSplitFrom) or record.has_tag(
        OccurrenceRecordTag.CommentFromDatabase
    ):
        return
    yield (
        f"{record}: taxon {record.taxon} differs from classification-entry mapping "
        f"{inferred} without an explanation [taxon_mapping]"
    )


def check_location(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    hints = list(record.get_tags(record.tags, OccurrenceRecordTag.LocationHint))
    if record.location is None:
        inferred = get_inferred_location(record)
        if inferred is None:
            yield f"{record}: cannot infer location [missing_location]"
        else:
            message = f"{record}: location should be {inferred} [missing_location]"
            if cfg.autofix:
                print(message)
                record.location = inferred
                record.remove_tags(OccurrenceRecordTag.LocationHint)
            else:
                yield message
        return
    inferred = get_inferred_location(record)
    if hints and inferred == record.location:
        message = f"{record}: remove resolved LocationHint [location_hint]"
        if cfg.autofix:
            print(message)
            record.remove_tags(OccurrenceRecordTag.LocationHint)
        else:
            yield message
        return
    if hints and inferred is not None and inferred != record.location:
        yield (
            f"{record}: location {record.location} differs from LocationHint mapping "
            f"{inferred} [location_mapping]"
        )


def check_basis_tags(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    if record.basis is not OccurrenceBasis.observation and record.has_tag(
        OccurrenceRecordTag.ObservationKind
    ):
        yield f"{record}: ObservationKind requires observation basis [basis_tags]"
    if record.basis is not OccurrenceBasis.voucher and record.has_tag(
        OccurrenceRecordTag.SpecimenDetail
    ):
        yield f"{record}: SpecimenDetail requires voucher basis [basis_tags]"


def _replace_tag(
    record: OccurrenceRecord, old_tag: OccurrenceRecordTag, new_tag: OccurrenceRecordTag
) -> None:
    replaced = False
    new_tags = []
    for tag in record.tags:
        if not replaced and tag == old_tag:
            new_tags.append(new_tag)
            replaced = True
        else:
            new_tags.append(tag)
    record.tags = tuple(new_tags)  # type: ignore[assignment]


def parse_verbatim_coordinates(text: str) -> tuple[str, str] | None:
    coordinates_from_text = helpers.extract_coordinates(text)
    if coordinates_from_text is not None:
        return coordinates_from_text
    match = _SIGNED_DECIMAL_COORDINATES.fullmatch(text)
    if match is None:
        return None
    try:
        latitude, _ = coordinate_lint.standardize_coordinate(
            match.group("latitude"), is_latitude=True
        )
        longitude, _ = coordinate_lint.standardize_coordinate(
            match.group("longitude"), is_latitude=False
        )
    except helpers.InvalidCoordinates:
        return None
    return latitude, longitude


def parse_verbatim_elevation(text: str) -> tuple[str, AltitudeUnit] | None:
    match = _ELEVATION.fullmatch(text)
    if match is None:
        return None
    elevation = re.sub(r"\s*[-–]\s*", "-", match.group("elevation"))
    elevation = elevation.replace(",", "")
    unit_text = match.group("unit").lower()
    unit = AltitudeUnit.m if unit_text.startswith("m") else AltitudeUnit.ft
    return elevation, unit


def _parse_iso_date(text: str) -> str | None:
    match = _ISO_DATE.fullmatch(text)
    if match is None:
        return None
    year = int(match.group("year"))
    month_text = match.group("month")
    day_text = match.group("day")
    if month_text is None:
        return f"{year:04d}"
    month = int(month_text)
    if day_text is None:
        if not 1 <= month <= 12:
            return None
        return f"{year:04d}-{month:02d}"
    day = int(day_text)
    try:
        parsed = datetime.date(year, month, day)
    except ValueError:
        return None
    return parsed.isoformat()


def parse_verbatim_date(text: str) -> str | None:
    """Return a source date in a queryable ISO 8601-style representation."""
    text = text.strip()
    if parsed := _parse_iso_date(text):
        return parsed
    try:
        standardized = helpers.standardize_date(text)
    except ValueError:
        return None
    if standardized is None:
        return None
    before = standardized.startswith("<")
    standardized = standardized.removeprefix("<")
    if parsed := _parse_iso_date(standardized):
        return f"<{parsed}" if before else parsed
    for fmt, output_fmt in (("%B %Y", "%Y-%m"), ("%d %B %Y", "%Y-%m-%d")):
        try:
            parsed_date = datetime.datetime.strptime(standardized, fmt).replace(
                tzinfo=datetime.UTC
            )
        except ValueError:
            continue
        output = parsed_date.strftime(output_fmt)
        return f"<{output}" if before else output
    return None


def _add_inferred_tag(
    record: OccurrenceRecord,
    tag: OccurrenceRecordTag,
    source_tag: OccurrenceRecordTag,
    cfg: LintConfig,
) -> Iterable[str]:
    message = f"{record}: add {tag} inferred from {source_tag} [source_data]"
    if cfg.autofix:
        print(message)
        record.add_tag(tag)
    else:
        yield message


def _check_verbatim_coordinates(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[str]:
    normalized = list(record.get_tags(record.tags, OccurrenceRecordTag.Coordinates))
    for tag in record.get_tags(record.tags, OccurrenceRecordTag.VerbatimCoordinates):
        parsed = parse_verbatim_coordinates(tag.text)
        if parsed is None:
            if not normalized:
                yield (
                    f"{record}: cannot parse coordinates from {tag.text!r}; add "
                    "Coordinates manually [source_data]"
                )
            continue
        expected = OccurrenceRecordTag.Coordinates(*parsed)
        if expected not in normalized:
            if normalized:
                yield (
                    f"{record}: {tag} parses as {expected}, inconsistent with "
                    f"{normalized} [source_data]"
                )
            else:
                yield from _add_inferred_tag(record, expected, tag, cfg)
                if cfg.autofix:
                    normalized.append(expected)


def _check_verbatim_elevations(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[str]:
    normalized = list(record.get_tags(record.tags, OccurrenceRecordTag.Elevation))
    for tag in record.get_tags(record.tags, OccurrenceRecordTag.VerbatimElevation):
        parsed = parse_verbatim_elevation(tag.text)
        if parsed is None:
            if not normalized:
                yield (
                    f"{record}: cannot parse elevation from {tag.text!r}; add "
                    "Elevation manually [source_data]"
                )
            continue
        expected = OccurrenceRecordTag.Elevation(*parsed)
        if expected not in normalized:
            if normalized:
                yield (
                    f"{record}: {tag} parses as {expected}, inconsistent with "
                    f"{normalized} [source_data]"
                )
            else:
                yield from _add_inferred_tag(record, expected, tag, cfg)
                if cfg.autofix:
                    normalized.append(expected)


def _check_verbatim_dates(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    normalized = list(record.get_tags(record.tags, OccurrenceRecordTag.Date))
    for tag in record.get_tags(record.tags, OccurrenceRecordTag.VerbatimDate):
        parsed = parse_verbatim_date(tag.text)
        if parsed is None:
            if not normalized:
                yield (
                    f"{record}: cannot parse date from {tag.text!r}; add Date "
                    "manually [source_data]"
                )
            continue
        expected = OccurrenceRecordTag.Date(parsed)
        if expected not in normalized:
            if normalized:
                yield (
                    f"{record}: {tag} parses as {expected}, inconsistent with "
                    f"{normalized} [source_data]"
                )
            else:
                yield from _add_inferred_tag(record, expected, tag, cfg)
                if cfg.autofix:
                    normalized.append(expected)


def _standardize_normalized_tags(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[str]:
    for tag in tuple(record.get_tags(record.tags, OccurrenceRecordTag.Coordinates)):
        try:
            latitude, _ = coordinate_lint.standardize_coordinate(
                tag.latitude, is_latitude=True
            )
            longitude, _ = coordinate_lint.standardize_coordinate(
                tag.longitude, is_latitude=False
            )
        except helpers.InvalidCoordinates:
            yield f"{record}: invalid normalized coordinates {tag} [source_data]"
            continue
        expected = OccurrenceRecordTag.Coordinates(latitude, longitude)
        if tag != expected:
            message = f"{record}: replace {tag} with {expected} [source_data]"
            if cfg.autofix:
                print(message)
                _replace_tag(record, tag, expected)
            else:
                yield message
    for tag in tuple(record.get_tags(record.tags, OccurrenceRecordTag.Elevation)):
        elevation_parsed = parse_verbatim_elevation(f"{tag.elevation} {tag.unit.name}")
        if elevation_parsed is None:
            yield f"{record}: invalid normalized elevation {tag} [source_data]"
            continue
        expected = OccurrenceRecordTag.Elevation(*elevation_parsed)
        if tag != expected:
            message = f"{record}: replace {tag} with {expected} [source_data]"
            if cfg.autofix:
                print(message)
                _replace_tag(record, tag, expected)
            else:
                yield message
    for tag in tuple(record.get_tags(record.tags, OccurrenceRecordTag.Date)):
        date_parsed = parse_verbatim_date(tag.date.removeprefix("<"))
        if date_parsed is None:
            yield f"{record}: invalid normalized date {tag} [source_data]"
            continue
        if tag.date.startswith("<"):
            date_parsed = f"<{date_parsed}"
        expected = OccurrenceRecordTag.Date(date_parsed)
        if tag != expected:
            message = f"{record}: replace {tag} with {expected} [source_data]"
            if cfg.autofix:
                print(message)
                _replace_tag(record, tag, expected)
            else:
                yield message


def check_source_data_tags(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    yield from _standardize_normalized_tags(record, cfg)
    for normalized_type, verbatim_type in (
        (OccurrenceRecordTag.Coordinates, OccurrenceRecordTag.VerbatimCoordinates),
        (OccurrenceRecordTag.Elevation, OccurrenceRecordTag.VerbatimElevation),
        (OccurrenceRecordTag.Date, OccurrenceRecordTag.VerbatimDate),
    ):
        if record.has_tag(normalized_type) and not record.has_tag(verbatim_type):
            yield (
                f"{record}: {normalized_type.__name__} requires "
                f"{verbatim_type.__name__} [source_data]"
            )
    yield from _check_verbatim_coordinates(record, cfg)
    yield from _check_verbatim_elevations(record, cfg)
    yield from _check_verbatim_dates(record, cfg)


def _distance_km(first: coordinates.Point, second: coordinates.Point) -> float:
    """Return the great-circle distance between two points in kilometres."""
    earth_radius_km = 6371.0088
    lat1 = math.radians(first.latitude)
    lat2 = math.radians(second.latitude)
    delta_lat = lat2 - lat1
    delta_lon = math.radians(second.longitude - first.longitude)
    haversine = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
    )
    return 2 * earth_radius_km * math.asin(math.sqrt(haversine))


def check_coordinate_consistency(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[str]:
    coordinate_tags = list(
        record.get_tags(record.tags, OccurrenceRecordTag.Coordinates)
    )
    if not coordinate_tags or record.location is None:
        return
    if record.location.latitude is None or record.location.longitude is None:
        yield (
            f"{record}: has source coordinates but Location {record.location} has "
            "no coordinates [coordinate_location]"
        )
        return
    location_point = coordinate_lint.make_point(
        record.location.latitude, record.location.longitude
    )
    if location_point is None:
        return
    for tag in coordinate_tags:
        occurrence_point = coordinate_lint.make_point(tag.latitude, tag.longitude)
        if occurrence_point is None:
            continue
        distance = _distance_km(occurrence_point, location_point)
        if distance > COORDINATE_LOCATION_TOLERANCE_KM:
            yield (
                f"{record}: source coordinates {tag.latitude}, {tag.longitude} are "
                f"{distance:.1f} km from Location {record.location} coordinates "
                f"{record.location.latitude}, {record.location.longitude} "
                "[coordinate_location]"
            )


def check_status_tags(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    for tag_type in (
        OccurrenceRecordTag.StatusFromSource,
        OccurrenceRecordTag.StatusAssessment,
    ):
        statuses = [tag.status for tag in record.get_tags(record.tags, tag_type)]
        duplicate_statuses = sorted(
            {status for status in statuses if statuses.count(status) > 1},
            key=lambda status: status.value,
        )
        for status in duplicate_statuses:
            yield (
                f"{record}: has duplicate {tag_type.__name__} for {status.name} "
                "[status_tags]"
            )


def check_split(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    tags = list(record.get_tags(record.tags, OccurrenceRecordTag.TaxonomicSplitFrom))
    if len(tags) > 1:
        yield f"{record}: has multiple TaxonomicSplitFrom tags [taxonomic_split]"
        return
    if not tags:
        return
    canonical = tags[0].record
    if canonical == record:
        yield f"{record}: TaxonomicSplitFrom points to itself [taxonomic_split]"
        return
    if canonical.has_tag(OccurrenceRecordTag.TaxonomicSplitFrom):
        yield f"{record}: TaxonomicSplitFrom points to a derived record [taxonomic_split]"
    for field in ("classification_entry", "locality_text", "page", "basis"):
        if getattr(record, field) != getattr(canonical, field):
            yield (
                f"{record}: {field} differs from canonical split record {canonical} "
                "[taxonomic_split]"
            )
    if record.taxon is not None and record.taxon == canonical.taxon:
        yield f"{record}: split record has the same taxon as {canonical} [taxonomic_split]"


def check_duplicate(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    if record.has_tag(OccurrenceRecordTag.TaxonomicSplitFrom):
        return
    candidates = OccurrenceRecord.select().filter(
        OccurrenceRecord.classification_entry == record.classification_entry,
        OccurrenceRecord.locality_text == record.locality_text,
        OccurrenceRecord.page == record.page,
        OccurrenceRecord.basis == record.basis,
    )
    for candidate in candidates:
        if candidate != record and not candidate.has_tag(
            OccurrenceRecordTag.TaxonomicSplitFrom
        ):
            yield f"{record}: duplicates primary record {candidate} [duplicate]"
