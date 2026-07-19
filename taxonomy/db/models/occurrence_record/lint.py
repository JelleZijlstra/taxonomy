from __future__ import annotations

import datetime
import re
from collections.abc import Collection, Iterable
from dataclasses import replace

from taxonomy.db import coordinate_lint, helpers, models
from taxonomy.db.constants import AltitudeUnit, OccurrenceBasis
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import IgnoreLint, Lint
from taxonomy.db.models.location import Location, LocationStatus
from taxonomy.db.models.taxon import Taxon

from .model import OccurrenceRecord, OccurrenceRecordTag

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


def remove_unused_ignores(record: OccurrenceRecord, unused: Collection[str]) -> None:
    new_tags = []
    for tag in record.tags:
        if (
            isinstance(tag, OccurrenceRecordTag.IgnoreLintOccurrenceRecord)
            and tag.label in unused
        ):
            print(f"{record}: removing unused IgnoreLint tag: {tag}")
        else:
            new_tags.append(tag)
    record.tags = new_tags  # type: ignore[assignment]


def get_ignores(record: OccurrenceRecord) -> Iterable[IgnoreLint]:
    return record.get_tags(record.tags, OccurrenceRecordTag.IgnoreLintOccurrenceRecord)


def add_ignore(record: OccurrenceRecord, label: str, comment: str) -> None:
    record.add_tag(
        OccurrenceRecordTag.IgnoreLintOccurrenceRecord(label, comment=comment)
    )


LINT = Lint(OccurrenceRecord, get_ignores, remove_unused_ignores, add_ignore)


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


@LINT.add("missing_taxon")
def check_missing_taxon(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    if record.taxon is not None:
        return
    inferred = get_inferred_taxon(record)
    if inferred is None:
        yield "cannot infer taxon from classification entry"
    else:
        message = f"taxon should be {inferred}"
        if cfg.autofix and not LINT.is_ignoring_lint(record, "missing_taxon"):
            print(f"{record}: {message}")
            record.taxon = inferred
        else:
            yield message


@LINT.add("taxon_mapping")
def check_taxon_mapping(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    if record.taxon is None:
        return
    inferred = get_inferred_taxon(record)
    if inferred is None or record.taxon == inferred:
        return
    if record.has_tag(OccurrenceRecordTag.TaxonomicSplitFrom) or record.has_tag(
        OccurrenceRecordTag.CommentFromDatabase
    ):
        return
    yield f"taxon {record.taxon} differs from classification-entry mapping {inferred} without an explanation"


@LINT.add("missing_location")
def check_missing_location(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    if record.location is not None:
        return
    inferred = get_inferred_location(record)
    if inferred is None:
        yield "cannot infer location"
    else:
        message = f"location should be {inferred}"
        if cfg.autofix and not LINT.is_ignoring_lint(record, "missing_location"):
            print(f"{record}: {message}")
            record.location = inferred
            record.remove_tags(OccurrenceRecordTag.LocationHint)
        else:
            yield message


@LINT.add("location_hint")
def check_location_hint(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    if record.location is None:
        return
    hints = list(record.get_tags(record.tags, OccurrenceRecordTag.LocationHint))
    if not hints:
        return
    inferred = get_inferred_location(record)
    if inferred == record.location:
        message = "remove resolved LocationHint"
        if cfg.autofix and not LINT.is_ignoring_lint(record, "location_hint"):
            print(f"{record}: {message}")
            record.remove_tags(OccurrenceRecordTag.LocationHint)
        else:
            yield message


@LINT.add("location_mapping")
def check_location_mapping(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    if record.location is None:
        return
    hints = list(record.get_tags(record.tags, OccurrenceRecordTag.LocationHint))
    if not hints:
        return
    inferred = get_inferred_location(record)
    if inferred is not None and inferred != record.location:
        yield f"location {record.location} differs from LocationHint mapping {inferred}"


@LINT.add("basis_tags")
def check_basis_tags(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    if record.basis is not OccurrenceBasis.observation and record.has_tag(
        OccurrenceRecordTag.ObservationKind
    ):
        yield "ObservationKind requires observation basis"
    if record.basis is not OccurrenceBasis.voucher and record.has_tag(
        OccurrenceRecordTag.SpecimenDetail
    ):
        yield "SpecimenDetail requires voucher basis"


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
    message = f"add {tag} inferred from {source_tag}"
    if cfg.autofix:
        print(f"{record}: {message}")
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
                    f"cannot parse coordinates from {tag.text!r}; add Coordinates "
                    "manually"
                )
            continue
        expected = OccurrenceRecordTag.Coordinates(*parsed)
        if expected not in normalized:
            if normalized:
                yield (f"{tag} parses as {expected}, inconsistent with {normalized}")
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
                    f"cannot parse elevation from {tag.text!r}; add Elevation manually"
                )
            continue
        expected = OccurrenceRecordTag.Elevation(*parsed)
        if expected not in normalized:
            if normalized:
                yield (f"{tag} parses as {expected}, inconsistent with {normalized}")
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
                yield (f"cannot parse date from {tag.text!r}; add Date manually")
            continue
        expected = OccurrenceRecordTag.Date(parsed)
        if expected not in normalized:
            if normalized:
                yield (f"{tag} parses as {expected}, inconsistent with {normalized}")
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
            yield f"invalid normalized coordinates {tag}"
            continue
        expected = OccurrenceRecordTag.Coordinates(latitude, longitude)
        if tag != expected:
            message = f"replace {tag} with {expected}"
            if cfg.autofix:
                print(f"{record}: {message}")
                _replace_tag(record, tag, expected)
            else:
                yield message
    for tag in tuple(record.get_tags(record.tags, OccurrenceRecordTag.Elevation)):
        elevation_parsed = parse_verbatim_elevation(f"{tag.elevation} {tag.unit.name}")
        if elevation_parsed is None:
            yield f"invalid normalized elevation {tag}"
            continue
        expected = OccurrenceRecordTag.Elevation(*elevation_parsed)
        if tag != expected:
            message = f"replace {tag} with {expected}"
            if cfg.autofix:
                print(f"{record}: {message}")
                _replace_tag(record, tag, expected)
            else:
                yield message
    for tag in tuple(record.get_tags(record.tags, OccurrenceRecordTag.Date)):
        date_parsed = parse_verbatim_date(tag.date.removeprefix("<"))
        if date_parsed is None:
            yield f"invalid normalized date {tag}"
            continue
        if tag.date.startswith("<"):
            date_parsed = f"<{date_parsed}"
        expected = OccurrenceRecordTag.Date(date_parsed)
        if tag != expected:
            message = f"replace {tag} with {expected}"
            if cfg.autofix:
                print(f"{record}: {message}")
                _replace_tag(record, tag, expected)
            else:
                yield message


@LINT.add("source_data")
def check_source_data_tags(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    if LINT.is_ignoring_lint(record, "source_data"):
        cfg = replace(cfg, autofix=False, interactive=False)
    yield from _standardize_normalized_tags(record, cfg)
    for normalized_type, verbatim_type in (
        (OccurrenceRecordTag.Coordinates, OccurrenceRecordTag.VerbatimCoordinates),
        (OccurrenceRecordTag.Elevation, OccurrenceRecordTag.VerbatimElevation),
        (OccurrenceRecordTag.Date, OccurrenceRecordTag.VerbatimDate),
    ):
        if record.has_tag(normalized_type) and not record.has_tag(verbatim_type):
            yield (f"{normalized_type.__name__} requires {verbatim_type.__name__}")
    yield from _check_verbatim_coordinates(record, cfg)
    yield from _check_verbatim_elevations(record, cfg)
    yield from _check_verbatim_dates(record, cfg)


@LINT.add("coordinate_location")
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
            f"has source coordinates but Location {record.location} has no coordinates"
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
        distance = coordinate_lint.distance_km(occurrence_point, location_point)
        if distance > coordinate_lint.COORDINATE_TOLERANCE_KM:
            yield (
                f"source coordinates {tag.latitude}, {tag.longitude} are "
                f"{distance:.1f} km from Location {record.location} coordinates "
                f"{record.location.latitude}, {record.location.longitude}"
            )


@LINT.add("status_tags")
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
            yield (f"has duplicate {tag_type.__name__} for {status.name}")


@LINT.add("taxonomic_split")
def check_split(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    tags = list(record.get_tags(record.tags, OccurrenceRecordTag.TaxonomicSplitFrom))
    if len(tags) > 1:
        yield "has multiple TaxonomicSplitFrom tags"
        return
    if not tags:
        return
    canonical = tags[0].record
    if canonical == record:
        yield "TaxonomicSplitFrom points to itself"
        return
    if canonical.has_tag(OccurrenceRecordTag.TaxonomicSplitFrom):
        yield "TaxonomicSplitFrom points to a derived record"
    for field in ("classification_entry", "locality_text", "page", "basis"):
        if getattr(record, field) != getattr(canonical, field):
            yield (f"{field} differs from canonical split record {canonical}")
    if record.taxon is not None and record.taxon == canonical.taxon:
        yield f"split record has the same taxon as {canonical}"


@LINT.add("duplicate")
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
            yield f"duplicates primary record {candidate}"
