import datetime
import re
from collections.abc import Collection, Iterable
from dataclasses import replace
from typing import Protocol

from taxonomy.db import coordinate_lint, helpers, models
from taxonomy.db.constants import (
    AgeClass,
    AltitudeUnit,
    OccurrenceBasis,
    OccurrenceValidity,
    Rank,
)
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import (
    IgnoreLint,
    Lint,
    add_tag_fix,
    add_tag_issue,
    field_fix,
    field_issue,
    fixes_issue,
    remove_tag_fix,
    replace_tag_issue,
)
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.location import Location, LocationStatus
from taxonomy.db.models.location.age import (
    is_non_recent_location,
    is_pre_pleistocene_location,
    is_recent_location,
)
from taxonomy.db.models.taxon import Taxon

from .model import OccurrenceRecord, OccurrenceRecordStatus, OccurrenceRecordTag

_SIGNED_DECIMAL_COORDINATES = re.compile(
    r"^\s*(?P<latitude>[+-]?\d+(?:\.\d+)?)\s*[,;/]\s*"
    r"(?P<longitude>[+-]?\d+(?:\.\d+)?)\s*$"
)
_ELEVATION = re.compile(
    r"^\s*(?:elev(?:ation)?\.?|alt(?:itude)?\.?)?\s*"
    r"(?P<approximate>~|ca\.?|approximately)?\s*"
    r"(?P<elevation>-?\d[\d,]*(?:\.\d+)?(?:\s*[-–]\s*\d[\d,]*(?:\.\d+)?)?)"
    r"\s*(?P<unit>m|met(?:er|re)s?|ft|feet|foot)\.?\s*$",
    re.IGNORECASE,
)
_ISO_DATE = re.compile(r"^(?P<year>\d{4})(?:-(?P<month>\d{2})(?:-(?P<day>\d{2}))?)?$")
_RECENT_TAXON_AGES = frozenset({AgeClass.extant, AgeClass.recently_extinct})
_SOURCE_COORDINATE_REWRITE_TOLERANCE_KM = 0.001


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
    classification_entry = record.classification_entry
    mapped_name = classification_entry.mapped_name
    if mapped_name is None:
        # A source-faithful indeterminate species entry such as ``Sorex sp.``
        # cannot map to a species Name. It can, however, be assigned safely to the
        # mapped genus used as its parent in the same classification.
        parent = classification_entry.parent
        if (
            classification_entry.rank is Rank.species
            and re.fullmatch(r"[A-Z][A-Za-z-]+ sp\.?", classification_entry.name)
            and parent is not None
            and parent.rank is Rank.genus
            and parent.mapped_name is not None
        ):
            parent_taxon = parent.mapped_name.taxon.resolve_redirect()
            if parent_taxon.rank is Rank.genus:
                return parent_taxon
            return parent_taxon.parent_of_rank(Rank.genus)
        return None
    taxon = mapped_name.taxon.resolve_redirect()
    if (
        taxon.rank is Rank.subspecies
        and taxon.is_nominate_subspecies()
        and record.classification_entry.rank is not Rank.subspecies
    ):
        return taxon.parent_of_rank(Rank.species)
    return taxon


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
def check_missing_taxon(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[LintResult]:
    if record.taxon is not None:
        return
    inferred = get_inferred_taxon(record)
    if inferred is None:
        yield "cannot infer taxon from classification entry"
    else:
        message = f"taxon should be {inferred}"
        yield field_issue(message, record, "taxon", inferred)


@LINT.add("taxon_mapping")
def check_taxon_mapping(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[LintResult]:
    if record.taxon is None:
        return
    inferred = get_inferred_taxon(record)
    if inferred is None or record.taxon == inferred:
        return
    if record.has_tag(OccurrenceRecordTag.TaxonomicSplitFrom) or record.has_tag(
        OccurrenceRecordTag.CommentFromDatabase
    ):
        return
    if (
        record.taxon.rank is Rank.subspecies
        and record.taxon.is_nominate_subspecies()
        and record.taxon.parent_of_rank(Rank.species) == inferred
    ):
        message = f"change taxon from {record.taxon} to {inferred}"
        yield field_issue(message, record, "taxon", inferred)
        return
    yield f"taxon {record.taxon} differs from classification-entry mapping {inferred} without an explanation"


@LINT.add("missing_location")
def check_missing_location(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[LintResult]:
    if record.location is not None:
        return
    inferred = get_inferred_location(record)
    if inferred is None:
        yield "cannot infer location"
    else:
        message = f"location should be {inferred}"
        hints = tuple(
            tag
            for tag in record.tags
            if isinstance(tag, OccurrenceRecordTag.LocationHint)
        )
        yield fixes_issue(
            message,
            field_fix(record, "location", inferred),
            *(remove_tag_fix(record, tag) for tag in hints),
        )


@LINT.add("location_hint")
def check_location_hint(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[LintResult]:
    if record.location is None:
        return
    hints = list(record.get_tags(record.tags, OccurrenceRecordTag.LocationHint))
    if not hints:
        return
    inferred = get_inferred_location(record)
    if inferred == record.location:
        message = "remove resolved LocationHint"
        yield fixes_issue(message, *(remove_tag_fix(record, tag) for tag in hints))


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


@LINT.add("location_age")
def check_location_age(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    if record.location is None or record.taxon is None:
        return

    if is_recent_location(record.location):
        if record.taxon.age not in _RECENT_TAXON_AGES:
            yield (
                f"location {record.location} is Recent, but taxon {record.taxon} "
                f"has age {record.taxon.age.name.replace('_', ' ')}"
            )
        return

    if record.taxon.age not in _RECENT_TAXON_AGES or not is_non_recent_location(
        record.location
    ):
        return

    if record.basis is OccurrenceBasis.observation:
        yield (
            f"location {record.location} is non-Recent, but an observation of "
            f"{record.taxon} is expected to be Recent"
        )
    if is_pre_pleistocene_location(record.location):
        yield (
            f"location {record.location} is pre-Pleistocene, but the occurrence "
            f"is assigned to {record.taxon.age.name.replace('_', ' ')} taxon "
            f"{record.taxon}"
        )


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
    if match.group("approximate") is not None:
        elevation = f"~{elevation}"
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
    if "/" in text:
        parts = text.split("/")
        if len(parts) == 2 and all(_parse_iso_date(part) is not None for part in parts):
            return text
    if parsed := _parse_verbatim_date_range(text):
        return parsed
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


def _parse_date_endpoint(text: str, *, default_year: int | None = None) -> str | None:
    """Parse one endpoint of a source date interval."""
    text = text.strip()
    if default_year is not None and re.search(r"\b\d{4}\b", text) is None:
        text = f"{text} {default_year}"
    return parse_verbatim_date(text)


def _parse_verbatim_date_range(text: str) -> str | None:
    """Parse common prose date ranges as ISO 8601 intervals."""
    month_range = re.fullmatch(
        r"(?P<start>[A-Za-z]+)\s+to\s+(?P<end>[A-Za-z]+)\s+(?P<year>\d{4})",
        text,
        flags=re.IGNORECASE,
    )
    if month_range is not None:
        year = int(month_range.group("year"))
        start = _parse_date_endpoint(month_range.group("start"), default_year=year)
        end = _parse_date_endpoint(month_range.group("end"), default_year=year)
        if start is not None and end is not None:
            return f"{start}/{end}"

    between_range = re.fullmatch(
        r"between\s+(?P<start>.+?)\s+and\s+(?P<end>.+)", text, flags=re.IGNORECASE
    )
    if between_range is not None:
        end_text = between_range.group("end")
        year_match = re.search(r"\b(?P<year>\d{4})\b", end_text)
        default_year = int(year_match.group("year")) if year_match is not None else None
        start = _parse_date_endpoint(
            between_range.group("start"), default_year=default_year
        )
        end = _parse_date_endpoint(end_text)
        if start is not None and end is not None:
            return f"{start}/{end}"

    dash_range = re.fullmatch(r"(?P<start>.+?)[–—](?P<end>.+)", text)
    if dash_range is not None:
        end_text = dash_range.group("end")
        year_match = re.search(r"\b(?P<year>\d{4})\b", end_text)
        default_year = int(year_match.group("year")) if year_match is not None else None
        start = _parse_date_endpoint(
            dash_range.group("start"), default_year=default_year
        )
        end = _parse_date_endpoint(end_text)
        if start is not None and end is not None:
            return f"{start}/{end}"
    return None


def _add_inferred_tag(
    record: OccurrenceRecord,
    tag: OccurrenceRecordTag,
    source_tag: OccurrenceRecordTag,
    cfg: LintConfig,
) -> Iterable[LintResult]:
    message = f"add {tag} inferred from {source_tag}"
    yield add_tag_issue(message, record, tag)


def _check_verbatim_coordinates(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[LintResult]:
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
        if expected in normalized:
            continue
        if not normalized:
            yield from _add_inferred_tag(record, expected, tag, cfg)
            normalized.append(expected)
            continue

        expected_extent = coordinate_lint.make_extent(*parsed)
        if expected_extent is None:
            yield f"cannot parse normalized coordinates from {tag}"
            continue
        distances = []
        for index, normalized_tag in enumerate(normalized):
            normalized_extent = coordinate_lint.make_extent(
                normalized_tag.latitude, normalized_tag.longitude
            )
            if normalized_extent is not None:
                distances.append(
                    (
                        coordinate_lint.extent_distance_km(
                            expected_extent, normalized_extent
                        ),
                        index,
                        normalized_tag,
                        normalized_extent,
                    )
                )
        if not distances:
            yield f"{tag} parses as {expected}, inconsistent with {normalized}"
            continue
        distance, index, closest, closest_extent = min(
            distances, key=lambda item: item[0]
        )
        if distance > coordinate_lint.COORDINATE_TOLERANCE_KM:
            yield (
                f"{tag} parses as {expected}, {distance:.1f} km from closest "
                f"normalized coordinates {closest}"
            )
            continue
        if (
            distance < _SOURCE_COORDINATE_REWRITE_TOLERANCE_KM
            and expected_extent.point is not None
            and closest_extent.point is not None
        ):
            message = (
                f"replace {closest} with {expected} to preserve source coordinate "
                "format"
            )
            yield replace_tag_issue(message, record, closest, expected)
            normalized[index] = expected


def _check_verbatim_elevations(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[LintResult]:
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
        if expected in normalized:
            continue
        equivalent = next(
            (
                (index, normalized_tag)
                for index, normalized_tag in enumerate(normalized)
                if normalized_tag.unit is expected.unit
                and normalized_tag.elevation.removeprefix("~")
                == expected.elevation.removeprefix("~")
            ),
            None,
        )
        if equivalent is not None:
            index, normalized_tag = equivalent
            message = (
                f"replace {normalized_tag} with {expected} to preserve source "
                "elevation precision"
            )
            yield replace_tag_issue(message, record, normalized_tag, expected)
            normalized[index] = expected
        elif normalized:
            yield f"{tag} parses as {expected}, inconsistent with {normalized}"
        else:
            yield from _add_inferred_tag(record, expected, tag, cfg)
            normalized.append(expected)


def _check_verbatim_dates(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[LintResult]:
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
                if len(normalized) == 1 and _is_date_refinement(
                    expected.date, normalized[0].date
                ):
                    message = (
                        f"replace {normalized[0]} with {expected} to preserve source "
                        "date precision"
                    )
                    yield replace_tag_issue(message, record, normalized[0], expected)
                    normalized[0] = expected
                else:
                    yield f"{tag} parses as {expected}, inconsistent with {normalized}"
            else:
                yield from _add_inferred_tag(record, expected, tag, cfg)
                normalized.append(expected)


def _is_date_refinement(expected: str, existing: str) -> bool:
    """Return whether an exact source date refines one stored coarse date."""
    if expected == existing or "/" in existing:
        return False
    return all(part.startswith(existing) for part in expected.split("/"))


def _standardize_normalized_tags(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[LintResult]:
    for tag in tuple(record.get_tags(record.tags, OccurrenceRecordTag.Coordinates)):
        try:
            latitude, _ = coordinate_lint.standardize_coordinate_interval(
                tag.latitude, is_latitude=True
            )
            longitude, _ = coordinate_lint.standardize_coordinate_interval(
                tag.longitude, is_latitude=False
            )
        except helpers.InvalidCoordinates:
            yield f"invalid normalized coordinates {tag}"
            continue
        expected = OccurrenceRecordTag.Coordinates(latitude, longitude)
        if tag != expected:
            message = f"replace {tag} with {expected}"
            yield replace_tag_issue(message, record, tag, expected)
    for tag in tuple(record.get_tags(record.tags, OccurrenceRecordTag.Elevation)):
        elevation_parsed = parse_verbatim_elevation(f"{tag.elevation} {tag.unit.name}")
        if elevation_parsed is None:
            yield f"invalid normalized elevation {tag}"
            continue
        expected = OccurrenceRecordTag.Elevation(*elevation_parsed)
        if tag != expected:
            message = f"replace {tag} with {expected}"
            yield replace_tag_issue(message, record, tag, expected)
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
            yield replace_tag_issue(message, record, tag, expected)


@LINT.add("source_data")
def check_source_data_tags(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[LintResult]:
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
    location_extent = coordinate_lint.make_extent(
        record.location.latitude, record.location.longitude
    )
    if location_extent is None:
        return
    for tag in coordinate_tags:
        occurrence_extent = coordinate_lint.make_extent(tag.latitude, tag.longitude)
        if occurrence_extent is None:
            continue
        distance = coordinate_lint.extent_distance_km(
            occurrence_extent, location_extent
        )
        if distance > coordinate_lint.COORDINATE_TOLERANCE_KM:
            yield (
                f"source coordinates {tag.latitude}, {tag.longitude} are "
                f"{distance:.1f} km from Location {record.location} coordinates "
                f"{record.location.latitude}, {record.location.longitude}"
            )


def _yield_duplicate_values(
    record: OccurrenceRecord, tag_type: type[OccurrenceRecordTag], attribute: str
) -> Iterable[str]:
    values = [getattr(tag, attribute) for tag in record.get_tags(record.tags, tag_type)]
    duplicates = sorted(
        {value for value in values if values.count(value) > 1},
        key=lambda value: value.value,
    )
    for value in duplicates:
        yield f"has duplicate {tag_type.__name__} for {value.name}"


def _yield_mutually_exclusive_values(
    record: OccurrenceRecord, tag_type: type[OccurrenceRecordTag], attribute: str
) -> Iterable[str]:
    values = {getattr(tag, attribute) for tag in record.get_tags(record.tags, tag_type)}
    if len(values) > 1:
        yield (
            f"has conflicting {tag_type.__name__} values: "
            f"{', '.join(sorted(value.name for value in values))}"
        )


@LINT.add("status_tags")
def check_status_tags(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[str]:
    for tag_type in (
        OccurrenceRecordTag.ValidityFromSource,
        OccurrenceRecordTag.ValidityAssessment,
    ):
        yield from _yield_duplicate_values(record, tag_type, "validity")
        validities = {tag.validity for tag in record.get_tags(record.tags, tag_type)}
        if (
            OccurrenceValidity.valid in validities
            or OccurrenceValidity.rejected in validities
        ) and len(validities) > 1:
            yield (
                f"{tag_type.__name__} {', '.join(sorted(v.name for v in validities))} "
                "cannot be combined"
            )
    for tag_type, attribute in (
        (OccurrenceRecordTag.OriginFromSource, "origin"),
        (OccurrenceRecordTag.PresenceFromSource, "presence"),
    ):
        yield from _yield_duplicate_values(record, tag_type, attribute)
        yield from _yield_mutually_exclusive_values(record, tag_type, attribute)

    reviews = list(record.get_tags(record.tags, OccurrenceRecordTag.ReviewedInLightOf))
    review_keys = [(tag.article, tag.taxon) for tag in reviews]
    for article, taxon in {key for key in review_keys if review_keys.count(key) > 1}:
        yield f"has duplicate ReviewedInLightOf for {article} and {taxon}"
    if record.taxon is not None:
        for tag in reviews:
            if tag.taxon != record.taxon:
                yield (
                    f"ReviewedInLightOf for {tag.taxon} does not apply to current "
                    f"taxon {record.taxon}"
                )


class _CutoffYearTag(Protocol):
    cutoff_year: int | None
    source: models.Article


def _tag_cutoff_year(tag: _CutoffYearTag) -> int | None:
    if tag.cutoff_year is not None:
        return tag.cutoff_year
    return tag.source.valid_numeric_year()


def _record_source_year(record: OccurrenceRecord) -> int | None:
    return record.classification_entry.article.valid_numeric_year()


def _has_review(
    record: OccurrenceRecord, article: models.Article, taxon: Taxon
) -> bool:
    return any(
        tag.article == article and tag.taxon == taxon
        for tag in record.get_tags(record.tags, OccurrenceRecordTag.ReviewedInLightOf)
    )


@LINT.add("distribution_rules")
def check_distribution_rules(
    record: OccurrenceRecord, cfg: LintConfig
) -> Iterable[LintResult]:
    if record.taxon is None or record.location is None:
        return
    taxon = record.taxon
    region = record.location.region
    record_year = _record_source_year(record)
    redirect_tags = [
        tag
        for tag in taxon.get_tags(taxon.tags, models.tags.TaxonTag.RedirectOccurrences)
        if models.tags.is_region_within(region, tag.region)
        and not _has_review(record, tag.source, taxon)
    ]
    move_tags = [
        tag
        for tag in redirect_tags
        if (cutoff_year := _tag_cutoff_year(tag)) is not None
        and record_year is not None
        and record_year < cutoff_year
    ]
    if move_tags:
        targets = []
        for tag in move_tags:
            if tag.target not in targets:
                targets.append(tag.target)
        if len(targets) > 1:
            yield (
                "matches conflicting RedirectOccurrences rules with targets "
                f"{', '.join(sorted(str(target) for target in targets))}"
            )
            return
        target = targets[0]
        sources = ", ".join(sorted({str(tag.source) for tag in move_tags}))
        message = (
            f"taxon should be moved from {taxon} to {target} under "
            f"RedirectOccurrences ({sources})"
        )
        comment = (
            f"Reassigned from {taxon} to {target} under "
            f"RedirectOccurrences ({sources})."
        )
        yield fixes_issue(
            message,
            field_fix(record, "taxon", target),
            add_tag_fix(record, OccurrenceRecordTag.CommentFromDatabase(comment)),
        )
        return

    for tag in redirect_tags:
        if tag in move_tags:
            continue
        yield (
            f"occurrence in {region} requires ReviewedInLightOf({tag.source}, "
            f"{taxon}) because of RedirectOccurrences"
        )

    for tag in taxon.get_tags(taxon.tags, models.tags.TaxonTag.ReassessOccurrences):
        if not models.tags.is_region_within(region, tag.region):
            continue
        if _has_review(record, tag.source, taxon):
            continue
        cutoff_year = _tag_cutoff_year(tag)
        if (
            cutoff_year is not None
            and record_year is not None
            and record_year > cutoff_year
        ):
            continue
        yield (
            f"occurrence in {region} requires ReviewedInLightOf({tag.source}, "
            f"{taxon}) because of ReassessOccurrences"
        )


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
def check_duplicate(record: OccurrenceRecord, cfg: LintConfig) -> Iterable[LintResult]:
    if record.has_tag(OccurrenceRecordTag.TaxonomicSplitFrom):
        return
    candidates = OccurrenceRecord.select_valid().filter(
        OccurrenceRecord.classification_entry == record.classification_entry,
        OccurrenceRecord.locality_text == record.locality_text,
        OccurrenceRecord.page == record.page,
        OccurrenceRecord.basis == record.basis,
    )
    primary_candidates = [
        candidate
        for candidate in candidates
        if not candidate.has_tag(OccurrenceRecordTag.TaxonomicSplitFrom)
    ]
    if not primary_candidates:
        return
    primary = min(primary_candidates, key=lambda candidate: candidate.id)
    if primary.id == record.id:
        return

    message = f"OR#{record.id} duplicates primary record OR#{primary.id}: {primary}"
    fields_match = all(
        getattr(record, field) == getattr(primary, field)
        for field in OccurrenceRecord.fields()
    )
    if fields_match:
        yield fixes_issue(
            message,
            add_tag_fix(record, OccurrenceRecordTag.RedirectTarget(primary)),
            field_fix(record, "status", OccurrenceRecordStatus.alias),
        )
    else:
        yield message
