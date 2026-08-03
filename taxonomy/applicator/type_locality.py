"""Backend for type-locality recommendations.

The public review, validation, and application CLI lives in
``scripts/apply_recommendations.py``.
"""

import json
import textwrap
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db import coordinate_lint
from taxonomy.db.constants import DistributionOrigin, OccurrenceValidity
from taxonomy.db.models import (
    Article,
    Location,
    Name,
    Period,
    Region,
    StratigraphicUnit,
    Taxon,
    TypeTag,
)
from taxonomy.db.models.location import LocationStatus
from taxonomy.db.models.tags import LocationTag, TaxonTag

SCHEMA_VERSION = 3
SUPPORTED_SCHEMA_VERSIONS = {1, 2, 3}
GENERAL_LOCATION_TAG = "General"
UNPLACED_LOCATION_TAG = "Unplaced"
ALLOWED_LOCATION_TAGS = {GENERAL_LOCATION_TAG, UNPLACED_LOCATION_TAG}

ADD_IMPRECISE_LOCALITY = "add_imprecise_locality"
MOVE_EXISTING_LOCATION = "move_existing_location"
CREATE_LOCATION = "create_location"
MANUAL_REVIEW = "manual_review"
NO_ACTION = "no_action"
ALLOWED_ACTIONS = {
    ADD_IMPRECISE_LOCALITY,
    MOVE_EXISTING_LOCATION,
    CREATE_LOCATION,
    MANUAL_REVIEW,
    NO_ACTION,
}
ACTIONABLE_ACTIONS = {ADD_IMPRECISE_LOCALITY, MOVE_EXISTING_LOCATION, CREATE_LOCATION}


class RecommendationError(Exception):
    pass


class _IdentifiedLike(Protocol):
    @property
    def id(self) -> int: ...


class RegionLike(_IdentifiedLike, Protocol):
    name: str


class ArticleLike(_IdentifiedLike, Protocol):
    name: str


class NamedLike(_IdentifiedLike, Protocol):
    name: str


class TaxonLike(_IdentifiedLike, Protocol):
    tags: Sequence[TaxonTag] | None

    def add_tag(self, tag: TaxonTag) -> None: ...


class LocationLike(_IdentifiedLike, Protocol):
    name: str
    region: RegionLike
    latitude: str | None
    longitude: str | None
    min_period: NamedLike | None
    max_period: NamedLike | None
    min_age: int | None
    max_age: int | None
    stratigraphic_unit: NamedLike | None
    deleted: LocationStatus
    tags: Sequence[LocationTag] | None

    def is_invalid(self) -> bool: ...

    def has_tag(self, tag_type: Any) -> bool: ...

    def add_tag(self, tag: LocationTag) -> None: ...


class NameLike(_IdentifiedLike, Protocol):
    type_locality: LocationLike | None
    type_tags: Sequence[TypeTag] | None
    taxon: TaxonLike

    def add_type_tag(self, tag: TypeTag) -> None: ...


@dataclass(frozen=True, slots=True)
class Evidence:
    source_id: int
    source_name: str
    text: str


@dataclass(frozen=True, slots=True)
class Target:
    location_id: int | None
    location_name: str
    region_id: int
    region_name: str
    latitude: str | None
    longitude: str | None
    coordinate_source: str | None
    coordinate_note: str | None
    location_tags: tuple[LocationTagSpec, ...]
    min_period_id: int | None = None
    min_period_name: str | None = None
    max_period_id: int | None = None
    max_period_name: str | None = None
    min_age: int | None = None
    max_age: int | None = None
    stratigraphic_unit_id: int | None = None
    stratigraphic_unit_name: str | None = None
    serialized_location_tags: tuple[LocationTag, ...] = ()


@dataclass(frozen=True, slots=True)
class LocationTagSpec:
    tag: str
    comment: str | None


@dataclass(frozen=True, slots=True)
class TypeLocalityValiditySpec:
    validity: OccurrenceValidity
    comment: str | None


@dataclass(frozen=True, slots=True)
class RegionalOriginSpec:
    region_id: int
    region_name: str
    origin: DistributionOrigin
    source_id: int
    source_name: str
    comment: str | None


@dataclass(frozen=True, slots=True)
class Recommendation:
    line_number: int
    name_id: int
    name: str
    current_location_id: int
    current_location_name: str
    action: str
    confidence: str
    reason: str
    review_note: str | None
    tag_comment: str | None
    current_location_tags: tuple[LocationTagSpec, ...]
    target: Target | None
    evidence: tuple[Evidence, ...]
    type_locality_validity: TypeLocalityValiditySpec | None
    regional_origins: tuple[RegionalOriginSpec, ...]


@dataclass(frozen=True, slots=True)
class NewLocationDefinition:
    target: Target
    region: RegionLike
    min_period: NamedLike | None
    max_period: NamedLike | None
    stratigraphic_unit: NamedLike | None


@dataclass(frozen=True, slots=True)
class PlannedUpdate:
    recommendation: Recommendation
    name: NameLike
    target: LocationLike | None
    new_location_name: str | None
    already_applied: bool


@dataclass(frozen=True, slots=True)
class PlannedLocationTagUpdate:
    location: LocationLike
    tags: tuple[LocationTagSpec, ...]


@dataclass(frozen=True, slots=True)
class PlannedSerializedLocationTagUpdate:
    location: LocationLike
    tags: tuple[LocationTag, ...]


@dataclass(frozen=True, slots=True)
class PlannedTypeLocalityValidityUpdate:
    recommendation: Recommendation
    name: NameLike
    spec: TypeLocalityValiditySpec
    already_applied: bool


@dataclass(frozen=True, slots=True)
class PlannedRegionalOriginUpdate:
    recommendation: Recommendation
    taxon: TaxonLike
    region: RegionLike
    source: ArticleLike
    spec: RegionalOriginSpec
    already_applied: bool


@dataclass(frozen=True, slots=True)
class RecommendationPlan:
    updates: tuple[PlannedUpdate, ...]
    new_locations: tuple[NewLocationDefinition, ...]
    location_tag_updates: tuple[PlannedLocationTagUpdate, ...]
    serialized_location_tag_updates: tuple[PlannedSerializedLocationTagUpdate, ...]
    type_locality_validity_updates: tuple[PlannedTypeLocalityValidityUpdate, ...]
    regional_origin_updates: tuple[PlannedRegionalOriginUpdate, ...]
    action_counts: Counter[str]


def _required_value(data: dict[str, Any], key: str, line_number: int) -> Any:
    try:
        value = data[key]
    except KeyError as exc:
        raise RecommendationError(f"line {line_number}: missing {key!r}") from exc
    if value is None or value == "":
        raise RecommendationError(f"line {line_number}: {key!r} is required")
    return value


def _required_str(data: dict[str, Any], key: str, line_number: int) -> str:
    value = _required_value(data, key, line_number)
    if not isinstance(value, str):
        raise RecommendationError(f"line {line_number}: {key!r} must be a string")
    return value


def _string_allow_empty(data: dict[str, Any], key: str, line_number: int) -> str:
    if key not in data:
        raise RecommendationError(f"line {line_number}: missing {key!r}")
    value = data[key]
    if not isinstance(value, str):
        raise RecommendationError(f"line {line_number}: {key!r} must be a string")
    return value


def _required_int(data: dict[str, Any], key: str, line_number: int) -> int:
    value = _required_value(data, key, line_number)
    if not isinstance(value, int) or isinstance(value, bool):
        raise RecommendationError(f"line {line_number}: {key!r} must be an integer")
    return value


def _optional_str(data: dict[str, Any], key: str, line_number: int) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise RecommendationError(
            f"line {line_number}: {key!r} must be a nonempty string or null"
        )
    return value


def _optional_int(data: dict[str, Any], key: str, line_number: int) -> int | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise RecommendationError(
            f"line {line_number}: {key!r} must be an integer or null"
        )
    return value


def _parse_location_tags(
    data: Any, line_number: int, *, required: bool
) -> tuple[LocationTagSpec, ...]:
    if data is None and not required:
        return ()
    if not isinstance(data, list):
        raise RecommendationError(f"line {line_number}: location tags must be a list")
    output: list[LocationTagSpec] = []
    seen: set[str] = set()
    for item in data:
        if not isinstance(item, dict):
            raise RecommendationError(
                f"line {line_number}: each location tag must be an object"
            )
        tag = _required_str(item, "tag", line_number)
        if tag not in ALLOWED_LOCATION_TAGS:
            raise RecommendationError(
                f"line {line_number}: unsupported Location tag {tag!r}"
            )
        if tag in seen:
            raise RecommendationError(
                f"line {line_number}: duplicate Location tag {tag!r}"
            )
        seen.add(tag)
        comment = _optional_str(item, "comment", line_number)
        if tag == GENERAL_LOCATION_TAG and comment is not None:
            raise RecommendationError(
                f"line {line_number}: General Location tag must not have a comment"
            )
        output.append(LocationTagSpec(tag, comment))
    return tuple(output)


def _parse_serialized_location_tags(
    data: Any, line_number: int, *, required: bool
) -> tuple[LocationTag, ...]:
    if data is None and not required:
        return ()
    if not isinstance(data, list):
        raise RecommendationError(
            f"line {line_number}: serialized_location_tags must be a list"
        )
    output: list[LocationTag] = []
    for serialized in data:
        if not isinstance(serialized, list):
            raise RecommendationError(
                f"line {line_number}: each serialized Location tag must be a list"
            )
        try:
            tag = LocationTag.unserialize(serialized)
        except Exception as exc:
            raise RecommendationError(
                f"line {line_number}: invalid serialized Location tag "
                f"{serialized!r}"
            ) from exc
        if tag in output:
            raise RecommendationError(
                f"line {line_number}: duplicate serialized Location tag "
                f"{serialized!r}"
            )
        output.append(tag)
    return tuple(output)


def _schema_three_optional_int(
    data: dict[str, Any], key: str, line_number: int, schema_version: int
) -> int | None:
    if schema_version < 3:
        return None
    if key not in data:
        raise RecommendationError(f"line {line_number}: missing {key!r}")
    return _optional_int(data, key, line_number)


def _schema_three_optional_str(
    data: dict[str, Any], key: str, line_number: int, schema_version: int
) -> str | None:
    if schema_version < 3:
        return None
    if key not in data:
        raise RecommendationError(f"line {line_number}: missing {key!r}")
    return _optional_str(data, key, line_number)


def _parse_target(
    data: Any, line_number: int, action: str, schema_version: int
) -> Target | None:
    if data is None:
        if action in {MOVE_EXISTING_LOCATION, CREATE_LOCATION}:
            raise RecommendationError(
                f"line {line_number}: action {action!r} requires a target"
            )
        return None
    if action not in {MOVE_EXISTING_LOCATION, CREATE_LOCATION}:
        raise RecommendationError(
            f"line {line_number}: action {action!r} must not have a target"
        )
    if not isinstance(data, dict):
        raise RecommendationError(f"line {line_number}: target must be an object")
    location_id = _optional_int(data, "location_id", line_number)
    if action == MOVE_EXISTING_LOCATION and location_id is None:
        raise RecommendationError(
            f"line {line_number}: move_existing_location requires target.location_id"
        )
    if action == CREATE_LOCATION and location_id is not None:
        raise RecommendationError(
            f"line {line_number}: create_location must not set target.location_id"
        )
    latitude = _optional_str(data, "latitude", line_number)
    longitude = _optional_str(data, "longitude", line_number)
    if (latitude is None) != (longitude is None):
        raise RecommendationError(
            f"line {line_number}: target coordinates require both axes"
        )
    if latitude is not None and longitude is not None:
        standardized = coordinate_lint.standardize_coordinate_pair(latitude, longitude)
        if standardized is None:
            raise RecommendationError(
                f"line {line_number}: invalid target coordinates {latitude}, {longitude}"
            )
        latitude, longitude = standardized[:2]
    min_period_id = _schema_three_optional_int(
        data, "min_period_id", line_number, schema_version
    )
    min_period_name = _schema_three_optional_str(
        data, "min_period_name", line_number, schema_version
    )
    max_period_id = _schema_three_optional_int(
        data, "max_period_id", line_number, schema_version
    )
    max_period_name = _schema_three_optional_str(
        data, "max_period_name", line_number, schema_version
    )
    stratigraphic_unit_id = _schema_three_optional_int(
        data, "stratigraphic_unit_id", line_number, schema_version
    )
    stratigraphic_unit_name = _schema_three_optional_str(
        data, "stratigraphic_unit_name", line_number, schema_version
    )
    for field, object_id, name in (
        ("min_period", min_period_id, min_period_name),
        ("max_period", max_period_id, max_period_name),
        ("stratigraphic_unit", stratigraphic_unit_id, stratigraphic_unit_name),
    ):
        if (object_id is None) != (name is None):
            raise RecommendationError(
                f"line {line_number}: {field} ID and name must both be set or null"
            )
    return Target(
        location_id=location_id,
        location_name=_required_str(data, "location_name", line_number),
        region_id=_required_int(data, "region_id", line_number),
        region_name=_required_str(data, "region_name", line_number),
        latitude=latitude,
        longitude=longitude,
        coordinate_source=_optional_str(data, "coordinate_source", line_number),
        coordinate_note=_optional_str(data, "coordinate_note", line_number),
        location_tags=_parse_location_tags(
            data.get("location_tags"), line_number, required=schema_version >= 2
        ),
        min_period_id=min_period_id,
        min_period_name=min_period_name,
        max_period_id=max_period_id,
        max_period_name=max_period_name,
        min_age=_schema_three_optional_int(
            data, "min_age", line_number, schema_version
        ),
        max_age=_schema_three_optional_int(
            data, "max_age", line_number, schema_version
        ),
        stratigraphic_unit_id=stratigraphic_unit_id,
        stratigraphic_unit_name=stratigraphic_unit_name,
        serialized_location_tags=_parse_serialized_location_tags(
            data.get("serialized_location_tags"),
            line_number,
            required=schema_version >= 3,
        ),
    )


def _parse_evidence(data: Any, line_number: int) -> tuple[Evidence, ...]:
    if not isinstance(data, list):
        raise RecommendationError(f"line {line_number}: evidence must be a list")
    output: list[Evidence] = []
    for item in data:
        if not isinstance(item, dict):
            raise RecommendationError(
                f"line {line_number}: each evidence item must be an object"
            )
        output.append(
            Evidence(
                source_id=_required_int(item, "source_id", line_number),
                source_name=_required_str(item, "source_name", line_number),
                text=_string_allow_empty(item, "text", line_number),
            )
        )
    return tuple(output)


def _parse_type_locality_validity(
    data: Any, line_number: int
) -> TypeLocalityValiditySpec | None:
    if data is None:
        return None
    if not isinstance(data, dict):
        raise RecommendationError(
            f"line {line_number}: type_locality_validity must be an object"
        )
    validity_name = _required_str(data, "validity", line_number)
    try:
        validity = OccurrenceValidity[validity_name]
    except KeyError as exc:
        raise RecommendationError(
            f"line {line_number}: unknown OccurrenceValidity {validity_name!r}"
        ) from exc
    if validity not in {
        OccurrenceValidity.occurrence_dubious,
        OccurrenceValidity.classification_dubious,
        OccurrenceValidity.incidental,
    }:
        raise RecommendationError(
            f"line {line_number}: unsupported TypeLocalityValidity {validity_name!r}"
        )
    return TypeLocalityValiditySpec(
        validity, _optional_str(data, "comment", line_number)
    )


def _parse_regional_origins(
    data: Any, line_number: int
) -> tuple[RegionalOriginSpec, ...]:
    if data is None:
        return ()
    if not isinstance(data, list):
        raise RecommendationError(
            f"line {line_number}: regional_origins must be a list"
        )
    output: list[RegionalOriginSpec] = []
    seen_regions: set[int] = set()
    for item in data:
        if not isinstance(item, dict):
            raise RecommendationError(
                f"line {line_number}: each regional origin must be an object"
            )
        region_id = _required_int(item, "region_id", line_number)
        if region_id in seen_regions:
            raise RecommendationError(
                f"line {line_number}: duplicate RegionalOrigin Region {region_id}"
            )
        seen_regions.add(region_id)
        origin_name = _required_str(item, "origin", line_number)
        try:
            origin = DistributionOrigin[origin_name]
        except KeyError as exc:
            raise RecommendationError(
                f"line {line_number}: unknown DistributionOrigin {origin_name!r}"
            ) from exc
        output.append(
            RegionalOriginSpec(
                region_id=region_id,
                region_name=_required_str(item, "region_name", line_number),
                origin=origin,
                source_id=_required_int(item, "source_id", line_number),
                source_name=_required_str(item, "source_name", line_number),
                comment=_optional_str(item, "comment", line_number),
            )
        )
    return tuple(output)


def parse_recommendation(data: Any, line_number: int) -> Recommendation:
    if not isinstance(data, dict):
        raise RecommendationError(f"line {line_number}: row must be an object")
    schema_version = _required_int(data, "schema_version", line_number)
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        raise RecommendationError(
            f"line {line_number}: unsupported schema_version {schema_version}"
        )
    action = _required_str(data, "action", line_number)
    if action not in ALLOWED_ACTIONS:
        raise RecommendationError(f"line {line_number}: invalid action {action!r}")
    target = _parse_target(data.get("target"), line_number, action, schema_version)
    tag_comment = _optional_str(data, "tag_comment", line_number)
    if action != ADD_IMPRECISE_LOCALITY and tag_comment is not None:
        raise RecommendationError(
            f"line {line_number}: tag_comment is only valid for add_imprecise_locality"
        )
    return Recommendation(
        line_number=line_number,
        name_id=_required_int(data, "name_id", line_number),
        name=_required_str(data, "name", line_number),
        current_location_id=_required_int(data, "current_location_id", line_number),
        current_location_name=_required_str(data, "current_location_name", line_number),
        action=action,
        confidence=_required_str(data, "confidence", line_number),
        reason=_required_str(data, "reason", line_number),
        review_note=_optional_str(data, "review_note", line_number),
        tag_comment=tag_comment,
        current_location_tags=_parse_location_tags(
            data.get("current_location_tags"), line_number, required=schema_version >= 2
        ),
        target=target,
        evidence=_parse_evidence(data.get("evidence"), line_number),
        type_locality_validity=_parse_type_locality_validity(
            data.get("type_locality_validity"), line_number
        ),
        regional_origins=_parse_regional_origins(
            data.get("regional_origins"), line_number
        ),
    )


def read_recommendations(path: Path) -> list[Recommendation]:
    recommendations: list[Recommendation] = []
    seen_ids: set[int] = set()
    try:
        lines = path.read_text().splitlines()
    except OSError as exc:
        raise RecommendationError(f"could not read {path}: {exc}") from exc
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RecommendationError(
                f"line {line_number}: invalid JSON: {exc.msg}"
            ) from exc
        recommendation = parse_recommendation(data, line_number)
        if recommendation.name_id in seen_ids:
            raise RecommendationError(
                f"line {line_number}: duplicate name_id {recommendation.name_id}"
            )
        seen_ids.add(recommendation.name_id)
        recommendations.append(recommendation)
    if not recommendations:
        raise RecommendationError("recommendation file is empty")
    return recommendations


def _name_label(name: Name) -> str:
    return name.corrected_original_name or name.original_name or name.root_name


def _get_name(name_id: int) -> NameLike:
    try:
        return cast(NameLike, Name.get(id=name_id))
    except Name.DoesNotExist as exc:
        raise RecommendationError(f"Name {name_id} no longer exists") from exc


def _get_location(location_id: int) -> LocationLike:
    try:
        return cast(LocationLike, Location.get(id=location_id))
    except Location.DoesNotExist as exc:
        raise RecommendationError(f"Location {location_id} no longer exists") from exc


def _get_region(region_id: int) -> RegionLike:
    try:
        return cast(RegionLike, Region.get(id=region_id))
    except Region.DoesNotExist as exc:
        raise RecommendationError(f"Region {region_id} no longer exists") from exc


def _get_article(article_id: int) -> ArticleLike:
    try:
        return cast(ArticleLike, Article.get(id=article_id))
    except Article.DoesNotExist as exc:
        raise RecommendationError(f"Article {article_id} no longer exists") from exc


def _get_period(period_id: int) -> NamedLike:
    try:
        return cast(NamedLike, Period.get(id=period_id))
    except Period.DoesNotExist as exc:
        raise RecommendationError(f"Period {period_id} no longer exists") from exc


def _get_stratigraphic_unit(unit_id: int) -> NamedLike:
    try:
        return cast(NamedLike, StratigraphicUnit.get(id=unit_id))
    except StratigraphicUnit.DoesNotExist as exc:
        raise RecommendationError(
            f"StratigraphicUnit {unit_id} no longer exists"
        ) from exc


def _find_location(name: str) -> LocationLike | None:
    matches = list(Location.select().filter(Location.name == name))
    if not matches:
        return None
    if len(matches) > 1:
        raise RecommendationError(f"multiple Locations are named {name!r}")
    return cast(LocationLike, matches[0])


def _has_imprecise_tag(name: NameLike) -> bool:
    return any(
        isinstance(tag, TypeTag.ImpreciseLocality) for tag in name.type_tags or ()
    )


def _has_type_locality_validity_tag(
    name: NameLike, spec: TypeLocalityValiditySpec
) -> bool:
    return any(
        isinstance(tag, TypeTag.TypeLocalityValidity)
        and tag.validity is spec.validity
        and tag.comment == spec.comment
        for tag in name.type_tags or ()
    )


def _has_regional_origin_tag(
    taxon: TaxonLike, spec: RegionalOriginSpec, region: RegionLike, source: ArticleLike
) -> bool:
    return any(
        isinstance(tag, TaxonTag.RegionalOrigin)
        and tag.region == region
        and tag.origin is spec.origin
        and tag.source == source
        and tag.comment == spec.comment
        for tag in taxon.tags or ()
    )


def _has_location_tag(location: LocationLike, spec: LocationTagSpec) -> bool:
    if spec.tag == GENERAL_LOCATION_TAG:
        return location.has_tag(LocationTag.General)
    assert spec.tag == UNPLACED_LOCATION_TAG
    return location.has_tag(LocationTag.Unplaced)


def _make_location_tag(spec: LocationTagSpec) -> LocationTag:
    if spec.tag == GENERAL_LOCATION_TAG:
        return cast(LocationTag, LocationTag.General)
    assert spec.tag == UNPLACED_LOCATION_TAG
    if spec.comment is None:
        return cast(LocationTag, LocationTag.Unplaced())
    return cast(LocationTag, LocationTag.Unplaced(comment=spec.comment))


def _validate_target_location(target: Target, location: LocationLike) -> None:
    if location.name != target.location_name:
        raise RecommendationError(
            f"Location {location.id} name changed from {target.location_name!r} "
            f"to {location.name!r}"
        )
    if (
        location.region.id != target.region_id
        or location.region.name != target.region_name
    ):
        raise RecommendationError(
            f"Location {location.id} Region does not match "
            f"{target.region_name!r} ({target.region_id})"
        )
    if location.is_invalid():
        raise RecommendationError(f"Location {location.id} is invalid")
    if target.latitude is not None and target.longitude is not None:
        if location.latitude is None and location.longitude is None:
            raise RecommendationError(
                f"Location {location.id} is missing coordinates required by the manifest"
            )
        if location.latitude is None or location.longitude is None:
            raise RecommendationError(
                f"Location {location.id} has only one coordinate axis set"
            )
        existing = coordinate_lint.standardize_coordinate_pair(
            location.latitude, location.longitude
        )
        if existing is None or existing[:2] != (target.latitude, target.longitude):
            raise RecommendationError(
                f"Location {location.id} has coordinates that differ from the manifest"
            )
    for field, expected_id, expected_name in (
        ("min_period", target.min_period_id, target.min_period_name),
        ("max_period", target.max_period_id, target.max_period_name),
        (
            "stratigraphic_unit",
            target.stratigraphic_unit_id,
            target.stratigraphic_unit_name,
        ),
    ):
        if expected_id is None and expected_name is None:
            continue
        actual = cast(NamedLike | None, getattr(location, field))
        if actual is None or actual.id != expected_id or actual.name != expected_name:
            raise RecommendationError(
                f"Location {location.id} {field} differs from the manifest"
            )
    if target.min_age is not None and location.min_age != target.min_age:
        raise RecommendationError(
            f"Location {location.id} min_age differs from the manifest"
        )
    if target.max_age is not None and location.max_age != target.max_age:
        raise RecommendationError(
            f"Location {location.id} max_age differs from the manifest"
        )


def build_plan(
    recommendations: Iterable[Recommendation],
    *,
    get_name: Callable[[int], NameLike] = _get_name,
    get_location: Callable[[int], LocationLike] = _get_location,
    get_region: Callable[[int], RegionLike] = _get_region,
    get_article: Callable[[int], ArticleLike] = _get_article,
    get_period: Callable[[int], NamedLike] = _get_period,
    get_stratigraphic_unit: Callable[[int], NamedLike] = _get_stratigraphic_unit,
    find_location: Callable[[str], LocationLike | None] = _find_location,
    label_name: Callable[[NameLike], str] | None = None,
) -> RecommendationPlan:
    rows = list(recommendations)
    action_counts = Counter(row.action for row in rows)
    updates: list[PlannedUpdate] = []
    new_definitions: dict[str, NewLocationDefinition] = {}
    location_tag_specs: dict[int, tuple[LocationLike, dict[str, LocationTagSpec]]] = {}
    serialized_location_tags: dict[int, tuple[LocationLike, set[LocationTag]]] = {}
    type_locality_validity_updates: list[PlannedTypeLocalityValidityUpdate] = []
    regional_origin_updates: list[PlannedRegionalOriginUpdate] = []
    planned_regional_origins: set[
        tuple[int, int, DistributionOrigin, int, str | None]
    ] = set()
    errors: list[str] = []
    label_name = label_name or (lambda name: _name_label(cast(Name, name)))

    def add_location_tags(
        location: LocationLike, specs: Sequence[LocationTagSpec], *, context: str
    ) -> None:
        if not specs:
            return
        _, by_tag = location_tag_specs.setdefault(location.id, (location, {}))
        for spec in specs:
            previous = by_tag.get(spec.tag)
            if previous is not None and previous != spec:
                raise RecommendationError(
                    f"inconsistent {spec.tag} tag definitions for Location "
                    f"{location.id} ({context})"
                )
            by_tag[spec.tag] = spec

    def add_serialized_location_tags(
        location: LocationLike, tags: Sequence[LocationTag]
    ) -> None:
        if not tags:
            return
        _, planned = serialized_location_tags.setdefault(location.id, (location, set()))
        planned.update(tags)

    for row in rows:
        try:
            current = get_location(row.current_location_id)
            if current.name != row.current_location_name:
                raise RecommendationError(
                    f"current Location {current.id} changed from "
                    f"{row.current_location_name!r} to {current.name!r}"
                )
            add_location_tags(
                current, row.current_location_tags, context=f"line {row.line_number}"
            )
            has_supplemental_action = row.type_locality_validity is not None or bool(
                row.regional_origins
            )
            if row.action not in ACTIONABLE_ACTIONS and not has_supplemental_action:
                continue

            name = get_name(row.name_id)
            actual_name = label_name(name)
            if actual_name != row.name:
                raise RecommendationError(
                    f"Name {row.name_id} changed from {row.name!r} to {actual_name!r}"
                )
            if row.type_locality_validity is not None:
                type_locality_validity_updates.append(
                    PlannedTypeLocalityValidityUpdate(
                        row,
                        name,
                        row.type_locality_validity,
                        _has_type_locality_validity_tag(
                            name, row.type_locality_validity
                        ),
                    )
                )
            for spec in row.regional_origins:
                region = get_region(spec.region_id)
                if region.name != spec.region_name:
                    raise RecommendationError(
                        f"Region {region.id} changed from {spec.region_name!r} "
                        f"to {region.name!r}"
                    )
                source = get_article(spec.source_id)
                if source.name != spec.source_name:
                    raise RecommendationError(
                        f"Article {source.id} changed from {spec.source_name!r} "
                        f"to {source.name!r}"
                    )
                regional_origin_key = (
                    name.taxon.id,
                    region.id,
                    spec.origin,
                    source.id,
                    spec.comment,
                )
                if regional_origin_key in planned_regional_origins:
                    continue
                planned_regional_origins.add(regional_origin_key)
                regional_origin_updates.append(
                    PlannedRegionalOriginUpdate(
                        row,
                        name.taxon,
                        region,
                        source,
                        spec,
                        _has_regional_origin_tag(name.taxon, spec, region, source),
                    )
                )
            if row.action not in ACTIONABLE_ACTIONS:
                continue
            if row.action == ADD_IMPRECISE_LOCALITY:
                if name.type_locality is None or name.type_locality.id != current.id:
                    raise RecommendationError(
                        f"Name {name.id} is no longer assigned to Location {current.id}"
                    )
                updates.append(
                    PlannedUpdate(row, name, None, None, _has_imprecise_tag(name))
                )
                continue

            assert row.target is not None
            target_location: LocationLike | None
            new_location_name: str | None = None
            if row.action == MOVE_EXISTING_LOCATION:
                assert row.target.location_id is not None
                target_location = get_location(row.target.location_id)
                _validate_target_location(row.target, target_location)
                add_location_tags(
                    target_location,
                    row.target.location_tags,
                    context=f"line {row.line_number}",
                )
                add_serialized_location_tags(
                    target_location, row.target.serialized_location_tags
                )
            else:
                region = get_region(row.target.region_id)
                if region.name != row.target.region_name:
                    raise RecommendationError(
                        f"Region {region.id} changed from {row.target.region_name!r} "
                        f"to {region.name!r}"
                    )
                target_location = find_location(row.target.location_name)
                if target_location is not None:
                    _validate_target_location(row.target, target_location)
                    add_location_tags(
                        target_location,
                        row.target.location_tags,
                        context=f"line {row.line_number}",
                    )
                    add_serialized_location_tags(
                        target_location, row.target.serialized_location_tags
                    )
                else:
                    min_period = (
                        get_period(row.target.min_period_id)
                        if row.target.min_period_id is not None
                        else None
                    )
                    max_period = (
                        get_period(row.target.max_period_id)
                        if row.target.max_period_id is not None
                        else None
                    )
                    stratigraphic_unit = (
                        get_stratigraphic_unit(row.target.stratigraphic_unit_id)
                        if row.target.stratigraphic_unit_id is not None
                        else None
                    )
                    for actual, expected_name, label in (
                        (min_period, row.target.min_period_name, "minimum period"),
                        (max_period, row.target.max_period_name, "maximum period"),
                        (
                            stratigraphic_unit,
                            row.target.stratigraphic_unit_name,
                            "stratigraphic unit",
                        ),
                    ):
                        if actual is not None and actual.name != expected_name:
                            raise RecommendationError(
                                f"{label} {actual.id} changed from {expected_name!r} "
                                f"to {actual.name!r}"
                            )
                    definition = NewLocationDefinition(
                        row.target, region, min_period, max_period, stratigraphic_unit
                    )
                    previous = new_definitions.get(row.target.location_name)
                    if previous is not None and previous.target != row.target:
                        raise RecommendationError(
                            f"inconsistent definitions for new Location "
                            f"{row.target.location_name!r}"
                        )
                    new_definitions[row.target.location_name] = definition
                    new_location_name = row.target.location_name

            already_applied = (
                target_location is not None
                and name.type_locality is not None
                and name.type_locality.id == target_location.id
            )
            if not already_applied and (
                name.type_locality is None or name.type_locality.id != current.id
            ):
                raise RecommendationError(
                    f"Name {name.id} is neither at current Location {current.id} "
                    "nor at its recommended target"
                )
            updates.append(
                PlannedUpdate(
                    row, name, target_location, new_location_name, already_applied
                )
            )
        except RecommendationError as exc:
            errors.append(f"line {row.line_number}: {exc}")
    if errors:
        raise RecommendationError(
            "refusing to continue because database validation failed:\n- "
            + "\n- ".join(errors)
        )
    return RecommendationPlan(
        tuple(updates),
        tuple(
            sorted(new_definitions.values(), key=lambda item: item.target.location_name)
        ),
        tuple(
            PlannedLocationTagUpdate(
                location,
                tuple(
                    spec
                    for spec in by_tag.values()
                    if not _has_location_tag(location, spec)
                ),
            )
            for location, by_tag in sorted(
                location_tag_specs.values(), key=lambda item: item[0].id
            )
            if any(not _has_location_tag(location, spec) for spec in by_tag.values())
        ),
        tuple(
            PlannedSerializedLocationTagUpdate(
                location,
                tuple(
                    sorted(
                        (tag for tag in tags if tag not in tuple(location.tags or ())),
                        key=repr,
                    )
                ),
            )
            for location, tags in sorted(
                serialized_location_tags.values(), key=lambda item: item[0].id
            )
            if any(tag not in tuple(location.tags or ()) for tag in tags)
        ),
        tuple(type_locality_validity_updates),
        tuple(regional_origin_updates),
        action_counts,
    )


def _make_imprecise_tag(comment: str | None) -> TypeTag:
    if comment is None:
        return TypeTag.ImpreciseLocality()
    return TypeTag.ImpreciseLocality(comment=comment)


def _make_type_locality_validity_tag(spec: TypeLocalityValiditySpec) -> TypeTag:
    if spec.comment is None:
        return TypeTag.TypeLocalityValidity(spec.validity)
    return TypeTag.TypeLocalityValidity(spec.validity, comment=spec.comment)


def _make_regional_origin_tag(
    spec: RegionalOriginSpec, region: RegionLike, source: ArticleLike
) -> TaxonTag:
    if spec.comment is None:
        return TaxonTag.RegionalOrigin(
            cast(Region, region), spec.origin, cast(Article, source)
        )
    return TaxonTag.RegionalOrigin(
        cast(Region, region), spec.origin, cast(Article, source), comment=spec.comment
    )


def _coordinate_comment(target: Target) -> str | None:
    if target.coordinate_source is None:
        return None
    comment = f"Coordinates from {target.coordinate_source}."
    if target.coordinate_note is not None:
        comment += f" {target.coordinate_note}"
    return comment


def _review_target(row: Recommendation) -> str:
    if row.action == ADD_IMPRECISE_LOCALITY:
        description = f"ImpreciseLocality @ {row.current_location_name}"
    elif row.target is None:
        description = "-"
    else:
        target = row.target
        if target.location_id is not None:
            description = f"{target.location_name} [existing #{target.location_id}]"
        else:
            description = f"{target.location_name} [new in {target.region_name}]"
        if target.latitude is not None and target.longitude is not None:
            description += f" {target.latitude}, {target.longitude}"
        if target.location_tags:
            description += " tags=" + ",".join(
                spec.tag for spec in target.location_tags
            )
        if target.serialized_location_tags:
            description += " tags=" + ",".join(
                repr(tag) for tag in target.serialized_location_tags
            )
    if row.type_locality_validity is not None:
        description += f" + validity={row.type_locality_validity.validity.name}"
    for origin in row.regional_origins:
        description += f" + {origin.region_name}:{origin.origin.name}"
    return description


def _shorten(value: str, width: int) -> str:
    single_line = " ".join(value.split())
    return textwrap.shorten(single_line, width=width, placeholder="…")


def print_review_table(
    recommendations: Iterable[Recommendation], *, actions: set[str] | None = None
) -> None:
    rows = [row for row in recommendations if actions is None or row.action in actions]
    print(
        f"{'ID':>7}  {'ACTION':<25} {'CONF':<10} {'NAME':<32} "
        f"{'TARGET':<48} EVIDENCE"
    )
    print("-" * 200)
    for row in rows:
        evidence = " | ".join(item.text for item in row.evidence if item.text.strip())
        if not evidence:
            evidence = row.reason
        print(
            f"{row.name_id:>7}  {row.action:<25} {row.confidence:<10} "
            f"{_shorten(row.name, 32):<32} {_shorten(_review_target(row), 48):<48} "
            f"{_shorten(evidence, 90)}"
        )
    counts = Counter(row.action for row in rows)
    print(f"\n{len(rows)} recommendation(s): {dict(counts)}")


def print_full_manual_reviews(
    recommendations: Iterable[Recommendation], *, include_summary: bool = True
) -> None:
    """Print unresolved rows and actionable review notes without truncation."""
    rows = [
        row
        for row in recommendations
        if row.action == MANUAL_REVIEW or row.review_note is not None
    ]
    if not rows:
        print("No manual_review recommendations or actionable review notes.")
        return
    for index, row in enumerate(rows):
        if index:
            print()
        if row.action == MANUAL_REVIEW:
            heading = "MANUAL_REVIEW"
        else:
            heading = f"REVIEW_NOTE action={row.action}"
        print(f"{heading} name_id={row.name_id} name={row.name!r}")
        print(
            f"Current Location: L{row.current_location_id} {row.current_location_name}"
        )
        print(f"Confidence: {row.confidence}")
        if row.review_note is not None:
            print("Review note:")
            print(row.review_note)
        print("Reason:")
        print(row.reason)
        if row.tag_comment is not None:
            print("Tag comment:")
            print(row.tag_comment)
        print("Evidence:")
        if not row.evidence:
            print("- (none)")
        for item in row.evidence:
            print(f"- source_id={item.source_id} source_name={item.source_name!r}")
            print(item.text)
    manual_count = sum(row.action == MANUAL_REVIEW for row in rows)
    note_count = sum(
        row.action != MANUAL_REVIEW and row.review_note is not None for row in rows
    )
    if include_summary:
        print(
            f"\n{len(rows)} review item(s): {manual_count} manual_review, "
            f"{note_count} actionable review note(s)."
        )


def add_virtual_models(plan: RecommendationPlan, builder: ProposalBuilder) -> None:
    """Apply type-locality plan actions to shared virtual model copies."""
    new_locations: dict[str, Location] = {}

    for location_tag_update in plan.location_tag_updates:
        if not isinstance(location_tag_update.location, Location):
            continue
        location_proposal = builder.copy(
            location_tag_update.location,
            context=(
                "type-locality Location tags for "
                f"{location_tag_update.location.name}"
            ),
        )
        for spec in location_tag_update.tags:
            location_proposal.add_tag(_make_location_tag(spec))

    for serialized_tag_update in plan.serialized_location_tag_updates:
        if not isinstance(serialized_tag_update.location, Location):
            continue
        location_proposal = builder.copy(
            serialized_tag_update.location,
            context=(
                "type-locality serialized Location tags for "
                f"{serialized_tag_update.location.name}"
            ),
        )
        for tag in serialized_tag_update.tags:
            location_proposal.add_tag(tag)

    recent: Period | None = None
    for definition in plan.new_locations:
        if not isinstance(definition.region, Region):
            continue
        target = definition.target
        min_period = definition.min_period
        if min_period is None:
            if recent is None:
                recent = Period.get(name="Recent")
            min_period = recent
        if not isinstance(min_period, Period):
            continue
        max_period = definition.max_period or min_period
        if not isinstance(max_period, Period):
            continue
        stratigraphic_unit = definition.stratigraphic_unit
        if stratigraphic_unit is not None and not isinstance(
            stratigraphic_unit, StratigraphicUnit
        ):
            continue
        tags = (
            *(_make_location_tag(spec) for spec in target.location_tags),
            *target.serialized_location_tags,
        )
        new_location = builder.create(
            Location,
            context=f"new Location {target.location_name!r}",
            name=target.location_name,
            min_period=min_period,
            max_period=max_period,
            min_age=target.min_age,
            max_age=target.max_age,
            stratigraphic_unit=stratigraphic_unit,
            region=definition.region,
            comment=_coordinate_comment(target),
            latitude=target.latitude,
            longitude=target.longitude,
            location_detail="None",
            age_detail="None",
            deleted=LocationStatus.valid,
            tags=tags,
        )
        new_locations[target.location_name] = new_location

    for validity_update in plan.type_locality_validity_updates:
        if validity_update.already_applied or not isinstance(
            validity_update.name, Name
        ):
            continue
        name_proposal = builder.copy(
            validity_update.name,
            context=(
                "type-locality manifest line "
                f"{validity_update.recommendation.line_number}"
            ),
        )
        name_proposal.add_type_tag(
            _make_type_locality_validity_tag(validity_update.spec)
        )

    for origin_update in plan.regional_origin_updates:
        if origin_update.already_applied or not isinstance(origin_update.taxon, Taxon):
            continue
        taxon_proposal = builder.copy(
            origin_update.taxon,
            context=(
                "type-locality manifest line "
                f"{origin_update.recommendation.line_number}"
            ),
        )
        taxon_proposal.add_tag(
            _make_regional_origin_tag(
                origin_update.spec, origin_update.region, origin_update.source
            )
        )

    for planned_update in plan.updates:
        if planned_update.already_applied or not isinstance(planned_update.name, Name):
            continue
        row = planned_update.recommendation
        name_proposal = builder.copy(
            planned_update.name,
            context=f"type-locality manifest line {row.line_number}",
        )
        if row.action == ADD_IMPRECISE_LOCALITY:
            name_proposal.add_type_tag(_make_imprecise_tag(row.tag_comment))
            continue
        maybe_target = planned_update.target
        if isinstance(maybe_target, Location):
            name_proposal.type_locality = builder.replacement(maybe_target)
        elif planned_update.new_location_name is not None:
            maybe_new_location = new_locations.get(planned_update.new_location_name)
            if maybe_new_location is not None:
                name_proposal.type_locality = maybe_new_location


def execute_plan(plan: RecommendationPlan, *, apply: bool) -> None:
    created: dict[str, Location] = {}
    needs_recent = any(
        definition.min_period is None for definition in plan.new_locations
    )
    recent = Period.get(name="Recent") if apply and needs_recent else None
    for location_tag_update in plan.location_tag_updates:
        tag_names = ",".join(spec.tag for spec in location_tag_update.tags)
        print(
            f"{'ADD_LOCATION_TAGS' if apply else 'WOULD_ADD_LOCATION_TAGS'} "
            f"location_id={location_tag_update.location.id} "
            f"name={location_tag_update.location.name!r} "
            f"tags={tag_names}"
        )
        if apply:
            for spec in location_tag_update.tags:
                location_tag_update.location.add_tag(_make_location_tag(spec))

    for serialized_tag_update in plan.serialized_location_tag_updates:
        print(
            f"{'ADD_SERIALIZED_LOCATION_TAGS' if apply else 'WOULD_ADD_SERIALIZED_LOCATION_TAGS'} "
            f"location_id={serialized_tag_update.location.id} "
            f"name={serialized_tag_update.location.name!r} "
            f"tags={','.join(map(repr, serialized_tag_update.tags))}"
        )
        if apply:
            for tag in serialized_tag_update.tags:
                serialized_tag_update.location.add_tag(tag)

    for definition in plan.new_locations:
        target = definition.target
        tags = [spec.tag for spec in target.location_tags]
        tags.extend(repr(tag) for tag in target.serialized_location_tags)
        tag_names = ",".join(tags) or "-"
        temporal_context = (
            f"periods={(target.min_period_name, target.max_period_name)!r} "
            f"ages={(target.min_age, target.max_age)!r} "
            f"stratigraphic_unit={target.stratigraphic_unit_name!r}"
        )
        print(
            f"{'CREATE_LOCATION' if apply else 'WOULD_CREATE_LOCATION'} "
            f"name={target.location_name!r} region={target.region_name!r} "
            f"coordinates={(target.latitude, target.longitude)!r} "
            f"{temporal_context} tags={tag_names}"
        )
        if apply:
            region = cast(Region, definition.region)
            period = definition.min_period or recent
            assert period is not None
            location = Location.make(
                target.location_name,
                region,
                cast(Period, period),
                comment=_coordinate_comment(target),
                stratigraphic_unit=cast(
                    StratigraphicUnit | None, definition.stratigraphic_unit
                ),
            )
            if definition.max_period is not None:
                location.max_period = cast(Period, definition.max_period)
            location.min_age = target.min_age
            location.max_age = target.max_age
            if target.latitude is not None and target.longitude is not None:
                location.latitude = target.latitude
                location.longitude = target.longitude
            for spec in target.location_tags:
                location.add_tag(_make_location_tag(spec))
            for tag in target.serialized_location_tags:
                location.add_tag(tag)
            created[target.location_name] = location

    validity_applied = validity_already_applied = 0
    for validity_update in plan.type_locality_validity_updates:
        row = validity_update.recommendation
        if validity_update.already_applied:
            validity_already_applied += 1
            print(
                f"SKIP_ALREADY_APPLIED name_id={row.name_id} "
                f"tag=TypeLocalityValidity({validity_update.spec.validity.name})"
            )
            continue
        print(
            f"{'ADD_TYPE_LOCALITY_VALIDITY' if apply else 'WOULD_ADD_TYPE_LOCALITY_VALIDITY'} "
            f"name_id={row.name_id} name={row.name!r} "
            f"validity={validity_update.spec.validity.name}"
        )
        if apply:
            validity_update.name.add_type_tag(
                _make_type_locality_validity_tag(validity_update.spec)
            )
        validity_applied += 1

    origin_applied = origin_already_applied = 0
    for origin_update in plan.regional_origin_updates:
        row = origin_update.recommendation
        if origin_update.already_applied:
            origin_already_applied += 1
            print(
                f"SKIP_ALREADY_APPLIED taxon_id={origin_update.taxon.id} "
                f"tag=RegionalOrigin({origin_update.region.name}, "
                f"{origin_update.spec.origin.name})"
            )
            continue
        print(
            f"{'ADD_REGIONAL_ORIGIN' if apply else 'WOULD_ADD_REGIONAL_ORIGIN'} "
            f"taxon_id={origin_update.taxon.id} via_name_id={row.name_id} "
            f"region={origin_update.region.name!r} "
            f"origin={origin_update.spec.origin.name} "
            f"source_id={origin_update.source.id}"
        )
        if apply:
            origin_update.taxon.add_tag(
                _make_regional_origin_tag(
                    origin_update.spec, origin_update.region, origin_update.source
                )
            )
        origin_applied += 1

    applied = already_applied = 0
    for planned_update in plan.updates:
        row = planned_update.recommendation
        if planned_update.already_applied:
            already_applied += 1
            print(f"SKIP_ALREADY_APPLIED name_id={row.name_id} action={row.action}")
            continue
        if row.action == ADD_IMPRECISE_LOCALITY:
            print(
                f"{'ADD_IMPRECISE_LOCALITY' if apply else 'WOULD_ADD_IMPRECISE_LOCALITY'} "
                f"name_id={row.name_id} name={row.name!r} confidence={row.confidence}"
            )
            if apply:
                planned_update.name.add_type_tag(_make_imprecise_tag(row.tag_comment))
        else:
            location_target = planned_update.target
            if location_target is None:
                assert planned_update.new_location_name is not None
                location_target = cast(
                    LocationLike, created.get(planned_update.new_location_name)
                )
            target_description = (
                f"location_id={location_target.id} name={location_target.name!r}"
                if location_target is not None
                else f"new_location={planned_update.new_location_name!r}"
            )
            print(
                f"{'MOVE' if apply else 'WOULD_MOVE'} name_id={row.name_id} "
                f"name={row.name!r} to={target_description} confidence={row.confidence}"
            )
            if apply:
                assert location_target is not None
                planned_update.name.type_locality = location_target
        applied += 1

    mode = "Applied" if apply else "Dry run"
    print(
        f"{mode}: {applied} update(s), {already_applied} already applied, "
        f"{len(plan.new_locations)} new Location(s), "
        f"{len(plan.location_tag_updates)} existing Location tag update(s), "
        f"{len(plan.serialized_location_tag_updates)} serialized Location tag "
        "update(s), "
        f"{validity_applied} TypeLocalityValidity update(s) "
        f"({validity_already_applied} already applied), "
        f"{origin_applied} RegionalOrigin update(s) "
        f"({origin_already_applied} already applied)."
    )
    print(f"Recommended actions: {dict(plan.action_counts)}")
    if not apply:
        print("No database changes made. Review this plan, then add --apply.")
