"""Backend for Location edit, rename, and merge recommendations.

Every row snapshots the Location IDs, names, Regions, periods, and stratigraphic
context used during review; any unexpected database change aborts the complete plan
before writes. The public CLI lives in ``scripts/apply_recommendations.py``.
An evidence-backed merge may set ``allow_temporal_context_conflicts`` when a
stored period or stratigraphic assignment is obsolete; Region conflicts are
never allowed, and the target's conflicting metadata is retained.

``edit_location`` can change any ordinary persisted Location field. Status and
alias-parent changes remain the responsibility of the merge workflow; tags are
changed explicitly through ``add_tags`` and ``remove_tags``.
"""

import json
import textwrap
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from taxonomy.db.models import Article, Location, Period, Region, StratigraphicUnit
from taxonomy.db.models.location import LocationStatus
from taxonomy.db.models.tags import LocationTag

SCHEMA_VERSION = 1
RENAME_LOCATION = "rename_location"
MERGE_LOCATION = "merge_location"
EDIT_LOCATION = "edit_location"
ALLOWED_ACTIONS = {RENAME_LOCATION, MERGE_LOCATION, EDIT_LOCATION}
ALLOWED_CONFIDENCES = {"high", "medium", "low"}

SCALAR_FIELDS: dict[str, tuple[type, ...]] = {
    "name": (str,),
    "min_age": (int, type(None)),
    "max_age": (int, type(None)),
    "comment": (str, type(None)),
    "latitude": (str, type(None)),
    "longitude": (str, type(None)),
    "location_detail": (str, type(None)),
    "age_detail": (str, type(None)),
}
RELATED_FIELDS = {
    "min_period": Period,
    "max_period": Period,
    "stratigraphic_unit": StratigraphicUnit,
    "region": Region,
    "source": Article,
}
EDITABLE_FIELDS = {*SCALAR_FIELDS, *RELATED_FIELDS}


class RecommendationError(Exception):
    pass


class NamedLike(Protocol):
    id: int
    name: str


class LocationLike(Protocol):
    id: int
    name: str
    region: NamedLike
    min_period: NamedLike | None
    max_period: NamedLike | None
    stratigraphic_unit: NamedLike | None
    min_age: int | None
    max_age: int | None
    source: object | None
    deleted: LocationStatus
    parent: LocationLike | None
    latitude: str | None
    longitude: str | None
    comment: str | None
    location_detail: str | None
    age_detail: str | None
    tags: Iterable[Any] | None

    def is_invalid(self) -> bool: ...

    def merge(self, other: LocationLike) -> None: ...

    def add_tag(self, tag: Any) -> None: ...


@dataclass(frozen=True, slots=True)
class Evidence:
    kind: str
    text: str


@dataclass(frozen=True, slots=True)
class LocationSpec:
    location_id: int
    location_name: str
    region_id: int
    region_name: str
    min_period_id: int | None
    min_period_name: str | None
    max_period_id: int | None
    max_period_name: str | None
    stratigraphic_unit_id: int | None
    stratigraphic_unit_name: str | None
    latitude: str | None
    longitude: str | None
    has_coordinate_snapshot: bool


@dataclass(frozen=True, slots=True)
class FieldChange:
    field: str
    old_value: str | int | None
    new_value: str | int | None


@dataclass(frozen=True, slots=True)
class Recommendation:
    line_number: int
    action: str
    confidence: str
    reason: str
    evidence: tuple[Evidence, ...]
    location: LocationSpec | None
    new_name: str | None
    source: LocationSpec | None
    target: LocationSpec | None
    allow_temporal_context_conflicts: bool
    changes: tuple[FieldChange, ...] = ()
    add_tags: tuple[LocationTag, ...] = ()
    remove_tags: tuple[LocationTag, ...] = ()
    review_note: str | None = None


@dataclass(frozen=True, slots=True)
class PlannedRename:
    recommendation: Recommendation
    location: LocationLike
    already_applied: bool


@dataclass(frozen=True, slots=True)
class PlannedMerge:
    recommendation: Recommendation
    source: LocationLike
    target: LocationLike
    already_applied: bool


@dataclass(frozen=True, slots=True)
class PlannedEdit:
    recommendation: Recommendation
    location: LocationLike
    already_applied: bool


@dataclass(frozen=True, slots=True)
class RecommendationPlan:
    actions: tuple[PlannedRename | PlannedMerge | PlannedEdit, ...]
    action_counts: Counter[str]


PlannedAction = PlannedRename | PlannedMerge | PlannedEdit


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


def _optional_bool(
    data: dict[str, Any], key: str, line_number: int, *, default: bool = False
) -> bool:
    value = data.get(key, default)
    if not isinstance(value, bool):
        raise RecommendationError(f"line {line_number}: {key!r} must be a boolean")
    return value


def _parse_evidence(data: Any, line_number: int) -> tuple[Evidence, ...]:
    if not isinstance(data, list) or not data:
        raise RecommendationError(
            f"line {line_number}: evidence must be a nonempty list"
        )
    output: list[Evidence] = []
    for item in data:
        if not isinstance(item, dict):
            raise RecommendationError(
                f"line {line_number}: each evidence item must be an object"
            )
        output.append(
            Evidence(
                _required_str(item, "kind", line_number),
                _required_str(item, "text", line_number),
            )
        )
    return tuple(output)


def _parse_location_spec(data: Any, line_number: int, key: str) -> LocationSpec:
    if not isinstance(data, dict):
        raise RecommendationError(f"line {line_number}: {key} must be an object")
    has_latitude = "latitude" in data
    has_longitude = "longitude" in data
    if has_latitude != has_longitude:
        raise RecommendationError(
            f"line {line_number}: {key} must contain both latitude and longitude "
            "or neither"
        )
    return LocationSpec(
        location_id=_required_int(data, "location_id", line_number),
        location_name=_required_str(data, "location_name", line_number),
        region_id=_required_int(data, "region_id", line_number),
        region_name=_required_str(data, "region_name", line_number),
        min_period_id=_optional_int(data, "min_period_id", line_number),
        min_period_name=_optional_str(data, "min_period_name", line_number),
        max_period_id=_optional_int(data, "max_period_id", line_number),
        max_period_name=_optional_str(data, "max_period_name", line_number),
        stratigraphic_unit_id=_optional_int(data, "stratigraphic_unit_id", line_number),
        stratigraphic_unit_name=_optional_str(
            data, "stratigraphic_unit_name", line_number
        ),
        latitude=_optional_str(data, "latitude", line_number),
        longitude=_optional_str(data, "longitude", line_number),
        has_coordinate_snapshot=has_latitude,
    )


def _parse_field_value(
    value: Any, field: str, line_number: int, *, label: str
) -> str | int | None:
    if field in RELATED_FIELDS:
        if value is not None and (
            not isinstance(value, int) or isinstance(value, bool)
        ):
            raise RecommendationError(
                f"line {line_number}: {label} for related field {field!r} must be "
                "an integer ID or null"
            )
        if field == "region" and value is None:
            raise RecommendationError(
                f"line {line_number}: Region cannot be changed to null"
            )
        return value
    expected_types = SCALAR_FIELDS[field]
    if not isinstance(value, expected_types) or (
        isinstance(value, bool) and int in expected_types
    ):
        expected = "string, integer, or null"
        if expected_types == (str,):
            expected = "string"
        elif int not in expected_types:
            expected = "string or null"
        raise RecommendationError(
            f"line {line_number}: {label} for field {field!r} must be {expected}"
        )
    if field == "name" and not value:
        raise RecommendationError(f"line {line_number}: Location name cannot be empty")
    return cast(str | int | None, value)


def _parse_changes(data: Any, line_number: int) -> tuple[FieldChange, ...]:
    if not isinstance(data, list):
        raise RecommendationError(f"line {line_number}: changes must be a list")
    output: list[FieldChange] = []
    seen: set[str] = set()
    for item in data:
        if not isinstance(item, dict):
            raise RecommendationError(
                f"line {line_number}: each field change must be an object"
            )
        field = _required_str(item, "field", line_number)
        if field not in EDITABLE_FIELDS:
            allowed = ", ".join(sorted(EDITABLE_FIELDS))
            raise RecommendationError(
                f"line {line_number}: field {field!r} is not editable; allowed: {allowed}"
            )
        if field in seen:
            raise RecommendationError(
                f"line {line_number}: field {field!r} is changed more than once"
            )
        seen.add(field)
        if "old_value" not in item or "new_value" not in item:
            raise RecommendationError(
                f"line {line_number}: change for {field!r} requires old_value and "
                "new_value"
            )
        old_value = _parse_field_value(
            item["old_value"], field, line_number, label="old_value"
        )
        new_value = _parse_field_value(
            item["new_value"], field, line_number, label="new_value"
        )
        if old_value == new_value:
            raise RecommendationError(
                f"line {line_number}: change for {field!r} has identical values"
            )
        output.append(FieldChange(field, old_value, new_value))
    return tuple(output)


def _parse_tags(data: Any, line_number: int, key: str) -> tuple[LocationTag, ...]:
    if data is None:
        return ()
    if not isinstance(data, list):
        raise RecommendationError(f"line {line_number}: {key} must be a list")
    output: list[LocationTag] = []
    for serialized in data:
        if not isinstance(serialized, list):
            raise RecommendationError(
                f"line {line_number}: each {key} entry must be a serialized tag list"
            )
        try:
            tag = LocationTag.unserialize(serialized)
        except Exception as exc:
            raise RecommendationError(
                f"line {line_number}: invalid serialized Location tag in {key}: "
                f"{serialized!r}"
            ) from exc
        if tag in output:
            raise RecommendationError(
                f"line {line_number}: duplicate tag in {key}: {serialized!r}"
            )
        output.append(tag)
    return tuple(output)


def parse_recommendation(data: Any, line_number: int) -> Recommendation:
    if not isinstance(data, dict):
        raise RecommendationError(f"line {line_number}: row must be a JSON object")
    schema_version = _required_int(data, "schema_version", line_number)
    if schema_version != SCHEMA_VERSION:
        raise RecommendationError(
            f"line {line_number}: unsupported schema_version {schema_version}"
        )
    action = _required_str(data, "action", line_number)
    if action not in ALLOWED_ACTIONS:
        raise RecommendationError(f"line {line_number}: unsupported action {action!r}")
    confidence = _required_str(data, "confidence", line_number)
    if confidence not in ALLOWED_CONFIDENCES:
        raise RecommendationError(
            f"line {line_number}: unsupported confidence {confidence!r}"
        )
    reason = _required_str(data, "reason", line_number)
    evidence = _parse_evidence(data.get("evidence"), line_number)
    review_note = _optional_str(data, "review_note", line_number)
    if action == RENAME_LOCATION:
        return Recommendation(
            line_number=line_number,
            action=action,
            confidence=confidence,
            reason=reason,
            evidence=evidence,
            location=_parse_location_spec(
                data.get("location"), line_number, "location"
            ),
            new_name=_required_str(data, "new_name", line_number),
            source=None,
            target=None,
            allow_temporal_context_conflicts=False,
            review_note=review_note,
        )
    if action == EDIT_LOCATION:
        changes = _parse_changes(data.get("changes", []), line_number)
        add_tags = _parse_tags(data.get("add_tags"), line_number, "add_tags")
        remove_tags = _parse_tags(data.get("remove_tags"), line_number, "remove_tags")
        if not changes and not add_tags and not remove_tags:
            raise RecommendationError(
                f"line {line_number}: edit_location must change a field or tags"
            )
        overlap = set(add_tags) & set(remove_tags)
        if overlap:
            raise RecommendationError(
                f"line {line_number}: tags cannot be both added and removed: "
                f"{sorted(map(repr, overlap))}"
            )
        return Recommendation(
            line_number=line_number,
            action=action,
            confidence=confidence,
            reason=reason,
            evidence=evidence,
            location=_parse_location_spec(
                data.get("location"), line_number, "location"
            ),
            new_name=None,
            source=None,
            target=None,
            allow_temporal_context_conflicts=False,
            changes=changes,
            add_tags=add_tags,
            remove_tags=remove_tags,
            review_note=review_note,
        )
    return Recommendation(
        line_number=line_number,
        action=action,
        confidence=confidence,
        reason=reason,
        evidence=evidence,
        location=None,
        new_name=None,
        source=_parse_location_spec(data.get("source"), line_number, "source"),
        target=_parse_location_spec(data.get("target"), line_number, "target"),
        allow_temporal_context_conflicts=_optional_bool(
            data, "allow_temporal_context_conflicts", line_number
        ),
        review_note=review_note,
    )


def read_recommendations(path: Path) -> list[Recommendation]:
    output: list[Recommendation] = []
    mutated_ids: set[int] = set()
    merge_target_ids: set[int] = set()
    with path.open() as file:
        for line_number, raw_line in enumerate(file, start=1):
            if not raw_line.strip():
                continue
            try:
                data = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                raise RecommendationError(
                    f"line {line_number}: invalid JSON: {exc.msg}"
                ) from exc
            row = parse_recommendation(data, line_number)
            mutated_spec = (
                row.location
                if row.action in {RENAME_LOCATION, EDIT_LOCATION}
                else row.source
            )
            assert mutated_spec is not None
            if (
                mutated_spec.location_id in mutated_ids
                or mutated_spec.location_id in merge_target_ids
            ):
                raise RecommendationError(
                    f"line {line_number}: Location {mutated_spec.location_id} is "
                    "mutated by more than one recommendation"
                )
            mutated_ids.add(mutated_spec.location_id)
            if row.action == MERGE_LOCATION:
                assert row.target is not None
                if row.target.location_id in mutated_ids:
                    raise RecommendationError(
                        f"line {line_number}: merge target Location "
                        f"{row.target.location_id} is mutated by another recommendation"
                    )
                merge_target_ids.add(row.target.location_id)
            output.append(row)
    if not output:
        raise RecommendationError("recommendation file contains no rows")
    return output


def _get_location(location_id: int) -> LocationLike:
    try:
        return cast(LocationLike, Location.get(id=location_id))
    except Location.DoesNotExist as exc:
        raise RecommendationError(
            f"Location {location_id} from the manifest no longer exists"
        ) from exc


def _find_location_by_name(name: str) -> LocationLike | None:
    try:
        return cast(LocationLike, Location.select().filter(Location.name == name).get())
    except Location.DoesNotExist:
        return None


def _validate_named_object(
    actual: NamedLike | None,
    expected_id: int | None,
    expected_name: str | None,
    *,
    label: str,
) -> None:
    if expected_id is None:
        if expected_name is not None:
            raise RecommendationError(f"{label} name is set but ID is null")
        if actual is not None:
            raise RecommendationError(
                f"{label} changed from null to {actual.id}: {actual.name!r}"
            )
        return
    if expected_name is None:
        raise RecommendationError(f"{label} ID is set but name is null")
    if actual is None:
        raise RecommendationError(
            f"{label} changed from {expected_id}: {expected_name!r} to null"
        )
    if actual.id != expected_id or actual.name != expected_name:
        raise RecommendationError(
            f"{label} changed from {expected_id}: {expected_name!r} to "
            f"{actual.id}: {actual.name!r}"
        )


def _validate_location_context(spec: LocationSpec, location: LocationLike) -> None:
    if location.region.id != spec.region_id or location.region.name != spec.region_name:
        raise RecommendationError(
            f"Location {location.id} Region changed from "
            f"{spec.region_id}: {spec.region_name!r} to "
            f"{location.region.id}: {location.region.name!r}"
        )
    _validate_named_object(
        location.min_period,
        spec.min_period_id,
        spec.min_period_name,
        label=f"Location {location.id} minimum period",
    )
    _validate_named_object(
        location.max_period,
        spec.max_period_id,
        spec.max_period_name,
        label=f"Location {location.id} maximum period",
    )
    _validate_named_object(
        location.stratigraphic_unit,
        spec.stratigraphic_unit_id,
        spec.stratigraphic_unit_name,
        label=f"Location {location.id} stratigraphic unit",
    )


def _contexts_match(first: LocationLike, second: LocationLike) -> bool:
    def compatible(first_id: int | None, second_id: int | None) -> bool:
        return first_id is None or second_id is None or first_id == second_id

    return (
        first.region.id == second.region.id
        and compatible(
            first.min_period.id if first.min_period is not None else None,
            second.min_period.id if second.min_period is not None else None,
        )
        and compatible(
            first.max_period.id if first.max_period is not None else None,
            second.max_period.id if second.max_period is not None else None,
        )
        and compatible(
            (
                first.stratigraphic_unit.id
                if first.stratigraphic_unit is not None
                else None
            ),
            (
                second.stratigraphic_unit.id
                if second.stratigraphic_unit is not None
                else None
            ),
        )
    )


def _current_field_value(location: LocationLike, field: str) -> str | int | None:
    value = getattr(location, field)
    if field in RELATED_FIELDS:
        return None if value is None else cast(NamedLike, value).id
    return cast(str | int | None, value)


def _validate_edit_snapshot(
    spec: LocationSpec, location: LocationLike, changed_fields: set[str]
) -> None:
    if "name" not in changed_fields and location.name != spec.location_name:
        raise RecommendationError(
            f"Location {location.id} name changed from {spec.location_name!r} "
            f"to {location.name!r}"
        )
    if "region" not in changed_fields and (
        location.region.id != spec.region_id or location.region.name != spec.region_name
    ):
        raise RecommendationError(
            f"Location {location.id} Region changed from "
            f"{spec.region_id}: {spec.region_name!r} to "
            f"{location.region.id}: {location.region.name!r}"
        )
    for field, expected_id, expected_name, label in (
        ("min_period", spec.min_period_id, spec.min_period_name, "minimum period"),
        ("max_period", spec.max_period_id, spec.max_period_name, "maximum period"),
        (
            "stratigraphic_unit",
            spec.stratigraphic_unit_id,
            spec.stratigraphic_unit_name,
            "stratigraphic unit",
        ),
    ):
        if field not in changed_fields:
            _validate_named_object(
                cast(NamedLike | None, getattr(location, field)),
                expected_id,
                expected_name,
                label=f"Location {location.id} {label}",
            )
    if spec.has_coordinate_snapshot:
        for field, expected in (
            ("latitude", spec.latitude),
            ("longitude", spec.longitude),
        ):
            if field not in changed_fields and getattr(location, field) != expected:
                raise RecommendationError(
                    f"Location {location.id} {field} changed from {expected!r} "
                    f"to {getattr(location, field)!r}"
                )


def _resolve_new_field_value(field: str, value: str | int | None) -> Any:
    model_cls = RELATED_FIELDS.get(field)
    if model_cls is None or value is None:
        return value
    assert isinstance(value, int)
    try:
        return model_cls.get(id=value)
    except model_cls.DoesNotExist as exc:
        raise RecommendationError(
            f"new value for {field!r} refers to missing {model_cls.__name__} {value}"
        ) from exc


def _planned_name(action: PlannedAction) -> str | None:
    if isinstance(action, PlannedRename):
        assert action.recommendation.new_name is not None
        return action.recommendation.new_name
    if isinstance(action, PlannedEdit):
        for change in action.recommendation.changes:
            if change.field == "name":
                assert isinstance(change.new_value, str)
                return change.new_value
    return None


def _validate_and_order_name_changes(
    actions: list[PlannedAction],
    find_location_by_name: Callable[[str], LocationLike | None],
) -> list[PlannedAction]:
    """Validate the complete final namespace and order names that must be vacated."""
    name_actions: dict[int, PlannedRename | PlannedEdit] = {}
    final_name_owners: dict[str, PlannedRename | PlannedEdit] = {}
    errors: list[str] = []
    for action in actions:
        new_name = _planned_name(action)
        if new_name is None:
            continue
        assert isinstance(action, (PlannedRename, PlannedEdit))
        name_actions[action.location.id] = action
        previous = final_name_owners.get(new_name)
        if previous is not None and previous.location.id != action.location.id:
            errors.append(
                f"lines {previous.recommendation.line_number} and "
                f"{action.recommendation.line_number}: Locations "
                f"{previous.location.id} and {action.location.id} would both be "
                f"named {new_name!r}"
            )
        else:
            final_name_owners[new_name] = action

    dependencies: dict[int, int] = {}
    for location_id, action in name_actions.items():
        new_name = _planned_name(action)
        assert new_name is not None
        if action.location.name == new_name:
            continue
        collision = find_location_by_name(new_name)
        if collision is None or collision.id == location_id:
            continue
        collision_action = name_actions.get(collision.id)
        collision_new_name = (
            None if collision_action is None else _planned_name(collision_action)
        )
        if collision_action is None or collision_new_name == new_name:
            errors.append(
                f"line {action.recommendation.line_number}: recommended name "
                f"{new_name!r} is already used by Location {collision.id}"
            )
            continue
        dependencies[location_id] = collision.id

    if errors:
        raise RecommendationError("\n- ".join(errors))

    ordered_location_ids: list[int] = []
    visiting: list[int] = []
    visited: set[int] = set()

    def visit(location_id: int) -> None:
        if location_id in visited:
            return
        if location_id in visiting:
            cycle = [*visiting[visiting.index(location_id) :], location_id]
            lines = [
                name_actions[item].recommendation.line_number for item in cycle[:-1]
            ]
            raise RecommendationError(
                f"lines {', '.join(map(str, lines))}: Location name changes form "
                f"an unsupported cycle ({' -> '.join(map(str, cycle))})"
            )
        visiting.append(location_id)
        dependency = dependencies.get(location_id)
        if dependency is not None:
            visit(dependency)
        visiting.pop()
        visited.add(location_id)
        ordered_location_ids.append(location_id)

    for location_id in name_actions:
        visit(location_id)

    ordered_name_actions = [name_actions[item] for item in ordered_location_ids]
    reordered: list[PlannedAction] = []
    inserted_names = False
    for action in actions:
        if _planned_name(action) is not None:
            if not inserted_names:
                reordered.extend(ordered_name_actions)
                inserted_names = True
            continue
        reordered.append(action)
    return reordered


def build_plan(
    recommendations: Iterable[Recommendation],
    *,
    get_location: Callable[[int], LocationLike] = _get_location,
    find_location_by_name: Callable[[str], LocationLike | None] = (
        _find_location_by_name
    ),
) -> RecommendationPlan:
    actions: list[PlannedRename | PlannedMerge | PlannedEdit] = []
    counts: Counter[str] = Counter()
    errors: list[str] = []
    for row in recommendations:
        counts[row.action] += 1
        try:
            if row.action == RENAME_LOCATION:
                assert row.location is not None
                assert row.new_name is not None
                location = get_location(row.location.location_id)
                _validate_location_context(row.location, location)
                if location.is_invalid():
                    raise RecommendationError(
                        f"Location {location.id} is no longer valid"
                    )
                already_applied = location.name == row.new_name
                if not already_applied and location.name != row.location.location_name:
                    raise RecommendationError(
                        f"Location {location.id} name changed from "
                        f"{row.location.location_name!r} to {location.name!r}"
                    )
                actions.append(PlannedRename(row, location, already_applied))
                continue

            if row.action == EDIT_LOCATION:
                assert row.location is not None
                location = get_location(row.location.location_id)
                if location.is_invalid():
                    raise RecommendationError(
                        f"Location {location.id} is no longer valid"
                    )
                changed_fields = {change.field for change in row.changes}
                _validate_edit_snapshot(row.location, location, changed_fields)
                field_states: list[bool] = []
                for change in row.changes:
                    actual = _current_field_value(location, change.field)
                    if actual not in {change.old_value, change.new_value}:
                        raise RecommendationError(
                            f"Location {location.id} field {change.field!r} changed "
                            f"from expected {change.old_value!r} to {actual!r}"
                        )
                    field_states.append(actual == change.new_value)
                current_tags = tuple(location.tags or ())
                missing_add_tags = [
                    tag for tag in row.add_tags if tag not in current_tags
                ]
                present_remove_tags = [
                    tag for tag in row.remove_tags if tag in current_tags
                ]
                already_applied = (
                    all(field_states)
                    and not missing_add_tags
                    and not present_remove_tags
                )
                actions.append(PlannedEdit(row, location, already_applied))
                continue

            assert row.source is not None
            assert row.target is not None
            source = get_location(row.source.location_id)
            target = get_location(row.target.location_id)
            _validate_location_context(row.source, source)
            _validate_location_context(row.target, target)
            if source.id == target.id:
                raise RecommendationError("merge source and target are the same")
            if target.is_invalid():
                raise RecommendationError(
                    f"merge target Location {target.id} is no longer valid"
                )
            if target.name != row.target.location_name:
                raise RecommendationError(
                    f"target Location {target.id} name changed from "
                    f"{row.target.location_name!r} to {target.name!r}"
                )
            already_applied = (
                source.deleted is LocationStatus.alias
                and source.parent is not None
                and source.parent.id == target.id
            )
            if already_applied:
                if source.name != row.source.location_name:
                    raise RecommendationError(
                        f"alias Location {source.id} name changed from "
                        f"{row.source.location_name!r} to {source.name!r}"
                    )
            else:
                if source.is_invalid():
                    redirect = (
                        f" -> {source.parent.id}" if source.parent is not None else ""
                    )
                    raise RecommendationError(
                        f"merge source Location {source.id} is invalid{redirect}"
                    )
                if source.name != row.source.location_name:
                    raise RecommendationError(
                        f"source Location {source.id} name changed from "
                        f"{row.source.location_name!r} to {source.name!r}"
                    )
                if source.region.id != target.region.id:
                    raise RecommendationError(
                        f"Locations {source.id} and {target.id} do not have the "
                        "same Region"
                    )
                if not row.allow_temporal_context_conflicts and not _contexts_match(
                    source, target
                ):
                    raise RecommendationError(
                        f"Locations {source.id} and {target.id} no longer have "
                        "matching Region, period, and stratigraphic context"
                    )
            actions.append(PlannedMerge(row, source, target, already_applied))
        except RecommendationError as exc:
            errors.append(f"line {row.line_number}: {exc}")
    if not errors:
        try:
            actions = _validate_and_order_name_changes(actions, find_location_by_name)
        except RecommendationError as exc:
            errors.append(str(exc))
    if errors:
        raise RecommendationError(
            "refusing to continue because database validation failed:\n- "
            + "\n- ".join(errors)
        )
    return RecommendationPlan(tuple(actions), counts)


def _metadata_summary(source: LocationLike, target: LocationLike) -> str:
    fields: list[str] = []
    for field in ("min_period", "max_period", "stratigraphic_unit"):
        source_value = getattr(source, field)
        target_value = getattr(target, field)
        if source_value is not None and target_value is None:
            fields.append(f"{field}=copy")
        elif (
            source_value is not None
            and target_value is not None
            and source_value.id != target_value.id
        ):
            fields.append(f"{field}=conflict")
    for field in ("min_age", "max_age", "source"):
        source_value = getattr(source, field)
        target_value = getattr(target, field)
        if source_value is not None and target_value is None:
            fields.append(f"{field}=copy")
        elif (
            source_value is not None
            and target_value is not None
            and source_value != target_value
        ):
            fields.append(f"{field}=conflict")

    source_pair = (source.latitude, source.longitude)
    target_pair = (target.latitude, target.longitude)
    if source_pair not in ((None, None), target_pair):
        compatible = all(
            source_value is None or target_value is None or source_value == target_value
            for source_value, target_value in zip(source_pair, target_pair, strict=True)
        )
        fields.append(
            "coordinates=copy_or_complete" if compatible else "coordinates=conflict"
        )

    for field in ("comment", "location_detail", "age_detail"):
        source_text = getattr(source, field)
        target_text = getattr(target, field)
        if source_text and source_text not in ("None", target_text):
            action = "copy" if not target_text or target_text == "None" else "append"
            fields.append(f"{field}={action}")

    target_tags = tuple(target.tags or ())
    new_tag_count = sum(tag not in target_tags for tag in source.tags or ())
    if new_tag_count:
        fields.append(f"tags=add_{new_tag_count}")
    return ", ".join(fields) or "none"


def _shorten(value: str, width: int) -> str:
    return textwrap.shorten(" ".join(value.split()), width=width, placeholder="…")


def print_review_table(recommendations: Iterable[Recommendation]) -> None:
    print(f"{'ACTION':<18} {'CONF':<8} {'CURRENT':<52} TARGET")
    print("-" * 150)
    counts: Counter[str] = Counter()
    for row in recommendations:
        counts[row.action] += 1
        if row.action == RENAME_LOCATION:
            assert row.location is not None
            assert row.new_name is not None
            current = f"L{row.location.location_id} {row.location.location_name}"
            target = row.new_name
        elif row.action == EDIT_LOCATION:
            assert row.location is not None
            current = f"L{row.location.location_id} {row.location.location_name}"
            parts = [f"{change.field}={change.new_value!r}" for change in row.changes]
            parts.extend(f"add {tag!r}" for tag in row.add_tags)
            parts.extend(f"remove {tag!r}" for tag in row.remove_tags)
            target = "; ".join(parts)
        else:
            assert row.source is not None
            assert row.target is not None
            current = f"L{row.source.location_id} {row.source.location_name}"
            target = f"L{row.target.location_id} {row.target.location_name}"
        print(
            f"{row.action:<18} {row.confidence:<8} "
            f"{_shorten(current, 52):<52} {_shorten(target, 65)}"
        )
    print(f"\n{sum(counts.values())} recommendation(s): {dict(counts)}")


def print_full_review_notes(recommendations: Iterable[Recommendation]) -> None:
    """Print actionable Location rows whose caveats need full-text review."""
    rows = [row for row in recommendations if row.review_note is not None]
    for index, row in enumerate(rows):
        if index:
            print()
        if row.location is not None:
            current = f"L{row.location.location_id} {row.location.location_name}"
        else:
            assert row.source is not None
            current = f"L{row.source.location_id} {row.source.location_name}"
        print(f"REVIEW_NOTE action={row.action} location={current!r}")
        print(f"Confidence: {row.confidence}")
        print("Review note:")
        print(row.review_note)
        print("Reason:")
        print(row.reason)
        print("Evidence:")
        for item in row.evidence:
            print(f"- kind={item.kind!r}")
            print(item.text)
    if rows:
        print(f"\n{len(rows)} Location actionable review note(s).")


def execute_plan(
    plan: RecommendationPlan,
    *,
    apply: bool,
    clear_caches: Callable[[], None] = Location.clear_lint_caches,
) -> None:
    pending = already_applied = 0
    for action in plan.actions:
        row = action.recommendation
        if action.already_applied:
            already_applied += 1
            if isinstance(action, (PlannedRename, PlannedEdit)):
                description = f"location_id={action.location.id}"
            else:
                description = (
                    f"source_id={action.source.id} target_id={action.target.id}"
                )
            print(f"SKIP_ALREADY_APPLIED action={row.action} {description}")
            continue
        pending += 1
        if isinstance(action, PlannedRename):
            assert row.new_name is not None
            print(
                f"{'RENAME_LOCATION' if apply else 'WOULD_RENAME_LOCATION'} "
                f"location_id={action.location.id} old_name={action.location.name!r} "
                f"new_name={row.new_name!r} confidence={row.confidence!r}"
            )
            if apply:
                action.location.name = row.new_name
            continue

        if isinstance(action, PlannedEdit):
            changes = ", ".join(
                f"{change.field}={change.new_value!r}" for change in row.changes
            )
            tag_changes = ", ".join(
                [
                    *(f"add {tag!r}" for tag in row.add_tags),
                    *(f"remove {tag!r}" for tag in row.remove_tags),
                ]
            )
            summary = ", ".join(part for part in (changes, tag_changes) if part)
            print(
                f"{'EDIT_LOCATION' if apply else 'WOULD_EDIT_LOCATION'} "
                f"location_id={action.location.id} location_name={action.location.name!r} "
                f"changes={summary!r} confidence={row.confidence!r}"
            )
            if apply:
                for change in row.changes:
                    if (
                        _current_field_value(action.location, change.field)
                        != change.new_value
                    ):
                        setattr(
                            action.location,
                            change.field,
                            _resolve_new_field_value(change.field, change.new_value),
                        )
                tags = tuple(action.location.tags or ())
                if row.remove_tags:
                    tags = tuple(tag for tag in tags if tag not in row.remove_tags)
                    action.location.tags = tags
                for tag in row.add_tags:
                    if tag not in tuple(action.location.tags or ()):
                        action.location.add_tag(tag)
                action.location.tags = tuple(sorted(set(action.location.tags or ())))
            continue

        print(
            f"{'MERGE_LOCATION' if apply else 'WOULD_MERGE_LOCATION'} "
            f"source_id={action.source.id} source_name={action.source.name!r} "
            f"target_id={action.target.id} target_name={action.target.name!r} "
            f"confidence={row.confidence!r}"
        )
        metadata = _metadata_summary(action.source, action.target)
        if (
            not apply
            and row.allow_temporal_context_conflicts
            and not _contexts_match(action.source, action.target)
        ):
            print(
                "  REVIEWED_TEMPORAL_CONTEXT_CONFLICT "
                f"source_id={action.source.id} target_id={action.target.id} "
                "target_metadata_will_be_retained"
            )
        if not apply and metadata != "none":
            print(
                "  SOURCE_METADATA_WILL_BE_MERGED_WHERE_COMPATIBLE "
                f"source_id={action.source.id} {metadata}"
            )
        if apply:
            action.source.merge(action.target)
    if apply and pending:
        clear_caches()
    mode = "Applied" if apply else "Dry run"
    print(
        f"{mode}: {pending} action(s), {already_applied} already applied. "
        f"Recommended actions: {dict(plan.action_counts)}"
    )
    if not apply:
        print("No database changes made. Re-run with --apply to write these updates.")
