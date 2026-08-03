import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NotRequired, TypedDict, cast

from data_import import lib
from taxonomy.db import coordinate_lint, helpers, models
from taxonomy.db.models.location import LocationStatus, get_expected_general_name
from taxonomy.db.models.tags import LocationTag


class LocationFileError(ValueError):
    pass


class LocationDict(TypedDict):
    name: str
    region: models.Region
    period: models.Period
    latitude: NotRequired[str]
    longitude: NotRequired[str]
    source: NotRequired[models.Article]
    location_detail: NotRequired[str]
    comment: NotRequired[str]
    tags: NotRequired[list[LocationTag]]


_REQUIRED_FIELDS = frozenset({"name", "region", "period"})
_KNOWN_FIELDS = frozenset(LocationDict.__annotations__)


def companion_path(ce_path: Path) -> Path:
    if ce_path.name.endswith(".ce.jsonl"):
        base = ce_path.name.removesuffix(".ce.jsonl")
    elif ce_path.name.endswith(".jsonl"):
        base = ce_path.name.removesuffix(".jsonl")
    else:
        base = ce_path.name
    return ce_path.with_name(f"{base}.locations.jsonl")


def serialize_location(location: LocationDict) -> dict[str, Any]:
    output: dict[str, Any] = dict(location)
    for key in ("region", "period", "source"):
        if key in output:
            output[key] = output[key].name
    if "tags" in output:
        output["tags"] = [
            {"kind": type(tag).__name__, "data": tag.serialize()}
            for tag in output["tags"]
        ]
    return output


def write_location_file(path: Path, locations: Iterable[LocationDict]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for location in locations:
            json.dump(
                serialize_location(location), file, ensure_ascii=False, sort_keys=True
            )
            file.write("\n")


def _get_named_model(
    model: type[models.BaseModel], value: object, *, field: str, context: str
) -> models.BaseModel:
    if not isinstance(value, str):
        raise LocationFileError(f"{context}: {field} must be a string")
    try:
        return model.get(name=value)
    except model.DoesNotExist:
        raise LocationFileError(
            f"{context}: no {model.__name__} named {value!r}"
        ) from None


def deserialize_location(data: object, *, line_number: int) -> LocationDict:
    context = f"line {line_number}"
    if not isinstance(data, dict):
        raise LocationFileError(f"{context}: expected a JSON object")
    unknown = set(data) - _KNOWN_FIELDS
    if unknown:
        raise LocationFileError(f"{context}: unknown fields: {sorted(unknown)}")
    missing = _REQUIRED_FIELDS - set(data)
    if missing:
        raise LocationFileError(f"{context}: missing fields: {sorted(missing)}")
    output = dict(data)
    name = output["name"]
    if not isinstance(name, str) or not name.strip():
        raise LocationFileError(f"{context}: name must be a nonempty string")
    output["region"] = _get_named_model(
        models.Region, output["region"], field="region", context=context
    )
    output["period"] = _get_named_model(
        models.Period, output["period"], field="period", context=context
    )
    if "source" in output:
        output["source"] = _get_named_model(
            models.Article, output["source"], field="source", context=context
        )
    for field in ("latitude", "longitude", "location_detail", "comment"):
        if field in output and not isinstance(output[field], str):
            raise LocationFileError(f"{context}: {field} must be a string")
    has_latitude = "latitude" in output
    has_longitude = "longitude" in output
    if has_latitude != has_longitude:
        raise LocationFileError(
            f"{context}: latitude and longitude must be provided together"
        )
    if has_latitude:
        try:
            latitude, _ = coordinate_lint.standardize_coordinate(
                output["latitude"], is_latitude=True
            )
            longitude, _ = coordinate_lint.standardize_coordinate(
                output["longitude"], is_latitude=False
            )
        except helpers.InvalidCoordinates as exc:
            raise LocationFileError(f"{context}: invalid coordinates: {exc}") from exc
        output["latitude"] = latitude
        output["longitude"] = longitude
    if "tags" in output:
        raw_tags = output["tags"]
        if not isinstance(raw_tags, list):
            raise LocationFileError(f"{context}: tags must be a list")
        tags = []
        for raw_tag in raw_tags:
            if not isinstance(raw_tag, dict) or set(raw_tag) != {"kind", "data"}:
                raise LocationFileError(f"{context}: invalid tag {raw_tag!r}")
            tag = LocationTag.unserialize(raw_tag["data"])
            if type(tag).__name__ != raw_tag["kind"]:
                raise LocationFileError(
                    f"{context}: tag kind does not match serialized data"
                )
            tags.append(tag)
        output["tags"] = tags
    return cast(LocationDict, output)


@dataclass
class LocationFileReport:
    locations: list[LocationDict]
    errors: list[str]


def read_location_file_report(path: Path) -> LocationFileReport:
    locations = []
    errors = []
    with path.open(encoding="utf-8") as file:
        for line_number, line in enumerate(file, 1):
            if not line.strip():
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append(f"line {line_number}: {exc.msg}")
                continue
            try:
                locations.append(deserialize_location(data, line_number=line_number))
            except LocationFileError as exc:
                errors.append(str(exc))
    names = [location["name"] for location in locations]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        errors.append(f"duplicate location proposals: {duplicates}")
    return LocationFileReport(locations, errors)


def read_location_file(path: Path) -> list[LocationDict]:
    report = read_location_file_report(path)
    if report.errors:
        raise LocationFileError("\n".join(report.errors))
    return report.locations


def iter_mapped_location_names(entries: Iterable[lib.CEDict]) -> Iterable[str]:
    for entry in entries:
        for occurrence in entry.get("occurrences", []):
            if mapped_location := occurrence.get("mapped_location"):
                yield mapped_location


def _locations_with_name(name: str) -> list[models.Location]:
    return list(models.Location.select().filter(models.Location.name == name))


def _existing_location(name: str) -> models.Location | None:
    # Location.format() applies clean_string() after creation. On a retry, look
    # through that normalization as well as preserving exact-name precedence.
    lookup_names = dict.fromkeys((name, helpers.clean_string(name)))
    for lookup_name in lookup_names:
        candidates = _locations_with_name(lookup_name)
        if len(candidates) != 1:
            continue
        location = candidates[0]
        if location.deleted is LocationStatus.alias:
            return location.parent
        if location.deleted is LocationStatus.valid:
            return location
    return None


def _restorable_deleted_location(
    name: str, proposal: LocationDict
) -> models.Location | None:
    if name != get_expected_general_name(proposal["region"], proposal["period"]):
        return None
    candidates = [
        location
        for location in _locations_with_name(name)
        if location.deleted is LocationStatus.deleted
    ]
    if len(candidates) == 1:
        return candidates[0]
    return None


def _proposal_conflicts(existing: models.Location, proposal: LocationDict) -> list[str]:
    conflicts = []
    if existing.region != proposal["region"]:
        conflicts.append(
            f"region is {existing.region.name!r}, proposed {proposal['region'].name!r}"
        )
    if (
        existing.min_period != proposal["period"]
        or existing.max_period != proposal["period"]
    ):
        conflicts.append(
            f"period is {existing.min_period!r}–{existing.max_period!r}, "
            f"proposed {proposal['period'].name!r}"
        )
    for field in ("latitude", "longitude"):
        proposed = proposal.get(field)
        current = getattr(existing, field)
        if proposed is not None and current != proposed:
            conflicts.append(f"{field} is {current!r}, proposed {proposed!r}")
    return conflicts


@dataclass
class LocationPlan:
    used_names: set[str]
    locations: dict[str, models.Location | None]
    proposals: dict[str, LocationDict]
    statuses: dict[str, str]
    errors: list[str]
    warnings: list[str]

    @property
    def is_clean(self) -> bool:
        return not self.errors


def build_plan(
    entries: Iterable[lib.CEDict],
    proposals: Iterable[LocationDict],
    *,
    proposal_errors: Iterable[str] = (),
) -> LocationPlan:
    used_names = set(iter_mapped_location_names(entries))
    proposals_by_name = {proposal["name"]: proposal for proposal in proposals}
    locations: dict[str, models.Location | None] = {}
    statuses: dict[str, str] = {}
    errors = list(proposal_errors)
    warnings = []
    for name in sorted(used_names):
        existing = _existing_location(name)
        proposal = proposals_by_name.get(name)
        locations[name] = existing
        if existing is not None:
            statuses[name] = "existing"
            if proposal is not None:
                for conflict in _proposal_conflicts(existing, proposal):
                    errors.append(f"{name!r}: existing Location conflict: {conflict}")
            continue
        if proposal is not None:
            deleted = _restorable_deleted_location(name, proposal)
            if deleted is not None:
                locations[name] = deleted
                statuses[name] = "restore"
                for conflict in _proposal_conflicts(deleted, proposal):
                    errors.append(f"{name!r}: deleted Location conflict: {conflict}")
            else:
                statuses[name] = "create"
        else:
            statuses[name] = "unresolved"
            warnings.append(
                f"{name!r}: no matching Location and no companion-file proposal"
            )
    for name in sorted(set(proposals_by_name) - used_names):
        warnings.append(f"{name!r}: unused location proposal")
    return LocationPlan(
        used_names, locations, proposals_by_name, statuses, errors, warnings
    )


def print_plan(plan: LocationPlan) -> None:
    counts = {
        status: sum(value == status for value in plan.statuses.values())
        for status in ("existing", "restore", "create", "unresolved")
    }
    print(
        "Occurrence locations:",
        ", ".join(f"{status}={counts[status]}" for status in counts),
    )
    for name in sorted(plan.used_names):
        status = plan.statuses[name]
        location = plan.locations[name]
        if location is not None:
            target_name = location.name
            identifier = location.id
            target = target_name
            if identifier is not None:
                target += f" (#{identifier})"
            if target_name != name:
                target = f"{name} -> {target}"
            region = location.region.name
            if location.min_period is None and location.max_period is None:
                period = "period unset"
            elif location.min_period is None:
                assert location.max_period is not None
                period = location.max_period.name
            elif (
                location.max_period is None
                or location.min_period == location.max_period
            ):
                period = location.min_period.name
            else:
                period = f"{location.min_period.name}–{location.max_period.name}"
            print(f"  [{status}] {target}; {region}; {period}")
        elif status == "create":
            proposal = plan.proposals[name]
            coordinates = ""
            if "latitude" in proposal:
                coordinates = f"; {proposal['latitude']} {proposal['longitude']}"
            print(
                f"  [create] {name}; {proposal['region'].name}; "
                f"{proposal['period'].name}{coordinates}"
            )
        else:
            print(f"  [{status}] {name}")
    for error in plan.errors:
        print(f"[error] {error}")
    for warning in plan.warnings:
        print(f"[warning] {warning}")


def apply_plan(plan: LocationPlan) -> dict[str, models.Location | None]:
    if plan.errors:
        raise LocationFileError("cannot apply a location plan with conflicts")
    for name in sorted(plan.used_names):
        status = plan.statuses[name]
        if status not in {"create", "restore"}:
            continue
        proposal = plan.proposals[name]
        if status == "restore":
            location = plan.locations[name]
            assert location is not None
            assert location.deleted is LocationStatus.deleted
            location.deleted = LocationStatus.valid
            if location.comment is None and proposal.get("comment") is not None:
                location.comment = proposal["comment"]
            if location.source is None and proposal.get("source") is not None:
                location.source = proposal["source"]
            if (
                not location.location_detail
                and proposal.get("location_detail") is not None
            ):
                location.location_detail = proposal["location_detail"]  # type: ignore[assignment]
            existing_tags = list(location.tags)
            location.tags = (  # type: ignore[assignment]
                *existing_tags,
                *(tag for tag in proposal.get("tags", []) if tag not in existing_tags),
            )
        else:
            location = models.Location.make(
                name,
                proposal["region"],
                proposal["period"],
                comment=proposal.get("comment"),
            )
            location.latitude = proposal.get("latitude")
            location.longitude = proposal.get("longitude")
            location.source = proposal.get("source")
            if "location_detail" in proposal:
                location.location_detail = proposal["location_detail"]  # type: ignore[assignment]
            location.tags = tuple(proposal.get("tags", []))  # type: ignore[assignment]
        location.format(quiet=True)
        location.edit_until_clean()
        plan.locations[name] = location
        plan.statuses[name] = "existing"
        print(f"{status}d Location: {location}")
    return plan.locations
