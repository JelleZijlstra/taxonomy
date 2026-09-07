"""Backend for generic object, field, ADT-tag, and manual-review recommendations.

These actions provide an extensible baseline for simple database changes that do not
need a domain-specific planner. Values are guarded by an explicit old-value snapshot;
foreign-key and enum values use structured JSON representations.
"""

import enum
import json
import re
import typing
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from taxonomy import adt
from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db import constants, models
from taxonomy.db.models import BaseModel
from taxonomy.db.models.base import ADTField
from taxonomy.db.models.person import AuthorTag, normalize_orcid

SCHEMA_VERSION = 1
SCHEMA_VERSION_2 = 2
CREATE_OBJECT = "create_object"
UPDATE_OBJECT = "update_object"
SET_FIELD = "set_field"
ADD_TAG = "add_tag"
REMOVE_TAG = "remove_tag"
MANUAL_REVIEW = "manual_review"
MERGE_COLLECTION = "merge_collection"
MERGE_PERSON = "merge_person"
REASSIGN_PERSON_REFERENCES = "reassign_person_references"
MERGE_REGION = "merge_region"
DELETE_REGION = "delete_region"
ALLOWED_ACTIONS = {
    CREATE_OBJECT,
    UPDATE_OBJECT,
    SET_FIELD,
    ADD_TAG,
    REMOVE_TAG,
    MANUAL_REVIEW,
    MERGE_COLLECTION,
    MERGE_PERSON,
    REASSIGN_PERSON_REFERENCES,
    MERGE_REGION,
    DELETE_REGION,
}
ALLOWED_CONFIDENCES = {"high", "medium", "low"}

# Structured recommendation manifests use constructor names because they are easier
# to review than numeric ADT tags. Keep manifests written before the GraphQL tag-name
# cleanup executable; persisted database values already use the unchanged numeric tags.
_LEGACY_ADT_MEMBER_NAMES = {
    ("ArticleTag", "ArticleISSN"): "ISSN",
    ("ArticleTag", "BiblioNoteArticle"): "BiblioNote",
    ("ArticleTag", "LSIDArticle"): "LSID",
    ("ArticleTag", "Date"): "PublicationDate",
    ("CitationGroupTag", "CitationGroupComment"): "Comment",
    ("CitationGroupTag", "CitationGroupURL"): "URL",
    ("CitationGroupTag", "IgnoreLintCitationGroup"): "IgnoreLint",
    ("CitationGroupTag", "OnlineRepository"): "Repository",
    ("ClassificationEntryTag", "AgeClassCE"): "AgeClass",
    ("ClassificationEntryTag", "IgnoreLintClassificationEntry"): "IgnoreLint",
    ("ClassificationEntryTag", "LSIDCE"): "LSID",
    ("IssueDateTag", "CommentIssueDate"): "Comment",
    ("LocationTag", "IgnoreLintLocation"): "IgnoreLint",
    ("OccurrenceRecordTag", "IgnoreLintOccurrenceRecord"): "IgnoreLint",
    ("TaxonTag", "IgnoreLintTaxon"): "IgnoreLint",
    ("TypeTag", "IgnoreLintName"): "IgnoreLint",
    ("TypeTag", "LSIDName"): "LSID",
    ("TypeTag", "RejectedLSIDName"): "RejectedLSID",
}


class RecommendationError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class Evidence:
    kind: str
    text: str


@dataclass(frozen=True, slots=True)
class ObjectSpec:
    model: str
    object_id: int | None
    ref: str | None
    label: str


@dataclass(frozen=True, slots=True)
class Recommendation:
    line_number: int
    action: str
    confidence: str
    reason: str
    evidence: tuple[Evidence, ...]
    object: ObjectSpec
    field: str | None
    old_value: Any = None
    new_value: Any = None
    tag: Any = None
    values: Mapping[str, Any] | None = None
    match: Mapping[str, Any] | None = None
    changes: tuple[Mapping[str, Any], ...] = ()
    target: ObjectSpec | None = None
    object_updates: tuple[Mapping[str, Any], ...] = ()
    schema_version: int = SCHEMA_VERSION


@dataclass(frozen=True, slots=True)
class PlannedAction:
    recommendation: Recommendation
    object: Any
    old_value: Any
    new_value: Any
    already_applied: bool
    changes: tuple[tuple[str, str, Any, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class RecommendationPlan:
    actions: tuple[PlannedAction, ...]
    action_counts: Counter[str]
    references: Mapping[str, BaseModel] | None = None


def _required_value(data: dict[str, Any], key: str, line_number: int) -> Any:
    if key not in data:
        raise RecommendationError(f"line {line_number}: missing {key!r}")
    value = data[key]
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


def _parse_evidence(data: Any, line_number: int) -> tuple[Evidence, ...]:
    if not isinstance(data, list) or not data:
        raise RecommendationError(
            f"line {line_number}: evidence must be a nonempty list"
        )
    output = []
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


def _parse_object(data: Any, line_number: int) -> ObjectSpec:
    if not isinstance(data, dict):
        raise RecommendationError(f"line {line_number}: object must be an object")
    object_id = data.get("id")
    ref = data.get("ref")
    if object_id is not None and (
        not isinstance(object_id, int) or isinstance(object_id, bool)
    ):
        raise RecommendationError(f"line {line_number}: object id must be an integer")
    if ref is not None and (not isinstance(ref, str) or not ref):
        raise RecommendationError(
            f"line {line_number}: object ref must be a nonempty string"
        )
    if (object_id is None) == (ref is None):
        raise RecommendationError(
            f"line {line_number}: object must contain exactly one of 'id' or 'ref'"
        )
    return ObjectSpec(
        _required_str(data, "model", line_number),
        object_id,
        ref,
        _required_str(data, "label", line_number),
    )


def _parse_merge_object_updates(
    data: Any, line_number: int
) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(data, list) or not data:
        raise RecommendationError(
            f"line {line_number}: merge_collection requires nonempty object_updates"
        )
    output: list[Mapping[str, Any]] = []
    for update_index, update in enumerate(data, start=1):
        if not isinstance(update, dict):
            raise RecommendationError(
                f"line {line_number}: object update {update_index} must be an object"
            )
        object_spec = _parse_object(update.get("object"), line_number)
        if object_spec.object_id is None:
            raise RecommendationError(
                f"line {line_number}: merge_collection object updates require ids"
            )
        changes = update.get("changes")
        if not isinstance(changes, list) or not changes:
            raise RecommendationError(
                f"line {line_number}: object update {update_index} requires changes"
            )
        parsed_changes: list[Mapping[str, Any]] = []
        for change_index, change in enumerate(changes, start=1):
            if not isinstance(change, dict):
                raise RecommendationError(
                    f"line {line_number}: object update {update_index} change "
                    f"{change_index} must be an object"
                )
            if change.get("operation") != "set":
                raise RecommendationError(
                    f"line {line_number}: merge_collection object changes must "
                    "use operation 'set'"
                )
            if not isinstance(change.get("field"), str) or not change["field"]:
                raise RecommendationError(
                    f"line {line_number}: object update {update_index} change "
                    f"{change_index} requires field"
                )
            if "old_value" not in change or "new_value" not in change:
                raise RecommendationError(
                    f"line {line_number}: object update {update_index} change "
                    f"{change_index} requires old_value and new_value"
                )
            parsed_changes.append(change)
        output.append({"object": object_spec, "changes": tuple(parsed_changes)})
    return tuple(output)


def parse_recommendation(data: dict[str, Any], line_number: int) -> Recommendation:
    version = _required_int(data, "schema_version", line_number)
    if version not in {SCHEMA_VERSION, SCHEMA_VERSION_2}:
        raise RecommendationError(
            f"line {line_number}: unsupported schema_version {version}"
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
    object_spec = _parse_object(data.get("object"), line_number)
    if action == DELETE_REGION:
        if version != SCHEMA_VERSION_2:
            raise RecommendationError(
                f"line {line_number}: delete_region requires schema_version 2"
            )
        if object_spec.model != "Region" or object_spec.object_id is None:
            raise RecommendationError(
                f"line {line_number}: delete_region object must be an existing Region"
            )
        guard = data.get("guard")
        if not isinstance(guard, dict) or not guard:
            raise RecommendationError(
                f"line {line_number}: delete_region requires a nonempty guard"
            )
        return Recommendation(
            line_number,
            action,
            confidence,
            reason,
            evidence,
            object_spec,
            None,
            match=guard,
            schema_version=version,
        )
    if action in {
        MERGE_COLLECTION,
        MERGE_PERSON,
        REASSIGN_PERSON_REFERENCES,
        MERGE_REGION,
    }:
        if version != SCHEMA_VERSION_2:
            raise RecommendationError(
                f"line {line_number}: {action} requires schema_version 2"
            )
        expected_model = {
            MERGE_COLLECTION: "Collection",
            MERGE_PERSON: "Person",
            REASSIGN_PERSON_REFERENCES: "Person",
            MERGE_REGION: "Region",
        }[action]
        if object_spec.model != expected_model or object_spec.object_id is None:
            raise RecommendationError(
                f"line {line_number}: {action} object must be an existing "
                f"{expected_model}"
            )
        target = _parse_object(data.get("target"), line_number)
        if target.model != expected_model or target.object_id is None:
            raise RecommendationError(
                f"line {line_number}: {action} target must be an existing "
                f"{expected_model}"
            )
        if target.object_id == object_spec.object_id:
            raise RecommendationError(
                f"line {line_number}: {action} source and target must differ"
            )
        person_guard: Mapping[str, Any] | None = None
        person_resolution: Mapping[str, Any] | None = None
        if action in {MERGE_PERSON, REASSIGN_PERSON_REFERENCES}:
            guard = data.get("guard")
            if not isinstance(guard, dict):
                raise RecommendationError(
                    f"line {line_number}: {action} requires a guard object"
                )
            if set(guard) != {"source", "target", "references"}:
                raise RecommendationError(
                    f"line {line_number}: {action} guard must contain exactly "
                    "source, target, and references"
                )
            if not isinstance(guard["source"], dict) or not isinstance(
                guard["target"], dict
            ):
                raise RecommendationError(
                    f"line {line_number}: {action} source and target guards "
                    "must be objects"
                )
            references = guard["references"]
            if not isinstance(references, dict) or not all(
                isinstance(name, str)
                and isinstance(ids, list)
                and all(isinstance(object_id, int) for object_id in ids)
                for name, ids in references.items()
            ):
                raise RecommendationError(
                    f"line {line_number}: {action} references must map names to "
                    "integer ID lists"
                )
            person_guard = guard
            if action == MERGE_PERSON:
                resolved_naming_convention = data.get("resolved_naming_convention")
                if resolved_naming_convention is not None:
                    if not isinstance(resolved_naming_convention, dict):
                        raise RecommendationError(
                            f"line {line_number}: merge_person "
                            "resolved_naming_convention must be an enum value"
                        )
                    person_resolution = {
                        "naming_convention": resolved_naming_convention
                    }
            else:
                person_resolution = {"orcid": _required_str(data, "orcid", line_number)}
        return Recommendation(
            line_number,
            action,
            confidence,
            reason,
            evidence,
            object_spec,
            None,
            target=target,
            values=person_resolution,
            object_updates=(
                _parse_merge_object_updates(data.get("object_updates"), line_number)
                if action == MERGE_COLLECTION
                else ()
            ),
            match=person_guard,
            schema_version=version,
        )
    if action == CREATE_OBJECT:
        if object_spec.ref is None:
            raise RecommendationError(
                f"line {line_number}: create_object requires object.ref"
            )
        values = data.get("values")
        if not isinstance(values, dict) or not values:
            raise RecommendationError(
                f"line {line_number}: create_object requires nonempty values"
            )
        if not all(isinstance(key, str) and key for key in values):
            raise RecommendationError(
                f"line {line_number}: create_object field names must be strings"
            )
        match = data.get("match")
        if version == SCHEMA_VERSION_2 and (not isinstance(match, dict) or not match):
            raise RecommendationError(
                f"line {line_number}: schema-version 2 create_object requires "
                "nonempty match"
            )
        if match is not None and (
            not isinstance(match, dict)
            or not all(isinstance(key, str) and key for key in match)
        ):
            raise RecommendationError(
                f"line {line_number}: match must be an object with string field names"
            )
        return Recommendation(
            line_number,
            action,
            confidence,
            reason,
            evidence,
            object_spec,
            None,
            values=values,
            match=match,
            schema_version=version,
        )
    if action == UPDATE_OBJECT:
        changes = data.get("changes")
        if version != SCHEMA_VERSION_2:
            raise RecommendationError(
                f"line {line_number}: update_object requires schema_version 2"
            )
        if not isinstance(changes, list) or not changes:
            raise RecommendationError(
                f"line {line_number}: update_object requires nonempty changes"
            )
        parsed_changes: list[Mapping[str, Any]] = []
        for index, change in enumerate(changes, start=1):
            if not isinstance(change, dict):
                raise RecommendationError(
                    f"line {line_number}: change {index} must be an object"
                )
            operation = change.get("operation")
            if operation not in {"set", "add", "remove", "remove_raw", "normalize"}:
                raise RecommendationError(
                    f"line {line_number}: change {index} has unsupported operation "
                    f"{operation!r}"
                )
            if not isinstance(change.get("field"), str) or not change["field"]:
                raise RecommendationError(
                    f"line {line_number}: change {index} requires field"
                )
            if operation == "set":
                required = {"old_value", "new_value"}
            elif operation == "remove_raw":
                required = {"raw_value"}
                raw_value = change.get("raw_value")
                if (
                    not isinstance(raw_value, list)
                    or not raw_value
                    or not isinstance(raw_value[0], int)
                ):
                    raise RecommendationError(
                        f"line {line_number}: change {index} raw_value must be a "
                        "serialized ADT list"
                    )
            elif operation in {"add", "remove"}:
                required = {"value"}
            else:
                required = set()
            missing = required - change.keys()
            if missing:
                raise RecommendationError(
                    f"line {line_number}: change {index} is missing "
                    f"{', '.join(sorted(missing))}"
                )
            parsed_changes.append(change)
        return Recommendation(
            line_number,
            action,
            confidence,
            reason,
            evidence,
            object_spec,
            None,
            changes=tuple(parsed_changes),
            schema_version=version,
        )
    if action == MANUAL_REVIEW:
        if object_spec.object_id is None:
            raise RecommendationError(
                f"line {line_number}: manual_review requires an existing object id"
            )
        return Recommendation(
            line_number,
            action,
            confidence,
            reason,
            evidence,
            object_spec,
            None,
            schema_version=version,
        )
    field = _required_str(data, "field", line_number)
    if action == SET_FIELD:
        if "old_value" not in data or "new_value" not in data:
            raise RecommendationError(
                f"line {line_number}: set_field requires old_value and new_value"
            )
        return Recommendation(
            line_number,
            action,
            confidence,
            reason,
            evidence,
            object_spec,
            field,
            old_value=data["old_value"],
            new_value=data["new_value"],
            schema_version=version,
        )
    if "tag" not in data:
        raise RecommendationError(f"line {line_number}: {action} requires tag")
    if not isinstance(data["tag"], list):
        raise RecommendationError(
            f"line {line_number}: tag must be a serialized ADT list"
        )
    return Recommendation(
        line_number,
        action,
        confidence,
        reason,
        evidence,
        object_spec,
        field,
        tag=data["tag"],
        schema_version=version,
    )


def get_model_registry() -> dict[str, type[BaseModel]]:
    registry: dict[str, type[BaseModel]] = {}
    for name in models.__all__:
        value = getattr(models, name, None)
        if (
            isinstance(value, type)
            and issubclass(value, BaseModel)
            and value is not BaseModel
        ):
            registry[name] = value
    return registry


def _get_object(model: type[BaseModel], object_id: int) -> BaseModel:
    try:
        return model.get(id=object_id)
    except model.DoesNotExist as exc:
        raise RecommendationError(
            f"{model.__name__} {object_id} does not exist"
        ) from exc


def _object_label(model: type[BaseModel], obj: Any) -> str:
    return str(getattr(obj, model.label_field))


def _decode_reference(
    data: Any,
    expected_model: type[BaseModel],
    *,
    context: str,
    references: Mapping[str, BaseModel],
) -> BaseModel | None:
    if data is None:
        return None
    if not isinstance(data, dict):
        raise RecommendationError(
            f"{context}: foreign-key value must be an object or null"
        )
    model_name = data.get("model")
    if model_name != expected_model.__name__:
        raise RecommendationError(
            f"{context}: expected model {expected_model.__name__!r}, got {model_name!r}"
        )
    object_id = data.get("id")
    ref = data.get("ref")
    label = data.get("label")
    if object_id is not None and (
        not isinstance(object_id, int) or isinstance(object_id, bool)
    ):
        raise RecommendationError(f"{context}: foreign-key id must be an integer")
    if ref is not None and (not isinstance(ref, str) or not ref):
        raise RecommendationError(
            f"{context}: foreign-key ref must be a nonempty string"
        )
    if (object_id is None) == (ref is None):
        raise RecommendationError(
            f"{context}: foreign-key value must contain exactly one of 'id' or 'ref'"
        )
    if label is not None and (not isinstance(label, str) or not label):
        raise RecommendationError(
            f"{context}: foreign-key label must be a nonempty string"
        )
    if object_id is not None and label is None:
        raise RecommendationError(
            f"{context}: persisted foreign-key value requires a snapshot label"
        )
    if ref is not None:
        try:
            obj = references[ref]
        except KeyError as exc:
            raise RecommendationError(
                f"{context}: unknown or forward object ref {ref!r}"
            ) from exc
        if not isinstance(obj, expected_model):
            raise RecommendationError(
                f"{context}: ref {ref!r} is a {type(obj).__name__}, expected "
                f"{expected_model.__name__}"
            )
    else:
        assert object_id is not None
        obj = _get_object(expected_model, object_id)
    actual_label = _object_label(expected_model, obj)
    if label is not None and actual_label != label:
        identity = f"ref {ref!r}" if ref is not None else str(object_id)
        raise RecommendationError(
            f"{context}: {expected_model.__name__} {identity} changed from "
            f"{label!r} to {actual_label!r}"
        )
    return obj


def _decode_enum(data: Any, enum_type: type[enum.Enum], *, context: str) -> enum.Enum:
    if isinstance(data, str):
        try:
            return enum_type[data]
        except KeyError as exc:
            raise RecommendationError(
                f"{context}: unknown {enum_type.__name__} member {data!r}"
            ) from exc
    if not isinstance(data, dict):
        raise RecommendationError(f"{context}: enum value must be an object")
    if data.get("enum") != enum_type.__name__:
        raise RecommendationError(
            f"{context}: expected enum {enum_type.__name__!r}, got {data.get('enum')!r}"
        )
    name = data.get("name")
    if not isinstance(name, str):
        raise RecommendationError(f"{context}: enum name must be a string")
    try:
        return enum_type[name]
    except KeyError as exc:
        raise RecommendationError(
            f"{context}: unknown {enum_type.__name__} member {name!r}"
        ) from exc


def _decode_field_value(
    field: Any, data: Any, *, context: str, references: Mapping[str, BaseModel]
) -> Any:
    field.resolve_type()
    value_type = field._type_object
    allow_none = field._allow_none
    if data is None:
        if not allow_none:
            raise RecommendationError(f"{context}: value cannot be null")
        return None
    if isinstance(value_type, type) and issubclass(value_type, BaseModel):
        return _decode_reference(
            data, value_type, context=context, references=references
        )
    if isinstance(value_type, type) and issubclass(value_type, enum.Enum):
        return _decode_enum(data, value_type, context=context)
    if value_type not in {str, int, float, bool}:
        raise RecommendationError(
            f"{context}: field type {value_type!r} is not supported by set_field; "
            "add a specialized action"
        )
    if type(data) is not value_type:
        raise RecommendationError(
            f"{context}: expected {value_type.__name__}, got {type(data).__name__}"
        )
    return data


def _decode_adt_argument(
    argument_type: object,
    data: Any,
    *,
    context: str,
    references: Mapping[str, BaseModel],
) -> Any:
    origin = typing.get_origin(argument_type)
    if origin in {typing.Required, typing.NotRequired}:
        (argument_type,) = typing.get_args(argument_type)
    argument_type = adt.unwrap_type(argument_type)
    if isinstance(argument_type, type) and issubclass(argument_type, BaseModel):
        return _decode_reference(
            data, argument_type, context=context, references=references
        )
    if isinstance(argument_type, type) and issubclass(argument_type, enum.Enum):
        return _decode_enum(data, argument_type, context=context)
    if isinstance(argument_type, type) and issubclass(argument_type, adt.ADT):
        return _decode_structured_adt(
            argument_type, data, context=context, references=references
        )
    return data


def _decode_structured_adt(
    adt_type: type[adt.ADT],
    data: Any,
    *,
    context: str,
    references: Mapping[str, BaseModel],
) -> adt.ADT:
    if not isinstance(data, dict):
        raise RecommendationError(f"{context}: structured tag must be an object")
    tag_name = data.get("tag")
    arguments = data.get("arguments", {})
    if not isinstance(tag_name, str) or not isinstance(arguments, dict):
        raise RecommendationError(
            f"{context}: structured tag requires string tag and object arguments"
        )
    tag_name = _LEGACY_ADT_MEMBER_NAMES.get((adt_type.__name__, tag_name), tag_name)
    member = next(
        (
            candidate
            for candidate in adt_type._tag_to_member.values()
            if getattr(candidate, "__name__", type(candidate).__name__) == tag_name
        ),
        None,
    )
    if member is None:
        raise RecommendationError(f"{context}: unknown tag {tag_name!r}")
    expected = set(getattr(member, "_attributes", ()))
    required = set(getattr(member, "__required_attrs__", expected))
    if not required <= set(arguments) or not set(arguments) <= expected:
        raise RecommendationError(
            f"{context}: {tag_name} arguments require "
            f"{', '.join(sorted(required)) or '(none)'} and allow only "
            f"{', '.join(sorted(expected)) or '(none)'}"
        )
    if not getattr(member, "_has_args", False):
        return cast(adt.ADT, member)
    decoded = {
        name: _decode_adt_argument(
            argument_type,
            arguments[name],
            context=f"{context}.{tag_name}.{name}",
            references=references,
        )
        for name, argument_type in member._attributes.items()
        if name in arguments
    }
    return member(**decoded)


def _decode_tag(
    field: ADTField[Any],
    data: Any,
    *,
    context: str,
    references: Mapping[str, BaseModel] | None = None,
) -> adt.ADT:
    if isinstance(data, dict):
        return _decode_structured_adt(
            field.adt_type, data, context=context, references=references or {}
        )
    try:
        return field.adt_type.unserialize(data)
    except Exception as exc:
        raise RecommendationError(f"{context}: invalid serialized tag: {exc}") from exc


def _format_serialized_adt_argument(argument_type: object, data: Any) -> str:
    origin = typing.get_origin(argument_type)
    if origin in {typing.Required, typing.NotRequired}:
        (argument_type,) = typing.get_args(argument_type)
    argument_type = adt.unwrap_type(argument_type)
    if isinstance(argument_type, type) and issubclass(argument_type, enum.Enum):
        try:
            member = argument_type(data)
        except TypeError, ValueError:
            return repr(data)
        return f"{argument_type.__name__}.{member.name}"
    if isinstance(argument_type, type) and issubclass(argument_type, BaseModel):
        return f"{argument_type.__name__}(id={data})"
    if isinstance(argument_type, type) and issubclass(argument_type, adt.ADT):
        return _format_serialized_adt(argument_type, data)
    return repr(data)


def _format_serialized_adt(adt_type: type[adt.ADT], data: Any) -> str:
    """Render a serialized ADT without resolving database-backed references."""
    if not isinstance(data, list) or not data or not isinstance(data[0], int):
        return repr(data)
    member = adt_type._tag_to_member.get(data[0])
    if member is None:
        return repr(data)
    if not member._has_args:
        return member.__name__ if isinstance(member, type) else type(member).__name__
    arguments = [
        f"{name}={_format_serialized_adt_argument(argument_type, value)}"
        for (name, argument_type), value in zip(
            member._attributes.items(), data[1:], strict=False
        )
    ]
    return f"{member.__name__}({', '.join(arguments)})"


def _format_serialized_tag(field: ADTField[Any], data: Any) -> str:
    if isinstance(data, Mapping):
        tag_name = data.get("tag")
        arguments = data.get("arguments", {})
        if isinstance(tag_name, str) and isinstance(arguments, Mapping):
            if not arguments:
                return tag_name
            formatted_arguments = ", ".join(
                f"{name}={_format_manifest_value(value)}"
                for name, value in arguments.items()
            )
            return f"{tag_name}({formatted_arguments})"
    return _format_serialized_adt(field.adt_type, data)


def _format_manifest_value(value: Any) -> str:
    """Render a typed manifest value without resolving database objects."""
    if isinstance(value, Mapping):
        model = value.get("model")
        label = value.get("label")
        if isinstance(model, str):
            parts: list[str] = []
            if "id" in value:
                parts.append(f"id={value['id']!r}")
            elif "ref" in value:
                parts.append(f"ref={value['ref']!r}")
            if label is not None:
                parts.append(f"label={label!r}")
            return f"{model}({', '.join(parts)})"
        enum_name = value.get("enum")
        member_name = value.get("name")
        if isinstance(enum_name, str) and isinstance(member_name, str):
            return f"{enum_name}.{member_name}"
    return repr(value)


def _format_update_change(
    row: Recommendation,
    change: Mapping[str, Any],
    registry: Mapping[str, type[BaseModel]],
) -> str:
    operation = cast(str, change["operation"])
    field_name = cast(str, change["field"])
    if operation == "set":
        return (
            f"set {field_name}: {_format_manifest_value(change['old_value'])} -> "
            f"{_format_manifest_value(change['new_value'])}"
        )
    if operation == "remove_raw":
        return f"remove_raw {field_name}: {change['raw_value']!r}"
    if operation == "normalize":
        return f"normalize {field_name}"
    model = registry.get(row.object.model)
    field = None if model is None else model.clirm_fields.get(field_name)
    value = change["value"]
    formatted_value = (
        _format_serialized_tag(field, value)
        if isinstance(field, ADTField)
        else repr(value)
    )
    return f"{operation} {field_name}: {formatted_value}"


def _format_create_value(
    model: type[BaseModel] | None, field_name: str, value: Any
) -> str:
    field = None if model is None else model.clirm_fields.get(field_name)
    if isinstance(field, ADTField) and isinstance(value, list):
        return (
            "[" + ", ".join(_format_serialized_tag(field, tag) for tag in value) + "]"
        )
    return _format_manifest_value(value)


def _is_empty_review_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str | bytes | Mapping | Sequence | set | frozenset):
        return len(value) == 0
    return False


def _print_review_detail(label: str, text: str) -> None:
    lines = text.splitlines() or [""]
    print(f"    - {label}: {lines[0]}")
    for line in lines[1:]:
        print(f"      {line}")


def _decode_create_values(
    model: type[BaseModel],
    data: Mapping[str, Any],
    *,
    context: str,
    references: Mapping[str, BaseModel],
    require_label: bool = True,
) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for name, serialized in data.items():
        field = model.clirm_fields.get(name)
        if field is None or name == "id":
            raise RecommendationError(
                f"{context}: {model.__name__} has no creatable field {name!r}"
            )
        field_context = f"{context} {model.__name__}.{name}"
        if isinstance(field, ADTField):
            if not isinstance(serialized, list):
                raise RecommendationError(
                    f"{field_context}: ADT field value must be a list of serialized tags"
                )
            decoded = tuple(
                _decode_tag(field, item, context=field_context, references=references)
                for item in serialized
            )
            values[name] = decoded if field.is_ordered else tuple(sorted(set(decoded)))
        else:
            values[name] = _decode_field_value(
                field, serialized, context=field_context, references=references
            )
    label_field = model.label_field
    if require_label and label_field != "id" and label_field not in values:
        raise RecommendationError(
            f"{context}: create_object values must include label field {label_field!r}"
        )
    return values


def _find_objects_by_label(model: type[BaseModel], label: str) -> list[BaseModel]:
    field = model.clirm_fields[model.label_field]
    return list(model.select().filter(field == label))


def _find_objects_by_match(
    model: type[BaseModel], match: Mapping[str, Any]
) -> list[BaseModel]:
    query = model.select()
    for name, value in match.items():
        query = query.filter(model.clirm_fields[name] == value)
    return list(query)


def _quote_sqlite_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _get_unique_constraints(model: type[BaseModel]) -> tuple[tuple[str, ...], ...]:
    """Return ordinary-field UNIQUE constraints declared by the SQLite schema."""
    table = _quote_sqlite_identifier(model.clirm_table_name)
    constraints: list[tuple[str, ...]] = []
    for index_row in model.clirm.conn.execute(f"PRAGMA index_list({table})"):
        _sequence, index_name, is_unique, _origin, is_partial = index_row
        if not is_unique or is_partial:
            continue
        quoted_index = _quote_sqlite_identifier(index_name)
        columns = tuple(
            row[2]
            for row in model.clirm.conn.execute(f"PRAGMA index_info({quoted_index})")
            if row[2] is not None
        )
        if columns and all(column in model.clirm_fields for column in columns):
            constraints.append(columns)
    return tuple(constraints)


def _find_objects_by_unique_constraint(
    model: type[BaseModel], field_names: tuple[str, ...], values: tuple[Any, ...]
) -> list[BaseModel]:
    query = model.select()
    for field_name, value in zip(field_names, values, strict=True):
        query = query.filter(model.clirm_fields[field_name] == value)
    return list(query)


def _iter_refs(value: Any) -> Iterable[str]:
    if isinstance(value, Mapping):
        ref = value.get("ref")
        model = value.get("model")
        if isinstance(ref, str) and isinstance(model, str):
            yield ref
        for nested in value.values():
            yield from _iter_refs(nested)
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes):
        for nested in value:
            yield from _iter_refs(nested)


def _order_by_dependencies(
    rows: Sequence[Recommendation], *, available_refs: set[str]
) -> list[Recommendation]:
    producers: dict[str, Recommendation] = {}
    for row in rows:
        if row.action != CREATE_OBJECT:
            continue
        assert row.object.ref is not None
        if row.object.ref in available_refs or row.object.ref in producers:
            raise RecommendationError(
                f"line {row.line_number}: duplicate object ref {row.object.ref!r}"
            )
        producers[row.object.ref] = row
    dependencies: dict[int, set[str]] = {}
    for row in rows:
        values: list[Any] = [
            row.values,
            row.match,
            row.changes,
            row.target,
            row.object_updates,
        ]
        if row.action != CREATE_OBJECT and row.object.ref is not None:
            values.append({"model": row.object.model, "ref": row.object.ref})
        refs = {ref for value in values for ref in _iter_refs(value)}
        if row.action == CREATE_OBJECT:
            refs.discard(row.object.ref)
        missing = refs - available_refs - producers.keys()
        if missing:
            raise RecommendationError(
                f"line {row.line_number}: unknown object ref(s) "
                f"{', '.join(sorted(missing))}"
            )
        dependencies[row.line_number] = refs & producers.keys()
    pending = list(rows)
    resolved = set(available_refs)
    ordered: list[Recommendation] = []
    while pending:
        ready = [row for row in pending if dependencies[row.line_number] <= resolved]
        if not ready:
            lines = ", ".join(str(row.line_number) for row in pending)
            raise RecommendationError(
                f"reference dependency cycle involving manifest lines {lines}"
            )
        for row in ready:
            pending.remove(row)
            ordered.append(row)
            if row.action == CREATE_OBJECT:
                assert row.object.ref is not None
                resolved.add(row.object.ref)
    return ordered


def _values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, BaseModel) and isinstance(right, BaseModel):
        return type(left) is type(right) and left.id == right.id
    return left == right


_PERSON_MERGE_GUARD_FIELDS = (
    "family_name",
    "given_names",
    "initials",
    "suffix",
    "tussenvoegsel",
    "birth",
    "death",
    "tags",
    "naming_convention",
    "type",
    "target",
    "bio",
    "ol_id",
)
_PERSON_MERGE_REFERENCE_FIELDS = (
    "books",
    "articles",
    "names",
    "patronyms",
    "collected",
    "involved",
    "targets",
)
_PERSON_REASSIGN_REFERENCE_FIELDS = tuple(
    field_name
    for field_name in _PERSON_MERGE_REFERENCE_FIELDS
    if field_name != "targets"
)
_PERSON_MERGE_TRANSFER_FIELDS = ("birth", "death", "bio", "ol_id")


def _person_reference_snapshot(person: models.Person) -> dict[str, list[int]]:
    snapshot = {
        field_name: sorted(person.get_raw_derived_field(field_name) or ())
        for field_name in _PERSON_MERGE_REFERENCE_FIELDS
        if field_name != "targets"
    }
    snapshot["targets"] = sorted(
        alias.id
        for alias in models.Person.select().filter(models.Person.target == person)
    )
    return snapshot


def _person_reassign_reference_snapshot(person: models.Person) -> dict[str, list[int]]:
    return {
        field_name: sorted(person.get_raw_derived_field(field_name) or ())
        for field_name in _PERSON_REASSIGN_REFERENCE_FIELDS
    }


def _person_tags_without_orcid(tags: Iterable[Any], orcid: str) -> tuple[Any, ...]:
    return tuple(
        tag
        for tag in tags
        if not (
            isinstance(tag, models.tags.PersonTag.ORCID)
            and normalize_orcid(tag.text) == orcid
        )
    )


def _person_tags_contain_orcid(tags: Iterable[Any], orcid: str) -> bool:
    return any(
        isinstance(tag, models.tags.PersonTag.ORCID)
        and normalize_orcid(tag.text) == orcid
        for tag in tags
    )


def _combine_person_naming_conventions(
    *conventions: constants.NamingConvention,
) -> constants.NamingConvention:
    """Combine compatible conventions, treating ``general`` as a fallback."""
    specific = {
        convention
        for convention in conventions
        if convention
        not in {
            constants.NamingConvention.unspecified,
            constants.NamingConvention.general,
        }
    }
    if len(specific) > 1:
        raise RecommendationError("merge_person has conflicting naming conventions")
    if specific:
        return next(iter(specific))
    if constants.NamingConvention.general in conventions:
        return constants.NamingConvention.general
    return constants.NamingConvention.unspecified


def _validate_create_invariants(
    model: type[BaseModel], values: Mapping[str, Any], *, context: str
) -> None:
    if model.__name__ == "ClassificationEntry":
        page = values.get("page")
        if (
            not isinstance(page, str)
            or re.fullmatch(r"[1-9][0-9]*(?:, [1-9][0-9]*)*", page) is None
        ):
            raise RecommendationError(
                f"{context}: ClassificationEntry.page must be one or more "
                "comma-separated page numbers"
            )
        parent = values.get("parent")
        article = values.get("article")
        if parent is not None and getattr(parent, "article", None) != article:
            raise RecommendationError(
                f"{context}: ClassificationEntry parent must belong to the same Article"
            )
    elif model.__name__ == "OccurrenceRecord":
        location = values.get("location")
        tags = values.get("tags") or ()
        if location is None and not any(
            type(tag).__name__ == "LocationHint" for tag in tags
        ):
            raise RecommendationError(
                f"{context}: OccurrenceRecord requires a Location or LocationHint"
            )


def _same_model(left: BaseModel, right: BaseModel) -> bool:
    return type(left) is type(right) and left.id == right.id


def _contains_model(value: Any, target: BaseModel) -> bool:
    return any(
        _same_model(candidate, target) for candidate in _iter_model_values(value)
    )


def _find_collection_references(collection: BaseModel) -> list[tuple[BaseModel, str]]:
    """Return indexed ordinary-field references to one Collection."""
    output: list[tuple[BaseModel, str]] = []
    for field, obj in collection.get_direct_backrefs(include_invalid=True):
        field_name = next(
            name
            for name, candidate in type(obj).clirm_fields.items()
            if candidate is field
        )
        output.append((obj, field_name))
    return output


def _decode_complete_field_value(
    field: Any, data: Any, *, context: str, references: Mapping[str, BaseModel]
) -> Any:
    if not isinstance(field, ADTField):
        return _decode_field_value(field, data, context=context, references=references)
    if not isinstance(data, list):
        raise RecommendationError(
            f"{context}: a complete ADT field snapshot must be a list"
        )
    decoded = tuple(
        _decode_tag(
            field, serialized, context=f"{context}[{index}]", references=references
        )
        for index, serialized in enumerate(data, start=1)
    )
    return decoded if field.is_ordered else tuple(sorted(set(decoded)))


def _normalize_merge_target_snapshot(
    data: Any, *, target_spec: ObjectSpec, target: BaseModel
) -> Any:
    """Validate the planned target label, then decode against its current DB label."""
    if isinstance(data, Mapping):
        if (
            data.get("model") == "Collection"
            and data.get("id") == target_spec.object_id
        ):
            if data.get("label") != target_spec.label:
                raise RecommendationError(
                    "merge target reference label does not match the target snapshot"
                )
            return {**data, "label": _object_label(type(target), target)}
        return {
            key: _normalize_merge_target_snapshot(
                value, target_spec=target_spec, target=target
            )
            for key, value in data.items()
        }
    if isinstance(data, list):
        return [
            _normalize_merge_target_snapshot(
                value, target_spec=target_spec, target=target
            )
            for value in data
        ]
    return data


def build_plan(
    recommendations: Iterable[Recommendation],
    *,
    model_registry: Mapping[str, type[BaseModel]] | None = None,
    get_object: Callable[[type[BaseModel], int], Any] = _get_object,
    find_objects_by_label: Callable[
        [type[BaseModel], str], list[BaseModel]
    ] = _find_objects_by_label,
    find_objects_by_match: Callable[
        [type[BaseModel], Mapping[str, Any]], list[BaseModel]
    ] = _find_objects_by_match,
    get_unique_constraints: Callable[
        [type[BaseModel]], tuple[tuple[str, ...], ...]
    ] = _get_unique_constraints,
    find_objects_by_unique_constraint: Callable[
        [type[BaseModel], tuple[str, ...], tuple[Any, ...]], list[BaseModel]
    ] = _find_objects_by_unique_constraint,
    initial_references: Mapping[str, BaseModel] | None = None,
    allow_manual_label_changes: bool = False,
    find_collection_references: Callable[
        [BaseModel], list[tuple[BaseModel, str]]
    ] = _find_collection_references,
) -> RecommendationPlan:
    original_rows = list(recommendations)
    try:
        rows = _order_by_dependencies(
            original_rows, available_refs=set(initial_references or {})
        )
    except RecommendationError as exc:
        raise RecommendationError(
            f"refusing to continue because manifest validation failed: {exc}"
        ) from exc
    registry = model_registry or get_model_registry()
    actions: list[PlannedAction] = []
    errors: list[str] = []
    seen: set[tuple[str, str, str | None, str]] = set()
    planned_values: dict[tuple[str, str, str], Any] = {}
    references: dict[str, BaseModel] = dict(initial_references or {})
    planned_creates: list[tuple[Recommendation, BaseModel]] = []
    object_identities: dict[int, str] = {}
    unique_constraints: dict[type[BaseModel], tuple[tuple[str, ...], ...]] = {}

    def check_unique_assignment(
        model: type[BaseModel], obj: BaseModel, identity: str, field: str, value: Any
    ) -> None:
        if model not in unique_constraints:
            unique_constraints[model] = get_unique_constraints(model)
        for constraint in unique_constraints[model]:
            if field not in constraint:
                continue
            values = tuple(
                (
                    value
                    if name == field
                    else planned_values.get(
                        (model.__name__, identity, name), getattr(obj, name)
                    )
                )
                for name in constraint
            )
            if any(item is None for item in values):
                continue
            candidates = find_objects_by_unique_constraint(model, constraint, values)
            candidates.extend(
                action.object for action in actions if type(action.object) is model
            )
            for candidate in candidates:
                if candidate is obj or (
                    obj.id is not None
                    and candidate.id is not None
                    and obj.id == candidate.id
                ):
                    continue
                candidate_identity = object_identities.get(
                    id(candidate), f"id:{candidate.id}"
                )
                candidate_values = tuple(
                    planned_values.get(
                        (model.__name__, candidate_identity, name),
                        getattr(candidate, name),
                    )
                    for name in constraint
                )
                if candidate_values == values:
                    raise RecommendationError(
                        f"update violates UNIQUE constraint {model.__name__}"
                        f"({', '.join(constraint)}) with values {values!r}; "
                        f"conflicts with {model.__name__} {candidate.id}"
                    )

    for row in rows:
        try:
            model = registry.get(row.object.model)
            if model is None:
                raise RecommendationError(f"unknown model {row.object.model!r}")
            identity = (
                f"id:{row.object.object_id}"
                if row.object.object_id is not None
                else f"ref:{row.object.ref}"
            )
            if row.action == CREATE_OBJECT:
                assert row.object.ref is not None
                assert row.values is not None
                if row.object.ref in references:
                    raise RecommendationError(
                        f"duplicate create_object ref {row.object.ref!r}"
                    )
                values = _decode_create_values(
                    model,
                    row.values,
                    context=f"line {row.line_number}",
                    references=references,
                )
                if row.schema_version == SCHEMA_VERSION_2:
                    _validate_create_invariants(
                        model, values, context=f"line {row.line_number}"
                    )
                if model.label_field in values:
                    label = str(values[model.label_field])
                    if label != row.object.label:
                        raise RecommendationError(
                            f"create_object label {row.object.label!r} does not match "
                            f"values.{model.label_field} {label!r}"
                        )
                elif row.match is None:
                    raise RecommendationError(
                        "create_object for a model with an auto-generated ID requires "
                        "a match guard"
                    )
                if row.match is None:
                    matches = find_objects_by_label(model, row.object.label)
                else:
                    decoded_match = _decode_create_values(
                        model,
                        row.match,
                        context=f"line {row.line_number} match",
                        references=references,
                        require_label=False,
                    )
                    matches = find_objects_by_match(model, decoded_match)
                if len(matches) > 1:
                    raise RecommendationError(
                        f"cannot resume create_object because {len(matches)} "
                        f"{model.__name__} objects have label {row.object.label!r}"
                    )
                if matches:
                    obj = matches[0]
                    mismatches = [
                        name
                        for name, value in values.items()
                        if not _values_equal(getattr(obj, name), value)
                    ]
                    if mismatches:
                        raise RecommendationError(
                            "an object with the requested label already exists but "
                            f"differs in fields {', '.join(mismatches)}"
                        )
                    already_applied = True
                else:
                    obj = model.virtual(**values)
                    already_applied = False
                    for constraint in get_unique_constraints(model):
                        constraint_values = tuple(
                            getattr(obj, field_name) for field_name in constraint
                        )
                        # SQLite permits repeated NULL values in UNIQUE indexes.
                        if any(value is None for value in constraint_values):
                            continue
                        conflicts: list[tuple[str, BaseModel]] = [
                            (f"existing {model.__name__} {conflict.id}", conflict)
                            for conflict in find_objects_by_unique_constraint(
                                model, constraint, constraint_values
                            )
                        ]
                        conflicts.extend(
                            (f"manifest line {create_row.line_number}", candidate)
                            for create_row, candidate in planned_creates
                            if type(candidate) is model
                        )
                        for conflict_label, conflict in conflicts:
                            conflict_identity = object_identities.get(id(conflict)) or (
                                next(
                                    f"ref:{create_row.object.ref}"
                                    for create_row, candidate in planned_creates
                                    if candidate is conflict
                                )
                                if conflict.id is None
                                else f"id:{conflict.id}"
                            )
                            final_conflict_values = tuple(
                                planned_values.get(
                                    (model.__name__, conflict_identity, field_name),
                                    getattr(conflict, field_name),
                                )
                                for field_name in constraint
                            )
                            if final_conflict_values == constraint_values:
                                fields = ", ".join(constraint)
                                values_text = ", ".join(
                                    repr(value) for value in constraint_values
                                )
                                raise RecommendationError(
                                    f"create_object violates UNIQUE constraint "
                                    f"{model.__name__}({fields}) with values "
                                    f"({values_text}); conflicts with {conflict_label}"
                                )
                    planned_creates.append((row, obj))
                    object_identities[id(obj)] = f"ref:{row.object.ref}"
                references[row.object.ref] = obj
                actions.append(
                    PlannedAction(
                        row, obj, None, values, already_applied=already_applied
                    )
                )
                continue
            if row.action in {
                MANUAL_REVIEW,
                UPDATE_OBJECT,
                MERGE_COLLECTION,
                MERGE_PERSON,
                REASSIGN_PERSON_REFERENCES,
                MERGE_REGION,
                DELETE_REGION,
            }:
                mutation = row.action
            elif row.action == SET_FIELD:
                mutation = "field"
            else:
                mutation = repr(row.tag)
            key = (row.object.model, identity, row.field, mutation)
            if key in seen:
                raise RecommendationError(
                    "duplicate recommendation for this object field"
                )
            seen.add(key)
            if row.object.ref is not None:
                try:
                    obj = references[row.object.ref]
                except KeyError as exc:
                    raise RecommendationError(
                        f"unknown or forward object ref {row.object.ref!r}"
                    ) from exc
                if not isinstance(obj, model):
                    raise RecommendationError(
                        f"ref {row.object.ref!r} is a {type(obj).__name__}, expected "
                        f"{model.__name__}"
                    )
            else:
                assert row.object.object_id is not None
                obj = get_object(model, row.object.object_id)
                object_identities[id(obj)] = identity
            label = _object_label(model, obj)
            allowed_object_labels = {row.object.label}
            if row.action == UPDATE_OBJECT:
                allowed_object_labels.update(
                    change["new_value"]
                    for change in row.changes
                    if change["operation"] == "set"
                    and change["field"] == model.label_field
                    and isinstance(change["new_value"], str)
                )
            if label not in allowed_object_labels and not (
                allow_manual_label_changes and row.action == MANUAL_REVIEW
            ):
                raise RecommendationError(
                    f"{model.__name__} {obj.id} changed from {row.object.label!r} "
                    f"to {label!r}"
                )
            if row.action == MERGE_COLLECTION:
                assert row.target is not None
                assert row.target.object_id is not None
                target = get_object(model, row.target.object_id)
                target_identity = f"id:{row.target.object_id}"
                target_label = planned_values.get(
                    ("Collection", target_identity, model.label_field),
                    _object_label(model, target),
                )
                if target_label != row.target.label:
                    raise RecommendationError(
                        f"Collection {target.id} changed from {row.target.label!r} "
                        f"to {target_label!r}"
                    )
                target_removed = planned_values.get(
                    ("Collection", target_identity, "removed"), target.removed
                )
                if target_removed:
                    raise RecommendationError("merge target is removed")

                planned_updates: list[
                    tuple[BaseModel, tuple[tuple[str, Any, Any], ...]]
                ] = []
                covered_fields: set[tuple[type[BaseModel], int, str]] = set()
                updates_applied = True
                for update_index, update in enumerate(row.object_updates, start=1):
                    update_spec = cast(ObjectSpec, update["object"])
                    update_model = registry.get(update_spec.model)
                    if update_model is None:
                        raise RecommendationError(
                            f"object update {update_index}: unknown model "
                            f"{update_spec.model!r}"
                        )
                    assert update_spec.object_id is not None
                    update_obj = get_object(update_model, update_spec.object_id)
                    actual_update_label = _object_label(update_model, update_obj)
                    if actual_update_label != update_spec.label:
                        raise RecommendationError(
                            f"object update {update_index}: {update_model.__name__} "
                            f"{update_obj.id} changed from {update_spec.label!r} to "
                            f"{actual_update_label!r}"
                        )
                    update_identity = f"id:{update_obj.id}"
                    decoded_changes: list[tuple[str, Any, Any]] = []
                    update_fields: set[str] = set()
                    for change_index, change in enumerate(
                        cast(Sequence[Mapping[str, Any]], update["changes"]), start=1
                    ):
                        field_name = cast(str, change["field"])
                        if field_name in update_fields:
                            raise RecommendationError(
                                f"object update {update_index}: duplicate field "
                                f"{field_name!r}"
                            )
                        update_fields.add(field_name)
                        field = update_model.clirm_fields.get(field_name)
                        if field is None or field_name == "id":
                            raise RecommendationError(
                                f"object update {update_index} change {change_index}: "
                                f"{update_model.__name__} has no editable field "
                                f"{field_name!r}"
                            )
                        field_key = (update_model.__name__, update_identity, field_name)
                        actual = planned_values.get(
                            field_key, getattr(update_obj, field_name)
                        )
                        old_value = _decode_complete_field_value(
                            field,
                            change["old_value"],
                            context=(
                                f"object update {update_index} "
                                f"{update_model.__name__}.{field_name} old_value"
                            ),
                            references=references,
                        )
                        new_value = _decode_complete_field_value(
                            field,
                            _normalize_merge_target_snapshot(
                                change["new_value"],
                                target_spec=row.target,
                                target=target,
                            ),
                            context=(
                                f"object update {update_index} "
                                f"{update_model.__name__}.{field_name} new_value"
                            ),
                            references=references,
                        )
                        if _values_equal(actual, new_value):
                            change_applied = True
                        elif _values_equal(actual, old_value):
                            change_applied = False
                        else:
                            raise RecommendationError(
                                f"object update {update_index} change {change_index}: "
                                f"field {field_name!r} is neither the snapshotted "
                                "old value nor the recommended new value"
                            )
                        if _contains_model(new_value, obj):
                            raise RecommendationError(
                                f"object update {update_index} change {change_index}: "
                                "new value still references the merged Collection"
                            )
                        planned_values[field_key] = new_value
                        decoded_changes.append((field_name, old_value, new_value))
                        covered_fields.add(
                            (type(update_obj), update_obj.id, field_name)
                        )
                        updates_applied &= change_applied
                    planned_updates.append((update_obj, tuple(decoded_changes)))

                uncovered = [
                    (referencing_obj, field_name)
                    for referencing_obj, field_name in find_collection_references(obj)
                    if (type(referencing_obj), referencing_obj.id, field_name)
                    not in covered_fields
                ]
                if uncovered:
                    descriptions = ", ".join(
                        f"{type(referencing_obj).__name__} {referencing_obj.id}."
                        f"{field_name}"
                        for referencing_obj, field_name in uncovered[:10]
                    )
                    suffix = " ..." if len(uncovered) > 10 else ""
                    raise RecommendationError(
                        "merge does not account for Collection references: "
                        f"{descriptions}{suffix}"
                    )

                source_parent_key = ("Collection", identity, "parent")
                source_removed_key = ("Collection", identity, "removed")
                source_collection = cast(models.Collection, obj)
                actual_parent = planned_values.get(
                    source_parent_key, source_collection.parent
                )
                actual_removed = planned_values.get(
                    source_removed_key, source_collection.removed
                )
                if actual_parent is not None and not _values_equal(
                    actual_parent, target
                ):
                    raise RecommendationError(
                        "merge source parent is neither null nor the target"
                    )
                if actual_removed not in {False, True}:
                    raise RecommendationError("merge source removed state is invalid")
                source_applied = bool(actual_removed) and _values_equal(
                    actual_parent, target
                )
                planned_values[source_parent_key] = target
                planned_values[source_removed_key] = True
                actions.append(
                    PlannedAction(
                        row,
                        obj,
                        {"parent": actual_parent, "removed": actual_removed},
                        {"target": target, "updates": tuple(planned_updates)},
                        already_applied=source_applied and updates_applied,
                    )
                )
                continue
            if row.action == REASSIGN_PERSON_REFERENCES:
                if not isinstance(obj, models.Person):
                    raise RecommendationError(
                        "reassign_person_references object is not a Person"
                    )
                assert row.target is not None
                assert row.target.object_id is not None
                target = get_object(model, row.target.object_id)
                if not isinstance(target, models.Person):
                    raise RecommendationError(
                        "reassign_person_references target is not a Person"
                    )
                if _object_label(model, target) != row.target.label:
                    raise RecommendationError(
                        f"Person {target.id} changed from {row.target.label!r} to "
                        f"{_object_label(model, target)!r}"
                    )
                if obj.type not in {
                    constants.PersonType.unchecked,
                    constants.PersonType.checked,
                }:
                    raise RecommendationError(
                        "reassign_person_references source is not canonical"
                    )
                if target.type not in {
                    constants.PersonType.unchecked,
                    constants.PersonType.checked,
                }:
                    raise RecommendationError(
                        "reassign_person_references target is not canonical"
                    )
                assert row.match is not None
                assert row.values is not None
                source_guard = cast(Mapping[str, Any], row.match["source"])
                target_guard = cast(Mapping[str, Any], row.match["target"])
                references_guard = cast(Mapping[str, Any], row.match["references"])
                if set(source_guard) != set(_PERSON_MERGE_GUARD_FIELDS) or set(
                    target_guard
                ) != set(_PERSON_MERGE_GUARD_FIELDS):
                    raise RecommendationError(
                        "reassign_person_references source and target guards must "
                        "snapshot every Person field"
                    )
                if set(references_guard) != set(_PERSON_REASSIGN_REFERENCE_FIELDS):
                    raise RecommendationError(
                        "reassign_person_references references guard has unexpected "
                        "fields"
                    )

                def decode_reassign_guard(
                    person: models.Person, guard: Mapping[str, Any], role: str
                ) -> dict[str, Any]:
                    decoded: dict[str, Any] = {}
                    for field_name in _PERSON_MERGE_GUARD_FIELDS:
                        field = type(person).clirm_fields[field_name]
                        decoded[field_name] = _decode_complete_field_value(
                            field,
                            guard[field_name],
                            context=(
                                f"reassign_person_references {role}.{field_name} guard"
                            ),
                            references=references,
                        )
                    return decoded

                expected_source = decode_reassign_guard(obj, source_guard, "source")
                expected_target = decode_reassign_guard(target, target_guard, "target")
                orcid = cast(str, row.values["orcid"])
                target_identity = f"id:{row.target.object_id}"
                current_target_tags = tuple(
                    planned_values.get(
                        ("Person", target_identity, "tags"), target.tags or ()
                    )
                )
                if not (
                    _person_tags_contain_orcid(expected_source["tags"], orcid)
                    or _person_tags_contain_orcid(current_target_tags, orcid)
                ):
                    raise RecommendationError(
                        "reassign_person_references source or target does not contain "
                        f"ORCID {orcid}, including prior planned changes"
                    )
                expected_source_tags = _person_tags_without_orcid(
                    expected_source["tags"], orcid
                )
                actual_references = _person_reassign_reference_snapshot(obj)
                target_has_orcid = _person_tags_contain_orcid(target.tags, orcid)
                source_applied = (
                    not any(actual_references.values())
                    and _values_equal(tuple(obj.tags or ()), expected_source_tags)
                    and target_has_orcid
                )
                for field_name, expected in expected_source.items():
                    if field_name == "tags" and source_applied:
                        expected = expected_source_tags
                    if not _values_equal(getattr(obj, field_name), expected):
                        raise RecommendationError(
                            "reassign_person_references source guard for "
                            f"{field_name!r} does not match"
                        )
                if not source_applied:
                    expected_references = {
                        field_name: sorted(
                            cast(Sequence[int], references_guard[field_name])
                        )
                        for field_name in _PERSON_REASSIGN_REFERENCE_FIELDS
                    }
                    if actual_references != expected_references:
                        raise RecommendationError(
                            "reassign_person_references source references changed "
                            "from the guard"
                        )

                for field_name, expected in expected_target.items():
                    if field_name == "tags" and target_has_orcid:
                        if not all(tag in tuple(target.tags or ()) for tag in expected):
                            raise RecommendationError(
                                "reassign_person_references target tags changed "
                                "incompatibly from the guard"
                            )
                        continue
                    if not _values_equal(getattr(target, field_name), expected):
                        raise RecommendationError(
                            "reassign_person_references target guard for "
                            f"{field_name!r} does not match"
                        )

                assert row.object.object_id is not None
                source_identity = f"id:{row.object.object_id}"
                final_target_tags = current_target_tags
                if not _person_tags_contain_orcid(final_target_tags, orcid):
                    final_target_tags = tuple(
                        sorted({*final_target_tags, models.tags.PersonTag.ORCID(orcid)})
                    )
                planned_values[("Person", source_identity, "tags")] = (
                    expected_source_tags
                )
                planned_values[("Person", target_identity, "tags")] = final_target_tags
                actions.append(
                    PlannedAction(
                        row,
                        obj,
                        {
                            "source": expected_source,
                            "target": expected_target,
                            "references": dict(references_guard),
                        },
                        {
                            "target": target,
                            "source_tags": expected_source_tags,
                            "target_tags": final_target_tags,
                        },
                        already_applied=source_applied,
                    )
                )
                continue
            if row.action == MERGE_PERSON:
                if not isinstance(obj, models.Person):
                    raise RecommendationError("merge_person object is not a Person")
                assert row.target is not None
                assert row.target.object_id is not None
                target = get_object(model, row.target.object_id)
                if not isinstance(target, models.Person):
                    raise RecommendationError("merge_person target is not a Person")
                if _object_label(model, target) != row.target.label:
                    raise RecommendationError(
                        f"Person {target.id} changed from {row.target.label!r} to "
                        f"{_object_label(model, target)!r}"
                    )
                if target.type not in {
                    constants.PersonType.unchecked,
                    constants.PersonType.checked,
                }:
                    raise RecommendationError("merge_person target is not canonical")
                assert row.match is not None
                source_guard = cast(Mapping[str, Any], row.match["source"])
                target_guard = cast(Mapping[str, Any], row.match["target"])
                references_guard = cast(Mapping[str, Any], row.match["references"])
                expected_guard_fields = set(_PERSON_MERGE_GUARD_FIELDS)
                if (
                    set(source_guard) != expected_guard_fields
                    or set(target_guard) != expected_guard_fields
                ):
                    raise RecommendationError(
                        "merge_person source and target guards must snapshot every "
                        "Person field"
                    )
                if set(references_guard) != set(_PERSON_MERGE_REFERENCE_FIELDS):
                    raise RecommendationError(
                        "merge_person references guard has unexpected fields"
                    )

                def decode_guard(
                    person: models.Person, guard: Mapping[str, Any], role: str
                ) -> dict[str, Any]:
                    decoded: dict[str, Any] = {}
                    for field_name in _PERSON_MERGE_GUARD_FIELDS:
                        field = type(person).clirm_fields[field_name]
                        decoded[field_name] = _decode_complete_field_value(
                            field,
                            guard[field_name],
                            context=f"merge_person {role}.{field_name} guard",
                            references=references,
                        )
                    return decoded

                expected_source = decode_guard(obj, source_guard, "source")
                expected_target = decode_guard(target, target_guard, "target")
                assert row.object.object_id is not None
                source_identity = f"id:{row.object.object_id}"
                planned_source_tags = tuple(
                    planned_values.get(
                        ("Person", source_identity, "tags"), expected_source["tags"]
                    )
                    or ()
                )
                source_applied = (
                    obj.type is constants.PersonType.hard_redirect
                    and _values_equal(obj.target, target)
                )
                if source_applied:
                    for field_name in (
                        "family_name",
                        "given_names",
                        "initials",
                        "suffix",
                        "tussenvoegsel",
                        "naming_convention",
                    ):
                        if not _values_equal(
                            getattr(obj, field_name), expected_source[field_name]
                        ):
                            raise RecommendationError(
                                f"already-merged source changed field {field_name!r}"
                            )
                    actual_source_tags = tuple(obj.tags or ())
                    if actual_source_tags and not _values_equal(
                        actual_source_tags, planned_source_tags
                    ):
                        raise RecommendationError(
                            "already-merged source tags changed from the planned value"
                        )
                    if any(
                        getattr(obj, field_name) is not None
                        for field_name in _PERSON_MERGE_TRANSFER_FIELDS
                    ):
                        raise RecommendationError(
                            "already-merged source retains transferred metadata"
                        )
                    if any(_person_reference_snapshot(obj).values()):
                        raise RecommendationError(
                            "already-merged source still has references"
                        )
                else:
                    if obj.type not in {
                        constants.PersonType.unchecked,
                        constants.PersonType.checked,
                    }:
                        raise RecommendationError(
                            "merge_person source is not canonical"
                        )
                    for field_name, expected in expected_source.items():
                        if not _values_equal(getattr(obj, field_name), expected):
                            raise RecommendationError(
                                f"merge_person source guard for {field_name!r} "
                                "does not match"
                            )
                    actual_references = _person_reference_snapshot(obj)
                    expected_references = {
                        field_name: sorted(
                            cast(Sequence[int], references_guard[field_name])
                        )
                        for field_name in _PERSON_MERGE_REFERENCE_FIELDS
                    }
                    if actual_references != expected_references:
                        raise RecommendationError(
                            "merge_person source references changed from the guard"
                        )

                for field_name in (
                    "family_name",
                    "given_names",
                    "initials",
                    "suffix",
                    "tussenvoegsel",
                    "target",
                ):
                    if not _values_equal(
                        getattr(target, field_name), expected_target[field_name]
                    ):
                        raise RecommendationError(
                            f"merge_person target guard for {field_name!r} does not "
                            "match"
                        )

                target_identity = f"id:{row.target.object_id}"
                target_values: dict[str, Any] = {}
                resolved_convention = None
                if row.values is not None:
                    resolved_convention = _decode_field_value(
                        models.Person.clirm_fields["naming_convention"],
                        row.values["naming_convention"],
                        context="merge_person resolved_naming_convention",
                        references=references,
                    )
                    if resolved_convention not in {
                        expected_source["naming_convention"],
                        expected_target["naming_convention"],
                    }:
                        raise RecommendationError(
                            "merge_person resolved naming convention must be one "
                            "of the guarded source or target conventions"
                        )
                row_convention = (
                    resolved_convention
                    if resolved_convention is not None
                    else _combine_person_naming_conventions(
                        expected_source["naming_convention"],
                        expected_target["naming_convention"],
                    )
                )
                actual_target_convention = target.naming_convention
                if actual_target_convention not in {
                    expected_target["naming_convention"],
                    row_convention,
                }:
                    raise RecommendationError(
                        "merge_person target naming convention changed incompatibly"
                    )
                convention_key = ("Person", target_identity, "naming_convention")
                planned_convention = planned_values.get(
                    convention_key, actual_target_convention
                )
                final_convention = (
                    resolved_convention
                    if resolved_convention is not None
                    else _combine_person_naming_conventions(
                        planned_convention, row_convention
                    )
                )
                target_values["naming_convention"] = final_convention
                planned_values[convention_key] = final_convention

                final_type = (
                    constants.PersonType.checked
                    if constants.PersonType.checked
                    in {expected_source["type"], expected_target["type"]}
                    else constants.PersonType.unchecked
                )
                if target.type not in {expected_target["type"], final_type}:
                    raise RecommendationError(
                        "merge_person target type changed incompatibly"
                    )
                target_type_key = ("Person", target_identity, "type")
                planned_type = planned_values.get(target_type_key, target.type)
                if planned_type not in {
                    constants.PersonType.unchecked,
                    constants.PersonType.checked,
                }:
                    raise RecommendationError(
                        "merge_person planned target type is invalid"
                    )
                final_type = (
                    constants.PersonType.checked
                    if constants.PersonType.checked in {planned_type, final_type}
                    else constants.PersonType.unchecked
                )
                target_values["type"] = final_type
                planned_values[target_type_key] = final_type

                for field_name in _PERSON_MERGE_TRANSFER_FIELDS:
                    source_value = expected_source[field_name]
                    guarded_target_value = expected_target[field_name]
                    actual_target_value = getattr(target, field_name)
                    if not _values_equal(
                        actual_target_value, guarded_target_value
                    ) and not (
                        guarded_target_value is None
                        and _values_equal(actual_target_value, source_value)
                    ):
                        raise RecommendationError(
                            f"merge_person target field {field_name!r} changed "
                            "incompatibly from the guard"
                        )
                    field_key = ("Person", target_identity, field_name)
                    current_value = planned_values.get(field_key, actual_target_value)
                    if (
                        current_value is not None
                        and source_value is not None
                        and not _values_equal(current_value, source_value)
                    ):
                        raise RecommendationError(
                            f"merge_person has conflicting {field_name!r} values"
                        )
                    final_value = (
                        current_value if current_value is not None else source_value
                    )
                    target_values[field_name] = final_value
                    planned_values[field_key] = final_value

                guarded_target_tags = tuple(expected_target["tags"] or ())
                actual_target_tags = tuple(target.tags or ())
                if not all(tag in actual_target_tags for tag in guarded_target_tags):
                    raise RecommendationError(
                        "merge_person target tags no longer contain the guarded set"
                    )
                target_tags_key = ("Person", target_identity, "tags")
                current_target_tags = tuple(
                    planned_values.get(target_tags_key, actual_target_tags) or ()
                )
                final_target_tags = tuple(
                    sorted({*current_target_tags, *planned_source_tags})
                )
                orcid_tags = {
                    tag.text
                    for tag in final_target_tags
                    if isinstance(tag, models.tags.PersonTag.ORCID)
                }
                if len(orcid_tags) > 1 and not any(
                    isinstance(tag, models.tags.PersonTag.IgnoreLint)
                    and tag.label == "multiple_orcids"
                    for tag in final_target_tags
                ):
                    final_target_tags = tuple(
                        sorted(
                            {
                                *final_target_tags,
                                models.tags.PersonTag.IgnoreLint(
                                    "multiple_orcids",
                                    comment=(
                                        "Person records with independently matching "
                                        "public ORCID identities were merged."
                                    ),
                                ),
                            }
                        )
                    )
                target_values["tags"] = final_target_tags
                planned_values[target_tags_key] = final_target_tags

                planned_values[("Person", source_identity, "type")] = (
                    constants.PersonType.hard_redirect
                )
                planned_values[("Person", source_identity, "target")] = target
                planned_values[("Person", source_identity, "tags")] = (
                    planned_source_tags
                )
                for field_name in _PERSON_MERGE_TRANSFER_FIELDS:
                    planned_values[("Person", source_identity, field_name)] = None
                actions.append(
                    PlannedAction(
                        row,
                        obj,
                        {
                            "source": expected_source,
                            "target": expected_target,
                            "references": dict(references_guard),
                        },
                        {"target": target, "target_values": target_values},
                        already_applied=source_applied
                        and all(
                            _values_equal(getattr(target, field_name), value)
                            for field_name, value in target_values.items()
                        ),
                    )
                )
                continue
            if row.action == MERGE_REGION:
                assert isinstance(obj, models.Region)
                assert row.target is not None
                assert row.target.object_id is not None
                target = get_object(model, row.target.object_id)
                assert isinstance(target, models.Region)
                target_identity = f"id:{row.target.object_id}"
                target_label = planned_values.get(
                    ("Region", target_identity, model.label_field),
                    _object_label(model, target),
                )
                if target_label != row.target.label:
                    raise RecommendationError(
                        f"Region {target.id} changed from {row.target.label!r} "
                        f"to {target_label!r}"
                    )
                target_kind = planned_values.get(
                    ("Region", target_identity, "kind"), target.kind
                )
                if target_kind in {
                    constants.RegionKind.redirect,
                    constants.RegionKind.deleted,
                }:
                    raise RecommendationError("merge target is invalid")

                source_parent_key = ("Region", identity, "parent")
                source_kind_key = ("Region", identity, "kind")
                source_tags_key = ("Region", identity, "tags")
                actual_parent = planned_values.get(source_parent_key, obj.parent)
                actual_kind = planned_values.get(source_kind_key, obj.kind)
                actual_tags = planned_values.get(source_tags_key, obj.tags)
                if actual_kind is constants.RegionKind.deleted:
                    raise RecommendationError("merge source is deleted")
                if actual_kind is constants.RegionKind.redirect:
                    if not _values_equal(actual_parent, target):
                        raise RecommendationError(
                            "merge source already redirects to a different Region"
                        )
                    already_applied = True
                else:
                    if target.has_parent(obj):
                        raise RecommendationError(
                            "cannot merge a Region into one of its descendants"
                        )
                    already_applied = False
                planned_values[source_parent_key] = target
                planned_values[source_kind_key] = constants.RegionKind.redirect
                planned_values[source_tags_key] = actual_tags
                actions.append(
                    PlannedAction(
                        row,
                        obj,
                        {
                            "parent": actual_parent,
                            "kind": actual_kind,
                            "tags": actual_tags,
                        },
                        {"target": target},
                        already_applied=already_applied,
                    )
                )
                continue
            if row.action == DELETE_REGION:
                if not isinstance(obj, models.Region):
                    raise RecommendationError("delete_region object is not a Region")
                if obj.kind is constants.RegionKind.redirect:
                    raise RecommendationError("delete_region source is a redirect")
                if obj.kind is constants.RegionKind.deleted:
                    actions.append(
                        PlannedAction(
                            row,
                            obj,
                            {"kind": obj.kind, "tags": obj.tags},
                            None,
                            already_applied=True,
                        )
                    )
                    continue
                assert row.match is not None
                for field_name, serialized_expected in row.match.items():
                    field = model.clirm_fields.get(field_name)
                    if field is None or field_name == "id":
                        raise RecommendationError(
                            f"delete_region guard names unknown field {field_name!r}"
                        )
                    expected = _decode_complete_field_value(
                        field,
                        serialized_expected,
                        context=f"Region.{field_name} guard",
                        references=references,
                    )
                    actual = getattr(obj, field_name)
                    if not _values_equal(actual, expected):
                        raise RecommendationError(
                            f"delete_region guard for {field_name!r} does not match "
                            "the current Region"
                        )
                direct_references = list(obj.get_direct_backrefs())
                if direct_references:
                    descriptions = ", ".join(
                        f"{type(referencing_obj).__name__} {referencing_obj.id}."
                        f"{field.attribute_name}"
                        for field, referencing_obj in direct_references[:10]
                    )
                    suffix = " ..." if len(direct_references) > 10 else ""
                    raise RecommendationError(
                        "cannot delete a Region with valid references: "
                        f"{descriptions}{suffix}"
                    )
                actions.append(
                    PlannedAction(
                        row,
                        obj,
                        {"kind": obj.kind, "tags": obj.tags},
                        None,
                        already_applied=False,
                    )
                )
                continue
            if row.action == MANUAL_REVIEW:
                actions.append(
                    PlannedAction(row, obj, None, None, already_applied=False)
                )
                continue
            if row.action == UPDATE_OBJECT:
                planned_changes: list[tuple[str, str, Any, Any]] = []
                all_applied = True
                for index, change in enumerate(row.changes, start=1):
                    operation = cast(str, change["operation"])
                    field_name = cast(str, change["field"])
                    field = model.clirm_fields.get(field_name)
                    if field is None or field_name == "id":
                        raise RecommendationError(
                            f"change {index}: {model.__name__} has no editable field "
                            f"{field_name!r}"
                        )
                    field_key = (row.object.model, identity, field_name)
                    if operation == "remove_raw":
                        if not isinstance(field, ADTField):
                            raise RecommendationError(
                                f"change {index}: field {field_name!r} is not an "
                                "ADT tag field"
                            )
                        # Work around https://github.com/JelleZijlstra/pycroscope/issues/520.
                        field = cast(  # type: ignore[redundant-cast]
                            ADTField[Any], field
                        )
                        if field_key in planned_values:
                            raise RecommendationError(
                                f"change {index}: remove_raw must be the first change "
                                f"to field {field_name!r}"
                            )
                        raw_value = change["raw_value"]
                        raw_tags = obj.get_raw_tags_field(field_name)
                        matches = sum(tag == raw_value for tag in raw_tags)
                        if matches > 1:
                            raise RecommendationError(
                                f"change {index}: raw tag occurs {matches} times in "
                                f"field {field_name!r}"
                            )
                        cleaned_raw = [tag for tag in raw_tags if tag != raw_value]
                        try:
                            decoded = tuple(field.deserialize(json.dumps(cleaned_raw)))
                        except Exception as exc:
                            raise RecommendationError(
                                f"change {index}: field {field_name!r} remains "
                                f"undecodable after raw removal: {exc}"
                            ) from exc
                        new_value = (
                            decoded if field.is_ordered else tuple(sorted(set(decoded)))
                        )
                        old_value = tuple(raw_tags)
                        planned_values[field_key] = new_value
                        planned_changes.append(
                            (operation, field_name, old_value, new_value)
                        )
                        all_applied &= matches == 0
                        continue
                    actual = (
                        planned_values[field_key]
                        if field_key in planned_values
                        else getattr(obj, field_name)
                    )
                    if operation == "set":
                        if isinstance(field, ADTField):
                            raise RecommendationError(
                                f"change {index}: set does not replace ADT fields"
                            )
                        old_value = _decode_field_value(
                            field,
                            change["old_value"],
                            context=f"{model.__name__}.{field_name} old_value",
                            references=references,
                        )
                        new_value = _decode_field_value(
                            field,
                            change["new_value"],
                            context=f"{model.__name__}.{field_name} new_value",
                            references=references,
                        )
                        if _values_equal(actual, new_value):
                            change_applied = True
                        elif _values_equal(actual, old_value):
                            change_applied = False
                        else:
                            raise RecommendationError(
                                f"change {index}: field {field_name!r} is neither "
                                "the snapshotted old value nor the recommended new value"
                            )
                        if not change_applied:
                            check_unique_assignment(
                                model, obj, identity, field_name, new_value
                            )
                    else:
                        if not isinstance(field, ADTField):
                            raise RecommendationError(
                                f"change {index}: field {field_name!r} is not an "
                                "ADT tag field"
                            )
                        old_value = tuple(cast(Sequence[adt.ADT] | None, actual) or ())
                        if operation == "normalize":
                            if field.is_ordered:
                                raise RecommendationError(
                                    f"change {index}: normalize is only valid for "
                                    "unordered ADT tag fields"
                                )
                            candidate = old_value
                        else:
                            tag = _decode_tag(
                                field,
                                change["value"],
                                context=f"{model.__name__}.{field_name}",
                                references=references,
                            )
                            if operation == "add":
                                candidate = (
                                    old_value if tag in old_value else (*old_value, tag)
                                )
                            else:
                                candidate = tuple(
                                    item for item in old_value if item != tag
                                )
                        new_value = (
                            candidate
                            if field.is_ordered
                            else tuple(sorted(set(candidate)))
                        )
                        change_applied = new_value == old_value
                    planned_values[field_key] = new_value
                    planned_changes.append(
                        (operation, field_name, old_value, new_value)
                    )
                    all_applied &= change_applied
                actions.append(
                    PlannedAction(
                        row, obj, None, None, all_applied, tuple(planned_changes)
                    )
                )
                continue
            assert row.field is not None
            field = model.clirm_fields.get(row.field)
            if field is None or row.field == "id":
                raise RecommendationError(
                    f"{model.__name__} has no editable field {row.field!r}"
                )
            field_key = (row.object.model, identity, row.field)
            actual = planned_values.get(field_key, getattr(obj, row.field))
            if row.action == SET_FIELD:
                if isinstance(field, ADTField):
                    raise RecommendationError(
                        "set_field does not replace ADT fields; use add_tag or remove_tag"
                    )
                old_value = _decode_field_value(
                    field,
                    row.old_value,
                    context=f"{model.__name__}.{row.field} old_value",
                    references=references,
                )
                new_value = _decode_field_value(
                    field,
                    row.new_value,
                    context=f"{model.__name__}.{row.field} new_value",
                    references=references,
                )
                if _values_equal(actual, new_value):
                    already_applied = True
                elif _values_equal(actual, old_value):
                    already_applied = False
                else:
                    raise RecommendationError(
                        f"field {row.field!r} is neither the snapshotted old value "
                        "nor the recommended new value"
                    )
                if not already_applied:
                    check_unique_assignment(model, obj, identity, row.field, new_value)
            else:
                if not isinstance(field, ADTField):
                    raise RecommendationError(
                        f"field {row.field!r} is not an ADT tag field"
                    )
                tag = _decode_tag(
                    field,
                    row.tag,
                    context=f"{model.__name__}.{row.field}",
                    references=references,
                )
                current_tags = tuple(cast(Sequence[adt.ADT] | None, actual) or ())
                if row.action == ADD_TAG:
                    old_value = current_tags
                    candidate_value = (
                        current_tags if tag in current_tags else (*current_tags, tag)
                    )
                else:
                    old_value = current_tags
                    candidate_value = (
                        current_tags
                        if tag not in current_tags
                        else tuple(item for item in current_tags if item != tag)
                    )
                new_value = (
                    candidate_value
                    if field.is_ordered
                    else tuple(sorted(set(candidate_value)))
                )
                already_applied = new_value == current_tags
            planned_values[field_key] = new_value
            actions.append(
                PlannedAction(row, obj, old_value, new_value, already_applied)
            )
        except RecommendationError as exc:
            errors.append(f"line {row.line_number}: {exc}")
    if errors:
        raise RecommendationError(
            "refusing to continue because database validation failed:\n- "
            + "\n- ".join(errors)
        )
    return RecommendationPlan(
        tuple(actions), Counter(row.action for row in original_rows), references
    )


def print_review_table(recommendations: Iterable[Recommendation]) -> None:
    rows = list(recommendations)
    registry = get_model_registry()
    print(f"{'ACTION':<14} {'CONF':<8} {'OBJECT':<38} CHANGE")
    print("-" * 120)
    for row in rows:
        identity = (
            str(row.object.object_id)
            if row.object.object_id is not None
            else f"ref={row.object.ref}"
        )
        obj = f"{row.object.model} {identity} {row.object.label}"
        model = registry.get(row.object.model)
        visible_changes = row.changes
        if row.action == CREATE_OBJECT:
            assert row.values is not None
            visible_values = {
                field_name: value
                for field_name, value in row.values.items()
                if not _is_empty_review_value(value)
            }
            change = (
                f"create with fields {', '.join(visible_values)}"
                if visible_values
                else "create"
            )
        elif row.action == UPDATE_OBJECT:
            visible_changes = tuple(
                guarded_change
                for guarded_change in row.changes
                if not (
                    guarded_change["operation"] == "set"
                    and guarded_change["old_value"] == guarded_change["new_value"]
                )
            )
            change = f"{len(visible_changes)} guarded change(s)"
        elif row.action == MERGE_COLLECTION:
            assert row.target is not None
            change = (
                f"merge into Collection {row.target.object_id} {row.target.label}; "
                f"update {len(row.object_updates)} referenced object(s)"
            )
        elif row.action == MERGE_PERSON:
            assert row.target is not None
            change = (
                f"merge into Person {row.target.object_id} {row.target.label}; "
                "move all guarded references and retain a hard redirect"
            )
        elif row.action == REASSIGN_PERSON_REFERENCES:
            assert row.target is not None
            assert row.values is not None
            change = (
                f"move guarded references and ORCID {row.values['orcid']} to Person "
                f"{row.target.object_id} {row.target.label}; retain source Person"
            )
        elif row.action == MERGE_REGION:
            assert row.target is not None
            change = (
                f"merge into Region {row.target.object_id} {row.target.label}; "
                "redirect ordinary references"
            )
        elif row.action == DELETE_REGION:
            change = "mark deleted after verifying no valid references"
        elif row.action == MANUAL_REVIEW:
            change = "requires manual review"
        elif row.action == SET_FIELD:
            change = (
                f"{row.field}: {_format_manifest_value(row.old_value)} -> "
                f"{_format_manifest_value(row.new_value)}"
            )
        else:
            field = None if model is None else model.clirm_fields.get(row.field or "")
            formatted_tag = (
                _format_serialized_tag(field, row.tag)
                if isinstance(field, ADTField)
                else repr(row.tag)
            )
            change = f"{row.field}: {formatted_tag}"
        print(f"{row.action:<14} {row.confidence:<8} {obj:<38} {change}")
        if row.action == CREATE_OBJECT:
            visible_match = {
                field_name: value
                for field_name, value in (row.match or {}).items()
                if not _is_empty_review_value(value)
            }
            for field_name, value in visible_match.items():
                _print_review_detail(
                    "match",
                    f"{field_name}={_format_create_value(model, field_name, value)}",
                )
            for field_name, value in visible_values.items():
                if field_name in visible_match and visible_match[field_name] == value:
                    continue
                _print_review_detail(
                    "field",
                    f"{field_name}={_format_create_value(model, field_name, value)}",
                )
        elif row.action == UPDATE_OBJECT:
            for guarded_change in visible_changes:
                _print_review_detail(
                    "change", _format_update_change(row, guarded_change, registry)
                )
        elif row.action == DELETE_REGION:
            for field_name, value in (row.match or {}).items():
                _print_review_detail(
                    "guard",
                    f"{field_name}={_format_create_value(model, field_name, value)}",
                )
        elif row.action == MERGE_COLLECTION:
            for update in row.object_updates:
                update_spec = cast(ObjectSpec, update["object"])
                fields = ", ".join(
                    cast(str, change["field"])
                    for change in cast(Sequence[Mapping[str, Any]], update["changes"])
                )
                _print_review_detail(
                    "guarded update",
                    f"{update_spec.model} {update_spec.object_id} "
                    f"{update_spec.label}: {fields}",
                )
        elif row.action == MANUAL_REVIEW:
            for evidence in row.evidence:
                _print_review_detail(f"evidence ({evidence.kind})", evidence.text)
    print(f"\n{len(rows)} recommendation(s): {dict(Counter(r.action for r in rows))}")


def print_full_manual_reviews(
    recommendations: Iterable[Recommendation], *, include_summary: bool = True
) -> None:
    """Print generic manual-review rows without shortening their evidence."""
    rows = [row for row in recommendations if row.action == MANUAL_REVIEW]
    for index, row in enumerate(rows):
        if index:
            print()
        current = f"{row.object.model} {row.object.object_id} {row.object.label}"
        print(f"MANUAL_REVIEW object={current!r}")
        print(f"Confidence: {row.confidence}")
        print("Reason:")
        print(row.reason)
        print("Evidence:")
        for item in row.evidence:
            print(f"- kind={item.kind!r}")
            print(item.text)
    if rows and include_summary:
        print(f"\n{len(rows)} generic manual_review recommendation(s).")


def add_virtual_models(plan: RecommendationPlan, builder: ProposalBuilder) -> None:
    """Apply generic plan actions to shared virtual model copies."""
    for planned in plan.actions:
        row = planned.recommendation
        if row.action == CREATE_OBJECT:
            if planned.already_applied:
                continue
            assert row.values is not None
            values = {
                name: _replace_created_models(
                    value,
                    {
                        id(model_value): builder.replacement(model_value)
                        for model_value in _iter_model_values(value)
                    },
                )
                for name, value in cast(Mapping[str, Any], planned.new_value).items()
            }
            builder.create(
                type(planned.object),
                context=f"generic manifest line {row.line_number}",
                proposal_source=planned.object,
                **values,
            )
            continue
        if (
            row.action == MANUAL_REVIEW
            or planned.already_applied
            or not isinstance(planned.object, BaseModel)
        ):
            continue
        if row.action == MERGE_COLLECTION:
            assert row.target is not None
            merge_data = cast(Mapping[str, Any], planned.new_value)
            collection_target = cast(models.Collection, merge_data["target"])
            for update_obj, changes in cast(
                Sequence[tuple[BaseModel, Sequence[tuple[str, Any, Any]]]],
                merge_data["updates"],
            ):
                proposal = builder.replacement(update_obj)
                if proposal is update_obj:
                    proposal = builder.copy(
                        update_obj, context=f"generic manifest line {row.line_number}"
                    )
                for field_name, _old_value, new_value in changes:
                    setattr(
                        proposal,
                        field_name,
                        _replace_created_models(
                            new_value,
                            {
                                id(value): builder.replacement(value)
                                for value in _iter_model_values(new_value)
                            },
                        ),
                    )
            collection_source_proposal = builder.replacement(planned.object)
            if collection_source_proposal is planned.object:
                collection_source_proposal = builder.copy(
                    planned.object, context=f"generic manifest line {row.line_number}"
                )
            collection_source_proposal = cast(
                models.Collection, collection_source_proposal
            )
            collection_source_proposal.parent = builder.replacement(collection_target)
            collection_source_proposal.removed = True
            continue
        if row.action == REASSIGN_PERSON_REFERENCES:
            reassign_data = cast(Mapping[str, Any], planned.new_value)
            person_source = cast(models.Person, planned.object)
            person_target = cast(models.Person, reassign_data["target"])
            proposed_person_target = builder.replacement(person_target)
            if proposed_person_target is person_target:
                proposed_person_target = builder.copy(
                    person_target, context=f"generic manifest line {row.line_number}"
                )
            proposed_person_target.tags = reassign_data["target_tags"]
            source_proposal = builder.replacement(person_source)
            if source_proposal is person_source:
                source_proposal = builder.copy(
                    person_source, context=f"generic manifest line {row.line_number}"
                )
            source_proposal.tags = reassign_data["source_tags"]

            reference_specs = (
                ("books", "author_tags", AuthorTag.Author),
                ("articles", "author_tags", AuthorTag.Author),
                ("names", "author_tags", AuthorTag.Author),
                ("patronyms", "type_tags", models.TypeTag.NamedAfter),
                ("collected", "type_tags", models.TypeTag.CollectedBy),
                ("involved", "type_tags", models.TypeTag.Involved),
            )
            for derived_field, tag_field, tag_type in reference_specs:
                for referencing_obj in (
                    person_source.get_derived_field(derived_field) or ()
                ):
                    proposal = builder.replacement(referencing_obj)
                    if proposal is referencing_obj:
                        proposal = builder.copy(
                            referencing_obj,
                            context=f"generic manifest line {row.line_number}",
                        )
                    tags = getattr(proposal, tag_field)
                    setattr(
                        proposal,
                        tag_field,
                        tuple(
                            (
                                tag.replace(person=proposed_person_target)
                                if isinstance(tag, tag_type)
                                and tag.person == person_source
                                else tag
                            )
                            for tag in tags
                        ),
                    )
            continue
        if row.action == MERGE_PERSON:
            merge_data = cast(Mapping[str, Any], planned.new_value)
            person_target = cast(models.Person, merge_data["target"])
            proposed_person_target = builder.replacement(person_target)
            if proposed_person_target is person_target:
                proposed_person_target = builder.copy(
                    person_target, context=f"generic manifest line {row.line_number}"
                )
            for field_name, value in cast(
                Mapping[str, Any], merge_data["target_values"]
            ).items():
                setattr(proposed_person_target, field_name, value)

            reference_specs = (
                ("books", "author_tags", AuthorTag.Author),
                ("articles", "author_tags", AuthorTag.Author),
                ("names", "author_tags", AuthorTag.Author),
                ("patronyms", "type_tags", models.TypeTag.NamedAfter),
                ("collected", "type_tags", models.TypeTag.CollectedBy),
                ("involved", "type_tags", models.TypeTag.Involved),
            )
            person_source = cast(models.Person, planned.object)
            for derived_field, tag_field, tag_type in reference_specs:
                for referencing_obj in (
                    person_source.get_derived_field(derived_field) or ()
                ):
                    proposal = builder.replacement(referencing_obj)
                    if proposal is referencing_obj:
                        proposal = builder.copy(
                            referencing_obj,
                            context=f"generic manifest line {row.line_number}",
                        )
                    tags = getattr(proposal, tag_field)
                    setattr(
                        proposal,
                        tag_field,
                        tuple(
                            (
                                tag.replace(person=proposed_person_target)
                                if isinstance(tag, tag_type)
                                and tag.person == person_source
                                else tag
                            )
                            for tag in tags
                        ),
                    )
            for alias in models.Person.select().filter(
                models.Person.target == person_source
            ):
                alias_proposal = builder.replacement(alias)
                if alias_proposal is alias:
                    alias_proposal = builder.copy(
                        alias, context=f"generic manifest line {row.line_number}"
                    )
                alias_proposal.target = proposed_person_target

            source_proposal = builder.replacement(person_source)
            if source_proposal is person_source:
                source_proposal = builder.copy(
                    person_source, context=f"generic manifest line {row.line_number}"
                )
            source_proposal.type = constants.PersonType.hard_redirect
            source_proposal.target = proposed_person_target
            for field_name in _PERSON_MERGE_TRANSFER_FIELDS:
                setattr(source_proposal, field_name, None)
            continue
        if row.action == MERGE_REGION:
            merge_data = cast(Mapping[str, Any], planned.new_value)
            region_target = cast(models.Region, merge_data["target"])
            proposed_region_target = builder.replacement(region_target)
            for field, referencing_obj in planned.object.get_direct_backrefs(
                include_invalid=True
            ):
                reference_proposal = builder.replacement(referencing_obj)
                if reference_proposal is referencing_obj:
                    reference_proposal = builder.copy(
                        referencing_obj,
                        context=f"generic manifest line {row.line_number}",
                    )
                setattr(
                    reference_proposal, field.attribute_name, proposed_region_target
                )
            region_source_proposal = builder.replacement(planned.object)
            if region_source_proposal is planned.object:
                region_source_proposal = builder.copy(
                    planned.object, context=f"generic manifest line {row.line_number}"
                )
            region_source_proposal = cast(models.Region, region_source_proposal)
            region_source_proposal.parent = proposed_region_target
            region_source_proposal.kind = constants.RegionKind.redirect
            continue
        if row.action == DELETE_REGION:
            deleted_region_proposal = builder.replacement(planned.object)
            if deleted_region_proposal is planned.object:
                deleted_region_proposal = builder.copy(
                    planned.object, context=f"generic manifest line {row.line_number}"
                )
            deleted_region_proposal = cast(models.Region, deleted_region_proposal)
            deleted_region_proposal.kind = constants.RegionKind.deleted
            continue
        if row.action == UPDATE_OBJECT:
            proposal = builder.replacement(planned.object)
            if proposal is planned.object:
                raw_overrides = {
                    field_name: new_value
                    for operation, field_name, _old_value, new_value in planned.changes
                    if operation == "remove_raw"
                }
                if raw_overrides:
                    proposal = builder.copy_with_overrides(
                        planned.object,
                        context=f"generic manifest line {row.line_number}",
                        overrides=raw_overrides,
                    )
                else:
                    proposal = builder.copy(
                        planned.object,
                        context=f"generic manifest line {row.line_number}",
                    )
            for operation, field_name, old_value, new_value in planned.changes:
                replacements = {
                    id(value): builder.replacement(value)
                    for value in _iter_model_values((old_value, new_value))
                }
                new_value = _replace_created_models(new_value, replacements)
                if operation in {"add", "remove", "normalize"}:
                    new_value = _compose_tag_edit(
                        proposal,
                        field_name,
                        _replace_created_models(old_value, replacements),
                        new_value,
                    )
                setattr(proposal, field_name, new_value)
            continue
        assert row.field is not None
        proposal = builder.replacement(planned.object)
        if proposal is planned.object:
            proposal = builder.copy(
                planned.object, context=f"generic manifest line {row.line_number}"
            )
        new_value = planned.new_value
        replacements = {
            id(value): builder.replacement(value)
            for value in _iter_model_values((planned.old_value, new_value))
        }
        new_value = _replace_created_models(new_value, replacements)
        if row.action in {ADD_TAG, REMOVE_TAG}:
            new_value = _compose_tag_edit(
                proposal,
                row.field,
                _replace_created_models(planned.old_value, replacements),
                new_value,
            )
        setattr(proposal, row.field, new_value)


def _compose_tag_edit(
    obj: BaseModel, field_name: str, old_value: Any, new_value: Any
) -> tuple[adt.ADT, ...]:
    """Apply a planned tag delta without erasing earlier recommendation families."""
    old_tags = tuple(old_value or ())
    new_tags = tuple(new_value or ())
    current = tuple(getattr(obj, field_name) or ())
    removed = set(old_tags) - set(new_tags)
    result = tuple(tag for tag in current if tag not in removed)
    result += tuple(
        tag for tag in new_tags if tag not in old_tags and tag not in result
    )
    field = obj.clirm_fields[field_name]
    assert isinstance(field, ADTField)
    return result if field.is_ordered else tuple(sorted(set(result)))


def _iter_model_values(value: Any) -> Iterable[BaseModel]:
    if isinstance(value, BaseModel):
        yield value
    elif isinstance(value, adt.ADT):
        for name in getattr(value, "_attributes", ()):
            yield from _iter_model_values(getattr(value, name))
    elif isinstance(value, Mapping):
        for nested in value.values():
            yield from _iter_model_values(nested)
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes):
        for nested in value:
            yield from _iter_model_values(nested)


def _replace_created_models(value: Any, replacements: Mapping[int, BaseModel]) -> Any:
    if isinstance(value, BaseModel):
        return replacements.get(id(value), value)
    if isinstance(value, adt.ADT):
        attributes = getattr(value, "_attributes", {})
        if not attributes:
            return value
        decoded = {
            name: _replace_created_models(getattr(value, name), replacements)
            for name in attributes
        }
        return type(value)(**decoded)
    if isinstance(value, tuple):
        return tuple(_replace_created_models(item, replacements) for item in value)
    if isinstance(value, list):
        return [_replace_created_models(item, replacements) for item in value]
    if isinstance(value, Mapping):
        return {
            key: _replace_created_models(item, replacements)
            for key, item in value.items()
        }
    return value


def execute_plan(
    plan: RecommendationPlan,
    *,
    apply: bool,
    create_object: Callable[
        [type[BaseModel], Mapping[str, Any]], BaseModel
    ] = lambda model, values: model.create(**values),
    initial_replacements: Mapping[int, BaseModel] | None = None,
) -> Mapping[int, BaseModel]:
    applied = created = already_applied = manual = 0
    replacements: dict[int, BaseModel] = dict(initial_replacements or {})
    for planned in plan.actions:
        row = planned.recommendation
        if row.action == CREATE_OBJECT:
            assert row.object.ref is not None
            if planned.already_applied:
                already_applied += 1
                print(
                    f"SKIP_ALREADY_APPLIED action={row.action} "
                    f"object={row.object.model}:ref={row.object.ref!r}"
                )
                continue
            verb = "CREATE_OBJECT" if apply else "WOULD_CREATE_OBJECT"
            print(
                f"{verb} model={row.object.model!r} ref={row.object.ref!r} "
                f"label={row.object.label!r}"
            )
            if apply:
                values = {
                    name: _replace_created_models(value, replacements)
                    for name, value in cast(
                        Mapping[str, Any], planned.new_value
                    ).items()
                }
                created_object = create_object(type(planned.object), values)
                replacements[id(planned.object)] = created_object
            created += 1
            continue
        if row.action == MANUAL_REVIEW:
            manual += 1
            print(
                f"MANUAL_REVIEW object={row.object.model}:{row.object.object_id} "
                f"label={row.object.label!r} confidence={row.confidence!r}"
            )
            continue
        if planned.already_applied:
            already_applied += 1
            print(
                f"SKIP_ALREADY_APPLIED action={row.action} "
                f"object={row.object.model}:{row.object.object_id} field={row.field!r}"
            )
            continue
        if row.action == MERGE_COLLECTION:
            merge_data = cast(Mapping[str, Any], planned.new_value)
            target = cast(BaseModel, merge_data["target"])
            updates = cast(
                Sequence[tuple[BaseModel, Sequence[tuple[str, Any, Any]]]],
                merge_data["updates"],
            )
            verb = "MERGE_COLLECTION" if apply else "WOULD_MERGE_COLLECTION"
            assert (
                row.target is not None
            ), "merge target should be present in planned action"
            print(
                f"{verb} source=Collection:{row.object.object_id} "
                f"label={row.object.label!r} target=Collection:{target.id} "
                f"label={row.target.label!r} "
                f"referenced_objects={len(updates)}"
            )
            if apply:
                for update_obj, changes in updates:
                    resolved_obj = _replace_created_models(update_obj, replacements)
                    for field_name, _old_value, new_value in changes:
                        setattr(
                            resolved_obj,
                            field_name,
                            _replace_created_models(new_value, replacements),
                        )
                source = _replace_created_models(planned.object, replacements)
                source.parent = _replace_created_models(target, replacements)
                source.removed = True
            applied += 1
            continue
        if row.action == REASSIGN_PERSON_REFERENCES:
            reassign_data = cast(Mapping[str, Any], planned.new_value)
            target = cast(models.Person, reassign_data["target"])
            verb = (
                "REASSIGN_PERSON_REFERENCES"
                if apply
                else "WOULD_REASSIGN_PERSON_REFERENCES"
            )
            assert row.target is not None
            assert row.values is not None
            print(
                f"{verb} source=Person:{row.object.object_id} "
                f"label={row.object.label!r} target=Person:{target.id} "
                f"label={row.target.label!r} orcid={row.values['orcid']}"
            )
            if apply:
                person_source = cast(
                    models.Person, _replace_created_models(planned.object, replacements)
                )
                resolved_target = cast(
                    models.Person, _replace_created_models(target, replacements)
                )
                resolved_target.tags = reassign_data["target_tags"]
                person_source.tags = reassign_data["source_tags"]
                person_source.reassign_references(
                    target=resolved_target, respect_ignore_lint=False
                )
            applied += 1
            continue
        if row.action == MERGE_PERSON:
            merge_data = cast(Mapping[str, Any], planned.new_value)
            target = cast(models.Person, merge_data["target"])
            verb = "MERGE_PERSON" if apply else "WOULD_MERGE_PERSON"
            assert row.target is not None
            print(
                f"{verb} source=Person:{row.object.object_id} "
                f"label={row.object.label!r} target=Person:{target.id} "
                f"label={row.target.label!r}"
            )
            if apply:
                person_source = cast(
                    models.Person, _replace_created_models(planned.object, replacements)
                )
                resolved_target = cast(
                    models.Person, _replace_created_models(target, replacements)
                )
                for field_name, value in cast(
                    Mapping[str, Any], merge_data["target_values"]
                ).items():
                    setattr(
                        resolved_target,
                        field_name,
                        _replace_created_models(value, replacements),
                    )
                for alias in models.Person.select().filter(
                    models.Person.target == person_source
                ):
                    alias.target = resolved_target
                person_source.reassign_references(
                    target=resolved_target, respect_ignore_lint=False
                )
                person_source.type = constants.PersonType.hard_redirect
                person_source.target = resolved_target
                for field_name in _PERSON_MERGE_TRANSFER_FIELDS:
                    setattr(person_source, field_name, None)
            applied += 1
            continue
        if row.action == MERGE_REGION:
            merge_data = cast(Mapping[str, Any], planned.new_value)
            target = cast(models.Region, merge_data["target"])
            verb = "MERGE_REGION" if apply else "WOULD_MERGE_REGION"
            assert row.target is not None
            print(
                f"{verb} source=Region:{row.object.object_id} "
                f"label={row.object.label!r} target=Region:{target.id} "
                f"label={row.target.label!r}"
            )
            if apply:
                region_source = cast(
                    models.Region, _replace_created_models(planned.object, replacements)
                )
                resolved_region_target = cast(
                    models.Region, _replace_created_models(target, replacements)
                )
                region_source.merge(resolved_region_target)
            applied += 1
            continue
        if row.action == DELETE_REGION:
            verb = "DELETE_REGION" if apply else "WOULD_DELETE_REGION"
            print(
                f"{verb} source=Region:{row.object.object_id} "
                f"label={row.object.label!r}"
            )
            if apply:
                deleted_region = cast(
                    models.Region, _replace_created_models(planned.object, replacements)
                )
                deleted_region.remove()
            applied += 1
            continue
        verb = row.action.upper() if apply else f"WOULD_{row.action.upper()}"
        print(
            f"{verb} object={row.object.model}:{row.object.object_id} "
            f"label={row.object.label!r} field={row.field!r}"
        )
        if apply:
            obj = _replace_created_models(planned.object, replacements)
            if row.action == UPDATE_OBJECT:
                for operation, field_name, old_value, new_value in planned.changes:
                    new_value = _replace_created_models(new_value, replacements)
                    if operation in {"add", "remove", "normalize"}:
                        new_value = _compose_tag_edit(
                            obj,
                            field_name,
                            _replace_created_models(old_value, replacements),
                            new_value,
                        )
                    setattr(obj, field_name, new_value)
            else:
                assert row.field is not None
                new_value = _replace_created_models(planned.new_value, replacements)
                if row.action in {ADD_TAG, REMOVE_TAG}:
                    new_value = _compose_tag_edit(
                        obj,
                        row.field,
                        _replace_created_models(planned.old_value, replacements),
                        new_value,
                    )
                setattr(obj, row.field, new_value)
        applied += 1
    mode = "Applied" if apply else "Dry run"
    print(
        f"{mode}: {created} generic creation(s), {applied} generic update(s), "
        f"{already_applied} already applied, {manual} manual review(s)."
    )
    print(f"Recommended actions: {dict(plan.action_counts)}")
    if not apply:
        print("No database changes made. Review this plan, then add --apply.")
    return replacements
