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
from taxonomy.db import models
from taxonomy.db.models import BaseModel
from taxonomy.db.models.base import ADTField

SCHEMA_VERSION = 1
SCHEMA_VERSION_2 = 2
CREATE_OBJECT = "create_object"
UPDATE_OBJECT = "update_object"
SET_FIELD = "set_field"
ADD_TAG = "add_tag"
REMOVE_TAG = "remove_tag"
MANUAL_REVIEW = "manual_review"
MERGE_COLLECTION = "merge_collection"
ALLOWED_ACTIONS = {
    CREATE_OBJECT,
    UPDATE_OBJECT,
    SET_FIELD,
    ADD_TAG,
    REMOVE_TAG,
    MANUAL_REVIEW,
    MERGE_COLLECTION,
}
ALLOWED_CONFIDENCES = {"high", "medium", "low"}


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
    if action == MERGE_COLLECTION:
        if version != SCHEMA_VERSION_2:
            raise RecommendationError(
                f"line {line_number}: merge_collection requires schema_version 2"
            )
        if object_spec.model != "Collection" or object_spec.object_id is None:
            raise RecommendationError(
                f"line {line_number}: merge_collection object must be an existing "
                "Collection"
            )
        target = _parse_object(data.get("target"), line_number)
        if target.model != "Collection" or target.object_id is None:
            raise RecommendationError(
                f"line {line_number}: merge_collection target must be an existing "
                "Collection"
            )
        if target.object_id == object_spec.object_id:
            raise RecommendationError(
                f"line {line_number}: merge_collection source and target must differ"
            )
        return Recommendation(
            line_number,
            action,
            confidence,
            reason,
            evidence,
            object_spec,
            None,
            target=target,
            object_updates=_parse_merge_object_updates(
                data.get("object_updates"), line_number
            ),
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
            if operation not in {"set", "add", "remove", "remove_raw"}:
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
            else:
                required = {"value"}
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
    if require_label and label_field not in values:
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


def _validate_create_invariants(
    model: type[BaseModel], values: Mapping[str, Any], *, context: str
) -> None:
    if model.__name__ == "ClassificationEntry":
        page = values.get("page")
        if not isinstance(page, str) or re.fullmatch(r"[1-9][0-9]*", page) is None:
            raise RecommendationError(
                f"{context}: ClassificationEntry.page must be one page number"
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
                label = str(values[model.label_field])
                if label != row.object.label:
                    raise RecommendationError(
                        f"create_object label {row.object.label!r} does not match "
                        f"values.{model.label_field} {label!r}"
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
                references[row.object.ref] = obj
                actions.append(
                    PlannedAction(
                        row, obj, None, values, already_applied=already_applied
                    )
                )
                continue
            mutation = (
                MANUAL_REVIEW
                if row.action == MANUAL_REVIEW
                else (
                    UPDATE_OBJECT
                    if row.action == UPDATE_OBJECT
                    else "field" if row.action == SET_FIELD else repr(row.tag)
                )
            )
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
                actual_parent = planned_values.get(source_parent_key, obj.parent)
                actual_removed = planned_values.get(source_removed_key, obj.removed)
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
                    else:
                        if not isinstance(field, ADTField):
                            raise RecommendationError(
                                f"change {index}: field {field_name!r} is not an "
                                "ADT tag field"
                            )
                        tag = _decode_tag(
                            field,
                            change["value"],
                            context=f"{model.__name__}.{field_name}",
                            references=references,
                        )
                        old_value = tuple(cast(Sequence[adt.ADT] | None, actual) or ())
                        if operation == "add":
                            candidate = (
                                old_value if tag in old_value else (*old_value, tag)
                            )
                        else:
                            candidate = tuple(item for item in old_value if item != tag)
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
        if row.action == CREATE_OBJECT:
            change = f"create with fields {', '.join(row.values or ())}"
        elif row.action == UPDATE_OBJECT:
            change = f"{len(row.changes)} guarded change(s)"
        elif row.action == MERGE_COLLECTION:
            assert row.target is not None
            change = (
                f"merge into Collection {row.target.object_id} {row.target.label}; "
                f"update {len(row.object_updates)} referenced object(s)"
            )
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
            if row.match is not None:
                for field_name, value in row.match.items():
                    _print_review_detail(
                        "match",
                        f"{field_name}={_format_create_value(model, field_name, value)}",
                    )
            assert row.values is not None
            for field_name, value in row.values.items():
                _print_review_detail(
                    "field",
                    f"{field_name}={_format_create_value(model, field_name, value)}",
                )
        elif row.action == UPDATE_OBJECT:
            for guarded_change in row.changes:
                _print_review_detail(
                    "change", _format_update_change(row, guarded_change, registry)
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
            target = cast(BaseModel, merge_data["target"])
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
            source_proposal = builder.replacement(planned.object)
            if source_proposal is planned.object:
                source_proposal = builder.copy(
                    planned.object, context=f"generic manifest line {row.line_number}"
                )
            source_proposal.parent = builder.replacement(target)
            source_proposal.removed = True
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
            for _operation, field_name, _old_value, new_value in planned.changes:
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
            continue
        assert row.field is not None
        proposal = builder.replacement(planned.object)
        if proposal is planned.object:
            proposal = builder.copy(
                planned.object, context=f"generic manifest line {row.line_number}"
            )
        new_value = planned.new_value
        if isinstance(new_value, BaseModel):
            new_value = builder.replacement(new_value)
        setattr(proposal, row.field, new_value)


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
        verb = row.action.upper() if apply else f"WOULD_{row.action.upper()}"
        print(
            f"{verb} object={row.object.model}:{row.object.object_id} "
            f"label={row.object.label!r} field={row.field!r}"
        )
        if apply:
            obj = _replace_created_models(planned.object, replacements)
            if row.action == UPDATE_OBJECT:
                for _operation, field_name, _old_value, new_value in planned.changes:
                    setattr(
                        obj,
                        field_name,
                        _replace_created_models(new_value, replacements),
                    )
            else:
                assert row.field is not None
                new_value = _replace_created_models(planned.new_value, replacements)
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
