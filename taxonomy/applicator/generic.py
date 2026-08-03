"""Backend for generic object, field, ADT-tag, and manual-review recommendations.

These actions provide an extensible baseline for simple database changes that do not
need a domain-specific planner. Values are guarded by an explicit old-value snapshot;
foreign-key and enum values use structured JSON representations.
"""

import enum
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
CREATE_OBJECT = "create_object"
SET_FIELD = "set_field"
ADD_TAG = "add_tag"
REMOVE_TAG = "remove_tag"
MANUAL_REVIEW = "manual_review"
ALLOWED_ACTIONS = {CREATE_OBJECT, SET_FIELD, ADD_TAG, REMOVE_TAG, MANUAL_REVIEW}
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


@dataclass(frozen=True, slots=True)
class PlannedAction:
    recommendation: Recommendation
    object: Any
    old_value: Any
    new_value: Any
    already_applied: bool


@dataclass(frozen=True, slots=True)
class RecommendationPlan:
    actions: tuple[PlannedAction, ...]
    action_counts: Counter[str]


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


def parse_recommendation(data: dict[str, Any], line_number: int) -> Recommendation:
    version = _required_int(data, "schema_version", line_number)
    if version != SCHEMA_VERSION:
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
        return Recommendation(
            line_number,
            action,
            confidence,
            reason,
            evidence,
            object_spec,
            None,
            values=values,
        )
    if action == MANUAL_REVIEW:
        if object_spec.object_id is None:
            raise RecommendationError(
                f"line {line_number}: manual_review requires an existing object id"
            )
        return Recommendation(
            line_number, action, confidence, reason, evidence, object_spec, None
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
    if not isinstance(label, str) or not label:
        raise RecommendationError(f"{context}: foreign-key label is required")
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
    if actual_label != label:
        identity = f"ref {ref!r}" if ref is not None else str(object_id)
        raise RecommendationError(
            f"{context}: {expected_model.__name__} {identity} changed from "
            f"{label!r} to {actual_label!r}"
        )
    return obj


def _decode_enum(data: Any, enum_type: type[enum.Enum], *, context: str) -> enum.Enum:
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


def _decode_tag(field: ADTField[Any], data: Any, *, context: str) -> adt.ADT:
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
        return member.__name__
    arguments = [
        f"{name}={_format_serialized_adt_argument(argument_type, value)}"
        for (name, argument_type), value in zip(
            member._attributes.items(), data[1:], strict=False
        )
    ]
    return f"{member.__name__}({', '.join(arguments)})"


def _format_serialized_tag(field: ADTField[Any], data: Any) -> str:
    return _format_serialized_adt(field.adt_type, data)


def _decode_create_values(
    model: type[BaseModel],
    data: Mapping[str, Any],
    *,
    context: str,
    references: Mapping[str, BaseModel],
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
                _decode_tag(field, item, context=field_context) for item in serialized
            )
            values[name] = decoded if field.is_ordered else tuple(sorted(set(decoded)))
        else:
            values[name] = _decode_field_value(
                field, serialized, context=field_context, references=references
            )
    label_field = model.label_field
    if label_field not in values:
        raise RecommendationError(
            f"{context}: create_object values must include label field {label_field!r}"
        )
    return values


def _find_objects_by_label(model: type[BaseModel], label: str) -> list[BaseModel]:
    field = model.clirm_fields[model.label_field]
    return list(model.select().filter(field == label))


def _values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, BaseModel) and isinstance(right, BaseModel):
        return type(left) is type(right) and left.id == right.id
    return left == right


def build_plan(
    recommendations: Iterable[Recommendation],
    *,
    model_registry: Mapping[str, type[BaseModel]] | None = None,
    get_object: Callable[[type[BaseModel], int], Any] = _get_object,
    find_objects_by_label: Callable[
        [type[BaseModel], str], list[BaseModel]
    ] = _find_objects_by_label,
    allow_manual_label_changes: bool = False,
) -> RecommendationPlan:
    rows = list(recommendations)
    registry = model_registry or get_model_registry()
    actions: list[PlannedAction] = []
    errors: list[str] = []
    seen: set[tuple[str, str, str | None, str]] = set()
    planned_values: dict[tuple[str, str, str], Any] = {}
    references: dict[str, BaseModel] = {}
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
                label = str(values[model.label_field])
                if label != row.object.label:
                    raise RecommendationError(
                        f"create_object label {row.object.label!r} does not match "
                        f"values.{model.label_field} {label!r}"
                    )
                matches = find_objects_by_label(model, row.object.label)
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
                else "field" if row.action == SET_FIELD else repr(row.tag)
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
            if label != row.object.label and not (
                allow_manual_label_changes and row.action == MANUAL_REVIEW
            ):
                raise RecommendationError(
                    f"{model.__name__} {obj.id} changed from {row.object.label!r} "
                    f"to {label!r}"
                )
            if row.action == MANUAL_REVIEW:
                actions.append(
                    PlannedAction(row, obj, None, None, already_applied=False)
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
                    field, row.tag, context=f"{model.__name__}.{row.field}"
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
    return RecommendationPlan(tuple(actions), Counter(row.action for row in rows))


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
        if row.action == CREATE_OBJECT:
            change = f"create with fields {', '.join(row.values or ())}"
        elif row.action == MANUAL_REVIEW:
            change = "requires manual review"
        elif row.action == SET_FIELD:
            change = f"{row.field}: {row.old_value!r} -> {row.new_value!r}"
        else:
            model = registry.get(row.object.model)
            field = None if model is None else model.clirm_fields.get(row.field or "")
            formatted_tag = (
                _format_serialized_tag(field, row.tag)
                if isinstance(field, ADTField)
                else repr(row.tag)
            )
            change = f"{row.field}: {formatted_tag}"
        print(f"{row.action:<14} {row.confidence:<8} {obj:<38} {change}")
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
                name: (
                    builder.replacement(value)
                    if isinstance(value, BaseModel)
                    else value
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


def _replace_created_models(value: Any, replacements: Mapping[int, BaseModel]) -> Any:
    if isinstance(value, BaseModel):
        return replacements.get(id(value), value)
    return value


def execute_plan(
    plan: RecommendationPlan,
    *,
    apply: bool,
    create_object: Callable[
        [type[BaseModel], Mapping[str, Any]], BaseModel
    ] = lambda model, values: model.create(**values),
) -> None:
    applied = created = already_applied = manual = 0
    replacements: dict[int, BaseModel] = {}
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
        verb = row.action.upper() if apply else f"WOULD_{row.action.upper()}"
        print(
            f"{verb} object={row.object.model}:{row.object.object_id} "
            f"label={row.object.label!r} field={row.field!r}"
        )
        assert row.field is not None
        if apply:
            obj = _replace_created_models(planned.object, replacements)
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
