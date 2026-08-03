"""Backend for generic field, ADT-tag, and manual-review recommendations.

These actions provide an extensible baseline for simple database changes that do not
need a domain-specific planner. Values are guarded by an explicit old-value snapshot;
foreign-key and enum values use structured JSON representations.
"""

import enum
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from taxonomy import adt
from taxonomy.db import models
from taxonomy.db.models import BaseModel
from taxonomy.db.models.base import ADTField

SCHEMA_VERSION = 1
SET_FIELD = "set_field"
ADD_TAG = "add_tag"
REMOVE_TAG = "remove_tag"
MANUAL_REVIEW = "manual_review"
ALLOWED_ACTIONS = {SET_FIELD, ADD_TAG, REMOVE_TAG, MANUAL_REVIEW}
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
    object_id: int
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
    return ObjectSpec(
        _required_str(data, "model", line_number),
        _required_int(data, "id", line_number),
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
    if action == MANUAL_REVIEW:
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
    data: Any, expected_model: type[BaseModel], *, context: str
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
    label = data.get("label")
    if not isinstance(object_id, int) or isinstance(object_id, bool):
        raise RecommendationError(f"{context}: foreign-key id must be an integer")
    if not isinstance(label, str) or not label:
        raise RecommendationError(f"{context}: foreign-key label is required")
    obj = _get_object(expected_model, object_id)
    actual_label = _object_label(expected_model, obj)
    if actual_label != label:
        raise RecommendationError(
            f"{context}: {expected_model.__name__} {object_id} changed from "
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


def _decode_field_value(field: Any, data: Any, *, context: str) -> Any:
    field.resolve_type()
    value_type = field._type_object
    allow_none = field._allow_none
    if data is None:
        if not allow_none:
            raise RecommendationError(f"{context}: value cannot be null")
        return None
    if isinstance(value_type, type) and issubclass(value_type, BaseModel):
        return _decode_reference(data, value_type, context=context)
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


def _values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, BaseModel) and isinstance(right, BaseModel):
        return type(left) is type(right) and left.id == right.id
    return left == right


def build_plan(
    recommendations: Iterable[Recommendation],
    *,
    model_registry: Mapping[str, type[BaseModel]] | None = None,
    get_object: Callable[[type[BaseModel], int], Any] = _get_object,
    allow_manual_label_changes: bool = False,
) -> RecommendationPlan:
    rows = list(recommendations)
    registry = model_registry or get_model_registry()
    actions: list[PlannedAction] = []
    errors: list[str] = []
    seen: set[tuple[str, int, str | None, str]] = set()
    planned_values: dict[tuple[str, int, str], Any] = {}
    for row in rows:
        try:
            model = registry.get(row.object.model)
            if model is None:
                raise RecommendationError(f"unknown model {row.object.model!r}")
            mutation = (
                MANUAL_REVIEW
                if row.action == MANUAL_REVIEW
                else "field" if row.action == SET_FIELD else repr(row.tag)
            )
            key = (row.object.model, row.object.object_id, row.field, mutation)
            if key in seen:
                raise RecommendationError(
                    "duplicate recommendation for this object field"
                )
            seen.add(key)
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
            field_key = (row.object.model, row.object.object_id, row.field)
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
                )
                new_value = _decode_field_value(
                    field,
                    row.new_value,
                    context=f"{model.__name__}.{row.field} new_value",
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
    print(f"{'ACTION':<14} {'CONF':<8} {'OBJECT':<38} CHANGE")
    print("-" * 120)
    for row in rows:
        obj = f"{row.object.model} {row.object.object_id} {row.object.label}"
        if row.action == MANUAL_REVIEW:
            change = "requires manual review"
        elif row.action == SET_FIELD:
            change = f"{row.field}: {row.old_value!r} -> {row.new_value!r}"
        else:
            change = f"{row.field}: {row.tag!r}"
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


def execute_plan(plan: RecommendationPlan, *, apply: bool) -> None:
    applied = already_applied = manual = 0
    for planned in plan.actions:
        row = planned.recommendation
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
            setattr(planned.object, row.field, planned.new_value)
        applied += 1
    mode = "Applied" if apply else "Dry run"
    print(
        f"{mode}: {applied} generic update(s), {already_applied} already applied, "
        f"{manual} manual review(s)."
    )
    print(f"Recommended actions: {dict(plan.action_counts)}")
    if not apply:
        print("No database changes made. Review this plan, then add --apply.")
