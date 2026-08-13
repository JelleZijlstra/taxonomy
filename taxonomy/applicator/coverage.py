"""Non-mutating completeness assertions over persisted and proposed objects."""

from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from taxonomy.applicator import generic
from taxonomy.db.models import BaseModel

CHECK_COVERAGE = "check_coverage"
ALLOWED_ACTIONS = {CHECK_COVERAGE}


class RecommendationError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class Recommendation:
    line_number: int
    action: str
    confidence: str
    reason: str
    evidence: tuple[generic.Evidence, ...]
    model: str
    where: Mapping[str, Any]
    completeness: str
    covered_objects: tuple[Mapping[str, Any], ...]
    unexpected: str


@dataclass(frozen=True, slots=True)
class PlannedAction:
    recommendation: Recommendation
    scope: tuple[BaseModel, ...]
    covered: tuple[BaseModel, ...]
    unexpected: tuple[BaseModel, ...]


@dataclass(frozen=True, slots=True)
class RecommendationPlan:
    actions: tuple[PlannedAction, ...]
    action_counts: Counter[str]


def parse_recommendation(data: dict[str, Any], line_number: int) -> Recommendation:
    if data.get("schema_version") != 2:
        raise RecommendationError(
            f"line {line_number}: check_coverage requires schema_version 2"
        )
    scope = data.get("scope")
    if not isinstance(scope, dict) or not isinstance(scope.get("where"), dict):
        raise RecommendationError(
            f"line {line_number}: scope requires model and where objects"
        )
    model = scope.get("model")
    if not isinstance(model, str) or not model:
        raise RecommendationError(f"line {line_number}: scope.model is required")
    completeness = data.get("completeness")
    if completeness not in {"partial", "complete"}:
        raise RecommendationError(
            f"line {line_number}: completeness must be 'partial' or 'complete'"
        )
    unexpected = data.get("unexpected", "report")
    if unexpected not in {"report", "error"}:
        raise RecommendationError(
            f"line {line_number}: unexpected must be 'report' or 'error'"
        )
    covered = data.get("covered_objects")
    if not isinstance(covered, list) or not all(
        isinstance(item, dict) for item in covered
    ):
        raise RecommendationError(
            f"line {line_number}: covered_objects must be a list of object specs"
        )
    confidence = data.get("confidence")
    reason = data.get("reason")
    if (
        confidence not in generic.ALLOWED_CONFIDENCES
        or not isinstance(reason, str)
        or not reason
    ):
        raise RecommendationError(
            f"line {line_number}: valid confidence and nonempty reason are required"
        )
    evidence = generic._parse_evidence(data.get("evidence"), line_number)
    return Recommendation(
        line_number,
        CHECK_COVERAGE,
        confidence,
        reason,
        evidence,
        model,
        scope["where"],
        completeness,
        tuple(covered),
        unexpected,
    )


def _query(model: type[BaseModel], where: Mapping[str, Any]) -> list[BaseModel]:
    query = model.select()
    for name, value in where.items():
        query = query.filter(model.clirm_fields[name] == value)
    return list(query)


def build_plan(
    recommendations: Iterable[Recommendation],
    *,
    references: Mapping[str, BaseModel],
    model_registry: Mapping[str, type[BaseModel]] | None = None,
    query: Callable[[type[BaseModel], Mapping[str, Any]], list[BaseModel]] = _query,
) -> RecommendationPlan:
    registry = model_registry or generic.get_model_registry()
    actions: list[PlannedAction] = []
    errors: list[str] = []
    rows = list(recommendations)
    for row in rows:
        try:
            model = registry.get(row.model)
            if model is None:
                raise RecommendationError(f"unknown model {row.model!r}")
            where = generic._decode_create_values(
                model,
                row.where,
                context=f"line {row.line_number} scope.where",
                references=references,
                require_label=False,
            )
            scope = list(query(model, where))
            for proposed in references.values():
                if (
                    isinstance(proposed, model)
                    and proposed not in scope
                    and all(
                        generic._values_equal(getattr(proposed, name), value)
                        for name, value in where.items()
                    )
                ):
                    scope.append(proposed)
            covered: list[BaseModel] = []
            for index, spec in enumerate(row.covered_objects, start=1):
                obj = generic._decode_reference(
                    spec,
                    model,
                    context=f"line {row.line_number} covered_objects[{index}]",
                    references=references,
                )
                assert obj is not None
                covered.append(obj)
            unexpected = tuple(obj for obj in scope if obj not in covered)
            if (
                row.completeness == "complete"
                and unexpected
                and row.unexpected == "error"
            ):
                raise RecommendationError(
                    "coverage assertion found unexpected objects: "
                    + ", ".join(str(obj) for obj in unexpected)
                )
            actions.append(PlannedAction(row, tuple(scope), tuple(covered), unexpected))
        except (generic.RecommendationError, RecommendationError) as exc:
            errors.append(f"line {row.line_number}: {exc}")
    if errors:
        raise RecommendationError(
            "refusing to continue because coverage validation failed:\n- "
            + "\n- ".join(errors)
        )
    return RecommendationPlan(tuple(actions), Counter(row.action for row in rows))


def print_review_table(rows: Iterable[Recommendation]) -> None:
    for row in rows:
        print(
            f"line={row.line_number} action=check_coverage confidence={row.confidence} "
            f"scope={row.model} completeness={row.completeness} unexpected={row.unexpected}"
        )
        for field_name, value in row.where.items():
            generic._print_review_detail(
                "scope guard", f"{field_name}={generic._format_manifest_value(value)}"
            )
        for covered_object in row.covered_objects:
            generic._print_review_detail(
                "covered object", generic._format_manifest_value(covered_object)
            )


def execute_plan(plan: RecommendationPlan, *, apply: bool) -> None:
    del apply
    for action in plan.actions:
        row = action.recommendation
        print(
            f"COVERAGE line={row.line_number} model={row.model} "
            f"scope={len(action.scope)} covered={len(action.covered)} "
            f"unexpected={len(action.unexpected)} completeness={row.completeness}"
        )
        for obj in action.unexpected:
            print(f"  UNEXPECTED {type(obj).__name__}:{obj.id} {obj}")
    print("Coverage checks are non-mutating.")
