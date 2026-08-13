"""Review or apply heterogeneous taxonomy recommendations from JSONL.

The default mode validates the complete file against the current database and prints
a dry run. Registered action handlers let one file contain Articles, ordinary model
changes, Locations, type localities, Taxon/base-Name pairs, and coverage assertions. Use ``--review``
for a database-independent summary, ``--review-manual`` for the complete text of
manual-review rows and actionable rows carrying ``review_note``, and ``--edit-manual``
to open every manual-review object in the database editor after printing its complete
note. ``--apply`` performs both post-apply cleanup and manual-review editing by default;
use ``--no-edit-applied`` or ``--no-edit-manual`` to skip either phase. Use
``--review-each`` to review every
row in file order and choose yes (queue it for application), no (skip it), or edit
(open its affected database object and skip the automated recommendation). Add
``--virtual-lint`` to construct the final proposed model states in memory and run
advisory lint before the dry run or apply step. Use ``--virtual-lint-issues-only`` to
run the same lint while printing only unresolved ``VIRTUAL_LINT_ISSUES``. Flags
compose: static and classification review views run
first, followed by requested lint and dry-run output, application or per-row review,
affected-object cleanup, and finally manual editing. The only incompatible pair is ``--apply`` with
``--review-each``, because one applies every row while the other selects a subset.
"""

import argparse
import json
import traceback
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Self, cast

from clirm import readonly

from taxonomy import getinput
from taxonomy.applicator import article as article_recommendations
from taxonomy.applicator import coverage as coverage_recommendations
from taxonomy.applicator import generic as generic_recommendations
from taxonomy.applicator import location as location_recommendations
from taxonomy.applicator import proposals as virtual_proposals
from taxonomy.applicator import taxon as taxon_recommendations
from taxonomy.applicator import type_locality as type_recommendations
from taxonomy.db.models import Name
from taxonomy.db.models.base import BaseModel, LintConfig


class RecommendationError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class RegisteredActionHandler:
    """Compatibility adapter registered with the common manifest parser."""

    family: str
    actions: frozenset[str]
    parse: Callable[[dict[str, Any], int], Any]
    matches: Callable[[dict[str, Any]], bool] = lambda _data: True


ACTION_HANDLERS = (
    RegisteredActionHandler(
        "taxon",
        frozenset(taxon_recommendations.ALLOWED_ACTIONS),
        taxon_recommendations.parse_recommendation,
    ),
    RegisteredActionHandler(
        "coverage",
        frozenset(coverage_recommendations.ALLOWED_ACTIONS),
        coverage_recommendations.parse_recommendation,
    ),
    RegisteredActionHandler(
        "article",
        frozenset(article_recommendations.ALLOWED_ACTIONS),
        article_recommendations.parse_recommendation,
    ),
    RegisteredActionHandler(
        "generic",
        frozenset(generic_recommendations.ALLOWED_ACTIONS),
        generic_recommendations.parse_recommendation,
        lambda data: data.get("action") != generic_recommendations.MANUAL_REVIEW
        or "object" in data,
    ),
    RegisteredActionHandler(
        "location",
        frozenset(location_recommendations.ALLOWED_ACTIONS),
        location_recommendations.parse_recommendation,
    ),
    RegisteredActionHandler(
        "type_locality",
        frozenset(type_recommendations.ALLOWED_ACTIONS),
        type_recommendations.parse_recommendation,
    ),
)


class AppliedObjectRecorder:
    """Collect persistent models created or saved within a scoped execution."""

    def __init__(self, model_classes: Iterable[type[BaseModel]] | None = None) -> None:
        self._model_classes = (
            tuple(model_classes) if model_classes is not None else None
        )
        self._events: list[Any] = []
        self._objects: list[BaseModel] = []
        self._seen: set[tuple[type[BaseModel], object]] = set()
        self._callback = self._record

    @staticmethod
    def _all_model_classes() -> tuple[type[BaseModel], ...]:
        classes: list[type[BaseModel]] = []
        pending = list(BaseModel.__subclasses__())
        while pending:
            model = pending.pop()
            classes.append(model)
            pending.extend(model.__subclasses__())
        return tuple(classes)

    def _record(self, obj: BaseModel) -> None:
        object_id = getattr(obj, "id", None)
        identity: object = object_id if object_id is not None else id(obj)
        key = (type(obj), identity)
        if key in self._seen:
            return
        self._seen.add(key)
        self._objects.append(obj)

    def __enter__(self) -> Self:
        seen_events: set[int] = set()
        model_classes = self._model_classes or self._all_model_classes()
        for model in model_classes:
            for event_name in ("creation_event", "save_event"):
                event = getattr(model, event_name, None)
                if event is None or id(event) in seen_events:
                    continue
                seen_events.add(id(event))
                event.handlers.append(self._callback)
                self._events.append(event)
        return self

    def __exit__(self, *_exc_info: object) -> None:
        for event in self._events:
            event.handlers.remove(self._callback)
        self._events.clear()

    @property
    def objects(self) -> tuple[BaseModel, ...]:
        return tuple(self._objects)


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    affected_objects: tuple[BaseModel, ...]


def _persistent_identity(obj: BaseModel) -> tuple[type[BaseModel], object]:
    object_id = getattr(obj, "id", None)
    return type(obj), object_id if object_id is not None else id(obj)


def _deduplicate_objects(objects: Iterable[BaseModel]) -> tuple[BaseModel, ...]:
    output: list[BaseModel] = []
    seen: set[tuple[type[BaseModel], object]] = set()
    for obj in objects:
        if getattr(obj, "id", None) is None:
            continue
        key = _persistent_identity(obj)
        if key not in seen:
            seen.add(key)
            output.append(obj)
    return tuple(output)


def _declared_affected_objects(
    plans: AnyRecommendationPlans, replacements: Mapping[int, BaseModel]
) -> Iterable[BaseModel]:
    if isinstance(plans, UnifiedRecommendationPlan):
        article_plan = plans.article
        taxon_plan = plans.taxon
        generic_plan = plans.generic
        location_plan = plans.location
        type_plan = plans.type_locality
    else:
        article_plan, generic_plan, location_plan, type_plan = plans
        taxon_plan = None
    for article_action in article_plan.actions:
        if article_action.article is not None:
            yield article_action.article
    if taxon_plan is not None:
        for taxon_action in taxon_plan.actions:
            yield generic_recommendations._replace_created_models(
                taxon_action.taxon, replacements
            )
            yield generic_recommendations._replace_created_models(
                taxon_action.name, replacements
            )
    for generic_action in generic_plan.actions:
        if (
            generic_action.recommendation.action
            != generic_recommendations.MANUAL_REVIEW
        ):
            yield generic_recommendations._replace_created_models(
                generic_action.object, replacements
            )
    for location_action in location_plan.actions:
        if isinstance(
            location_action,
            (
                location_recommendations.PlannedRename,
                location_recommendations.PlannedEdit,
            ),
        ):
            if isinstance(location_action.location, BaseModel):
                yield location_action.location
        else:
            if isinstance(location_action.source, BaseModel):
                yield location_action.source
            if isinstance(location_action.target, BaseModel):
                yield location_action.target
    for type_update in type_plan.updates:
        if type_update.recommendation.action not in {
            type_recommendations.MANUAL_REVIEW,
            type_recommendations.NO_ACTION,
        } and isinstance(type_update.name, BaseModel):
            yield type_update.name
    for tag_update in type_plan.location_tag_updates:
        if isinstance(tag_update.location, BaseModel):
            yield tag_update.location
    for serialized_tag_update in type_plan.serialized_location_tag_updates:
        if isinstance(serialized_tag_update.location, BaseModel):
            yield serialized_tag_update.location
    for validity_update in type_plan.type_locality_validity_updates:
        if isinstance(validity_update.name, BaseModel):
            yield validity_update.name
    for origin_update in type_plan.regional_origin_updates:
        if isinstance(origin_update.taxon, BaseModel):
            yield origin_update.taxon


def edit_applied_objects(result: ExecutionResult) -> bool:
    """Clean affected objects and any additional objects their autofixes create."""
    failures: list[str] = []
    objects = list(result.affected_objects)
    seen = {_persistent_identity(obj) for obj in objects}
    # Structured lint may create related models (notably CE materialization). Keep
    # recording during cleanup and append those models in deterministic event order.
    with AppliedObjectRecorder() as recorder:
        index = 0
        while index < len(objects):
            obj = objects[index]
            label = getattr(obj, type(obj).label_field, None)
            print(
                f"POST_APPLY_CLEANUP {index + 1}/{len(objects)} "
                f"object={type(obj).__name__}:{obj.id} label={label!r}"
            )
            try:
                obj.reload()
                obj.format(quiet=True)
                obj.edit_until_clean()
                obj.reload()
                if not obj.is_lint_clean(
                    cfg=LintConfig(interactive=False, autofix=False)
                ):
                    failures.append(f"{type(obj).__name__}:{obj.id} has residual lint")
            except Exception as exc:
                traceback.print_exc()
                failures.append(f"{type(obj).__name__}:{obj.id}: {exc}")
            for discovered in recorder.objects:
                if getattr(discovered, "id", None) is None:
                    continue
                key = _persistent_identity(discovered)
                if key not in seen:
                    seen.add(key)
                    objects.append(discovered)
            index += 1
    print(
        f"POST_APPLY_CLEANUP total={len(objects)} "
        f"clean={len(objects) - len(failures)} incomplete={len(failures)}"
    )
    for failure in failures:
        print(f"  INCOMPLETE {failure}")
    return not failures


@dataclass(frozen=True, slots=True)
class Recommendations:
    article_rows: tuple[article_recommendations.Recommendation, ...]
    generic_rows: tuple[generic_recommendations.Recommendation, ...]
    location_rows: tuple[location_recommendations.Recommendation, ...]
    type_locality_rows: tuple[type_recommendations.Recommendation, ...]
    taxon_rows: tuple[taxon_recommendations.Recommendation, ...] = ()
    coverage_rows: tuple[coverage_recommendations.Recommendation, ...] = ()

    @property
    def count(self) -> int:
        return (
            len(self.article_rows)
            + len(self.generic_rows)
            + len(self.location_rows)
            + len(self.type_locality_rows)
            + len(self.taxon_rows)
            + len(self.coverage_rows)
        )


@dataclass(frozen=True, slots=True)
class ManualReviewObject:
    line_number: int
    recommendation: Any
    object: Any
    family: str


Recommendation = (
    article_recommendations.Recommendation
    | generic_recommendations.Recommendation
    | location_recommendations.Recommendation
    | type_recommendations.Recommendation
    | taxon_recommendations.Recommendation
    | coverage_recommendations.Recommendation
)
RecommendationPlans = tuple[
    article_recommendations.RecommendationPlan,
    generic_recommendations.RecommendationPlan,
    location_recommendations.RecommendationPlan,
    type_recommendations.RecommendationPlan,
]


@dataclass(frozen=True, slots=True)
class UnifiedRecommendationPlan:
    article: article_recommendations.RecommendationPlan
    taxon: taxon_recommendations.RecommendationPlan
    generic: generic_recommendations.RecommendationPlan
    location: location_recommendations.RecommendationPlan
    type_locality: type_recommendations.RecommendationPlan
    coverage: coverage_recommendations.RecommendationPlan
    proposals: tuple[virtual_proposals.ProposedModel, ...]
    references: Mapping[str, BaseModel]


AnyRecommendationPlans = RecommendationPlans | UnifiedRecommendationPlan


@dataclass(frozen=True, slots=True)
class IndividualReviewItem:
    line_number: int
    recommendation: Recommendation
    family: Literal[
        "article", "generic", "location", "type_locality", "taxon", "coverage"
    ]
    edit_object: Any | None


def _validate_location_rows(
    rows: list[location_recommendations.Recommendation],
) -> None:
    mutations: dict[int, str] = {}
    merge_source_ids: set[int] = set()
    merge_target_ids: set[int] = set()
    for row in rows:
        if row.action == location_recommendations.PROMOTE_LOCATION_ALIAS_NAME:
            assert row.source is not None
            assert row.target is not None
            for spec in (row.source, row.target):
                location_id = spec.location_id
                if location_id in mutations or location_id in merge_target_ids:
                    raise RecommendationError(
                        f"line {row.line_number}: Location {location_id} is mutated by "
                        "more than one recommendation"
                    )
            mutations[row.source.location_id] = row.action
            mutations[row.target.location_id] = row.action
            continue
        mutated_spec = (
            row.location
            if row.action
            in {
                location_recommendations.RENAME_LOCATION,
                location_recommendations.EDIT_LOCATION,
            }
            else row.source
        )
        assert mutated_spec is not None
        location_id = mutated_spec.location_id
        existing_mutation = mutations.get(location_id)
        composable_merge_source = (
            row.action == location_recommendations.MERGE_LOCATION
            and existing_mutation == location_recommendations.EDIT_LOCATION
        )
        composable_source_edit = (
            row.action == location_recommendations.EDIT_LOCATION
            and location_id in merge_source_ids
        )
        if (
            (
                existing_mutation is not None
                and not (composable_merge_source or composable_source_edit)
            )
            or (location_id in merge_source_ids and not composable_source_edit)
            or (
                location_id in merge_target_ids
                and row.action
                not in {
                    location_recommendations.RENAME_LOCATION,
                    location_recommendations.EDIT_LOCATION,
                }
            )
        ):
            raise RecommendationError(
                f"line {row.line_number}: Location {location_id} is mutated by more "
                "than one recommendation"
            )
        if not composable_merge_source:
            mutations[location_id] = row.action
        if row.action == location_recommendations.MERGE_LOCATION:
            assert row.target is not None
            merge_source_ids.add(location_id)
            target_id = row.target.location_id
            target_mutation = mutations.get(target_id)
            if target_mutation is not None and target_mutation not in {
                location_recommendations.RENAME_LOCATION,
                location_recommendations.EDIT_LOCATION,
            }:
                raise RecommendationError(
                    f"line {row.line_number}: merge target Location {target_id} is "
                    "mutated by another recommendation"
                )
            merge_target_ids.add(target_id)


def _validate_type_locality_rows(
    rows: list[type_recommendations.Recommendation],
) -> None:
    seen_ids: set[int] = set()
    for row in rows:
        if row.name_id in seen_ids:
            raise RecommendationError(
                f"line {row.line_number}: duplicate type-locality recommendation for "
                f"Name {row.name_id}"
            )
        seen_ids.add(row.name_id)


def read_recommendations(path: Path) -> Recommendations:
    article_rows: list[article_recommendations.Recommendation] = []
    generic_rows: list[generic_recommendations.Recommendation] = []
    location_rows: list[location_recommendations.Recommendation] = []
    type_rows: list[type_recommendations.Recommendation] = []
    taxon_rows: list[taxon_recommendations.Recommendation] = []
    coverage_rows: list[coverage_recommendations.Recommendation] = []
    try:
        lines = path.read_text().splitlines()
    except OSError as exc:
        raise RecommendationError(f"could not read {path}: {exc}") from exc
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            data: Any = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RecommendationError(
                f"line {line_number}: invalid JSON: {exc.msg}"
            ) from exc
        if not isinstance(data, dict):
            raise RecommendationError(f"line {line_number}: row must be an object")
        action = data.get("action")
        try:
            handler = next(
                (
                    candidate
                    for candidate in ACTION_HANDLERS
                    if action in candidate.actions and candidate.matches(data)
                ),
                None,
            )
            if handler is None:
                raise RecommendationError(
                    f"line {line_number}: unsupported action {action!r}"
                )
            parsed = handler.parse(data, line_number)
            destinations: dict[str, list[Any]] = {
                "article": article_rows,
                "generic": generic_rows,
                "location": location_rows,
                "type_locality": type_rows,
                "taxon": taxon_rows,
                "coverage": coverage_rows,
            }
            destinations[handler.family].append(parsed)
        except (
            generic_recommendations.RecommendationError,
            article_recommendations.RecommendationError,
            location_recommendations.RecommendationError,
            type_recommendations.RecommendationError,
            taxon_recommendations.RecommendationError,
            coverage_recommendations.RecommendationError,
        ) as exc:
            raise RecommendationError(str(exc)) from exc
    if not any(
        (
            article_rows,
            generic_rows,
            location_rows,
            type_rows,
            taxon_rows,
            coverage_rows,
        )
    ):
        raise RecommendationError("recommendation file contains no rows")
    _validate_location_rows(location_rows)
    _validate_type_locality_rows(type_rows)
    return Recommendations(
        tuple(article_rows),
        tuple(generic_rows),
        tuple(location_rows),
        tuple(type_rows),
        tuple(taxon_rows),
        tuple(coverage_rows),
    )


def print_review(
    recommendations: Recommendations, *, actions: set[str] | None = None
) -> None:
    article_rows = tuple(
        row
        for row in recommendations.article_rows
        if actions is None or row.action in actions
    )
    generic_rows = tuple(
        row
        for row in recommendations.generic_rows
        if actions is None or row.action in actions
    )
    location_rows = tuple(
        row
        for row in recommendations.location_rows
        if actions is None or row.action in actions
    )
    type_rows = tuple(
        row
        for row in recommendations.type_locality_rows
        if actions is None or row.action in actions
    )
    taxon_rows = tuple(
        row
        for row in recommendations.taxon_rows
        if actions is None or row.action in actions
    )
    coverage_rows = tuple(
        row
        for row in recommendations.coverage_rows
        if actions is None or row.action in actions
    )
    if article_rows:
        print("ARTICLE RECOMMENDATIONS")
        article_recommendations.print_review_table(article_rows)
    if generic_rows:
        if article_rows:
            print()
        print("GENERIC RECOMMENDATIONS")
        generic_recommendations.print_review_table(generic_rows)
    if location_rows:
        if article_rows or generic_rows:
            print()
        print("LOCATION RECOMMENDATIONS")
        location_recommendations.print_review_table(location_rows)
    if type_rows:
        if article_rows or generic_rows or location_rows:
            print()
        print("TYPE-LOCALITY RECOMMENDATIONS")
        type_recommendations.print_review_table(type_rows)
    if taxon_rows:
        if article_rows or generic_rows or location_rows or type_rows:
            print()
        print("TAXON RECOMMENDATIONS")
        taxon_recommendations.print_review_table(taxon_rows)
    if coverage_rows:
        if article_rows or generic_rows or location_rows or type_rows or taxon_rows:
            print()
        print("COVERAGE ASSERTIONS")
        coverage_recommendations.print_review_table(coverage_rows)
    if not any(
        (
            article_rows,
            generic_rows,
            location_rows,
            type_rows,
            taxon_rows,
            coverage_rows,
        )
    ):
        print("No recommendations match the requested actions.")


def print_manual_reviews(recommendations: Recommendations) -> None:
    """Print complete manual-review rows and actionable notes without database I/O."""
    generic_rows = tuple(
        row
        for row in recommendations.generic_rows
        if row.action == generic_recommendations.MANUAL_REVIEW
    )
    location_rows = tuple(
        row for row in recommendations.location_rows if row.review_note is not None
    )
    type_rows = tuple(
        row
        for row in recommendations.type_locality_rows
        if row.action == type_recommendations.MANUAL_REVIEW
        or row.review_note is not None
    )
    if not generic_rows and not location_rows and not type_rows:
        print("No manual_review recommendations or actionable review notes.")
        return
    if generic_rows:
        generic_recommendations.print_full_manual_reviews(generic_rows)
    if location_rows:
        if generic_rows:
            print()
        location_recommendations.print_full_review_notes(location_rows)
    if type_rows:
        if generic_rows or location_rows:
            print()
        type_recommendations.print_full_manual_reviews(type_rows)


def print_classification_review(recommendations: Recommendations) -> None:
    """Render the source-local CE trees directly from native object rows."""
    rows = [
        row
        for row in recommendations.generic_rows
        if row.action == generic_recommendations.CREATE_OBJECT
        and row.object.model == "ClassificationEntry"
    ]
    if not rows:
        print("No native ClassificationEntry creation recommendations.")
        return
    by_ref = {row.object.ref: row for row in rows}
    children: dict[str | None, list[generic_recommendations.Recommendation]] = {}
    for row in rows:
        assert row.values is not None
        parent = row.values.get("parent")
        parent_ref = parent.get("ref") if isinstance(parent, dict) else None
        children.setdefault(parent_ref, []).append(row)

    def display(row: generic_recommendations.Recommendation, depth: int) -> None:
        assert row.values is not None and row.object.ref is not None
        article = row.values.get("article")
        article_ref = article.get("ref") if isinstance(article, dict) else article
        occurrences = sum(
            1
            for candidate in recommendations.generic_rows
            if candidate.action == generic_recommendations.CREATE_OBJECT
            and candidate.object.model == "OccurrenceRecord"
            and candidate.values is not None
            and isinstance(candidate.values.get("classification_entry"), dict)
            and candidate.values["classification_entry"].get("ref") == row.object.ref
        )
        print(
            f"{'  ' * depth}{row.object.label} "
            f"[{row.values.get('rank')}; page {row.values.get('page')}; "
            f"article={article_ref}; occurrences={occurrences}]"
        )
        for child in children.get(row.object.ref, ()):
            display(child, depth + 1)

    roots = [
        row
        for row in rows
        if not (
            isinstance((row.values or {}).get("parent"), dict)
            and (row.values or {})["parent"].get("ref") in by_ref
        )
    ]
    for index, root in enumerate(roots):
        if index:
            print()
        display(root, 0)


def print_reconciliation_review(plans: AnyRecommendationPlans) -> None:
    if isinstance(plans, UnifiedRecommendationPlan):
        generic_plan = plans.generic
    else:
        generic_plan = plans[1]
    ce_actions = [
        action
        for action in generic_plan.actions
        if action.recommendation.action == generic_recommendations.CREATE_OBJECT
        and action.recommendation.object.model == "ClassificationEntry"
    ]
    occurrence_actions = [
        action
        for action in generic_plan.actions
        if action.recommendation.action == generic_recommendations.CREATE_OBJECT
        and action.recommendation.object.model == "OccurrenceRecord"
    ]
    if not ce_actions and not occurrence_actions:
        print("No native classification objects to reconcile.")
        return
    for action in ce_actions:
        values = action.new_value
        mapped = values.get("mapped_name")
        if mapped is not None:
            status = f"MAPPED {mapped}"
        elif any(type(tag).__name__ == "Materialize" for tag in values.get("tags", ())):
            status = (
                "MATERIALIZE_NAME"
                if values["rank"].is_synonym
                else "MATERIALIZE_TAXON_AND_NAME"
            )
        else:
            name = values["name"]
            matches = list(Name.select().filter(Name.corrected_original_name == name))
            if len(matches) == 1:
                status = f"EXACT_CANDIDATE Name:{matches[0].id}"
            elif matches:
                status = f"AMBIGUOUS {len(matches)} candidates"
            else:
                status = "UNRECOGNIZED"
        print(
            f"RECONCILIATION ClassificationEntry ref={action.recommendation.object.ref!r} "
            f"name={values['name']!r} status={status}"
        )
    for action in occurrence_actions:
        values = action.new_value
        location = values.get("location")
        tags = values.get("tags") or ()
        declared_hint = any(type(tag).__name__ == "LocationHint" for tag in tags)
        status = (
            "RESOLVED"
            if location is not None
            else ("UNRESOLVED_DECLARED" if declared_hint else "MISSING")
        )
        print(
            f"RECONCILIATION OccurrenceRecord ref={action.recommendation.object.ref!r} "
            f"locality={values['locality_text']!r} location={status}"
        )


def _get_name_for_manual_review(name_id: int) -> Name:
    try:
        return Name.get(id=name_id)
    except Name.DoesNotExist as exc:
        raise RecommendationError(f"Name {name_id} no longer exists") from exc


def _name_label(name: Any) -> str:
    return name.corrected_original_name or name.original_name or name.root_name


def _resolve_manual_review_objects(
    recommendations: Recommendations,
    generic_plan: generic_recommendations.RecommendationPlan,
    *,
    get_name: Callable[[int], Any] = _get_name_for_manual_review,
    label_name: Callable[[Any], str] = _name_label,
    allow_name_label_changes: bool = False,
) -> tuple[ManualReviewObject, ...]:
    """Resolve and validate every manual-review object before opening any editor."""
    generic_objects = {
        planned.recommendation.line_number: planned.object
        for planned in generic_plan.actions
        if planned.recommendation.action == generic_recommendations.MANUAL_REVIEW
    }
    items = [
        ManualReviewObject(
            row.line_number, row, generic_objects[row.line_number], "generic"
        )
        for row in recommendations.generic_rows
        if row.action == generic_recommendations.MANUAL_REVIEW
    ]
    errors: list[str] = []
    for row in recommendations.type_locality_rows:
        if row.action != type_recommendations.MANUAL_REVIEW:
            continue
        try:
            name = get_name(row.name_id)
            actual_name = label_name(name)
            if actual_name != row.name and not allow_name_label_changes:
                raise RecommendationError(
                    f"Name {row.name_id} changed from {row.name!r} to {actual_name!r}"
                )
            items.append(
                ManualReviewObject(row.line_number, row, name, "type_locality")
            )
        except RecommendationError as exc:
            errors.append(f"line {row.line_number}: {exc}")
    if errors:
        raise RecommendationError(
            "refusing to open editors because database validation failed:\n- "
            + "\n- ".join(errors)
        )
    return tuple(sorted(items, key=lambda item: item.line_number))


def edit_manual_reviews(
    recommendations: Recommendations,
    generic_plan: generic_recommendations.RecommendationPlan,
    *,
    get_name: Callable[[int], Any] = _get_name_for_manual_review,
    label_name: Callable[[Any], str] = _name_label,
) -> None:
    """Print each manual-review note, then invoke its object's interactive editor."""
    items = _resolve_manual_review_objects(
        recommendations, generic_plan, get_name=get_name, label_name=label_name
    )

    _edit_manual_review_objects(items)


def _edit_manual_review_objects(items: tuple[ManualReviewObject, ...]) -> None:
    """Print and edit manual-review objects that were validated as one batch."""
    if not items:
        print("No manual_review recommendations.")
        return
    for index, item in enumerate(items, start=1):
        if index > 1:
            print()
        getinput.print_header(f"Manual review {index}/{len(items)}: {item.object}")
        if item.family == "generic":
            _print_generic_manual_review_for_edit(item.recommendation)
        else:
            _print_type_locality_manual_review_for_edit(item.recommendation)
        print()
        print(getinput.green(f"Opening editor for {item.object}..."))
        item.object.edit()


def _all_recommendation_rows(
    recommendations: Recommendations,
) -> tuple[
    tuple[
        Literal["article", "generic", "location", "type_locality", "taxon", "coverage"],
        Recommendation,
    ],
    ...,
]:
    rows: list[
        tuple[
            Literal[
                "article", "generic", "location", "type_locality", "taxon", "coverage"
            ],
            Recommendation,
        ]
    ] = [
        *(("article", row) for row in recommendations.article_rows),
        *(("generic", row) for row in recommendations.generic_rows),
        *(("location", row) for row in recommendations.location_rows),
        *(("type_locality", row) for row in recommendations.type_locality_rows),
        *(("taxon", row) for row in recommendations.taxon_rows),
        *(("coverage", row) for row in recommendations.coverage_rows),
    ]
    return tuple(sorted(rows, key=lambda item: item[1].line_number))


def _location_edit_object(action: location_recommendations.PlannedAction) -> Any:
    if isinstance(
        action,
        (location_recommendations.PlannedRename, location_recommendations.PlannedEdit),
    ):
        return action.location
    if isinstance(action, location_recommendations.PlannedMerge):
        return action.source
    # The canonical target remains valid and receives the promoted name.
    return action.target


def _resolve_individual_review_items(
    recommendations: Recommendations,
    plans: AnyRecommendationPlans,
    *,
    get_name: Callable[[int], Any] = _get_name_for_manual_review,
    label_name: Callable[[Any], str] = _name_label,
) -> tuple[IndividualReviewItem, ...]:
    """Resolve every possible editor target before opening the first editor."""
    if isinstance(plans, UnifiedRecommendationPlan):
        generic_plan = plans.generic
        location_plan = plans.location
    else:
        _, generic_plan, location_plan, _ = plans
    generic_objects = {
        action.recommendation.line_number: action.object
        for action in generic_plan.actions
        if action.recommendation.object.object_id is not None
    }
    location_objects = {
        action.recommendation.line_number: _location_edit_object(action)
        for action in location_plan.actions
    }
    type_objects: dict[int, Any] = {}
    errors: list[str] = []
    for type_row in recommendations.type_locality_rows:
        try:
            name = get_name(type_row.name_id)
            actual_name = label_name(name)
            if actual_name != type_row.name:
                raise RecommendationError(
                    f"Name {type_row.name_id} changed from {type_row.name!r} "
                    f"to {actual_name!r}"
                )
            type_objects[type_row.line_number] = name
        except RecommendationError as exc:
            errors.append(f"line {type_row.line_number}: {exc}")
    if errors:
        raise RecommendationError(
            "refusing to start individual review because editor-target validation "
            "failed:\n- " + "\n- ".join(errors)
        )

    items: list[IndividualReviewItem] = []
    for family, row in _all_recommendation_rows(recommendations):
        if family == "article":
            edit_object = None
        elif family == "generic":
            edit_object = generic_objects.get(row.line_number)
        elif family == "location":
            edit_object = location_objects[row.line_number]
        elif family == "type_locality":
            edit_object = type_objects[row.line_number]
        else:
            edit_object = None
        if edit_object is not None and not callable(getattr(edit_object, "edit", None)):
            edit_object = None
        items.append(IndividualReviewItem(row.line_number, row, family, edit_object))
    return tuple(items)


def _print_individual_recommendation(
    item: IndividualReviewItem, *, index: int, total: int
) -> None:
    getinput.print_header(
        f"Recommendation {index}/{total} (manifest line {item.line_number})"
    )
    row = item.recommendation
    if item.family == "article":
        assert isinstance(row, article_recommendations.Recommendation)
        article_recommendations.print_review_table((row,))
    elif item.family == "generic":
        assert isinstance(row, generic_recommendations.Recommendation)
        generic_recommendations.print_review_table((row,))
    elif item.family == "location":
        assert isinstance(row, location_recommendations.Recommendation)
        location_recommendations.print_review_table((row,))
    elif item.family == "type_locality":
        assert isinstance(row, type_recommendations.Recommendation)
        type_recommendations.print_review_table((row,))
    elif item.family == "taxon":
        assert isinstance(row, taxon_recommendations.Recommendation)
        taxon_recommendations.print_review_table((row,))
    else:
        assert isinstance(row, coverage_recommendations.Recommendation)
        coverage_recommendations.print_review_table((row,))

    print()
    print(getinput.blue("Evidence"))
    if not row.evidence:
        print("(none)")
    if isinstance(row, type_recommendations.Recommendation):
        for evidence_index, type_evidence in enumerate(row.evidence, start=1):
            source = f"A{type_evidence.source_id} {type_evidence.source_name}"
            print(f"{evidence_index}. {getinput.italicize(source)}")
            print(type_evidence.text)
    elif isinstance(row, location_recommendations.Recommendation):
        for evidence_index, location_evidence in enumerate(row.evidence, start=1):
            print(f"{evidence_index}. {getinput.italicize(location_evidence.kind)}")
            print(location_evidence.text)
    elif isinstance(row, generic_recommendations.Recommendation):
        for evidence_index, other_evidence in enumerate(row.evidence, start=1):
            print(f"{evidence_index}. {getinput.italicize(other_evidence.kind)}")
            print(other_evidence.text)
    elif isinstance(row, article_recommendations.Recommendation):
        for evidence_index, article_evidence in enumerate(row.evidence, start=1):
            print(f"{evidence_index}. {getinput.italicize(article_evidence.kind)}")
            print(article_evidence.text)
    else:
        for evidence_index, remaining_evidence in enumerate(row.evidence, start=1):
            simple_evidence = cast(Any, remaining_evidence)
            print(f"{evidence_index}. {getinput.italicize(simple_evidence.kind)}")
            print(simple_evidence.text)
    tag_comment = getattr(row, "tag_comment", None)
    if tag_comment is not None:
        print()
        print(getinput.blue("Tag comment"))
        print(tag_comment)
    review_note = getattr(row, "review_note", None)
    if review_note is not None:
        print()
        print(getinput.yellow("Review note"))
        print(review_note)
    print()
    print(getinput.yellow("Reason"))
    print(row.reason)
    if item.edit_object is None:
        print()
        print("Direct editing is not available for this recommendation.")


def _prompt_individual_review_choice(*, can_edit: bool) -> Literal["yes", "no", "edit"]:
    choices: dict[str, Literal["yes", "no", "edit"]] = {
        "y": "yes",
        "yes": "yes",
        "n": "no",
        "no": "no",
    }
    prompt = "Apply? [y]es / [n]o"
    if can_edit:
        choices.update({"e": "edit", "edit": "edit"})
        prompt += " / [e]dit"
    result = getinput.get_line(
        prompt + "> ",
        validate=lambda value: value.lower() in choices,
        allow_none=False,
        default="n",
        history_key=("apply_recommendations", "review_each", can_edit),
    )
    assert result is not None
    return choices[result.lower()]


def review_recommendations_individually(
    items: tuple[IndividualReviewItem, ...],
    *,
    choose: Callable[..., Literal["yes", "no", "edit"]] = (
        _prompt_individual_review_choice
    ),
) -> set[int] | None:
    """Collect apply choices, opening editors immediately when requested."""
    selected_lines: set[int] = set()
    print(
        "Reviewing recommendations in file order. Yes queues a recommendation for "
        "application; no skips it; edit opens the affected object and skips the "
        "automated recommendation. Queued rows are revalidated and applied together "
        "after the final choice."
    )
    for index, item in enumerate(items, start=1):
        print()
        _print_individual_recommendation(item, index=index, total=len(items))
        print()
        try:
            choice = choose(can_edit=item.edit_object is not None)
        except getinput.StopException:
            print()
            print(
                getinput.yellow(
                    "Individual review aborted. Queued recommendations were not "
                    "applied; edits already completed in an object editor are not "
                    "rolled back."
                )
            )
            return None
        if choice == "yes":
            selected_lines.add(item.line_number)
        elif choice == "edit":
            assert item.edit_object is not None
            print(getinput.green(f"Opening editor for {item.edit_object}..."))
            item.edit_object.edit()
    return selected_lines


def _filter_recommendations(
    recommendations: Recommendations, selected_lines: set[int]
) -> Recommendations:
    return Recommendations(
        tuple(
            row
            for row in recommendations.article_rows
            if row.line_number in selected_lines
        ),
        tuple(
            row
            for row in recommendations.generic_rows
            if row.line_number in selected_lines
        ),
        tuple(
            row
            for row in recommendations.location_rows
            if row.line_number in selected_lines
        ),
        tuple(
            row
            for row in recommendations.type_locality_rows
            if row.line_number in selected_lines
        ),
        tuple(
            row
            for row in recommendations.taxon_rows
            if row.line_number in selected_lines
        ),
        tuple(
            row
            for row in recommendations.coverage_rows
            if row.line_number in selected_lines
        ),
    )


def _confidence_text(confidence: str) -> str:
    color = {
        "high": getinput.green,
        "medium": getinput.yellow,
        "low": getinput.red,
    }.get(confidence, getinput.blue)
    return color(confidence)


def _print_generic_manual_review_for_edit(
    row: generic_recommendations.Recommendation,
) -> None:
    """Print one generic review with its reason nearest the editor prompt."""
    current = f"{row.object.model} {row.object.object_id} {row.object.label}"
    print(f"{getinput.blue('Object:')} {current}")
    print(f"{getinput.blue('Manifest line:')} {row.line_number}")
    print(f"{getinput.blue('Confidence:')} {_confidence_text(row.confidence)}")
    print()
    print(getinput.blue("Evidence"))
    for index, evidence in enumerate(row.evidence, start=1):
        print(f"{index}. {getinput.italicize(evidence.kind)}")
        print(evidence.text)
    print()
    print(getinput.yellow("Reason"))
    print(row.reason)


def _print_type_locality_manual_review_for_edit(
    row: type_recommendations.Recommendation,
) -> None:
    """Print one type-locality review with its reason nearest the editor prompt."""
    print(f"{getinput.blue('Name:')} N{row.name_id} {row.name}")
    print(
        f"{getinput.blue('Current Location:')} "
        f"L{row.current_location_id} {row.current_location_name}"
    )
    print(f"{getinput.blue('Manifest line:')} {row.line_number}")
    print(f"{getinput.blue('Confidence:')} {_confidence_text(row.confidence)}")
    print()
    print(getinput.blue("Evidence"))
    if not row.evidence:
        print("(none)")
    for index, evidence in enumerate(row.evidence, start=1):
        source = f"A{evidence.source_id} {evidence.source_name}"
        print(f"{index}. {getinput.italicize(source)}")
        print(evidence.text)
    if row.tag_comment is not None:
        print()
        print(getinput.blue("Tag comment"))
        print(row.tag_comment)
    if row.review_note is not None:
        print()
        print(getinput.yellow("Review note"))
        print(row.review_note)
    print()
    print(getinput.yellow("Reason"))
    print(row.reason)


def _uses_unified_plan(recommendations: Recommendations) -> bool:
    return bool(
        recommendations.taxon_rows
        or recommendations.coverage_rows
        or any(row.ref is not None for row in recommendations.article_rows)
        or any(row.schema_version == 2 for row in recommendations.generic_rows)
    )


def build_plans(recommendations: Recommendations) -> AnyRecommendationPlans:
    # Validate every family before any executor is allowed to write.
    try:
        article_plan = article_recommendations.build_plan(recommendations.article_rows)
        if _uses_unified_plan(recommendations):
            builder = virtual_proposals.ProposalBuilder()
            references: dict[str, BaseModel] = dict(
                article_recommendations.add_virtual_models(article_plan, builder)
            )
            taxon_plan = taxon_recommendations.build_plan(
                recommendations.taxon_rows, initial_references=references
            )
            references.update(taxon_plan.references)
            taxon_recommendations.add_virtual_models(taxon_plan, builder)
            generic_plan = generic_recommendations.build_plan(
                recommendations.generic_rows, initial_references=references
            )
            references.update(generic_plan.references or {})
        else:
            builder = None
            references = {}
            taxon_plan = taxon_recommendations.RecommendationPlan((), Counter(), {})
            generic_plan = generic_recommendations.build_plan(
                recommendations.generic_rows
            )
        location_plan = location_recommendations.build_plan(
            recommendations.location_rows
        )
        allowed_target_names: dict[int, set[str]] = {}
        for row in recommendations.location_rows:
            if row.location is None:
                continue
            final_names = allowed_target_names.setdefault(
                row.location.location_id, set()
            )
            if row.action == location_recommendations.RENAME_LOCATION:
                assert row.new_name is not None
                final_names.add(row.new_name)
            elif row.action == location_recommendations.EDIT_LOCATION:
                final_names.update(
                    change.new_value
                    for change in row.changes
                    if change.field == "name" and isinstance(change.new_value, str)
                )
        type_plan = type_recommendations.build_plan(
            recommendations.type_locality_rows,
            allowed_target_names=allowed_target_names,
        )
        if builder is not None:
            type_recommendations.add_virtual_models(type_plan, builder)
            location_recommendations.add_virtual_models(location_plan, builder)
            generic_recommendations.add_virtual_models(generic_plan, builder)
            coverage_plan = coverage_recommendations.build_plan(
                recommendations.coverage_rows, references=references
            )
            return UnifiedRecommendationPlan(
                article_plan,
                taxon_plan,
                generic_plan,
                location_plan,
                type_plan,
                coverage_plan,
                builder.build(),
                references,
            )
    except (
        generic_recommendations.RecommendationError,
        article_recommendations.RecommendationError,
        location_recommendations.RecommendationError,
        type_recommendations.RecommendationError,
        taxon_recommendations.RecommendationError,
        coverage_recommendations.RecommendationError,
    ) as exc:
        raise RecommendationError(str(exc)) from exc
    return article_plan, generic_plan, location_plan, type_plan


def build_generic_manual_review_plan(
    recommendations: Recommendations,
) -> generic_recommendations.RecommendationPlan:
    """Validate only the objects that edit-only mode will open."""
    rows = tuple(
        row
        for row in recommendations.generic_rows
        if row.action == generic_recommendations.MANUAL_REVIEW
    )
    try:
        return generic_recommendations.build_plan(rows, allow_manual_label_changes=True)
    except generic_recommendations.RecommendationError as exc:
        raise RecommendationError(str(exc)) from exc


def build_virtual_proposals(
    plans: AnyRecommendationPlans,
) -> tuple[virtual_proposals.ProposedModel, ...]:
    """Build the final in-memory state represented by all actionable plans."""
    if isinstance(plans, UnifiedRecommendationPlan):
        return plans.proposals
    article_plan, generic_plan, location_plan, type_plan = plans
    builder = virtual_proposals.ProposalBuilder()
    # Match execute_plans() ordering so actions that touch the same model compose.
    article_recommendations.add_virtual_models(article_plan, builder)
    type_recommendations.add_virtual_models(type_plan, builder)
    location_recommendations.add_virtual_models(location_plan, builder)
    generic_recommendations.add_virtual_models(generic_plan, builder)
    return builder.build()


def run_virtual_lint(
    plans: AnyRecommendationPlans, *, issues_only: bool = False
) -> None:
    proposals = build_virtual_proposals(plans)
    virtual_proposals.print_lint_results(
        virtual_proposals.lint_proposals(proposals), issues_only=issues_only
    )


def execute_plans(plans: AnyRecommendationPlans, *, apply: bool) -> ExecutionResult:
    recorder_context: AppliedObjectRecorder | nullcontext[None]
    recorder_context = AppliedObjectRecorder() if apply else nullcontext()
    with recorder_context as recorder:
        if isinstance(plans, UnifiedRecommendationPlan):
            replacements: dict[int, BaseModel] = {}
            if plans.article.action_counts:
                print("ARTICLE PLAN")
                article_outputs = article_recommendations.execute_plan(
                    plans.article, apply=apply
                )
                for ref, actual in article_outputs.items():
                    proposed = plans.references.get(ref)
                    if proposed is not None:
                        replacements[id(proposed)] = actual
            if plans.taxon.action_counts:
                if plans.article.action_counts:
                    print()
                print("TAXON PLAN")
                replacements.update(
                    taxon_recommendations.execute_plan(
                        plans.taxon, apply=apply, replacements=replacements
                    )
                )
            if plans.type_locality.action_counts:
                print()
                print("TYPE-LOCALITY PLAN")
                type_recommendations.execute_plan(plans.type_locality, apply=apply)
            if plans.location.action_counts:
                print()
                print("LOCATION PLAN")
                location_recommendations.execute_plan(plans.location, apply=apply)
            if plans.generic.action_counts:
                print()
                print("OBJECT PLAN")
                replacements.update(
                    generic_recommendations.execute_plan(
                        plans.generic, apply=apply, initial_replacements=replacements
                    )
                )
            if plans.coverage.action_counts:
                print()
                print("COVERAGE PLAN")
                coverage_recommendations.execute_plan(plans.coverage, apply=apply)
            if not apply:
                return ExecutionResult(())
            observed = (
                recorder.objects if isinstance(recorder, AppliedObjectRecorder) else ()
            )
            return ExecutionResult(
                _deduplicate_objects(
                    (*observed, *_declared_affected_objects(plans, replacements))
                )
            )

        article_plan, generic_plan, location_plan, type_plan = plans
        if article_plan.action_counts:
            print("ARTICLE PLAN")
            article_recommendations.execute_plan(article_plan, apply=apply)
        if type_plan.action_counts:
            if article_plan.action_counts:
                print()
            print("TYPE-LOCALITY PLAN")
            type_recommendations.execute_plan(type_plan, apply=apply)
        if location_plan.action_counts:
            if article_plan.action_counts or type_plan.action_counts:
                print()
            print("LOCATION PLAN")
            location_recommendations.execute_plan(location_plan, apply=apply)
        if generic_plan.action_counts:
            if (
                article_plan.action_counts
                or type_plan.action_counts
                or location_plan.action_counts
            ):
                print()
            print("GENERIC PLAN")
            generic_recommendations.execute_plan(generic_plan, apply=apply)
    if not apply:
        return ExecutionResult(())
    observed = recorder.objects if isinstance(recorder, AppliedObjectRecorder) else ()
    return ExecutionResult(
        _deduplicate_objects((*observed, *_declared_affected_objects(plans, {})))
    )


def _run(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    manual_items: tuple[ManualReviewObject, ...] | None = None
    individual_items: tuple[IndividualReviewItem, ...] | None = None
    plans: AnyRecommendationPlans | None = None
    printed_output = False
    run_virtual_lint_requested = args.virtual_lint or args.virtual_lint_issues_only
    execution_result: ExecutionResult | None = None

    def begin_output() -> None:
        nonlocal printed_output
        if printed_output:
            print()
        printed_output = True

    try:
        recommendations = read_recommendations(args.recommendations)
    except RecommendationError as exc:
        parser.error(str(exc))

    if args.review:
        begin_output()
        actions = set(args.review_action) if args.review_action else None
        print_review(recommendations, actions=actions)
    if args.review_manual:
        begin_output()
        print_manual_reviews(recommendations)
    if args.review_classification:
        begin_output()
        print_classification_review(recommendations)

    run_dry_run = args.dry_run or not any(
        (
            args.review,
            args.review_manual,
            args.review_classification,
            args.review_reconciliation,
            args.apply,
            args.review_each,
            args.edit_manual,
            args.virtual_lint_issues_only,
        )
    )
    needs_complete_plan = bool(
        run_dry_run
        or args.apply
        or args.review_each
        or args.review_reconciliation
        or run_virtual_lint_requested
    )
    if not needs_complete_plan and not args.edit_manual:
        return

    try:
        if needs_complete_plan:
            plans = build_plans(recommendations)
        else:
            assert args.edit_manual
            try:
                plans = build_plans(recommendations)
                generic_plan = (
                    plans.generic
                    if isinstance(plans, UnifiedRecommendationPlan)
                    else plans[1]
                )
            except RecommendationError as exc:
                begin_output()
                print(
                    getinput.yellow(
                        "Warning: the complete manifest no longer matches the "
                        "database. Because --edit-manual does not apply actionable "
                        "rows, it will validate the manual-review objects separately "
                        "and continue:"
                    )
                )
                print(exc)
                print()
                generic_plan = build_generic_manual_review_plan(recommendations)

        if args.review_each:
            assert plans is not None
            individual_items = _resolve_individual_review_items(recommendations, plans)
        if args.edit_manual:
            # Resolve every editor target before any automatic operation can write.
            # This avoids discovering a stale later manual-review row after a partial
            # run. Edit-only mode retains its tolerant stale-manifest behavior.
            if plans is None:
                manual_items = _resolve_manual_review_objects(
                    recommendations, generic_plan, allow_name_label_changes=True
                )
            else:
                manual_items = _resolve_manual_review_objects(
                    recommendations,
                    (
                        plans.generic
                        if isinstance(plans, UnifiedRecommendationPlan)
                        else plans[1]
                    ),
                )
    except RecommendationError as exc:
        parser.error(str(exc))

    if run_virtual_lint_requested and (not args.review_each or run_dry_run):
        assert plans is not None
        begin_output()
        run_virtual_lint(plans, issues_only=args.virtual_lint_issues_only)

    if args.review_reconciliation:
        assert plans is not None
        begin_output()
        print_reconciliation_review(plans)

    if run_dry_run:
        assert plans is not None
        begin_output()
        execute_plans(plans, apply=False)

    if args.apply:
        assert plans is not None
        begin_output()
        execution_result = execute_plans(plans, apply=True)

    if individual_items is not None:
        begin_output()
        selected_lines = review_recommendations_individually(individual_items)
        if selected_lines is None:
            return
        selected = _filter_recommendations(recommendations, selected_lines)
        if selected.count == 0:
            begin_output()
            print("No recommendations selected; no automated changes made.")
        else:
            try:
                # An editor may have changed database state, and omitted
                # create_object rows may invalidate selected ref-dependent rows.
                # Rebuild the selected plan as a single unit before any write.
                selected_plans = build_plans(selected)
            except RecommendationError as exc:
                parser.error(
                    "selected recommendations failed final database validation; no "
                    f"automated changes were made: {exc}"
                )
            if run_virtual_lint_requested:
                begin_output()
                run_virtual_lint(
                    selected_plans, issues_only=args.virtual_lint_issues_only
                )
            begin_output()
            execution_result = execute_plans(selected_plans, apply=True)

    if args.edit_applied and execution_result is not None:
        begin_output()
        if not edit_applied_objects(execution_result):
            print(
                getinput.yellow(
                    "Warning: post-apply cleanup was incomplete; recommendations "
                    "were already applied. Continuing."
                )
            )

    if manual_items is not None:
        begin_output()
        _edit_manual_review_objects(manual_items)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recommendations", type=Path)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "validate current database state and print the complete plan; may be "
            "combined with later write or edit operations"
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "write all validated recommendations, then edit affected and "
            "manual-review objects by default"
        ),
    )
    parser.add_argument(
        "--review",
        action="store_true",
        help=(
            "print static tables with untruncated action details and manual-review "
            "evidence without consulting the database"
        ),
    )
    parser.add_argument(
        "--review-action",
        action="append",
        choices=sorted(
            generic_recommendations.ALLOWED_ACTIONS
            | article_recommendations.ALLOWED_ACTIONS
            | location_recommendations.ALLOWED_ACTIONS
            | type_recommendations.ALLOWED_ACTIONS
            | taxon_recommendations.ALLOWED_ACTIONS
            | coverage_recommendations.ALLOWED_ACTIONS
        ),
        help="with --review, include only this action (repeatable)",
    )
    parser.add_argument(
        "--review-manual",
        action="store_true",
        help=(
            "print manual_review actions and actionable rows carrying review_note, "
            "including complete notes, reasons, and evidence, without consulting "
            "the database"
        ),
    )
    parser.add_argument(
        "--review-classification",
        action="store_true",
        help="print source-local ClassificationEntry trees without database access",
    )
    parser.add_argument(
        "--review-reconciliation",
        action="store_true",
        help="report Name and Location reconciliation over the validated object plan",
    )
    parser.add_argument(
        "--review-each",
        action="store_true",
        help=(
            "validate the complete manifest, then review each row in file order "
            "with yes (apply after all choices), no (skip), or edit (open the "
            "affected object and skip); accepted rows are revalidated together "
            "before writing"
        ),
    )
    parser.add_argument(
        "--edit-manual",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "validate the complete manifest, then for every manual_review row print "
            "its complete note and invoke edit() on the referenced object; generic "
            "rows edit their explicit object and type-locality rows edit their Name; "
            "enabled by default with --apply"
        ),
    )
    parser.add_argument(
        "--edit-applied",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "after successful --apply or accepted --review-each selections, run "
            "format() and edit_until_clean() on every created or modified object, "
            "including related objects created by structured lint autofixes; enabled "
            "by default with --apply"
        ),
    )
    parser.add_argument(
        "--virtual-lint",
        action="store_true",
        help=(
            "build virtual versions of all proposed models and run advisory, "
            "best-effort lint before dry-run output or writes; structured "
            "autofixes are reported separately from issues requiring manifest edits"
        ),
    )
    parser.add_argument(
        "--virtual-lint-issues-only",
        action="store_true",
        help=(
            "run advisory virtual lint but print only unresolved "
            "VIRTUAL_LINT_ISSUES, omitting autofixable findings, summaries, and "
            "the implicit default dry run"
        ),
    )
    args = parser.parse_args()
    if args.edit_manual is None:
        args.edit_manual = args.apply
    if args.edit_applied is None:
        args.edit_applied = args.apply
    if args.review_action and not args.review:
        parser.error("--review-action requires --review")
    if args.review_each and args.apply:
        parser.error(
            "--review-each selects individual rows, so it cannot be combined with "
            "--apply, which applies every row"
        )
    if args.edit_applied and not (args.apply or args.review_each):
        parser.error("--edit-applied requires --apply or --review-each")
    context = (
        nullcontext()
        if args.apply or args.edit_manual or args.review_each
        else readonly()
    )
    with context:
        _run(args, parser)


if __name__ == "__main__":
    main()
