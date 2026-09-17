"""Review or apply heterogeneous taxonomy recommendations from JSONL.

The default mode validates the complete file against the current database and prints
a dry run. Registered action handlers let one file contain Articles, ordinary model
changes, Locations, type localities, Taxon/base-Name pairs, and coverage assertions. Use ``--review``
for a database-independent summary, ``--review-manual`` for the complete text of
manual-review rows and actionable rows carrying ``review_note``, and ``--edit-manual``
to open every manual-review object in the database editor after printing its complete
note. ``--review-classification`` validates the manifest against the database and
prints the affected ClassificationEntry hierarchy, including existing ancestor entries
and proposed creations and updates, grouped by Article. ``--apply`` performs both
post-apply cleanup and manual-review editing by default. Cleanup first runs
noninteractive lint with autofixes on all affected objects, then interactively
resolves the objects with remaining issues;
use ``--no-edit-applied`` or ``--no-edit-manual`` to skip either phase. Use
``--edit-each`` to call ``edit()`` on every affected object during post-apply
cleanup, after its automatic formatting and lint cleanup. Use
``--review-each`` to review every
row in file order and choose yes (apply it immediately), no (skip it), or edit
(open its affected database object and skip the automated recommendation). Before each
immediate write, the cumulative accepted subset is rebuilt against the current database
so manifest-local references and dependencies remain available. Add
``--virtual-lint`` to construct the final proposed model states in memory and run
advisory lint before the dry run or apply step. Use ``--virtual-lint-issues-only`` to
run the same lint while printing only unresolved ``VIRTUAL_LINT_ISSUES``. Flags
compose: static review views run first, followed by the validated classification view,
requested lint and dry-run output, application or per-row review,
affected-object cleanup, and finally manual editing. The only incompatible pair is ``--apply`` with
``--review-each``, because one applies every row while the other selects a subset.
Use ``--skip-invalid`` to omit whole manifest rows whose validation errors identify
their line numbers, then rebuild and validate the remaining rows together before any
write. Validation failures that cannot be attributed to specific lines still abort.
"""

import argparse
import json
import re
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
from taxonomy.applicator import item_file as item_file_recommendations
from taxonomy.applicator import location as location_recommendations
from taxonomy.applicator import proposals as virtual_proposals
from taxonomy.applicator import staged_file as staged_file_recommendations
from taxonomy.applicator import taxon as taxon_recommendations
from taxonomy.applicator import type_locality as type_recommendations
from taxonomy.db.models import (
    Article,
    CitationGroup,
    ClassificationEntry,
    Location,
    Name,
)
from taxonomy.db.models.base import BaseModel, LintConfig
from taxonomy.db.models.lint_types import LintResult


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
        "item_file",
        frozenset(item_file_recommendations.ALLOWED_ACTIONS),
        item_file_recommendations.parse_recommendation,
    ),
    RegisteredActionHandler(
        "staged_file",
        frozenset(staged_file_recommendations.ALLOWED_ACTIONS),
        staged_file_recommendations.parse_recommendation,
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


@dataclass(frozen=True, slots=True)
class IndividualReviewResult:
    applied_lines: frozenset[int]
    execution_result: ExecutionResult
    aborted: bool


@dataclass(frozen=True, slots=True)
class SkippedInvalidRecommendation:
    line_number: int
    family: str
    action: str
    reason: str


def _persistent_identity(obj: BaseModel) -> tuple[type[BaseModel], object]:
    object_id = getattr(obj, "id", None)
    return type(obj), object_id if object_id is not None else id(obj)


def _deduplicate_objects(objects: Iterable[BaseModel]) -> tuple[BaseModel, ...]:
    output: list[BaseModel] = []
    seen: set[tuple[type[BaseModel], object]] = set()
    for obj in objects:
        # Plans represent created objects with in-memory models whose negative
        # VirtualIds have no corresponding database row. The execution recorder
        # supplies the persistent objects created from those proposals; never
        # send an unresolved proposal to post-apply reload/lint cleanup.
        if getattr(obj, "is_virtual", False):
            continue
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
        item_file_plan = plans.item_file
        taxon_plan = plans.taxon
        generic_plan = plans.generic
        location_plan = plans.location
        type_plan = plans.type_locality
    else:
        article_plan, generic_plan, location_plan, type_plan = plans
        item_file_plan = item_file_recommendations.RecommendationPlan((), Counter())
        taxon_plan = None
    for article_action in article_plan.actions:
        if article_action.article is not None:
            yield article_action.article
    for item_file_action in item_file_plan.actions:
        if item_file_action.item_file is not None:
            yield item_file_action.item_file
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
            if generic_action.recommendation.action in {
                generic_recommendations.MERGE_PERSON,
                generic_recommendations.REASSIGN_PERSON_REFERENCES,
                generic_recommendations.SYNONYMIZE_TAXON,
            }:
                merge_data = generic_action.new_value
                yield generic_recommendations._replace_created_models(
                    merge_data["target"], replacements
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


def edit_applied_objects(result: ExecutionResult, *, edit_each: bool = False) -> bool:
    """Batch automatic lint before prompting for unresolved issues."""
    failures: list[str] = []
    objects = list(result.affected_objects)
    seen = {_persistent_identity(obj) for obj in objects}
    # Structured lint may create related models (notably CE materialization). Keep
    # recording during cleanup and append those models in deterministic event order.
    with AppliedObjectRecorder() as recorder:

        def collect_discovered() -> None:
            for discovered in _deduplicate_objects(recorder.objects):
                key = _persistent_identity(discovered)
                if key not in seen:
                    seen.add(key)
                    objects.append(discovered)

        index = 0
        while index < len(objects):
            pending: list[tuple[BaseModel, list[LintResult]]] = []
            # Drain automatic work, including related objects created by lint,
            # before starting any interactive cleanup.
            while index < len(objects):
                obj = objects[index]
                label = getattr(obj, type(obj).label_field, None)
                print(
                    f"POST_APPLY_CLEANUP {index + 1}/{len(objects)} object={type(obj).__name__}:{obj.id} label={label!r}"
                )
                try:
                    obj.reload()
                    messages = list(
                        obj.general_lint(LintConfig(interactive=False, autofix=True))
                    )
                    if messages or edit_each:
                        pending.append((obj, messages))
                except Exception as exc:
                    traceback.print_exc()
                    failures.append(f"{type(obj).__name__}:{obj.id}: {exc}")
                collect_discovered()
                index += 1

            print(
                f"POST_APPLY_CLEANUP objects_with_issues={sum(bool(messages) for _, messages in pending)}"
            )
            for obj, messages in pending:
                try:
                    obj.reload()
                    if messages:
                        getinput.print_header(obj)
                        obj.display()
                        for message in messages:
                            print(message)
                        obj.edit_until_clean(
                            cfg=LintConfig(interactive=True, autofix=True)
                        )
                    if edit_each:
                        obj.edit()
                    obj.reload()
                    if not obj.is_lint_clean(
                        cfg=LintConfig(interactive=False, autofix=False)
                    ):
                        failures.append(
                            f"{type(obj).__name__}:{obj.id} has residual lint"
                        )
                except Exception as exc:
                    traceback.print_exc()
                    failures.append(f"{type(obj).__name__}:{obj.id}: {exc}")
            # Objects created during editing get their own batch of automatic
            # lint after the current prompts have finished.
            collect_discovered()
    print(
        f"POST_APPLY_CLEANUP total={len(objects)} clean={len(objects) - len(failures)} incomplete={len(failures)}"
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
    item_file_rows: tuple[item_file_recommendations.Recommendation, ...] = ()
    staged_file_rows: tuple[staged_file_recommendations.Recommendation, ...] = ()

    @property
    def count(self) -> int:
        return (
            len(self.article_rows)
            + len(self.generic_rows)
            + len(self.location_rows)
            + len(self.type_locality_rows)
            + len(self.taxon_rows)
            + len(self.coverage_rows)
            + len(self.item_file_rows)
            + len(self.staged_file_rows)
        )


@dataclass(frozen=True, slots=True)
class ManualReviewObject:
    line_number: int
    recommendation: Any
    object: Any
    family: str


Recommendation = (
    article_recommendations.Recommendation
    | item_file_recommendations.Recommendation
    | staged_file_recommendations.Recommendation
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
    item_file: item_file_recommendations.RecommendationPlan
    staged_file: staged_file_recommendations.RecommendationPlan
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
        "article",
        "item_file",
        "staged_file",
        "generic",
        "location",
        "type_locality",
        "taxon",
        "coverage",
    ]
    edit_object: Any | None


def _validate_proposed_location_name_uniqueness(
    proposals: tuple[virtual_proposals.ProposedModel, ...],
) -> None:
    """Reject cross-family Location name collisions before any executor writes."""
    by_name: dict[str, virtual_proposals.ProposedModel] = {}
    for proposal in proposals:
        if not isinstance(proposal.model, Location):
            continue
        name = proposal.model.name
        previous = by_name.get(name)
        if previous is None:
            by_name[name] = proposal
            continue
        previous_origin = previous.model.virtual_origin
        current_origin = proposal.model.virtual_origin
        previous_identity = (
            f"Location {previous_origin.id}"
            if previous_origin is not None
            else "a newly proposed Location"
        )
        current_identity = (
            f"Location {current_origin.id}"
            if current_origin is not None
            else "a newly proposed Location"
        )
        raise RecommendationError(
            f"proposed Location name {name!r} is not unique: {previous_identity} ({', '.join(previous.contexts)}) and {current_identity} ({', '.join(proposal.contexts)}) would both use it"
        )


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
                        f"line {row.line_number}: Location {location_id} is mutated by more than one recommendation"
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
                f"line {row.line_number}: Location {location_id} is mutated by more than one recommendation"
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
                    f"line {row.line_number}: merge target Location {target_id} is mutated by another recommendation"
                )
            merge_target_ids.add(target_id)


def _validate_type_locality_rows(
    rows: list[type_recommendations.Recommendation],
) -> None:
    seen_ids: set[int] = set()
    for row in rows:
        if row.name_id in seen_ids:
            raise RecommendationError(
                f"line {row.line_number}: duplicate type-locality recommendation for Name {row.name_id}"
            )
        seen_ids.add(row.name_id)


def read_recommendations(path: Path) -> Recommendations:
    article_rows: list[article_recommendations.Recommendation] = []
    item_file_rows: list[item_file_recommendations.Recommendation] = []
    staged_file_rows: list[staged_file_recommendations.Recommendation] = []
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
                "item_file": item_file_rows,
                "staged_file": staged_file_rows,
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
            item_file_recommendations.RecommendationError,
            staged_file_recommendations.RecommendationError,
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
            item_file_rows,
            staged_file_rows,
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
        tuple(item_file_rows),
        tuple(staged_file_rows),
    )


def print_review(
    recommendations: Recommendations, *, actions: set[str] | None = None
) -> None:
    article_rows = tuple(
        row
        for row in recommendations.article_rows
        if actions is None or row.action in actions
    )
    item_file_rows = tuple(
        row
        for row in recommendations.item_file_rows
        if actions is None or row.action in actions
    )
    staged_file_rows = tuple(
        row
        for row in recommendations.staged_file_rows
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
    if item_file_rows:
        if article_rows:
            print()
        print("ITEM FILE RECOMMENDATIONS")
        item_file_recommendations.print_review_table(item_file_rows)
    if staged_file_rows:
        if article_rows or item_file_rows:
            print()
        print("STAGED FILE RECOMMENDATIONS")
        staged_file_recommendations.print_review_table(staged_file_rows)
    if generic_rows:
        if article_rows or item_file_rows or staged_file_rows:
            print()
        print("GENERIC RECOMMENDATIONS")
        generic_recommendations.print_review_table(generic_rows)
    if location_rows:
        if article_rows or item_file_rows or staged_file_rows or generic_rows:
            print()
        print("LOCATION RECOMMENDATIONS")
        location_recommendations.print_review_table(location_rows)
    if type_rows:
        if (
            article_rows
            or item_file_rows
            or staged_file_rows
            or generic_rows
            or location_rows
        ):
            print()
        print("TYPE-LOCALITY RECOMMENDATIONS")
        type_recommendations.print_review_table(type_rows)
    if taxon_rows:
        if article_rows or item_file_rows or generic_rows or location_rows or type_rows:
            print()
        print("TAXON RECOMMENDATIONS")
        taxon_recommendations.print_review_table(taxon_rows)
    if coverage_rows:
        if (
            article_rows
            or item_file_rows
            or staged_file_rows
            or generic_rows
            or location_rows
            or type_rows
            or taxon_rows
        ):
            print()
        print("COVERAGE ASSERTIONS")
        coverage_recommendations.print_review_table(coverage_rows)
    if not any(
        (
            article_rows,
            item_file_rows,
            staged_file_rows,
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


@dataclass(slots=True)
class _ClassificationReviewEntry:
    object: ClassificationEntry
    article: Article
    name: str
    rank: Any
    parent: ClassificationEntry | None
    page: str | None
    authority: str | None
    year: str | None
    status_value: Any
    disposition: Literal["new", "updated", "existing"]


_CLASSIFICATION_REVIEW_FIELDS = frozenset(
    {"article", "name", "rank", "parent", "page", "authority", "year", "status"}
)


def _get_existing_classification_entries(
    article: Article,
) -> Iterable[ClassificationEntry]:
    if getattr(article, "is_virtual", False):
        return ()
    return article.get_classification_entries()


def print_classification_review(
    plan: generic_recommendations.RecommendationPlan,
    *,
    get_existing_entries: Callable[[Article], Iterable[ClassificationEntry]] = (
        _get_existing_classification_entries
    ),
) -> None:
    """Render affected CE trees after overlaying a validated plan on existing entries."""
    actions = [
        action
        for action in plan.actions
        if action.recommendation.object.model == "ClassificationEntry"
        and (
            action.recommendation.action == generic_recommendations.CREATE_OBJECT
            or (
                action.recommendation.action == generic_recommendations.UPDATE_OBJECT
                and any(
                    field_name in _CLASSIFICATION_REVIEW_FIELDS
                    for _, field_name, _, _ in action.changes
                )
            )
            or (
                action.recommendation.action == generic_recommendations.SET_FIELD
                and action.recommendation.field in _CLASSIFICATION_REVIEW_FIELDS
            )
        )
    ]
    if not actions:
        print("No ClassificationEntry creation or hierarchy-update recommendations.")
        return

    article_by_key: dict[tuple[type[BaseModel], object], Article] = {}

    def add_article(article: Article | None) -> None:
        if article is not None:
            article_by_key.setdefault(_persistent_identity(article), article)

    for action in actions:
        if action.recommendation.action == generic_recommendations.CREATE_OBJECT:
            add_article(action.new_value["article"])
            continue
        add_article(action.object.article)
        if action.recommendation.action == generic_recommendations.UPDATE_OBJECT:
            for _, field_name, _, new_value in action.changes:
                if field_name == "article":
                    add_article(new_value)
        elif action.recommendation.field == "article":
            add_article(action.new_value)

    entries: dict[tuple[type[BaseModel], object], _ClassificationReviewEntry] = {}

    def ensure_entry(
        obj: ClassificationEntry, disposition: Literal["new", "updated", "existing"]
    ) -> _ClassificationReviewEntry:
        key = _persistent_identity(obj)
        entry = entries.get(key)
        if entry is None:
            entry = _ClassificationReviewEntry(
                object=obj,
                article=obj.article,
                name=obj.name,
                rank=obj.rank,
                parent=obj.parent,
                page=obj.page,
                authority=obj.authority,
                year=obj.year,
                status_value=obj.status,
                disposition=disposition,
            )
            entries[key] = entry
        elif disposition == "new" or (
            disposition == "updated" and entry.disposition == "existing"
        ):
            entry.disposition = disposition
        return entry

    for article in article_by_key.values():
        for ce in get_existing_entries(article):
            ensure_entry(ce, "existing")

    for action in actions:
        row = action.recommendation
        if row.action == generic_recommendations.CREATE_OBJECT:
            disposition: Literal["new", "updated", "existing"] = (
                "existing" if action.already_applied else "new"
            )
            entry = ensure_entry(action.object, disposition)
            for field_name in _CLASSIFICATION_REVIEW_FIELDS:
                if field_name in action.new_value:
                    attribute = "status_value" if field_name == "status" else field_name
                    setattr(entry, attribute, action.new_value[field_name])
        elif row.action == generic_recommendations.UPDATE_OBJECT:
            entry = ensure_entry(
                action.object, "existing" if action.already_applied else "updated"
            )
            for _, field_name, _, new_value in action.changes:
                if field_name in _CLASSIFICATION_REVIEW_FIELDS:
                    attribute = "status_value" if field_name == "status" else field_name
                    setattr(entry, attribute, new_value)
        else:
            entry = ensure_entry(
                action.object, "existing" if action.already_applied else "updated"
            )
            assert row.field is not None
            attribute = "status_value" if row.field == "status" else row.field
            setattr(entry, attribute, action.new_value)

    def is_visible(entry: _ClassificationReviewEntry) -> bool:
        return getattr(entry.status_value, "name", None) not in {"removed", "redirect"}

    visible_entries = {
        key: entry for key, entry in entries.items() if is_visible(entry)
    }
    relevant_keys: set[tuple[type[BaseModel], object]] = set()
    for action in actions:
        key = _persistent_identity(action.object)
        while key in visible_entries and key not in relevant_keys:
            relevant_keys.add(key)
            parent = visible_entries[key].parent
            if parent is None:
                break
            parent_key = _persistent_identity(parent)
            parent_entry = visible_entries.get(parent_key)
            if parent_entry is None or _persistent_identity(
                parent_entry.article
            ) != _persistent_identity(visible_entries[key].article):
                break
            key = parent_key
    visible_entries = {
        key: entry for key, entry in visible_entries.items() if key in relevant_keys
    }
    entries_by_article: dict[
        tuple[type[BaseModel], object], list[_ClassificationReviewEntry]
    ] = {}
    for entry in visible_entries.values():
        entries_by_article.setdefault(_persistent_identity(entry.article), []).append(
            entry
        )

    def article_label(article: Article) -> str:
        object_id = getattr(article, "id", None)
        identity = (
            "Article (new)"
            if getattr(article, "is_virtual", False)
            else f"Article {object_id}"
        )
        label = getattr(article, "name", None)
        return f"{identity}: {label}" if label is not None else identity

    def rank_label(rank: Any) -> str:
        name = getattr(rank, "name", None)
        return name.removesuffix("_") if isinstance(name, str) else str(rank)

    def page_sort_key(entry: _ClassificationReviewEntry) -> tuple[int, str, str]:
        match = re.match(r"^(\d+)", entry.page or "")
        page = int(match.group(1)) if match is not None else 0
        return page, entry.page or "", entry.name

    marker = {"new": "+", "updated": "~", "existing": "="}

    def display(
        entry: _ClassificationReviewEntry,
        depth: int,
        *,
        grouped_keys: set[tuple[type[BaseModel], object]],
        children: Mapping[
            tuple[type[BaseModel], object], list[_ClassificationReviewEntry]
        ],
        visited: set[tuple[type[BaseModel], object]],
    ) -> None:
        key = _persistent_identity(entry.object)
        if key in visited:
            return
        visited.add(key)
        line = (
            f"{'  ' * depth}{marker[entry.disposition]} {entry.name} "
            f"({rank_label(entry.rank)})"
        )
        if entry.authority is not None:
            line += f" {entry.authority}"
            if entry.year is not None:
                line += f", {entry.year}"
        if entry.page is not None:
            line += f" [page {entry.page}]"
        parent_key = (
            _persistent_identity(entry.parent) if entry.parent is not None else None
        )
        if entry.parent is not None and parent_key not in grouped_keys:
            parent_id = getattr(entry.parent, "id", None)
            parent_name = getattr(entry.parent, "name", None)
            line += f" [parent CE {parent_id}: {parent_name}, outside Article]"
        print(line)
        for child in sorted(children.get(key, ()), key=page_sort_key):
            display(
                child,
                depth + 1,
                grouped_keys=grouped_keys,
                children=children,
                visited=visited,
            )

    print("Legend: + new, ~ updated, = existing")
    for article_index, (article_key, article) in enumerate(article_by_key.items()):
        grouped_entries = entries_by_article.get(article_key, [])
        if article_index:
            print()
        print(article_label(article))
        if not grouped_entries:
            print("  (no entries in final state)")
            continue
        grouped_keys = {_persistent_identity(entry.object) for entry in grouped_entries}
        children: dict[
            tuple[type[BaseModel], object], list[_ClassificationReviewEntry]
        ] = {}
        roots: list[_ClassificationReviewEntry] = []
        for entry in grouped_entries:
            group_parent_key = (
                _persistent_identity(entry.parent) if entry.parent is not None else None
            )
            if group_parent_key is None or group_parent_key not in grouped_keys:
                roots.append(entry)
            else:
                children.setdefault(group_parent_key, []).append(entry)

        visited: set[tuple[type[BaseModel], object]] = set()

        for root in sorted(roots, key=page_sort_key):
            display(
                root, 1, grouped_keys=grouped_keys, children=children, visited=visited
            )
        remaining = [
            entry
            for entry in grouped_entries
            if _persistent_identity(entry.object) not in visited
        ]
        if remaining:
            print("  Unattached or cyclic entries:")
            for entry in sorted(remaining, key=page_sort_key):
                display(
                    entry,
                    2,
                    grouped_keys=grouped_keys,
                    children=children,
                    visited=visited,
                )


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
            f"RECONCILIATION ClassificationEntry ref={action.recommendation.object.ref!r} name={values['name']!r} status={status}"
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
            f"RECONCILIATION OccurrenceRecord ref={action.recommendation.object.ref!r} locality={values['locality_text']!r} location={status}"
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
        Literal[
            "article",
            "item_file",
            "staged_file",
            "generic",
            "location",
            "type_locality",
            "taxon",
            "coverage",
        ],
        Recommendation,
    ],
    ...,
]:
    rows: list[
        tuple[
            Literal[
                "article",
                "item_file",
                "staged_file",
                "generic",
                "location",
                "type_locality",
                "taxon",
                "coverage",
            ],
            Recommendation,
        ]
    ] = [
        *(("article", row) for row in recommendations.article_rows),
        *(("item_file", row) for row in recommendations.item_file_rows),
        *(("staged_file", row) for row in recommendations.staged_file_rows),
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
                    f"Name {type_row.name_id} changed from {type_row.name!r} to {actual_name!r}"
                )
            type_objects[type_row.line_number] = name
        except RecommendationError as exc:
            errors.append(f"line {type_row.line_number}: {exc}")
    if errors:
        raise RecommendationError(
            "refusing to start individual review because editor-target validation failed:\n- "
            + "\n- ".join(errors)
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
    elif item.family == "item_file":
        assert isinstance(row, item_file_recommendations.Recommendation)
        item_file_recommendations.print_review_table((row,))
    elif item.family == "staged_file":
        assert isinstance(row, staged_file_recommendations.Recommendation)
        staged_file_recommendations.print_review_table((row,))
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
    elif isinstance(row, item_file_recommendations.Recommendation):
        for evidence_index, item_evidence in enumerate(row.evidence, start=1):
            print(f"{evidence_index}. {getinput.italicize(item_evidence.kind)}")
            print(item_evidence.text)
    elif isinstance(row, staged_file_recommendations.Recommendation):
        for evidence_index, file_evidence in enumerate(row.evidence, start=1):
            print(f"{evidence_index}. {getinput.italicize(file_evidence.kind)}")
            print(file_evidence.text)
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
    apply_selected: Callable[[set[int]], ExecutionResult],
    choose: Callable[..., Literal["yes", "no", "edit"]] = (
        _prompt_individual_review_choice
    ),
) -> IndividualReviewResult:
    """Apply accepted rows and open requested editors in manifest order."""
    selected_lines: set[int] = set()
    affected_objects: list[BaseModel] = []
    print(
        "Reviewing recommendations in file order. Yes rebuilds and applies the cumulative accepted subset immediately; no skips the recommendation; edit opens the affected object and skips the automated recommendation. Earlier changes are not rolled back if review is interrupted later."
    )
    for index, item in enumerate(items, start=1):
        print()
        _print_individual_recommendation(item, index=index, total=len(items))
        print()
        try:
            choice = choose(can_edit=item.edit_object is not None)
        except getinput.StopException:
            print()
            applied_summary = (
                f"{len(selected_lines)} accepted recommendation(s) were already applied"
                if selected_lines
                else "No recommendations were applied"
            )
            print(
                getinput.yellow(
                    f"Individual review aborted. {applied_summary}; applied changes and edits already completed in an object editor are not rolled back."
                )
            )
            return IndividualReviewResult(
                frozenset(selected_lines),
                ExecutionResult(_deduplicate_objects(affected_objects)),
                aborted=True,
            )
        if choice == "yes":
            selected_lines.add(item.line_number)
            try:
                result = apply_selected(selected_lines)
            except Exception:
                selected_lines.remove(item.line_number)
                print()
                print(
                    getinput.yellow(
                        f"Applying manifest line {item.line_number} failed. Earlier accepted changes and any partial changes from this attempt are not rolled back."
                    )
                )
                raise
            affected_objects.extend(result.affected_objects)
            print(
                getinput.green(
                    f"Applied recommendation from manifest line {item.line_number}."
                )
            )
        elif choice == "edit":
            assert item.edit_object is not None
            print(getinput.green(f"Opening editor for {item.edit_object}..."))
            item.edit_object.edit()
    return IndividualReviewResult(
        frozenset(selected_lines),
        ExecutionResult(_deduplicate_objects(affected_objects)),
        aborted=False,
    )


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
        tuple(
            row
            for row in recommendations.item_file_rows
            if row.line_number in selected_lines
        ),
        tuple(
            row
            for row in recommendations.staged_file_rows
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
        f"{getinput.blue('Current Location:')} L{row.current_location_id} {row.current_location_name}"
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
        recommendations.item_file_rows
        or recommendations.staged_file_rows
        or recommendations.taxon_rows
        or recommendations.coverage_rows
        or any(row.ref is not None for row in recommendations.article_rows)
        or any(row.schema_version == 2 for row in recommendations.generic_rows)
    )


def _validate_unique_intake_sources(
    article_plan: article_recommendations.RecommendationPlan,
    item_file_plan: item_file_recommendations.RecommendationPlan,
    staged_file_plan: staged_file_recommendations.RecommendationPlan,
) -> None:
    seen: dict[Path, tuple[int, str]] = {}
    actions: tuple[Any, ...] = (
        *(action for action in article_plan.actions if action.source_path is not None),
        *item_file_plan.actions,
        *staged_file_plan.actions,
    )
    for action in actions:
        source = action.source_path
        assert source is not None
        resolved = source.resolve(strict=False)
        line = action.recommendation.line_number
        previous = seen.get(resolved)
        if previous is not None:
            previous_line, previous_action = previous
            raise RecommendationError(
                f"line {line}: intake file {source} is also used by line "
                f"{previous_line} action {previous_action}"
            )
        seen[resolved] = (line, action.recommendation.action)


def build_plans(recommendations: Recommendations) -> AnyRecommendationPlans:
    # Validate every family before any executor is allowed to write.
    try:
        article_plan = article_recommendations.build_plan(recommendations.article_rows)
        item_file_plan = item_file_recommendations.build_plan(
            recommendations.item_file_rows,
            initial_citation_groups=(
                action.citation_group
                for action in article_plan.actions
                if action.citation_group is not None
            ),
        )
        staged_file_plan = staged_file_recommendations.build_plan(
            recommendations.staged_file_rows
        )
        if (
            recommendations.article_rows
            or recommendations.item_file_rows
            or recommendations.staged_file_rows
        ):
            _validate_unique_intake_sources(
                article_plan, item_file_plan, staged_file_plan
            )
        if _uses_unified_plan(recommendations):
            builder = virtual_proposals.ProposalBuilder()
            new_citation_groups: dict[str, CitationGroup] = {}
            references: dict[str, BaseModel] = dict(
                article_recommendations.add_virtual_models(
                    article_plan, builder, new_citation_groups=new_citation_groups
                )
            )
            taxon_plan = taxon_recommendations.build_plan(
                recommendations.taxon_rows, initial_references=references
            )
            synonymized_ids = {
                row.object.object_id
                for row in recommendations.generic_rows
                if row.action == generic_recommendations.SYNONYMIZE_TAXON
            }
            if any(
                not action.already_applied and action.parent.id in synonymized_ids
                for action in taxon_plan.actions
            ):
                raise generic_recommendations.RecommendationError(
                    "create_taxon parent overlaps synonymize_taxon source"
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
            item_file_recommendations.add_virtual_models(
                item_file_plan, builder, new_citation_groups=new_citation_groups
            )
            coverage_plan = coverage_recommendations.build_plan(
                recommendations.coverage_rows, references=references
            )
            proposals = builder.build()
            _validate_proposed_location_name_uniqueness(proposals)
            return UnifiedRecommendationPlan(
                article_plan,
                item_file_plan,
                staged_file_plan,
                taxon_plan,
                generic_plan,
                location_plan,
                type_plan,
                coverage_plan,
                proposals,
                references,
            )
    except (
        generic_recommendations.RecommendationError,
        article_recommendations.RecommendationError,
        location_recommendations.RecommendationError,
        type_recommendations.RecommendationError,
        taxon_recommendations.RecommendationError,
        coverage_recommendations.RecommendationError,
        item_file_recommendations.RecommendationError,
        staged_file_recommendations.RecommendationError,
    ) as exc:
        raise RecommendationError(str(exc)) from exc
    builder = virtual_proposals.ProposalBuilder()
    type_recommendations.add_virtual_models(type_plan, builder)
    location_recommendations.add_virtual_models(location_plan, builder)
    generic_recommendations.add_virtual_models(generic_plan, builder)
    _validate_proposed_location_name_uniqueness(builder.build())
    return article_plan, generic_plan, location_plan, type_plan


_LINE_VALIDATION_ERROR = re.compile(r"^(?:-\s*)?line (\d+):\s*(.+)$")


def _line_validation_errors(exc: RecommendationError) -> dict[int, str]:
    """Extract line-attributable failures from a plan-validation exception."""
    reasons: dict[int, list[str]] = {}
    for raw_line in str(exc).splitlines():
        match = _LINE_VALIDATION_ERROR.fullmatch(raw_line.strip())
        if match is None:
            continue
        line_number = int(match.group(1))
        reasons.setdefault(line_number, []).append(match.group(2))
    return {line: " | ".join(messages) for line, messages in reasons.items()}


def build_plans_skipping_invalid(
    recommendations: Recommendations,
) -> tuple[
    Recommendations, AnyRecommendationPlans, tuple[SkippedInvalidRecommendation, ...]
]:
    """Skip whole rows with line-attributable failures, then validate the remainder.

    Each retry starts from the original parsed manifest and filters out all rows that
    a prior validation pass identified as stale. No executor is called here. A global
    or otherwise unattributed failure remains fatal rather than guessing which row to
    omit.
    """
    rows_by_line = {
        row.line_number: (family, row)
        for family, row in _all_recommendation_rows(recommendations)
    }
    remaining_lines = set(rows_by_line)
    skipped: list[SkippedInvalidRecommendation] = []
    while True:
        filtered = _filter_recommendations(recommendations, remaining_lines)
        try:
            return filtered, build_plans(filtered), tuple(skipped)
        except RecommendationError as exc:
            reasons = _line_validation_errors(exc)
            failed_lines = sorted(remaining_lines & reasons.keys())
            if not failed_lines:
                raise RecommendationError(
                    "--skip-invalid could not attribute the validation failure to "
                    "specific remaining manifest lines; refusing to guess which rows "
                    f"to omit:\n{exc}"
                ) from exc
            for line_number in failed_lines:
                family, row = rows_by_line[line_number]
                skipped.append(
                    SkippedInvalidRecommendation(
                        line_number, family, row.action, reasons[line_number]
                    )
                )
            remaining_lines.difference_update(failed_lines)


def print_skipped_invalid(
    skipped: tuple[SkippedInvalidRecommendation, ...], *, remaining_count: int
) -> None:
    print("SKIPPED INVALID RECOMMENDATIONS")
    for item in skipped:
        print(
            f"SKIP_INVALID line={item.line_number} family={item.family} "
            f"action={item.action} reason={item.reason}"
        )
    print(
        f"Skipped {len(skipped)} invalid recommendation(s); "
        f"{remaining_count} recommendation(s) remain eligible."
    )


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
    cfg = LintConfig(autofix=False, interactive=False)
    virtual_proposals.print_lint_results(
        virtual_proposals.lint_proposals(proposals, cfg=cfg), issues_only=issues_only
    )
    for model_name, by_resource in sorted(
        virtual_proposals.get_skipped_lints(proposals, cfg=cfg).items()
    ):
        for resource, labels in sorted(by_resource.items(), key=lambda pair: pair[0]):
            print(
                f"VIRTUAL_LINT_INCOMPLETE model={model_name} "
                f"{resource.value}_incomplete_lints={','.join(labels)}"
            )


def execute_plans(plans: AnyRecommendationPlans, *, apply: bool) -> ExecutionResult:
    recorder_context: AppliedObjectRecorder | nullcontext[None]
    recorder_context = AppliedObjectRecorder() if apply else nullcontext()
    with recorder_context as recorder:
        if isinstance(plans, UnifiedRecommendationPlan):
            replacements: dict[int, BaseModel] = {}
            created_citation_groups: dict[str, CitationGroup] = {}
            if plans.article.action_counts:
                print("ARTICLE PLAN")
                article_outputs = article_recommendations.execute_plan(
                    plans.article,
                    apply=apply,
                    created_citation_groups=created_citation_groups,
                )
                for ref, actual in article_outputs.items():
                    proposed = plans.references.get(ref)
                    if proposed is not None:
                        replacements[id(proposed)] = actual
            if plans.item_file.action_counts:
                if plans.article.action_counts:
                    print()
                print("ITEM FILE PLAN")
                item_file_recommendations.execute_plan(
                    plans.item_file,
                    apply=apply,
                    created_citation_groups=created_citation_groups,
                )
            if plans.staged_file.action_counts:
                if plans.article.action_counts or plans.item_file.action_counts:
                    print()
                print("STAGED FILE PLAN")
                staged_file_recommendations.execute_plan(plans.staged_file, apply=apply)
            if plans.taxon.action_counts:
                if (
                    plans.article.action_counts
                    or plans.item_file.action_counts
                    or plans.staged_file.action_counts
                ):
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
    individual_review_aborted = False

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
        or args.review_classification
        or args.review_reconciliation
        or run_virtual_lint_requested
        or args.skip_invalid
    )
    if not needs_complete_plan and not args.edit_manual:
        return

    try:
        if needs_complete_plan:
            if args.skip_invalid:
                recommendations, plans, skipped = build_plans_skipping_invalid(
                    recommendations
                )
                if skipped:
                    begin_output()
                    print_skipped_invalid(
                        skipped, remaining_count=recommendations.count
                    )
            else:
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
                        "Warning: the complete manifest no longer matches the database. Because --edit-manual does not apply actionable rows, it will validate the manual-review objects separately and continue:"
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

    if args.review_classification:
        assert plans is not None
        generic_plan = (
            plans.generic if isinstance(plans, UnifiedRecommendationPlan) else plans[1]
        )
        begin_output()
        print_classification_review(generic_plan)

    if run_virtual_lint_requested:
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
        applied_count = 0

        def apply_selected(selected_lines: set[int]) -> ExecutionResult:
            nonlocal applied_count
            selected = _filter_recommendations(recommendations, selected_lines)
            try:
                # Rebuild the cumulative accepted subset after every choice. Earlier
                # rows are idempotent, while retaining them makes their manifest-local
                # references available to the newly accepted row.
                selected_plans = build_plans(selected)
            except RecommendationError as exc:
                if applied_count:
                    raise RecommendationError(
                        f"{applied_count} earlier accepted recommendation(s) remain applied; the newly selected recommendation failed database validation: {exc}"
                    ) from exc
                raise RecommendationError(
                    f"the selected recommendation failed database validation; no automated changes were made: {exc}"
                ) from exc
            result = execute_plans(selected_plans, apply=True)
            applied_count += 1
            return result

        try:
            review_result = review_recommendations_individually(
                individual_items, apply_selected=apply_selected
            )
        except RecommendationError as exc:
            parser.error(str(exc))
        execution_result = review_result.execution_result
        individual_review_aborted = review_result.aborted
        if not review_result.applied_lines:
            begin_output()
            print("No recommendations selected; no automated changes made.")

    if args.edit_applied and execution_result is not None:
        begin_output()
        if not edit_applied_objects(execution_result, edit_each=args.edit_each):
            print(
                getinput.yellow(
                    "Warning: post-apply cleanup was incomplete; recommendations were already applied. Continuing."
                )
            )

    if individual_review_aborted:
        return

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
            "validate current database state and print the complete plan; may be combined with later write or edit operations"
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "write all validated recommendations, then edit affected and manual-review objects by default"
        ),
    )
    parser.add_argument(
        "--review",
        action="store_true",
        help=(
            "print static tables with untruncated action details and manual-review evidence without consulting the database"
        ),
    )
    parser.add_argument(
        "--review-action",
        action="append",
        choices=sorted(
            generic_recommendations.ALLOWED_ACTIONS
            | article_recommendations.ALLOWED_ACTIONS
            | item_file_recommendations.ALLOWED_ACTIONS
            | staged_file_recommendations.ALLOWED_ACTIONS
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
            "print manual_review actions and actionable rows carrying review_note, including complete notes, reasons, and evidence, without consulting the database"
        ),
    )
    parser.add_argument(
        "--review-classification",
        action="store_true",
        help=(
            "validate and print affected ClassificationEntry trees grouped by Article, "
            "including existing ancestors and proposed creations and updates"
        ),
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
            "validate the complete manifest, then review each row in file order with yes (revalidate and apply immediately), no (skip), or edit (open the affected object and skip); cumulative accepted rows preserve manifest-local dependencies"
        ),
    )
    parser.add_argument(
        "--edit-manual",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "validate the complete manifest, then for every manual_review row print its complete note and invoke edit() on the referenced object; generic rows edit their explicit object and type-locality rows edit their Name; enabled by default with --apply"
        ),
    )
    parser.add_argument(
        "--edit-applied",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "after successful --apply or accepted --review-each selections, run noninteractive lint with autofixes on all created or modified objects, then interactively resolve objects with remaining issues, including related objects created by structured lint autofixes; enabled by default with --apply"
        ),
    )
    parser.add_argument(
        "--edit-each",
        action="store_true",
        help=(
            "during post-apply cleanup, invoke edit() on every created or modified object after automatic formatting and lint cleanup; requires --apply or --review-each"
        ),
    )
    parser.add_argument(
        "--virtual-lint",
        action="store_true",
        help=(
            "build virtual versions of all proposed models and run advisory, best-effort lint before dry-run output or writes; structured autofixes are reported separately from issues requiring manifest edits"
        ),
    )
    parser.add_argument(
        "--virtual-lint-issues-only",
        action="store_true",
        help=(
            "run advisory virtual lint but print only unresolved VIRTUAL_LINT_ISSUES, omitting autofixable findings, summaries, and the implicit default dry run"
        ),
    )
    parser.add_argument(
        "--skip-invalid",
        action="store_true",
        help=(
            "skip each entire manifest row whose validation failure identifies its "
            "line number, report every skipped row, and rebuild and validate all "
            "remaining rows together before dry-run or application; unattributed "
            "validation failures still abort"
        ),
    )
    args = parser.parse_args()
    if args.edit_manual is None:
        args.edit_manual = args.apply
    if args.edit_each and args.edit_applied is False:
        parser.error("--edit-each cannot be combined with --no-edit-applied")
    if args.edit_applied is None:
        args.edit_applied = args.apply or args.edit_each
    if args.review_action and not args.review:
        parser.error("--review-action requires --review")
    if args.review_each and args.apply:
        parser.error(
            "--review-each selects individual rows, so it cannot be combined with --apply, which applies every row"
        )
    if args.edit_each and not (args.apply or args.review_each):
        parser.error("--edit-each requires --apply or --review-each")
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
