"""Review or apply heterogeneous taxonomy recommendations from JSONL.

The default mode validates the complete file against the current database and prints
a dry run. Rows are dispatched by ``action`` to the Location or type-locality
recommendation implementations, so one file may contain both families. Use ``--review``
for a database-independent summary, ``--review-manual`` for the complete text of
manual-review rows and actionable rows carrying ``review_note``, and ``--edit-manual``
to open every manual-review object in the database editor after printing its complete
note. Combine ``--apply --edit-manual`` to apply actionable rows first and then work
through the unresolved objects interactively. Add ``--virtual-lint`` to construct the
final proposed model states in memory and run advisory lint before the dry run or
apply step.
"""

import argparse
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from taxonomy import getinput
from taxonomy.applicator import generic as generic_recommendations
from taxonomy.applicator import location as location_recommendations
from taxonomy.applicator import proposals as virtual_proposals
from taxonomy.applicator import type_locality as type_recommendations
from taxonomy.db.models import Name


class RecommendationError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class Recommendations:
    generic_rows: tuple[generic_recommendations.Recommendation, ...]
    location_rows: tuple[location_recommendations.Recommendation, ...]
    type_locality_rows: tuple[type_recommendations.Recommendation, ...]

    @property
    def count(self) -> int:
        return (
            len(self.generic_rows)
            + len(self.location_rows)
            + len(self.type_locality_rows)
        )


@dataclass(frozen=True, slots=True)
class ManualReviewObject:
    line_number: int
    recommendation: Any
    object: Any
    family: str


def _validate_location_rows(
    rows: list[location_recommendations.Recommendation],
) -> None:
    mutated_ids: set[int] = set()
    merge_target_ids: set[int] = set()
    for row in rows:
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
        if location_id in mutated_ids or location_id in merge_target_ids:
            raise RecommendationError(
                f"line {row.line_number}: Location {location_id} is mutated by more "
                "than one recommendation"
            )
        mutated_ids.add(location_id)
        if row.action == location_recommendations.MERGE_LOCATION:
            assert row.target is not None
            target_id = row.target.location_id
            if target_id in mutated_ids:
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
    generic_rows: list[generic_recommendations.Recommendation] = []
    location_rows: list[location_recommendations.Recommendation] = []
    type_rows: list[type_recommendations.Recommendation] = []
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
            if action == generic_recommendations.MANUAL_REVIEW and "object" in data:
                generic_rows.append(
                    generic_recommendations.parse_recommendation(data, line_number)
                )
            elif action == type_recommendations.MANUAL_REVIEW:
                type_rows.append(
                    type_recommendations.parse_recommendation(data, line_number)
                )
            elif action in generic_recommendations.ALLOWED_ACTIONS:
                generic_rows.append(
                    generic_recommendations.parse_recommendation(data, line_number)
                )
            elif action in location_recommendations.ALLOWED_ACTIONS:
                location_rows.append(
                    location_recommendations.parse_recommendation(data, line_number)
                )
            elif action in type_recommendations.ALLOWED_ACTIONS:
                type_rows.append(
                    type_recommendations.parse_recommendation(data, line_number)
                )
            else:
                raise RecommendationError(
                    f"line {line_number}: unsupported action {action!r}"
                )
        except (
            generic_recommendations.RecommendationError,
            location_recommendations.RecommendationError,
            type_recommendations.RecommendationError,
        ) as exc:
            raise RecommendationError(str(exc)) from exc
    if not generic_rows and not location_rows and not type_rows:
        raise RecommendationError("recommendation file contains no rows")
    _validate_location_rows(location_rows)
    _validate_type_locality_rows(type_rows)
    return Recommendations(tuple(generic_rows), tuple(location_rows), tuple(type_rows))


def print_review(
    recommendations: Recommendations, *, actions: set[str] | None = None
) -> None:
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
    if generic_rows:
        print("GENERIC RECOMMENDATIONS")
        generic_recommendations.print_review_table(generic_rows)
    if location_rows:
        if generic_rows:
            print()
        print("LOCATION RECOMMENDATIONS")
        location_recommendations.print_review_table(location_rows)
    if type_rows:
        if generic_rows or location_rows:
            print()
        print("TYPE-LOCALITY RECOMMENDATIONS")
        type_recommendations.print_review_table(type_rows)
    if not generic_rows and not location_rows and not type_rows:
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


def build_plans(
    recommendations: Recommendations,
) -> tuple[
    generic_recommendations.RecommendationPlan,
    location_recommendations.RecommendationPlan,
    type_recommendations.RecommendationPlan,
]:
    # Validate every family before any executor is allowed to write.
    try:
        generic_plan = generic_recommendations.build_plan(recommendations.generic_rows)
        location_plan = location_recommendations.build_plan(
            recommendations.location_rows
        )
        type_plan = type_recommendations.build_plan(recommendations.type_locality_rows)
    except (
        generic_recommendations.RecommendationError,
        location_recommendations.RecommendationError,
        type_recommendations.RecommendationError,
    ) as exc:
        raise RecommendationError(str(exc)) from exc
    return generic_plan, location_plan, type_plan


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
    plans: tuple[
        generic_recommendations.RecommendationPlan,
        location_recommendations.RecommendationPlan,
        type_recommendations.RecommendationPlan,
    ],
) -> tuple[virtual_proposals.ProposedModel, ...]:
    """Build the final in-memory state represented by all actionable plans."""
    generic_plan, location_plan, type_plan = plans
    builder = virtual_proposals.ProposalBuilder()
    # Match execute_plans() ordering so actions that touch the same model compose.
    type_recommendations.add_virtual_models(type_plan, builder)
    location_recommendations.add_virtual_models(location_plan, builder)
    generic_recommendations.add_virtual_models(generic_plan, builder)
    return builder.build()


def run_virtual_lint(
    plans: tuple[
        generic_recommendations.RecommendationPlan,
        location_recommendations.RecommendationPlan,
        type_recommendations.RecommendationPlan,
    ],
) -> None:
    proposals = build_virtual_proposals(plans)
    virtual_proposals.print_lint_results(virtual_proposals.lint_proposals(proposals))


def execute_plans(
    plans: tuple[
        generic_recommendations.RecommendationPlan,
        location_recommendations.RecommendationPlan,
        type_recommendations.RecommendationPlan,
    ],
    *,
    apply: bool,
) -> None:
    generic_plan, location_plan, type_plan = plans
    if type_plan.action_counts:
        print("TYPE-LOCALITY PLAN")
        type_recommendations.execute_plan(type_plan, apply=apply)
    if location_plan.action_counts:
        if type_plan.action_counts:
            print()
        print("LOCATION PLAN")
        location_recommendations.execute_plan(location_plan, apply=apply)
    if generic_plan.action_counts:
        if type_plan.action_counts or location_plan.action_counts:
            print()
        print("GENERIC PLAN")
        generic_recommendations.execute_plan(generic_plan, apply=apply)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recommendations", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="validate current database state and print the plan (default)",
    )
    parser.add_argument(
        "--apply", action="store_true", help="write all validated recommendations"
    )
    mode.add_argument(
        "--review",
        action="store_true",
        help="print compact tables without consulting the database",
    )
    parser.add_argument(
        "--review-action",
        action="append",
        choices=sorted(
            generic_recommendations.ALLOWED_ACTIONS
            | location_recommendations.ALLOWED_ACTIONS
            | type_recommendations.ALLOWED_ACTIONS
        ),
        help="with --review, include only this action (repeatable)",
    )
    mode.add_argument(
        "--review-manual",
        action="store_true",
        help=(
            "print manual_review actions and actionable rows carrying review_note, "
            "including complete notes, reasons, and evidence, without consulting "
            "the database"
        ),
    )
    parser.add_argument(
        "--edit-manual",
        action="store_true",
        help=(
            "validate the complete manifest, then for every manual_review row print "
            "its complete note and invoke edit() on the referenced object; generic "
            "rows edit their explicit object and type-locality rows edit their Name; "
            "may be combined with --apply to apply actionable rows first"
        ),
    )
    parser.add_argument(
        "--virtual-lint",
        action="store_true",
        help=(
            "build virtual versions of all proposed models and run advisory, "
            "best-effort lint before dry-run output or writes"
        ),
    )
    args = parser.parse_args()
    if args.review_action and not args.review:
        parser.error("--review-action requires --review")
    if args.virtual_lint and (args.review or args.review_manual):
        parser.error("--virtual-lint requires database-backed plan validation")
    if (args.apply or args.edit_manual) and (
        args.dry_run or args.review or args.review_manual
    ):
        parser.error(
            "--apply and --edit-manual may be combined with each other, but not with "
            "--dry-run, --review, or --review-manual"
        )
    manual_items: tuple[ManualReviewObject, ...] | None = None
    plans: (
        tuple[
            generic_recommendations.RecommendationPlan,
            location_recommendations.RecommendationPlan,
            type_recommendations.RecommendationPlan,
        ]
        | None
    ) = None
    try:
        recommendations = read_recommendations(args.recommendations)
        if args.review:
            actions = set(args.review_action) if args.review_action else None
            print_review(recommendations, actions=actions)
            return
        if args.review_manual:
            print_manual_reviews(recommendations)
            return
        if args.edit_manual and not args.apply:
            try:
                plans = build_plans(recommendations)
                generic_plan = plans[0]
            except RecommendationError as exc:
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
            manual_items = _resolve_manual_review_objects(
                recommendations, generic_plan, allow_name_label_changes=plans is None
            )
        else:
            plans = build_plans(recommendations)
        if args.edit_manual and args.apply:
            # Resolve every editor target before --apply is allowed to write. This
            # avoids discovering a stale later manual-review row after a partial run.
            assert plans is not None
            manual_items = _resolve_manual_review_objects(recommendations, plans[0])
    except RecommendationError as exc:
        parser.error(str(exc))
    if args.virtual_lint:
        if plans is None:
            parser.error("--virtual-lint requires a complete actionable plan")
        run_virtual_lint(plans)
        print()
    if args.apply:
        assert plans is not None
        execute_plans(plans, apply=True)
        if manual_items is not None:
            print()
            _edit_manual_review_objects(manual_items)
    elif manual_items is not None:
        _edit_manual_review_objects(manual_items)
    else:
        assert plans is not None
        execute_plans(plans, apply=False)


if __name__ == "__main__":
    main()
