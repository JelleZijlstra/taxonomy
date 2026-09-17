"""Guard and preview the effects of Taxon.synonymize without mutating the DB."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, cast

from taxonomy.applicator import generic
from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db.constants import AgeClass, Status
from taxonomy.db.models import BaseModel, Name, Occurrence, OccurrenceRecord, Taxon

REFERENCE_MODELS = {
    "names": Name,
    "children": Taxon,
    "occurrences": Occurrence,
    "occurrence_records": OccurrenceRecord,
}


def _state(taxon: Taxon) -> dict[str, Any]:
    return {
        "age": taxon.age.name,
        "rank": taxon.rank.name,
        "parent_id": taxon.parent.id if taxon.parent else None,
        "base_name_id": taxon.base_name.id,
        "base_name_status": taxon.base_name.status.name,
        "data": taxon.data,
    }


def _members(source: Taxon) -> dict[str, list[Any]]:
    return {
        "names": list(source.get_names()),
        "children": list(source.get_children()),
        # These reverse relations are installed dynamically by clirm.
        "occurrences": list(cast(Any, source).occurrences),
        "occurrence_records": list(cast(Any, source).occurrence_records),
    }


def snapshot(source: Taxon, target: Taxon) -> dict[str, Any]:
    """Return the JSON guard required by a synonymize_taxon row."""
    return {
        "source": _state(source),
        "target": _state(target),
        "references": {
            key: sorted(obj.id for obj in objects)
            for key, objects in _members(source).items()
        },
    }


def plan(
    row: generic.Recommendation,
    source: Taxon,
    target: Taxon,
    get_object: Callable[[type[BaseModel], int], Any],
) -> generic.PlannedAction:
    assert row.match is not None and row.target is not None
    guard = row.match
    if target.valid_name != row.target.label:
        raise generic.RecommendationError("synonymize_taxon target label changed")
    if source == target or target.is_invalid() or source.age is AgeClass.removed:
        raise generic.RecommendationError(
            "synonymize_taxon requires distinct valid taxa"
        )
    ancestor: Taxon | None = target
    seen = set()
    while ancestor is not None:
        if ancestor == source or ancestor.id in seen:
            raise generic.RecommendationError("synonymize_taxon would create a cycle")
        seen.add(ancestor.id)
        ancestor = ancestor.parent
    if source.group() != target.group():
        raise generic.RecommendationError(
            "synonymize_taxon taxa are in different groups"
        )
    already_applied = source.age is AgeClass.redirect
    expected_source = dict(guard["source"])
    if already_applied:
        expected_source.update(age="redirect", parent_id=target.id)
        if source.base_name != target.base_name:
            expected_source["base_name_status"] = "synonym"
    if _state(source) != expected_source or _state(target) != guard["target"]:
        raise generic.RecommendationError(
            "synonymize_taxon source or target guard changed"
        )
    if set(guard["references"]) != set(REFERENCE_MODELS):
        raise generic.RecommendationError(
            "synonymize_taxon references guard has unexpected fields"
        )
    for ids in guard["references"].values():
        if len(ids) != len(set(ids)):
            raise generic.RecommendationError(
                "synonymize_taxon reference IDs must be unique"
            )
    members = _members(source)
    target_occurrences = list(cast(Any, target).occurrences)
    if already_applied:
        # The model method leaves duplicate legacy Occurrences on the redirect,
        # merging their provenance into the target's occurrence instead.
        if any(members[key] for key in ("names", "children", "occurrence_records")):
            raise generic.RecommendationError(
                "synonymize_taxon redirect still has references"
            )
        for key, model in REFERENCE_MODELS.items():
            objects = [get_object(model, obj_id) for obj_id in guard["references"][key]]
            for obj in objects:
                owner = obj.parent if key == "children" else obj.taxon
                if owner == target:
                    continue
                if key == "occurrences" and owner == source:
                    comment = (
                        f"Also under _{source.name}_ with source {{{obj.source}}}."
                    )
                    if obj.comment is not None:
                        comment += " " + obj.comment
                    if any(
                        o.location == obj.location and comment in (o.comment or "")
                        for o in target_occurrences
                    ):
                        continue
                raise generic.RecommendationError(
                    "synonymize_taxon moved reference changed"
                )
            members[key] = objects
        if any(
            o.id not in guard["references"]["occurrences"]
            for o in cast(Any, source).occurrences
        ):
            raise generic.RecommendationError(
                "synonymize_taxon redirect has new occurrences"
            )
    elif {
        key: sorted(obj.id for obj in objects) for key, objects in members.items()
    } != guard["references"]:
        raise generic.RecommendationError("synonymize_taxon source references changed")
    elif source.base_name not in members["names"]:
        raise generic.RecommendationError(
            "synonymize_taxon base Name is not a source member"
        )
    return generic.PlannedAction(
        row,
        source,
        guard,
        {
            "target": target,
            "members": members,
            "target_occurrences": target_occurrences,
        },
        already_applied=already_applied,
    )


def validate_interactions(actions: Sequence[generic.PlannedAction]) -> None:
    merges = [a for a in actions if a.recommendation.action == generic.SYNONYMIZE_TAXON]
    for merge in merges:
        source = merge.object
        target = merge.new_value["target"]
        protected: dict[tuple[type[BaseModel], Any], set[str] | None] = {
            (Taxon, source.id): None,
            (Taxon, target.id): None,
            (Name, source.base_name.id): {"taxon", "status"},
            (Name, target.base_name.id): {"taxon", "status"},
        }
        ancestor = target.parent
        while ancestor is not None:
            protected.setdefault((Taxon, ancestor.id), {"parent"})
            ancestor = ancestor.parent
        for occurrence in merge.new_value["target_occurrences"]:
            protected[(Occurrence, occurrence.id)] = None
        for key, objects in merge.new_value["members"].items():
            for obj in objects:
                fields = (
                    {"taxon"}
                    if key == "names"
                    else {"parent"} if key == "children" else None
                )
                identity = (type(obj), obj.id)
                if identity not in protected:
                    protected[identity] = fields
        for other in actions:
            if other is merge or other.recommendation.action == generic.MANUAL_REVIEW:
                continue
            row = other.recommendation
            identity = (type(other.object), other.object.id)
            if row.action == generic.SYNONYMIZE_TAXON:
                if (
                    identity in protected
                    or (Taxon, other.new_value["target"].id) in protected
                ):
                    raise generic.RecommendationError(
                        "overlapping synonymize_taxon actions"
                    )
            elif row.action == generic.CREATE_OBJECT:
                for field in ("taxon", "parent"):
                    owner = other.new_value.get(field)
                    if isinstance(owner, Taxon) and owner in (source, target):
                        raise generic.RecommendationError(
                            "creation overlaps synonymize_taxon"
                        )
            elif identity in protected:
                fields = protected[identity]
                changed = (
                    {change["field"] for change in row.changes}
                    if row.action == generic.UPDATE_OBJECT
                    else {row.field}
                )
                if fields is None or fields & changed:
                    raise generic.RecommendationError(
                        "mutation overlaps synonymize_taxon guarded fields"
                    )


def add_virtual_models(action: generic.PlannedAction, builder: ProposalBuilder) -> None:
    context = (
        f"generic manifest line {action.recommendation.line_number} synonymize_taxon"
    )
    source = action.object
    target = action.new_value["target"]
    proposed_source = builder.copy(source, context=context)
    proposed_target = builder.copy(target, context=context)
    members = action.new_value["members"]
    for name in members["names"]:
        proposal = builder.copy(name, context=context)
        proposal.taxon = proposed_target
        if name == source.base_name and name != target.base_name:
            proposal.status = Status.synonym
    for child in members["children"]:
        builder.copy(child, context=context).parent = proposed_target
    for record in members["occurrence_records"]:
        builder.copy(record, context=context).taxon = proposed_target
    for occurrence in members["occurrences"]:
        duplicate = next(
            (
                o
                for o in action.new_value["target_occurrences"]
                if o.location == occurrence.location
            ),
            None,
        )
        if duplicate is None:
            proposal = builder.copy(occurrence, context=context)
            proposal.taxon = proposed_target
            proposal.add_comment(f"Previously under _{source.name}_.")
        else:
            comment = f"Also under _{source.name}_ with source {{{occurrence.source}}}."
            if occurrence.comment is not None:
                comment += " " + occurrence.comment
            builder.copy(duplicate, context=context).add_comment(comment)
    proposed_source.age = AgeClass.redirect
    proposed_source.parent = proposed_target
