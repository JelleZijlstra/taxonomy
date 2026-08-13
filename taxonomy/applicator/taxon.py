"""Atomic Taxon/base-Name recommendations for manifest-created classifications."""

import enum
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, TypeVar

from taxonomy.applicator import generic
from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db import helpers
from taxonomy.db.constants import AgeClass, NomenclatureStatus, Rank, Status
from taxonomy.db.models import BaseModel, Name, Taxon

CREATE_TAXON = "create_taxon"
ALLOWED_ACTIONS = {CREATE_TAXON}
SCHEMA_VERSION = 2


class RecommendationError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class Evidence:
    kind: str
    text: str


@dataclass(frozen=True, slots=True)
class Recommendation:
    line_number: int
    action: str
    confidence: str
    reason: str
    evidence: tuple[Evidence, ...]
    taxon_ref: str
    name_ref: str
    valid_name: str
    rank: Rank
    age: AgeClass
    parent: Mapping[str, Any]
    name_values: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class PlannedAction:
    recommendation: Recommendation
    taxon: Taxon
    name: Name
    parent: Taxon
    already_applied: bool


@dataclass(frozen=True, slots=True)
class RecommendationPlan:
    actions: tuple[PlannedAction, ...]
    action_counts: Counter[str]
    references: Mapping[str, BaseModel]


def _required(data: Mapping[str, Any], key: str, line: int) -> Any:
    value = data.get(key)
    if value is None or value == "":
        raise RecommendationError(f"line {line}: {key!r} is required")
    return value


def _string(data: Mapping[str, Any], key: str, line: int) -> str:
    value = _required(data, key, line)
    if not isinstance(value, str):
        raise RecommendationError(f"line {line}: {key!r} must be a string")
    return value


EnumT = TypeVar("EnumT", bound=enum.Enum)


def _enum(data: Mapping[str, Any], key: str, cls: type[EnumT], line: int) -> EnumT:
    value = _string(data, key, line)
    try:
        return cls[value]
    except KeyError as exc:
        raise RecommendationError(
            f"line {line}: unknown {cls.__name__} member {value!r}"
        ) from exc


def parse_recommendation(data: dict[str, Any], line_number: int) -> Recommendation:
    if data.get("schema_version") != SCHEMA_VERSION:
        raise RecommendationError(
            f"line {line_number}: create_taxon requires schema_version 2"
        )
    if data.get("action") != CREATE_TAXON:
        raise RecommendationError(
            f"line {line_number}: unsupported action {data.get('action')!r}"
        )
    confidence = _string(data, "confidence", line_number)
    if confidence not in generic.ALLOWED_CONFIDENCES:
        raise RecommendationError(
            f"line {line_number}: unsupported confidence {confidence!r}"
        )
    taxon = _required(data, "taxon", line_number)
    base_name = _required(data, "base_name", line_number)
    if not isinstance(taxon, dict) or not isinstance(base_name, dict):
        raise RecommendationError(
            f"line {line_number}: taxon and base_name must be objects"
        )
    parent = _required(taxon, "parent", line_number)
    if not isinstance(parent, dict):
        raise RecommendationError(f"line {line_number}: taxon.parent must be an object")
    name_values = base_name.get("values", {})
    if not isinstance(name_values, dict):
        raise RecommendationError(
            f"line {line_number}: base_name.values must be an object"
        )
    evidence_data = _required(data, "evidence", line_number)
    if not isinstance(evidence_data, list) or not evidence_data:
        raise RecommendationError(
            f"line {line_number}: evidence must be a nonempty list"
        )
    evidence: list[Evidence] = []
    for item in evidence_data:
        if not isinstance(item, dict):
            raise RecommendationError(
                f"line {line_number}: evidence entries must be objects"
            )
        evidence.append(
            Evidence(
                _string(item, "kind", line_number), _string(item, "text", line_number)
            )
        )
    return Recommendation(
        line_number=line_number,
        action=CREATE_TAXON,
        confidence=confidence,
        reason=_string(data, "reason", line_number),
        evidence=tuple(evidence),
        taxon_ref=_string(taxon, "ref", line_number),
        name_ref=_string(base_name, "ref", line_number),
        valid_name=_string(taxon, "valid_name", line_number),
        rank=_enum(taxon, "rank", Rank, line_number),
        age=_enum(taxon, "age", AgeClass, line_number),
        parent=parent,
        name_values=name_values,
    )


def _find_taxa(valid_name: str) -> list[Taxon]:
    return list(Taxon.select().filter(Taxon.valid_name == valid_name))


def _order_rows(
    rows: list[Recommendation], references: Mapping[str, BaseModel]
) -> list[Recommendation]:
    producers: dict[str, Recommendation] = {}
    for row in rows:
        for ref in (row.taxon_ref, row.name_ref):
            if ref in references or ref in producers:
                raise RecommendationError(
                    f"line {row.line_number}: duplicate object ref {ref!r}"
                )
            producers[ref] = row
    pending = list(rows)
    resolved = set(references)
    ordered: list[Recommendation] = []
    while pending:
        ready = []
        for row in pending:
            parent_ref = row.parent.get("ref")
            if parent_ref is None or parent_ref in resolved:
                ready.append(row)
            elif parent_ref not in producers:
                raise RecommendationError(
                    f"line {row.line_number}: unknown parent ref {parent_ref!r}"
                )
        if not ready:
            raise RecommendationError("create_taxon parent dependency cycle")
        for row in ready:
            pending.remove(row)
            ordered.append(row)
            resolved.update((row.taxon_ref, row.name_ref))
    return ordered


def build_plan(
    recommendations: Iterable[Recommendation],
    *,
    initial_references: Mapping[str, BaseModel] | None = None,
    find_taxa: Callable[[str], list[Taxon]] = _find_taxa,
) -> RecommendationPlan:
    rows = list(recommendations)
    references: dict[str, BaseModel] = dict(initial_references or {})
    actions: list[PlannedAction] = []
    errors: list[str] = []
    try:
        ordered = _order_rows(rows, references)
    except RecommendationError as exc:
        raise RecommendationError(str(exc)) from exc
    for row in ordered:
        try:
            parent = generic._decode_reference(
                row.parent,
                Taxon,
                context=f"line {row.line_number} taxon.parent",
                references=references,
            )
            assert isinstance(parent, Taxon)
            matches = find_taxa(row.valid_name)
            if len(matches) > 1:
                raise RecommendationError(f"multiple Taxa are named {row.valid_name!r}")
            if matches:
                taxon = matches[0]
                if (
                    taxon.rank is not row.rank
                    or taxon.age is not row.age
                    or taxon.parent != parent
                ):
                    raise RecommendationError(
                        f"existing Taxon {row.valid_name!r} conflicts with requested rank, age, or parent"
                    )
                name = taxon.base_name
                expected_group = helpers.group_of_rank(row.rank)
                expected_root = helpers.root_name_of_name(row.valid_name, row.rank)
                supplied = generic._decode_create_values(
                    Name,
                    row.name_values,
                    context=f"line {row.line_number} base_name.values",
                    references={**references, row.taxon_ref: taxon},
                    require_label=False,
                )
                if (
                    name.taxon != taxon
                    or name.group is not expected_group
                    or name.root_name != expected_root
                    or name.status is not Status.valid
                ):
                    raise RecommendationError(
                        f"existing Taxon {row.valid_name!r} has a conflicting base Name"
                    )
                mismatches = [
                    field
                    for field, value in supplied.items()
                    if not generic._values_equal(getattr(name, field), value)
                ]
                if mismatches:
                    raise RecommendationError(
                        f"existing base Name for {row.valid_name!r} conflicts in "
                        f"fields {', '.join(mismatches)}"
                    )
                already_applied = True
            else:
                taxon = Taxon.virtual(
                    valid_name=row.valid_name,
                    rank=row.rank,
                    age=row.age,
                    parent=parent,
                    data=None,
                    comments=None,
                    is_page_root=False,
                    tags=(),
                )
                local_refs = {**references, row.taxon_ref: taxon}
                supplied = generic._decode_create_values(
                    Name,
                    row.name_values,
                    context=f"line {row.line_number} base_name.values",
                    references=local_refs,
                    require_label=False,
                )
                fixed_name_values = {
                    "taxon": taxon,
                    "group": helpers.group_of_rank(row.rank),
                    "root_name": helpers.root_name_of_name(row.valid_name, row.rank),
                    "status": Status.valid,
                }
                conflicting_fixed = [
                    field
                    for field, value in fixed_name_values.items()
                    if field in supplied
                    and not generic._values_equal(supplied[field], value)
                ]
                if conflicting_fixed:
                    raise RecommendationError(
                        "base_name.values conflicts with Taxon invariants in fields "
                        + ", ".join(conflicting_fixed)
                    )
                name_values = {
                    **fixed_name_values,
                    "nomenclature_status": NomenclatureStatus.available,
                    **supplied,
                }
                name = Name.virtual(**name_values)
                taxon.base_name = name
                already_applied = False
            references[row.taxon_ref] = taxon
            references[row.name_ref] = name
            actions.append(PlannedAction(row, taxon, name, parent, already_applied))
        except (generic.RecommendationError, RecommendationError) as exc:
            errors.append(f"line {row.line_number}: {exc}")
    if errors:
        raise RecommendationError(
            "refusing to continue because taxon validation failed:\n- "
            + "\n- ".join(errors)
        )
    return RecommendationPlan(
        tuple(actions), Counter(row.action for row in rows), references
    )


def print_review_table(rows: Iterable[Recommendation]) -> None:
    for row in rows:
        parent = row.parent.get("ref", row.parent.get("id"))
        fixed_name_values = {
            "corrected_original_name": row.valid_name,
            "root_name": helpers.root_name_of_name(row.valid_name, row.rank),
            "status": Status.valid.name,
            "nomenclature_status": NomenclatureStatus.available.name,
        }
        print(
            f"line={row.line_number} action=create_taxon confidence={row.confidence} "
            f"taxon_ref={row.taxon_ref!r} name_ref={row.name_ref!r} "
            f"name={row.valid_name!r} rank={row.rank.name} parent={parent!r}"
        )
        generic._print_review_detail("taxon field", f"valid_name={row.valid_name!r}")
        generic._print_review_detail("taxon field", f"rank={row.rank.name}")
        generic._print_review_detail("taxon field", f"age={row.age.name}")
        generic._print_review_detail(
            "taxon field", f"parent={generic._format_manifest_value(row.parent)}"
        )
        for field_name, value in {**fixed_name_values, **row.name_values}.items():
            generic._print_review_detail(
                "base name field",
                f"{field_name}={generic._format_manifest_value(value)}",
            )


def add_virtual_models(plan: RecommendationPlan, builder: ProposalBuilder) -> None:
    for action in plan.actions:
        context = f"line {action.recommendation.line_number} create_taxon"
        if action.already_applied:
            taxon = builder.copy(action.taxon, context=context)
            name = builder.copy(action.name, context=context)
            taxon.base_name = name
            name.taxon = taxon
            continue
        taxon_values = {
            field: generic._replace_created_models(
                getattr(action.taxon, field),
                {
                    id(value): builder.replacement(value)
                    for value in generic._iter_model_values(
                        getattr(action.taxon, field)
                    )
                },
            )
            for field in Taxon.clirm_fields
            if field not in {"id", "base_name"} and hasattr(action.taxon, field)
        }
        taxon = builder.create(
            Taxon, context=context, proposal_source=action.taxon, **taxon_values
        )
        name_values = {
            field: generic._replace_created_models(
                getattr(action.name, field),
                {
                    id(value): builder.replacement(value)
                    for value in generic._iter_model_values(getattr(action.name, field))
                },
            )
            for field in Name.clirm_fields
            if field not in {"id", "taxon"} and hasattr(action.name, field)
        }
        name = builder.create(
            Name,
            context=context,
            proposal_source=action.name,
            taxon=taxon,
            **name_values,
        )
        taxon.base_name = name


def execute_plan(
    plan: RecommendationPlan,
    *,
    apply: bool,
    replacements: Mapping[int, BaseModel] | None = None,
    create_taxon: Callable[..., Taxon] = Taxon.create,
    create_name: Callable[..., Name] = Name.create,
) -> Mapping[int, BaseModel]:
    resolved = dict(replacements or {})
    for action in plan.actions:
        row = action.recommendation
        status = (
            "ALREADY_APPLIED"
            if action.already_applied
            else ("APPLY" if apply else "WOULD_CREATE")
        )
        print(
            f"{status} action=create_taxon taxon_ref={row.taxon_ref!r} "
            f"name_ref={row.name_ref!r} name={row.valid_name!r} rank={row.rank.name}"
        )
        if action.already_applied:
            resolved[id(action.taxon)] = action.taxon
            resolved[id(action.name)] = action.name
            continue
        if not apply:
            continue
        parent = generic._replace_created_models(action.parent, resolved)
        taxon = create_taxon(
            valid_name=row.valid_name,
            rank=row.rank,
            age=row.age,
            parent=parent,
            data=None,
            comments=None,
            is_page_root=False,
            tags=(),
        )
        values = {
            field: generic._replace_created_models(
                getattr(action.name, field), {**resolved, id(action.taxon): taxon}
            )
            for field in Name.clirm_fields
            if field != "id" and hasattr(action.name, field)
        }
        values["taxon"] = taxon
        name = create_name(**values)
        taxon.base_name = name
        resolved[id(action.taxon)] = taxon
        resolved[id(action.name)] = name
    mode = "Applied" if apply else "Dry run"
    print(f"{mode}: {len(plan.actions)} create_taxon recommendation(s).")
    if not apply:
        print("No database changes made. Review this plan, then add --apply.")
    return resolved
