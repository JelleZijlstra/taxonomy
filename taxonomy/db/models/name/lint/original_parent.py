from collections.abc import Iterable, Sequence

from taxonomy.db.constants import Group, NomenclatureStatus, Rank
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import field_issue
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name.name import Name, NameTag

from .base import LINT
from .helpers import get_parent, resolve_usage


def should_require_subgenus_original_parent(nam: Name) -> bool:
    if nam.original_rank not in (Rank.subgenus, Rank.other_subgeneric):
        return True
    # Should ideally be set for all names, but let's lock in the progress already made
    if nam.numeric_year() > 1909:
        return True
    if nam.original_citation is not None and nam.original_citation.id > 60332:
        return True
    return False


@LINT.add("check_original_parent")
def check_original_parent(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.original_parent is None:
        return

    if nam.group is not Group.species and nam.original_rank not in (
        Rank.subgenus,
        Rank.other_subgeneric,
    ):
        yield "original_parent should only be set for species-group names and subgenera"
        return

    if nam.original_parent.group is not Group.genus:
        yield f"original_parent is not a genus: {nam.original_parent}"
        return

    if (
        nam.year is not None
        and nam.nomenclature_status is not NomenclatureStatus.before_1758
        and nam.numeric_year() < nam.original_parent.numeric_year()
    ):
        yield f"original_parent {nam.original_parent} is younger than {nam}"
        candidates = list(
            nam.original_parent.taxon.get_names().filter(
                Name.root_name == nam.original_parent.root_name, Name.year == nam.year
            )
        )
        if len(candidates) == 1:
            alternative = candidates[0]
            message = f"original_parent {nam.original_parent} is younger than {nam} (change to {alternative})"
            yield field_issue(message, nam, "original_parent", alternative)

    if (
        target := nam.original_parent.get_tag_target(NameTag.UnavailableVersionOf)
    ) is not None and nam.get_date_object() >= target.get_date_object():
        message = f"original_parent is an unavailable version of {target}"
        yield field_issue(message, nam, "original_parent", target)

    if nam.original_parent.nomenclature_status in (
        NomenclatureStatus.subsequent_usage,
        NomenclatureStatus.misidentification,
    ):
        target = resolve_usage(
            nam.original_parent, resolve_unavailable_version_of=False
        )
        message = f"original_parent is a subsequent usage or misidentification: {nam.original_parent} (change to {target})"
        if nam.original_parent != target:
            yield field_issue(message, nam, "original_parent", target)
        else:
            yield message

    if (
        not nam.original_parent.nomenclature_status.can_preoccupy()
        and nam.original_citation != nam.original_parent.original_citation
    ):
        resolved = nam.original_parent.resolve_variant()
        alternatives = [
            parent
            for parent in sorted(
                nam.original_parent.taxon.get_names().filter(
                    Name.root_name == nam.original_parent.root_name
                ),
                key=lambda n: n.numeric_year(),
            )
            if parent.numeric_year() < nam.numeric_year()
            and parent.resolve_variant().nomenclature_status.can_preoccupy()
            and parent.resolve_variant() != resolved
        ]
        if alternatives:
            message = f"original_parent is not an available name: {nam.original_parent} (alternatives: {alternatives})"
            yield field_issue(message, nam, "original_parent", alternatives[0])

    if nam.group is Group.species and nam.corrected_original_name is not None:
        original_genus, *_ = nam.corrected_original_name.split()
        if original_genus != nam.original_parent.corrected_original_name:
            yield (
                f"original_parent {nam.original_parent} does not match corrected"
                f" original name {nam.corrected_original_name}"
            )
            # This happens if the original parent got a justified emendation.
            # Change it to the emendation.
            if original_genus == nam.original_parent.root_name:
                emended_version = nam.original_parent.get_tag_target(
                    NameTag.AsEmendedBy
                )
                if (
                    emended_version is not None
                    and emended_version.root_name == original_genus
                ):
                    message = f"original_parent {nam.original_parent} does not match corrected original name {nam.corrected_original_name} (change to {emended_version})"
                    yield field_issue(message, nam, "original_parent", emended_version)


@LINT.add("infer_original_parent")
def infer_original_parent(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if (
        nam.original_parent is not None
        or not nam.nomenclature_status.requires_original_parent()
    ):
        return
    if (
        nam.original_rank in (Rank.subgenus, Rank.other_subgeneric)
        and nam.type is not None
        and nam.type.original_citation == nam.original_citation
        and nam.type.original_parent is not None
    ):
        original_parent = nam.type.original_parent
        message = f"inferred original_parent to be {original_parent} from type"
        yield field_issue(message, nam, "original_parent", original_parent)
    elif (nam.group is Group.species) and nam.corrected_original_name is not None:
        candidates = _get_inferred_original_parent(nam)
        if len(candidates) != 1:
            return
        message = f"inferred original_parent to be {candidates} from {nam.corrected_original_name}"
        yield field_issue(message, nam, "original_parent", candidates[0])


def _get_inferred_original_parent(nam: Name) -> Sequence[Name]:
    if nam.corrected_original_name is None:
        return []
    original_genus, *_ = nam.corrected_original_name.split()

    # Is it still in the same genus? If so, use that genus.
    current_genus = get_parent(nam)
    if (
        current_genus is not None
        and current_genus.base_name.root_name == original_genus
    ):
        return [current_genus.base_name]

    # Is there only a single possibility, still placed in the same order?
    possible_genera = list(
        Name.select_valid().filter(
            Name.group == Group.genus,
            Name.root_name == original_genus,
            Name.nomenclature_status != NomenclatureStatus.subsequent_usage,
        )
    )
    possible_genera = [
        # give some buffer
        genus
        for genus in possible_genera
        if genus.numeric_year() <= nam.numeric_year() + 2
    ]
    if len(possible_genera) < 1:
        return []
    current_order = get_parent(nam, Rank.order)
    same_order_genera = [
        genus
        for genus in possible_genera
        if get_parent(genus, Rank.order) == current_order
    ]
    if len(same_order_genera) == 1:
        return same_order_genera

    # Try available names only
    available_genera = [genus for genus in possible_genera if genus.can_preoccupy()]
    if len(available_genera) == 1:
        return available_genera

    if nam.original_citation is not None:
        # Try to match the original citation
        same_citation_genera = [
            genus
            for genus in possible_genera
            if nam.original_citation == genus.original_citation
        ]
        if len(same_citation_genera) == 1:
            return same_citation_genera

    # Else give up.
    return possible_genera
