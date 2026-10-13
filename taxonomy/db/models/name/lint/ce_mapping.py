import itertools
import re
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from typing import TypeVar

from taxonomy.db.constants import Group, NomenclatureStatus, Rank, Status
from taxonomy.db.models import detail as detail_lint
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.classification_entry.ce import (
    DETAIL_METADATA_FIELDS,
    ClassificationEntry,
    ClassificationEntryTag,
)
from taxonomy.db.models.lint import add_tag_issue, field_issue, replace_tag_issue
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name.name import DETAIL_TAG_TO_CE_TAG, Name, NameTag, TypeTag
from taxonomy.db.models.name.page import parse_page_text

from .base import LINT
from .helpers import has_classification, resolve_usage
from .tags import get_applicable_statuses

T = TypeVar("T")


@LINT.add("infer_included_species")
def infer_included_species(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.group is not Group.genus:
        return
    if nam.original_citation is None:
        return
    ce = nam.get_mapped_classification_entry()
    if ce is None or ce.rank.is_synonym:
        return
    current_included_species_with_ces = [
        tag
        for tag in nam.type_tags
        if isinstance(tag, TypeTag.IncludedSpecies) and tag.classification_entry
    ]
    included = ce.get_children_of_rank(Rank.species)
    for child_ce in included:
        if child_ce.mapped_name is None:
            continue
        if any(
            tag.name == child_ce.mapped_name and tag.classification_entry == child_ce
            for tag in current_included_species_with_ces
        ):
            continue
        child_key = _included_species_key(child_ce.mapped_name)
        if child_key == child_ce.mapped_name and any(
            _included_species_key(tag.name) == child_key
            and _included_species_key(tag.name) != tag.name
            for tag in current_included_species_with_ces
        ):
            # The duplicate lint prefers a name combination or subsequent usage over
            # the original name. Do not repeatedly infer the tag that it would remove.
            continue
        if child_ce.parent is None:
            continue
        if child_ce.parent != ce:
            comment = f"in {child_ce.parent.get_rank_string()} {child_ce.parent.name}"
        else:
            comment = None
        if child_ce.page is not None:
            page = child_ce.page
        else:
            page = None
        tag = TypeTag.IncludedSpecies(
            child_ce.mapped_name,
            comment=comment,
            page=page,
            classification_entry=child_ce,
        )
        message = f"adding included species {child_ce.mapped_name} from {ce}: {tag}"
        yield add_tag_issue(message, nam, tag, field="type_tags")


@LINT.add("duplicate_included_species")
def check_duplicate_included_species(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    if nam.group is not Group.genus:
        return
    all_included: dict[  # type: ignore[name-defined]
        Name, tuple[list[TypeTag.IncludedSpecies], list[TypeTag.IncludedSpecies]]
    ] = defaultdict(lambda: ([], []))
    for tag in nam.type_tags:
        if isinstance(tag, TypeTag.IncludedSpecies):
            key_name = _included_species_key(tag.name)
            if key_name == tag.name:
                all_included[key_name][1].append(tag)
            else:
                all_included[key_name][0].append(tag)
    if not all_included:
        return
    tags_to_remove: set[TypeTag.IncludedSpecies] = set()  # type: ignore[name-defined]
    for combinations, originals in all_included.values():
        if len(combinations) + len(originals) == 1:
            continue
        if combinations:
            tags_to_remove.update(originals)
            if len({tag.name for tag in combinations}) > 1:
                yield f"remove one of duplicate tags {combinations}"
            else:
                to_remove, message = _prefer_commented(combinations)
                tags_to_remove.update(to_remove)
                if message is not None:
                    yield message
        else:
            to_remove, message = _prefer_commented(originals)
            tags_to_remove.update(to_remove)
            if message is not None:
                yield message
    if tags_to_remove:
        message = f"remove duplicate IncludedSpecies tags {tags_to_remove}"
        yield field_issue(
            message,
            nam,
            "type_tags",
            tuple(tag for tag in nam.type_tags if tag not in tags_to_remove),
        )


def _included_species_key(nam: Name) -> Name:
    tag_target = nam.get_tag_target(NameTag.NameCombinationOf)
    if tag_target is not None:
        nam = tag_target
    tag_target = nam.get_tag_target(NameTag.SubsequentUsageOf)
    if tag_target is not None:
        nam = tag_target
    return nam


def _prefer_commented(
    tags: list[TypeTag.IncludedSpecies],  # type: ignore[name-defined]
) -> tuple[list[TypeTag.IncludedSpecies], str | None]:  # type: ignore[name-defined]
    assert len({tag.name for tag in tags}) == 1
    with_ce = [tag for tag in tags if tag.classification_entry is not None]
    message: str | None = None
    if len(with_ce) > 1:
        message = f"remove one of duplicate tags {with_ce}"
        return [], message
    elif len(with_ce) == 1:
        preferred = with_ce[0]
        removable = {
            tag
            for tag in tags
            if tag != preferred
            and (tag.page is None or tag.page == preferred.page)
            and (tag.comment is None or tag.comment == preferred.comment)
        }
        non_removable = [
            tag for tag in tags if tag != preferred and tag not in removable
        ]
        if non_removable:
            message = f"remove one of duplicate tags {non_removable}"
        return sorted(removable), message
    commented = [
        tag for tag in tags if tag.comment or tag.page or tag.classification_entry
    ]
    uncommented = [
        tag
        for tag in tags
        if not tag.comment and not tag.page and not tag.classification_entry
    ]
    if commented:
        to_remove = uncommented
        if len(commented) > 1:
            message = f"remove one of duplicate tags {commented}"
    else:
        to_remove = []
        if len(uncommented) > 1:
            message = f"remove one of duplicate tags {uncommented}"
    return to_remove, message


def _location_detail_covers_classification_entry(
    tag: TypeTag.LocationDetail, ce: ClassificationEntry  # type: ignore[name-defined]
) -> bool:
    if tag.classification_entry == ce:
        return True
    return (
        ce.rank is Rank.subspecies
        and ce.parent == tag.classification_entry
        and tag.source == ce.article
        and tag.text == ce.type_locality
    )


def _infer_detail_from_classification_entry(
    nam: Name, expected: TypeTag, ce: ClassificationEntry
) -> Iterable[LintResult]:
    expected = expected.replace(text=detail_lint.normalized_text(expected))
    matching = [
        tag
        for tag in nam.get_tags(nam.type_tags, type(expected))
        if tag.source == expected.source
        and detail_lint.normalized_text(tag) == expected.text
        and tag.classification_entry in (None, ce)
    ]
    if not matching:
        message = f"adding {type(expected).__name__} from {ce} to {nam}: {expected}"
        yield add_tag_issue(message, nam, expected, field="type_tags")
        return
    for existing in matching:
        updated = existing.replace(classification_entry=ce)
        for field in DETAIL_METADATA_FIELDS:
            value = getattr(expected, field)
            current = getattr(existing, field)
            if value is not None:
                if current is None:
                    updated = updated.replace(**{field: value})
                elif current != value:
                    yield f"{existing} has {field} {current!r} that does not match classification entry {ce}: {value!r}"
        if updated != existing:
            yield replace_tag_issue(
                f"updating {type(expected).__name__} from {ce}",
                nam,
                existing,
                updated,
                field="type_tags",
            )


@LINT.add("infer_tags_from_mapped_entries")
def infer_tags_from_mapped_entries(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    ces = [
        ce if ce.is_virtual else ClassificationEntry.resolve_reference(ce.id)
        for ce in nam.get_classification_entries()
    ]
    ces = [ce for ce in ces if not ce.is_invalid() and ce.mapped_name == nam]
    if not ces:
        return
    tag_name = nam.resolve_variant(unavailable_version=False)
    for ce in ces:
        for tag_type, ce_tag_type in DETAIL_TAG_TO_CE_TAG.items():
            for ce_tag in ce.tags:
                if not isinstance(ce_tag, ce_tag_type):
                    continue
                tag = tag_type(
                    text=ce_tag.text,
                    source=ce.article,
                    classification_entry=ce,
                    **{
                        field: getattr(ce_tag, field)
                        for field in DETAIL_METADATA_FIELDS
                    },
                )
                yield from _infer_detail_from_classification_entry(tag_name, tag, ce)
        if nam.group is Group.species:
            location = ce.type_locality
            if location and not any(
                _location_detail_covers_classification_entry(tag, ce)
                for tag in tag_name.get_tags(tag_name.type_tags, TypeTag.LocationDetail)
            ):
                tag = TypeTag.LocationDetail(
                    location, ce.article, classification_entry=ce
                )
                message = f"adding location detail from {ce} to {tag_name}: {tag}"
                yield add_tag_issue(message, tag_name, tag, field="type_tags")
        if (
            tag_name.original_citation is None
            and ce.citation is not None
            and (nam.group is not Group.family or nam.year == ce.year)
        ):
            tag = TypeTag.CitationDetail(ce.citation, ce.article)
            if not any(
                existing.text == tag.text and existing.source == tag.source
                for existing in tag_name.get_tags(
                    tag_name.type_tags, TypeTag.CitationDetail
                )
            ) and not any(
                detail.text == ce.citation
                for detail in ce.get_tags(
                    ce.tags, ClassificationEntryTag.CitationDetail
                )
            ):
                message = f"adding verbatim citation from {ce} to {nam}: {tag}"
                yield add_tag_issue(message, tag_name, tag, field="type_tags")

    ce = nam.get_mapped_classification_entry()
    # Don't copy page links until we've aligned the page that it's on
    if ce is not None and ce.page == nam.page_described:
        for tag in ce.tags:
            if isinstance(tag, ClassificationEntryTag.PageLink):
                expected_tag = TypeTag.AuthorityPageLink(
                    url=tag.url, confirmed=True, page=tag.page
                )
                if expected_tag in nam.type_tags:
                    continue
                message = f"adding page link from {ce} to {nam}: {tag}"
                yield add_tag_issue(message, nam, expected_tag, field="type_tags")


@LINT.add("must_have_ce")
def check_must_have_ce(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.original_citation is None:
        return
    if not any(nam.original_citation.get_classification_entries()):
        return
    if nam.get_mapped_classification_entry() is not None:
        return
    possible_ces = list(nam.get_possible_mapped_classification_entries())
    if possible_ces:
        yield f"has multiple possible mapped classification entries: {possible_ces}"
    else:
        if not nam.is_invalid():
            movable_ces = [
                ce
                for ce in ClassificationEntry.select_valid().filter(
                    ClassificationEntry.article == nam.original_citation,
                    ClassificationEntry.name == nam.original_name,
                )
                if ce.get_corrected_name() == nam.corrected_original_name
                and ce.mapped_name is not None
                and ce.mapped_name.resolve_variant() == nam.resolve_variant()
            ]
            if len(movable_ces) == 1:
                ce = movable_ces[0]
                message = f"changing mapped name of {ce} from {ce.mapped_name} to {nam}"
                yield field_issue(message, ce, "mapped_name", nam)
        if not has_classification(nam.original_citation):
            return
        yield f"must have classification entries for {nam.original_citation}"


def can_transform(input: T, output: T, transforms: Sequence[Callable[[T], T]]) -> bool:
    if input == output:
        return True
    for i in range(1, len(transforms)):
        for funcs in itertools.permutations(transforms, i):
            transformed = input
            for func in funcs:
                transformed = func(transformed)
            if transformed == output:
                return True
    return False


_ALLOWED_TRANSFORMS: list[Callable[[str], str]] = [
    lambda s: s.replace("æ", "ae"),
    lambda s: s.replace("œ", "oe"),
    lambda s: s.replace("Æ", "Ae"),
    lambda s: s.replace("Œ", "Oe"),
    # Ab Cd -> Ab cd
    lambda s: re.sub(
        r"^([A-Z]+[a-z]+) ([A-Z]+[a-z]+)$",
        lambda m: f"{m.group(1)!s} {m.group(2).lower()!s}",
        s,
    ),
    # Canis familiaris γ. sibiricus -> Canis familiaris sibiricus
    lambda s: re.sub(r" [α-ω]\. ", " ", s),
    # uppercase genus name
    lambda s: s[:1].upper() + s[1:],
]


@LINT.add("matches_mapped")
def check_matches_mapped_classification_entry(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    ce = nam.get_mapped_classification_entry()
    if ce is None:
        return
    if ce.name != nam.original_name:
        if nam.original_name is None or can_transform(
            ce.name, nam.original_name, _ALLOWED_TRANSFORMS
        ):
            message = f"changing original name to {ce.name}"
            yield field_issue(message, nam, "original_name", ce.name)
        else:
            yield f"mapped to {ce}, but {ce.name=} != {nam.original_name=}"
    expected_con = ce.get_name_to_use_as_normalized_original_name()
    if expected_con != nam.corrected_original_name:
        yield f"mapped to {ce}, but {expected_con} != {nam.corrected_original_name}"
    if ce.page != nam.page_described:
        if nam.page_described is None:
            yield field_issue(
                f"inferred page {ce.page}", nam, "page_described", ce.page
            )
        elif set(parse_page_text(nam.page_described)) < set(parse_page_text(ce.page)):
            yield field_issue(
                f"extended page from {nam.page_described} to {ce.page}",
                nam,
                "page_described",
                ce.page,
            )
        else:
            yield f"mapped to {ce}, but {ce.page=} != {nam.page_described=}"
    yield from _check_matching_original_parent(nam, ce, cfg)
    if nam.original_rank is not ce.rank:
        if nam.original_rank is None or (
            nam.status is Status.synonym and ce.rank.is_synonym
        ):
            message = f"inferred rank {ce.rank!r} from {ce}"
            yield field_issue(message, nam, "original_rank", ce.rank)
        else:
            yield f"mapped to {ce}, but {ce.rank=!r} != {nam.original_rank=!r}"
    elif ce.rank.needs_textual_rank:
        ce_tags = list(ce.get_tags(ce.tags, ClassificationEntryTag.TextualRank))
        if ce_tags:
            nam_tags = list(nam.get_tags(nam.type_tags, TypeTag.TextualOriginalRank))
            if not nam_tags:
                tag = TypeTag.TextualOriginalRank(ce_tags[0].text)
                message = f"inferred textual rank from {ce}: {tag}"
                yield add_tag_issue(message, nam, tag, field="type_tags")
            elif ce_tags[0].text != nam_tags[0].text:
                yield f"mapped to {ce}, but textual ranks do not match: {ce_tags[0].text=} != {nam_tags[0].text=}"
    conditions = list(ce.get_tags(ce.tags, ClassificationEntryTag.CECondition))
    applicable_statuses = get_applicable_statuses(nam) | {
        tag.status for tag in nam.get_tags(nam.tags, NameTag.Condition)
    }
    # Ignore if the CE is a nomen nudum in these cases
    if nam.nomenclature_status in (
        NomenclatureStatus.reranking,
        NomenclatureStatus.name_combination,
    ):
        applicable_statuses.add(NomenclatureStatus.nomen_nudum)
    new_conditions = [
        tag for tag in conditions if tag.status not in applicable_statuses
    ]
    if new_conditions:
        for condition in new_conditions:
            tag = NameTag.Condition(condition.status, comment=condition.comment)
            yield add_tag_issue(
                f"mapped {ce} has condition {condition}; add to name", nam, tag
            )

    for tag in ce.tags:
        if isinstance(tag, ClassificationEntryTag.LSID):
            new_tag = TypeTag.LSID(tag.text)
            if new_tag not in nam.type_tags:
                message = f"adding LSID from {ce} to {nam}: {new_tag}"
                yield add_tag_issue(message, nam, new_tag, field="type_tags")


def _check_matching_original_parent(
    nam: Name, ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[LintResult]:
    if nam.original_parent is None:
        ce_parent = ce.get_original_parent_ce()
        if ce_parent is not None and ce_parent.mapped_name is not None:
            message = f"inferred original parent {ce_parent.mapped_name} from {ce}"
            yield field_issue(message, nam, "original_parent", ce_parent.mapped_name)
        return
    if ce.rank.is_synonym:
        return
    ce_parent = ce.get_original_parent_ce()
    if ce_parent is None or ce_parent.mapped_name is None:
        return
    if ce_parent.mapped_name == nam.original_parent:
        return
    mapped_parent = resolve_usage(
        ce_parent.mapped_name, resolve_unavailable_version_of=True
    )
    if mapped_parent == nam.original_parent:
        return
    ce_subgenus = ce.parent_of_rank(Rank.subgenus)
    if (
        ce_subgenus is not None
        and ce_subgenus.mapped_name is not None
        and resolve_usage(ce_subgenus.mapped_name, resolve_unavailable_version_of=True)
        == nam.original_parent
    ):
        return

    # Ignore cases where the CE doesn't use the right name combination for the genus
    if ce_parent is not None:
        genus_name = ce_parent.get_corrected_name()
        species_name = ce.get_corrected_name()
        if genus_name is not None and species_name is not None:
            if genus_name != species_name.split()[0]:
                return
    yield f"mapped to {ce}, but {ce_parent.mapped_name=} (mapped from {ce_parent}) != {nam.original_parent=}"
