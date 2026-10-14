from collections import defaultdict
from collections.abc import Iterable
from datetime import date

import Levenshtein

from taxonomy.db.constants import Group, NomenclatureStatus, Rank, Status
from taxonomy.db.models.article import Article
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.classification_entry.ce import (
    ClassificationEntry,
    ClassificationEntryTag,
)
from taxonomy.db.models.lint import (
    LintFixError,
    add_tag_fix,
    add_tag_issue,
    create_related_object_issue,
    field_fix,
    fields_issue,
    fixes_issue,
    remove_tag_fix,
)
from taxonomy.db.models.lint_types import LintIssue, LintResult
from taxonomy.db.models.name.name import ExperimentalLintCondition as Exp
from taxonomy.db.models.name.name import Name, NameComment, NameTag, TypeTag
from taxonomy.db.models.taxon import Taxon

from .base import LINT


def _maybe_add_name_variant(
    nam: Name, nomenclature_status: NomenclatureStatus, ce: ClassificationEntry
) -> Iterable[LintResult]:
    message = f"adding {nomenclature_status.name} based on {ce}"
    corrected_name = ce.get_corrected_name()
    if corrected_name is None:
        yield message
        return
    yield _create_name_variant_issue(
        message, nam, nomenclature_status, ce, corrected_name
    )


def _create_name_variant_issue(
    message: str,
    nam: Name,
    nomenclature_status: NomenclatureStatus,
    ce: ClassificationEntry,
    corrected_name: str,
) -> LintIssue:
    def create_name() -> Name:
        new_name = nam.add_variant(
            corrected_name.rsplit(maxsplit=1)[-1],
            status=nomenclature_status,
            paper=ce.article,
            page_described=ce.page,
            original_name=ce.name,
            interactive=False,
        )
        if new_name is None:
            raise LintFixError(f"could not create name variant for {ce}")
        new_name.corrected_original_name = corrected_name
        new_name.original_rank = ce.rank
        return new_name

    return create_related_object_issue(message, ce, "mapped_name", create_name)


def take_over_name_issue(message: str, nam: Name, ce: ClassificationEntry) -> LintIssue:
    citation = ce.article
    author_source = (
        citation.parent
        if citation.issupplement() and citation.parent is not None
        else citation
    )
    assert author_source.author_tags is not None, f"missing authors for {author_source}"
    return fixes_issue(
        message,
        field_fix(nam, "original_citation", citation),
        field_fix(nam, "page_described", ce.page),
        field_fix(nam, "original_name", ce.name),
        field_fix(nam, "author_tags", author_source.author_tags),
        field_fix(nam, "year", citation.year),
        *(
            remove_tag_fix(nam, tag, field="type_tags")
            for tag in nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink)
        ),
    )


def maybe_take_over_name(
    nam: Name, ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[LintResult]:
    if (
        nam.get_mapped_classification_entry() is not None
        and nam.nomenclature_status
        in (
            NomenclatureStatus.name_combination,
            NomenclatureStatus.incorrect_subsequent_spelling,
        )
    ):
        message = f"changing original citation of {nam} to {ce.article}"
        yield take_over_name_issue(message, nam, ce)
    elif should_report_unreplaceable_name(nam, cfg):
        yield f"replace name {nam} with {ce}"


def _name_variant_article_sort_key(art: Article) -> tuple[bool, date, int, int, int]:
    return (art.is_unpublished(), art.get_date_object(), art.id, 0, 0)


def name_combination_name_sort_key(nam: Name) -> tuple[bool, date, int, int, int]:
    if nam.original_citation is not None:
        return _name_variant_article_sort_key(nam.original_citation)
    if nam.original_rank is None:
        rank = 0
    else:
        rank = -nam.original_rank.value
    return (False, nam.get_date_object(), 0, rank, nam.id)


@LINT.add("infer_name_variants")
def infer_name_variants(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.group is not Group.species:
        return
    ces = nam.get_classification_entries()
    by_name: dict[str, list[ClassificationEntry]] = defaultdict(list)
    syns_by_name: dict[str, list[ClassificationEntry]] = defaultdict(list)
    for ce in ces:
        corrected_name = ce.get_corrected_name()
        if corrected_name is None:
            continue
        if ce.rank.is_synonym:
            root_name = corrected_name.split()[-1]
            if root_name in nam.get_root_name_forms():
                continue
            syns_by_name[root_name].append(ce)
        else:
            by_name[corrected_name].append(ce)
    expected_name_variants = [
        # Prefer variant that are published by the ICZN's rules
        min(ces, key=lambda ce: _name_variant_article_sort_key(ce.article))
        for ces in by_name.values()
    ]
    yield from _infer_name_variants_of_status(
        nam, cfg, expected_name_variants, NomenclatureStatus.name_combination
    )
    yield from _infer_name_variants_of_status(
        nam,
        cfg,
        expected_name_variants,
        NomenclatureStatus.incorrect_subsequent_spelling,
    )

    for root_name, ces in syns_by_name.items():
        expected_ce = min(
            ces, key=lambda ce: _name_variant_article_sort_key(ce.article)
        )
        existing_names = nam.taxon.get_names().filter(Name.root_name == root_name)
        if any(
            existing.get_date_object() < expected_ce.get_date_object()
            for existing in existing_names
        ):
            continue
        # Can happen with justified emendations
        if (
            nam.corrected_original_name is not None
            and root_name == nam.corrected_original_name.split()[-1]
        ):
            continue
        replaceable = [
            existing
            for existing in existing_names
            if _is_iss_from_synonym_without_full_name(existing)
        ]
        if replaceable:
            yield from maybe_take_over_name(replaceable[0], expected_ce, cfg)
        else:
            corrected_name = expected_ce.get_name_to_use_as_normalized_original_name()
            if corrected_name is None:
                yield f"add {expected_ce} as a name variant"
            else:
                yield _create_name_variant_issue(
                    f"add {expected_ce} as a name variant",
                    nam,
                    NomenclatureStatus.incorrect_subsequent_spelling,
                    expected_ce,
                    corrected_name,
                )


def _is_iss_from_synonym_without_full_name(nam: Name) -> bool:
    ce = nam.get_mapped_classification_entry()
    return ce is not None and ce.is_synonym_without_full_name()


def _redirect_name_issue(message: str, nam: Name, target: Name) -> LintIssue:
    assert nam != target
    return fields_issue(
        message, (nam, "status", Status.redirect), (nam, "target", target)
    )


def _infer_name_variants_of_status(
    nam: Name,
    cfg: LintConfig,
    expected_name_variants: list[ClassificationEntry],
    nomenclature_status: NomenclatureStatus,
) -> Iterable[LintResult]:
    if not expected_name_variants:
        return
    expected_base = nam.resolve_variant()
    for ce in expected_name_variants:
        corrected_name = ce.get_corrected_name()
        if corrected_name is None:
            continue
        if (
            corrected_name == nam.corrected_original_name
            and nam.nomenclature_status is not nomenclature_status
        ):
            continue
        ce_root_name = corrected_name.split()[-1]
        is_root_name_form = ce_root_name in nam.get_root_name_forms()
        if (
            nomenclature_status is NomenclatureStatus.name_combination
            and not is_root_name_form
        ):
            continue
        if (
            nomenclature_status is NomenclatureStatus.incorrect_subsequent_spelling
            and nam.nomenclature_status
            is not NomenclatureStatus.incorrect_subsequent_spelling
            and is_root_name_form
        ):
            continue
        existing = [
            nam
            for nam in Name.select_valid().filter(
                Name.corrected_original_name == corrected_name,
                Name.group == Group.species,
                Name.taxon == nam.taxon,
                Name.nomenclature_status == nomenclature_status,
            )
            if nam.resolve_variant() == expected_base
        ]
        match len(existing):
            case 0:
                yield from _maybe_add_name_variant(nam, nomenclature_status, ce)
            case 1:
                (existing_name,) = existing
                if (
                    existing_name.original_citation != ce.article
                    and _name_variant_article_sort_key(ce.article)
                    < name_combination_name_sort_key(existing_name)
                ):
                    yield from maybe_take_over_name(existing_name, ce, cfg)
            case _:
                # multiple; remove the newest
                existing.sort(key=name_combination_name_sort_key)
                for duplicate in existing[1:]:
                    can_replace = can_replace_name(duplicate)
                    message = (
                        f"removing duplicate {nomenclature_status.name} {duplicate}"
                    )
                    if can_replace is None:
                        yield _redirect_name_issue(message, duplicate, existing[0])
                    elif should_report_unreplaceable_name(duplicate, cfg):
                        yield f"{message} (cannot replace because of: {can_replace})"


@LINT.add("duplicate_variants")
def check_duplicate_variants(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    nomenclature_status = nam.nomenclature_status
    if nomenclature_status not in (
        NomenclatureStatus.name_combination,
        NomenclatureStatus.incorrect_subsequent_spelling,
    ):
        return
    if nam.original_rank is Rank.synonym:
        return
    dupes: Iterable[Name]
    if nam.original_rank is Rank.synonym_species:
        dupes = Name.select_valid().filter(
            Name.taxon == nam.taxon, Name.root_name == nam.root_name
        )
    else:
        dupes = Name.select_valid().filter(
            Name.nomenclature_status == nomenclature_status,
            Name.taxon == nam.taxon,
            Name.corrected_original_name == nam.corrected_original_name,
        )
    base = nam.resolve_variant()
    if nam.is_virtual:
        # Queries return persisted rows, including this proposal's own origin.
        # Resolve them before comparing dates or the proposed nominal identity.
        dupes = [
            dupe if dupe.is_virtual else Name.resolve_reference(dupe.id)
            for dupe in dupes
        ]
    earlier = sorted(
        [
            dupe
            for dupe in dupes
            if dupe != nam
            and not dupe.is_invalid()
            and dupe.get_date_object() < nam.get_date_object()
            and dupe.resolve_variant() == base
        ],
        key=lambda dupe: (dupe.get_date_object(), dupe.id),
    )
    if earlier:
        message = f"remove because of earlier names with status {nomenclature_status.name}: {', '.join(str(dupe) for dupe in earlier)}"
        can_replace = can_replace_name(nam)
        if can_replace is None:
            yield _redirect_name_issue(message, nam, earlier[0])
        elif (
            nomenclature_status is NomenclatureStatus.name_combination
            and nam.year
            and earlier[0].year
            and nam.corrected_original_name
            and nam.corrected_original_name == earlier[0].corrected_original_name
            and (
                combination := next(
                    iter(nam.get_tags(nam.tags, NameTag.NameCombinationOf)), None
                )
            )
            is not None
        ):
            # Retain source information that prevents removal, but do not treat
            # a repeated combination as independently established again.
            yield fixes_issue(
                f"marking as subsequent usage of earlier combination {earlier[0]}",
                remove_tag_fix(nam, combination),
                add_tag_fix(
                    nam,
                    NameTag.SubsequentUsageOf(earlier[0], comment=combination.comment),
                ),
                field_fix(
                    nam, "nomenclature_status", NomenclatureStatus.subsequent_usage
                ),
            )
        elif should_report_unreplaceable_name(nam, cfg):
            yield f"{message} (cannot replace because of: {can_replace})"


def can_replace_name(nam: Name) -> str | None:
    """Require the CE and nominal base to preserve all non-structural data.

    Redirecting does not transfer fields or comments. Explicitly account for
    reconstructible fields below; any other populated field requires review.
    """
    if nam.status not in (Status.synonym, Status.dubious):
        return f"status {nam.status.name}"
    identity = nam.virtual_origin or nam
    # Comments deliberately permit invalid Names, so ordinary foreign-key lint
    # does not move them off a redirect. Check the persisted origin in previews.
    if any(NameComment.select().filter(NameComment.name == identity)):
        return "has comments"
    if any(Taxon.select_valid().filter(Taxon.base_name == identity)):
        return "is a taxon's base name"
    ce = nam.get_mapped_classification_entry()
    if ce is None:
        return "has no classification entries"
    for tag in nam.type_tags:
        if isinstance(tag, TypeTag.AuthorityPageLink) and (
            ClassificationEntryTag.PageLink(tag.url, tag.page) in ce.tags
        ):
            continue
        return f"type tag {tag}"
    for tag in nam.tags:
        if isinstance(
            tag,
            (
                NameTag.NameCombinationOf,
                NameTag.SubsequentUsageOf,
                NameTag.UnjustifiedEmendationOf,
                NameTag.IncorrectSubsequentSpellingOf,
                NameTag.RerankingOf,
            ),
        ):
            if tag.comment:
                return f"comment on {type(tag).__name__} tag"
            continue
        return f"tag {tag}"

    article = ce.article
    author_source = (
        article.parent
        if article.issupplement() and article.parent is not None
        else article
    )
    base = nam.resolve_variant()
    if base == nam:
        return "has no resolved nominal base"
    parent_ce = ce.get_original_parent_ce()
    expected = {
        "original_citation": article,
        "original_name": ce.name,
        "corrected_original_name": ce.get_name_to_use_as_normalized_original_name(),
        "original_rank": ce.rank,
        "page_described": ce.page,
        "author_tags": author_source.author_tags,
        "year": article.year,
        "citation_group": article.citation_group,
        "original_parent": parent_ce.mapped_name if parent_ce is not None else None,
        "name_complex": base.name_complex,
        "species_name_complex": base.species_name_complex,
    }
    # Callers establish nominal identity; these fields describe the record's
    # placement and variant relationship rather than additional source evidence.
    structural = {
        "group",
        "root_name",
        "status",
        "taxon",
        "nomenclature_status",
        "tags",
        "type_tags",
        "data",  # Legacy data is deliberately ignored.
    }
    for field in Name.clirm_fields:
        if field in structural:
            continue
        value = getattr(nam, field)
        if value in (None, (), ""):
            continue
        if field not in expected:
            return f"extra field {field}"
        if value != expected[field]:
            return f"field {field} is not preserved by the classification entry or base name"
    return None


def should_report_unreplaceable_name(nam: Name, cfg: LintConfig) -> bool:
    if nam.original_citation is None:
        return False
    return (
        Exp().should_apply(nam, cfg)
        or nam.nomenclature_status is NomenclatureStatus.name_combination
        or (nam.group is Group.species and nam.numeric_year() > 1950)
    )


@LINT.add("mark_incorrect_subsequent_spelling_as_name_combination")
def mark_incorrect_subsequent_spelling_as_name_combination(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    if nam.nomenclature_status != NomenclatureStatus.incorrect_subsequent_spelling:
        return
    if nam.corrected_original_name is None:
        return
    original_root_name = nam.corrected_original_name.split()[-1]
    target = nam.get_tag_target(NameTag.IncorrectSubsequentSpellingOf)
    candidates = (
        candidate if candidate.is_virtual else Name.resolve_reference(candidate.id)
        for candidate in Name.select_valid().filter(
            Name.root_name == nam.root_name,
            Name.group == nam.group,
            Name.taxon == nam.taxon,
        )
    )
    existing = [
        candidate
        for candidate in candidates
        if not candidate.is_invalid()
        and candidate.group == nam.group
        and candidate.taxon == nam.taxon
        and candidate.get_tag_target(NameTag.IncorrectSubsequentSpellingOf) == target
        and candidate.root_name == original_root_name
    ]
    if not existing:
        return
    existing = sorted(
        existing,
        key=lambda nam: (
            nam.get_date_object(),
            -nam.original_rank.value if nam.original_rank is not None else 0,
            nam.id,
        ),
    )
    earliest, *rest = existing
    if not rest:
        return
    if nam == earliest:
        if nam.has_name_tag(NameTag.NameCombinationOf):
            yield f"is the earliest occurrence of misspelling {nam.root_name} and should not be marked as name combination"
        if nam.has_name_tag(NameTag.SubsequentUsageOf):
            yield f"is the earliest occurrence of misspelling {nam.root_name} and should not be marked as subsequent usage"
    else:
        if earliest.corrected_original_name == nam.corrected_original_name:
            expected_tag = NameTag.SubsequentUsageOf(earliest)
        else:
            expected_tag = NameTag.NameCombinationOf(earliest)
        if expected_tag not in nam.tags:
            message = f"changing to name combination of {earliest}"
            old_tags = [
                tag
                for tag in nam.tags
                if isinstance(
                    tag, (NameTag.SubsequentUsageOf, NameTag.NameCombinationOf)
                )
            ]
            yield fixes_issue(
                message,
                *(remove_tag_fix(nam, tag) for tag in old_tags),
                add_tag_fix(nam, expected_tag),
            )


@LINT.add("family_group_subsequent_usage")
def mark_family_group_subsequent_usage(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    if nam.group is not Group.family:
        return
    if nam.corrected_original_name is None or nam.original_rank is None:
        return
    if nam.has_name_tag(NameTag.SubsequentUsageOf):
        return
    resolved_nam = nam.resolve_variant()
    same_name = [
        other_name
        for other_name in nam.taxon.get_names().filter(
            Name.corrected_original_name == nam.corrected_original_name,
            Name.id != nam.id,
            Name.taxon == nam.taxon,
        )
        if other_name.numeric_year() < nam.numeric_year()
        and other_name.get_grouped_rank() == nam.get_grouped_rank()
        and other_name.resolve_variant() == resolved_nam
        and (
            other_name.nomenclature_status is NomenclatureStatus.available
            or other_name.nomenclature_status.is_variant()
        )
    ]
    if not same_name:
        return
    earliest_name = min(same_name, key=lambda name: name.numeric_year())
    tag = NameTag.SubsequentUsageOf(earliest_name)
    message = f"marking as subsequent usage of {earliest_name}"
    yield add_tag_issue(message, nam, tag)


@LINT.add("infer_unavalailable_version")
def infer_unavailable_version(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.nomenclature_status not in {
        NomenclatureStatus.nomen_nudum,
        NomenclatureStatus.not_based_on_a_generic_name,
        NomenclatureStatus.infrasubspecific,
        NomenclatureStatus.unpublished,
        NomenclatureStatus.before_1758,
        NomenclatureStatus.informal,
        NomenclatureStatus.not_latin_alphabet,
        NomenclatureStatus.inconsistently_binominal,
        NomenclatureStatus.not_used_as_valid,
        NomenclatureStatus.not_used_as_genus_plural,
        NomenclatureStatus.multiple_words,
        NomenclatureStatus.no_type_specified,
        NomenclatureStatus.anonymous_authorship,
        NomenclatureStatus.conditional,
        NomenclatureStatus.variety_or_form,
        NomenclatureStatus.not_explicitly_new,
        NomenclatureStatus.type_not_treated_as_valid,
        NomenclatureStatus.not_intended_as_a_scientific_name,
        NomenclatureStatus.not_nominative_singular,
        NomenclatureStatus.rejected_by_fiat,
        NomenclatureStatus.unpublished_thesis,
        NomenclatureStatus.unpublished_electronic,
        NomenclatureStatus.unpublished_pending,
        NomenclatureStatus.unpublished_supplement,
        NomenclatureStatus.placed_on_index,
        NomenclatureStatus.fully_suppressed,
        NomenclatureStatus.not_published_with_a_generic_name,
    }:
        return
    if nam.year is None:
        return
    if nam.has_name_tag(NameTag.UnavailableVersionOf):
        return
    candidates = [
        sibling
        for sibling in nam.taxon.get_names().filter(
            Name.year >= nam.year,
            Name.nomenclature_status.is_in(
                (
                    NomenclatureStatus.available,
                    NomenclatureStatus.preoccupied,
                    NomenclatureStatus.nomen_novum,
                )
            ),
            Name.group == nam.group,
            Name.corrected_original_name == nam.corrected_original_name,
        )
        if nam.root_name in sibling.get_root_name_forms()
    ]
    if not candidates:
        return
    if len(candidates) > 1:
        yield f"multiple candidates for available version: {candidates}"
        return
    tag = NameTag.UnavailableVersionOf(candidates[0])
    message = f"add UnavailableVersionOf tag: {tag}"
    yield add_tag_issue(message, nam, tag)


@LINT.add("should_be_variant")
def should_be_variant(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if (
        nam.group is not Group.genus
        or nam.nomenclature_status is not NomenclatureStatus.available
    ):
        return
    candidates = [
        sibling
        for sibling in nam.taxon.get_names().filter(
            Name.group == Group.genus,
            Name.nomenclature_status == NomenclatureStatus.available,
        )
        if Levenshtein.distance(nam.root_name, sibling.root_name) <= 2
        and sibling.numeric_year() < nam.numeric_year()
    ]
    if nam.type is not None:
        candidates = [sibling for sibling in candidates if nam.type == sibling.type]
    if candidates:
        yield f"should be marked as a variant of one of {candidates}"


@LINT.add("remove_redundant_name")
def remove_redundant_name(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.nomenclature_status is not NomenclatureStatus.subsequent_usage:
        return
    target = nam.get_tag_target(NameTag.SubsequentUsageOf)
    if target is None:
        return  # other linters will complain
    has_ces = nam.get_mapped_classification_entry() is not None
    cannot_replace_reason = can_replace_name(nam)
    if cannot_replace_reason is None:
        message = f"remove redundant name {nam} by redirecting to {target}"
        yield _redirect_name_issue(message, nam, target)
    elif any(
        isinstance(
            tag,
            (
                NameTag.Rejected,
                NameTag.Conserved,
                NameTag.FullySuppressedBy,
                NameTag.PartiallySuppressedBy,
            ),
        )
        for tag in nam.tags
    ):
        return  # ignore for now
    elif has_ces:
        yield f"cannot remove redundant name {nam} because {cannot_replace_reason}"
    elif nam.original_citation is not None:
        # Various groups of subsequent usages that should be eradicated. Let's
        # gradually add more here.
        if not has_ces and any(nam.original_citation.get_classification_entries()):
            yield "subsequent usage has no classification entries, but article has some"
        elif Exp(
            before_year=1800,
            after_year=2000,
            require_in_groups=(Group.high,),
            above_name_id=100_000,
        ).should_apply(nam, cfg):
            yield "subsequent usage should be replaced"
