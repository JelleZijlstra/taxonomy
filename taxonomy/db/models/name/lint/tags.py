import enum
import re
from collections.abc import Callable, Generator, Iterable, Sequence
from dataclasses import dataclass

from taxonomy import adt, getinput
from taxonomy.db.constants import ArticleType, Group, NomenclatureStatus, Rank, Status
from taxonomy.db.models.article import Article, ArticleTag
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.classification_entry.ce import (
    ClassificationEntry,
    ClassificationEntryTag,
)
from taxonomy.db.models.lint import add_tag_issue, field_issue
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name.name import (
    PREOCCUPIED_TAGS,
    STATUS_TO_TAG,
    Name,
    NameTag,
    NameTagCons,
)

from .base import LINT
from .helpers import get_parent
from .type_tags import check_selection_tag

TAG_TO_STATUS = {
    **{tag: status for status, tag in STATUS_TO_TAG.items()},
    NameTag.FullySuppressedBy: NomenclatureStatus.fully_suppressed,
    NameTag.PartiallySuppressedBy: NomenclatureStatus.partially_suppressed,
    NameTag.AsEmendedBy: NomenclatureStatus.as_emended,
}


def _check_preoccupation_tag(
    tag: NameTag, nam: Name, cfg: LintConfig
) -> Generator[str, None, NameTag]:
    if not isinstance(tag, PREOCCUPIED_TAGS):
        return tag
    senior_name = tag.name
    new_tag = tag
    if nam.group != senior_name.group:
        yield f"is of a different group than supposed senior name {senior_name}"
    if senior_name.nomenclature_status is NomenclatureStatus.subsequent_usage:
        senior_name = senior_name.get_tag_target(NameTag.SubsequentUsageOf)
        assert senior_name is not None
    if senior_name.nomenclature_status is NomenclatureStatus.name_combination:
        senior_name = senior_name.get_tag_target(NameTag.NameCombinationOf)
        assert senior_name is not None
    if senior_name.nomenclature_status is NomenclatureStatus.reranking:
        senior_name = senior_name.get_tag_target(NameTag.RerankingOf)
        assert senior_name is not None
    if senior_name.nomenclature_status is NomenclatureStatus.misidentification:
        senior_name = senior_name.get_tag_target(NameTag.MisidentificationOf)
        assert senior_name is not None
    if nam.has_priority_over(senior_name):
        yield f"has priority over supposed senior name {senior_name}"
    for other_tag in senior_name.tags:
        if isinstance(other_tag, NameTag.NomenOblitum) and other_tag.name == nam:
            yield f"senior name {senior_name} is marked as NomenOblitum for {nam}"
    if nam.group is Group.species:
        if nam.original_parent is None:
            my_original = None
        else:
            my_original = nam.original_parent.resolve_variant(misidentification=True)
        if senior_name.original_parent is None:
            senior_original = None
        else:
            senior_original = senior_name.original_parent.resolve_variant(
                misidentification=True
            )
        if isinstance(tag, NameTag.PrimaryHomonymOf):
            if my_original is None:
                yield f"{nam} is marked as a primary homonym of {senior_name}, but has no original genus"
            elif senior_original is None:
                yield f"{senior_name} is marked as a primary homonym, but has no original genus"
            elif my_original != senior_original:
                yield (
                    f"{nam} is marked as a primary homonym of {senior_name}, but has a"
                    f" different original genus ({my_original} vs. {senior_original})"
                )
        elif isinstance(tag, NameTag.SecondaryHomonymOf):
            if my_original is not None and my_original == senior_original:
                yield (
                    f"{nam} is marked as a secondary homonym of {senior_name}, but has"
                    " the same original genus, so it should be marked as a primary homonym instead"
                )
                new_tag = NameTag.PrimaryHomonymOf(tag.name, comment=tag.comment)
            my_genus = get_parent(nam)
            senior_genus = get_parent(senior_name)
            if my_genus != senior_genus:
                yield (
                    f"{nam} is marked as a secondary homonym of {senior_name}, but is not currently placed in the same genus"
                )
        elif isinstance(tag, NameTag.PreoccupiedBy):
            if my_original is not None and my_original == senior_original:
                new_tag = NameTag.PrimaryHomonymOf(tag.name, comment=tag.comment)
            elif get_parent(nam) == get_parent(senior_name):
                new_tag = NameTag.SecondaryHomonymOf(tag.name, comment=tag.comment)
            else:
                yield f"{nam} is marked as preoccupied by {senior_name}, but is not a primary or secondary homonym"
    elif isinstance(tag, (NameTag.PrimaryHomonymOf, NameTag.SecondaryHomonymOf)):
        yield f"{nam} is not a species-group name, but uses {type(tag).__name__} tag"
        new_tag = NameTag.PreoccupiedBy(tag.name, comment=tag.comment)
    my_normalized = nam.get_normalized_root_name_for_homonymy()
    their_normalized = senior_name.get_normalized_root_name_for_homonymy()
    if my_normalized != their_normalized:
        yield f"has a different root name ({my_normalized}) than supposed senior name {senior_name} ({their_normalized})"
    if not senior_name.can_preoccupy():
        yield f"senior name {senior_name} is not available"
    return new_tag


def _normalize_ii(name: str) -> str:
    # ICZN Art. 33.4: change between ii/i, iae/ae, iorum/orum, arum/arum
    # are always ISS, not unjustified emendations
    return re.sub(r"i(i|ae|orum|arum)", r"\1", name)


def _check_variant_tag(
    tag: NameTag, nam: Name, cfg: LintConfig
) -> Generator[LintResult, None, NameTag | None]:
    if not isinstance(
        tag,
        (
            NameTag.UnjustifiedEmendationOf,
            NameTag.IncorrectSubsequentSpellingOf,
            NameTag.VariantOf,
            NameTag.NomenNovumFor,
            NameTag.JustifiedEmendationOf,
            NameTag.SubsequentUsageOf,
            NameTag.MisidentificationOf,
            NameTag.NameCombinationOf,
            NameTag.RerankingOf,
        ),
    ):
        return tag
    new_tag: NameTag | None = tag
    if nam == tag.name:
        yield f"has a tag that points to itself: {tag}"
    if nam.get_date_object() < tag.name.get_date_object():
        yield f"predates supposed original name {tag.name}"
    # An available version need not share types with an attempted replacement
    # of the unavailable name. Leave this relationship for publication review.
    if (
        not isinstance(tag, NameTag.NomenNovumFor)
        and not tag.name.nomenclature_status.can_preoccupy()
    ):
        if (
            target := tag.name.get_tag_target(NameTag.UnavailableVersionOf)
        ) and nam.year > target.year:
            yield f"tag {tag} should instead point to available name {target}"
            new_tag = adt.replace(tag, name=target)
    if (
        nam.taxon != tag.name.taxon
        and not isinstance(tag, NameTag.MisidentificationOf)
        and nam.get_tag_target(NameTag.MisidentificationOf) is None
    ):
        yield f"{nam} is not assigned to the same name as {tag.name}"
        if nam.status is Status.synonym:
            yield field_issue(
                f"change taxon to {tag.name.taxon} to match {tag.name}",
                nam,
                "taxon",
                tag.name.taxon,
            )
    if (
        not isinstance(
            tag,
            (
                NameTag.SubsequentUsageOf,
                NameTag.MisidentificationOf,
                NameTag.JustifiedEmendationOf,
                NameTag.RerankingOf,
            ),
        )
        and nam.corrected_original_name == tag.name.corrected_original_name
    ):
        yield f"{nam} has the same corrected original name as {tag.name}, but is marked as {type(tag).__name__}"
        if isinstance(tag, NameTag.NameCombinationOf):
            new_tag = NameTag.SubsequentUsageOf(tag.name, comment=tag.comment)
    if not isinstance(
        tag,
        (
            NameTag.SubsequentUsageOf,
            NameTag.MisidentificationOf,
            NameTag.NameCombinationOf,
            NameTag.JustifiedEmendationOf,
            NameTag.RerankingOf,
        ),
    ) and (
        (nam.corrected_original_name == tag.name.corrected_original_name)
        if nam.group is Group.family
        else (
            nam.root_name == tag.name.root_name
            and (
                nam.species_name_complex is None
                or nam.species_name_complex == tag.name.species_name_complex
            )
        )
    ):
        yield f"{nam} has the same root name as {tag.name}, but is marked as {type(tag).__name__}"
    if (
        not isinstance(tag, NameTag.SubsequentUsageOf)
        and tag.name.nomenclature_status is NomenclatureStatus.name_combination
    ):
        message = f"{nam} is marked as a {type(tag).__name__} of a name combination"
        new_target = tag.name.get_tag_target(NameTag.NameCombinationOf)
        if new_target is not None:
            message += f" of {new_target}"
            new_tag = adt.replace(tag, name=new_target)
        yield message
    return new_tag


def _check_all_tags(
    tag: NameTag, nam: Name, cfg: LintConfig
) -> Generator[LintResult, None, NameTag | None]:
    match tag:
        case NameTag.RerankingOf():
            if nam.group is not Group.family:
                yield f"{nam} is not a family-group name, but uses RerankingOf tag"
            elif nam.type is None:
                yield f"{nam} is marked as a reranking of {tag.name}, but has no type"
            elif tag.name.type is None:
                yield f"{tag.name} is marked as the target of a reranking, but has no type"
            else:
                my_type = nam.type.resolve_variant()
                their_type = tag.name.type.resolve_variant()
                if my_type != their_type:
                    yield f"{nam} is marked as a reranking of {tag.name}, but has a different type"
                elif (
                    nam.corrected_original_name == tag.name.corrected_original_name
                    and nam.original_rank is not None
                    and nam.original_rank == tag.name.original_rank
                ):
                    yield f"{nam} is marked as a reranking of {tag.name}, but has the same corrected original name"
            return tag
        case NameTag.NameCombinationOf():
            if (
                nam.species_name_complex != tag.name.species_name_complex
                and not tag.name.has_name_tag(NameTag.AsEmendedBy)
            ):
                message = f"{nam} ({nam.species_name_complex}) is a name combination of {tag.name} ({tag.name.species_name_complex}), but has a different name complex"
                if tag.name.species_name_complex is not None:
                    yield field_issue(
                        message,
                        nam,
                        "species_name_complex",
                        tag.name.species_name_complex,
                    )
                else:
                    yield message
            if nam.root_name not in _get_extended_root_name_forms(tag.name):
                yield f"{nam} is a name combination of {tag.name}, but has a different root name"
                if cfg.interactive and getinput.yes_no(
                    "Mark as incorrect subsequent spelling instead?"
                ):
                    return NameTag.IncorrectSubsequentSpellingOf(
                        tag.name, comment=tag.comment
                    )
            if tag.name.nomenclature_status is NomenclatureStatus.name_combination:
                yield f"{nam} is marked as a name combination of {tag.name}, but that name is already a name combination"
                new_target = tag.name.get_tag_target(NameTag.NameCombinationOf)
                if new_target is not None:
                    return NameTag.NameCombinationOf(new_target, comment=tag.comment)
            elif (
                tag.name.nomenclature_status
                is NomenclatureStatus.incorrect_subsequent_spelling
            ):
                new_target = tag.name.get_tag_target(
                    NameTag.IncorrectSubsequentSpellingOf
                )
                self_iss_target = nam.get_tag_target(
                    NameTag.IncorrectSubsequentSpellingOf
                )
                if not (new_target is not None and new_target == self_iss_target):
                    yield f"{nam} is marked as a name combination of {tag.name}, but that is an incorrect subsequent spelling"
                    if new_target is not None:
                        return NameTag.IncorrectSubsequentSpellingOf(
                            new_target, comment=tag.comment
                        )
            return tag
        case NameTag.SubsequentUsageOf():
            if (
                nam.group is Group.species
                and nam.taxon == tag.name.taxon
                and nam.corrected_original_name != tag.name.corrected_original_name
            ):
                yield (
                    f"{nam} should be a name combination instead of a subsequent"
                    " usage, because it is assigned to the same taxon as its"
                    f" target {tag.name}"
                )
                return NameTag.NameCombinationOf(tag.name, comment=tag.comment)
            if nam.taxon != tag.name.taxon:
                yield f"{nam} is not assigned to the same name as {tag.name} and should be marked as a misidentification"
            if nam.group is not tag.name.group:
                yield f"{nam} is of a different group than its target {tag.name}"
            if nam.group is Group.family:
                if nam.corrected_original_name != tag.name.corrected_original_name:
                    yield f"{nam} is a subsequent usage of {tag.name} but has a different corrected original name"
                if nam.get_grouped_rank() != tag.name.get_grouped_rank():
                    yield f"{nam} is a subsequent usage of {tag.name} but has a different original rank"
            elif nam.root_name != tag.name.root_name:
                yield f"{nam} is a subsequent usage of {tag.name} but has a different root name"
        case NameTag.VariantOf():
            if nam.original_citation is not None:
                # should be specified to unjustified emendation or incorrect subsequent spelling
                yield f"{nam} is marked as a variant, but has an original citation"
        case NameTag.IncorrectSubsequentSpellingOf():
            if (
                tag.name.nomenclature_status
                is NomenclatureStatus.incorrect_subsequent_spelling
            ):
                message = f"{nam} is marked as an incorrect subsequent spelling of an incorrect subsequent spelling"
                new_target = tag.name.get_tag_target(
                    NameTag.IncorrectSubsequentSpellingOf
                )
                if new_target is not None:
                    message += f" of {new_target}"
                    tag = NameTag.IncorrectSubsequentSpellingOf(
                        new_target, comment=tag.comment
                    )
                yield message
            if nam.corrected_original_name is not None:
                if (
                    nam.corrected_original_name.split()[-1]
                    in tag.name.get_root_name_forms()
                    and nam.group is not Group.family
                ):
                    yield f"{nam} has the same root name as {tag.name}, but is marked as an incorrect subsequent spelling"
                    if nam.group is Group.species:
                        tag = NameTag.NameCombinationOf(
                            name=tag.name, comment=tag.comment
                        )
        case NameTag.UnjustifiedEmendationOf():
            if (
                nam.group is Group.species
                and nam.root_name != tag.name.root_name
                and _normalize_ii(nam.root_name) == _normalize_ii(tag.name.root_name)
            ):
                yield f"{nam} is marked as an unjustified emendation of {tag.name}, but has a root name that only differs in ii/i or similar"
                return NameTag.IncorrectSubsequentSpellingOf(
                    tag.name, comment=tag.comment
                )
        case NameTag.UnavailableVersionOf():
            if nam.nomenclature_status.can_preoccupy():
                yield "has an UnavailableVersionOf tag, but is available"
            if nam == tag.name:
                yield f"has a tag that points to itself: {tag}"
            if nam.get_date_object() > tag.name.get_date_object():
                yield f"postdates supposed available version {tag.name}"
            if nam.taxon != tag.name.taxon:
                yield f"{nam} is not assigned to the same name as {tag.name}"
            if not tag.name.nomenclature_status.can_preoccupy():
                yield f"senior name {tag.name} is not available"

        case NameTag.Conserved():
            if nam.nomenclature_status not in (
                NomenclatureStatus.available,
                NomenclatureStatus.as_emended,
                NomenclatureStatus.nomen_novum,
                NomenclatureStatus.preoccupied,
                NomenclatureStatus.reranking,
            ):
                yield f"{nam} is on the Official List, but is not marked as available."

        case NameTag.Condition():
            inherent = set(get_inherent_nomenclature_statuses(nam))
            if tag.status in inherent or (
                tag.status is NomenclatureStatus.inconsistently_binominal
                and NomenclatureStatus.placed_on_index in inherent
            ):
                yield f"has redundant Condition tag for {tag.status.name}"
                if not tag.comment:
                    return None
            else:
                statuses_from_tags = get_applicable_nomenclature_statuses_from_tags(
                    nam, exclude_condition=True
                )
                if tag.status in statuses_from_tags:
                    yield f"has Condition tag for {tag.status.name}, but already has a more specific tag"
                    if not tag.comment:
                        return None
                if (
                    tag.status is NomenclatureStatus.infrasubspecific
                    and NomenclatureStatus.variety_or_form in statuses_from_tags
                ):
                    yield "is marked as infrasubspecific, but also as variety or form"
                    if not tag.comment:
                        return None

                if tag.status is NomenclatureStatus.variety_or_form:
                    return NameTag.VarietyOrForm(comment=tag.comment)
                elif tag.status is NomenclatureStatus.not_used_as_valid:
                    return NameTag.NotUsedAsValid(comment=tag.comment)

                if tag.status is NomenclatureStatus.infrasubspecific:
                    possibility = should_be_infrasubspecific(nam)
                    if possibility is Possibility.no:
                        yield "is marked as infrasubspecific, but should not be"

            # TODO: lint against other statuses that have their own tag
        case NameTag.NotUsedAsValid():
            if nam.original_rank is not None and nam.original_rank.is_synonym:
                yield "redundant NotUsedAsValid tag for a synonym"
                if not tag.comment:
                    return None

        case NameTag.VarietyOrForm():
            possibility = should_be_infrasubspecific(nam)
            if possibility is Possibility.no:
                yield "is marked as a variety or form, but should not be"

        case NameTag.NeedsPrioritySelection():
            if nam.has_priority_over(tag.over):
                yield f"is marked as {tag}, but is known to have priority"
            elif tag.over.has_priority_over(nam):
                yield f"is marked as {tag}, but other name is known to have priority"

        case NameTag.SelectionOfPriority():
            # TODO maybe too strict in cases where there's disagreement over dates
            # if nam.get_date_object() != tag.over.get_date_object():
            #    yield f"has a SelectionOfPriority tag, but the date of {nam} ({nam.year}) does not match the date of {tag.over} ({tag.over.year})"
            tag = yield from check_selection_tag(tag, tag.optional_source, cfg, nam)

        case NameTag.SelectionOfSpelling():
            tag = yield from check_selection_tag(tag, tag.optional_source, cfg, nam)

        case NameTag.PermanentlyReplacedSecondaryHomonymOf():
            if tag.replacement_name is None:
                yield f"{nam} is marked as a permanently replaced secondary homonym, but has no replacement name"
            # Skip for now
            # if tag.optional_source is not None and tag.optional_source.numeric_year() >= 1961:
            #     yield f"{nam} is marked as a permanently replaced secondary homonym, but the source is from after 1961"
            tag = yield from check_selection_tag(tag, tag.optional_source, cfg, nam)

        case NameTag.NomenOblitum():
            if tag.comment is not None and (
                match := re.fullmatch(
                    r"(See )?{(?P<source>[^{}]+)} pp?\. (?P<page>\d+(-\d+)?)\.?",
                    tag.comment,
                )
            ):
                source = match.group("source")
                page = match.group("page")
                try:
                    art = Article.select_valid().filter(name=source).get()
                except Article.DoesNotExist:
                    pass
                else:
                    tag = tag.replace(comment=None, page=page, optional_source=art)
            tag = yield from check_selection_tag(tag, tag.optional_source, cfg, nam)

        case NameTag.TakesPriorityOf():
            if tag.optional_source is not None:
                if tag.optional_source.numeric_year() >= 1961:
                    yield f"{nam} is marked as a TakesPriorityOf, but the source is from after 1961"
            if nam.get_date_object() < tag.name.get_date_object():
                yield f"predates name taking priority {tag.name}"
            if tag.is_in_prevailing_usage is None:
                yield f"{tag} must set is_in_prevailing_usage"
            tag = yield from check_selection_tag(tag, tag.optional_source, cfg, nam)

        case NameTag.MappedClassificationEntry():
            return None  # deprecated
        case _:
            return tag
    return tag


type TagChecker = Callable[
    [NameTag, Name, LintConfig], Generator[LintResult, None, NameTag | None]
]

TAG_CHECKERS: list[TagChecker] = [
    _check_preoccupation_tag,
    _check_variant_tag,
    _check_all_tags,
]


@LINT.add("tags")
def check_tags_for_name(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    """Looks at all tags set on names and applies related changes."""
    try:
        tags = nam.tags
    except Exception:
        yield "could not deserialize tags"
        return
    if not tags:
        return

    new_tags = []
    for tag in tags:
        new_tag: NameTag | None = tag
        for checker in TAG_CHECKERS:
            if new_tag is None:
                break
            new_tag = yield from checker(  # static analysis: ignore[not_callable]
                new_tag, nam, cfg
            )
        if new_tag is not None:
            new_tags.append(new_tag)

    if tuple(new_tags) != tags:
        message = f"changing tags from {tags} to {new_tags}"
        getinput.print_diff(tags, new_tags)
        yield field_issue(message, nam, "tags", tuple(new_tags))


def get_applicable_nomenclature_statuses_from_tags(
    nam: Name, *, exclude_condition: bool = False
) -> Iterable[NomenclatureStatus]:
    for tag in nam.tags:
        if not exclude_condition and isinstance(tag, NameTag.Condition):
            # Art. 15.1: Conditional proposals are fine before 1961
            if tag.status is NomenclatureStatus.conditional and nam.year < "1961":
                continue
            yield tag.status
        elif isinstance(tag, PREOCCUPIED_TAGS):
            if (
                isinstance(tag, NameTag.PermanentlyReplacedSecondaryHomonymOf)
                and tag.is_in_use
            ):
                continue
            yield NomenclatureStatus.preoccupied
        elif isinstance(tag, NameTag.VarietyOrForm):
            if has_valid_use_for_variety_or_form(nam):
                continue
            yield NomenclatureStatus.variety_or_form
        elif isinstance(tag, NameTag.NotUsedAsValid):
            yield NomenclatureStatus.not_used_as_valid
        elif type(tag) in TAG_TO_STATUS:
            yield TAG_TO_STATUS[type(tag)]


def has_valid_use_for_variety_or_form(nam: Name) -> bool:
    # ICZN Art. 45.6.4.1: a name originally published as a variety or form before 1961
    # is available from its original date if it was used as valid before 1985
    if nam.numeric_year() < 1961:
        if any(
            isinstance(tag, NameTag.ValidUse) and tag.source.numeric_year() < 1985
            for tag in nam.tags
        ):
            return True
        if any(
            not ce.rank.is_synonym and ce.article.numeric_year() < 1985
            for ce in nam.get_classification_entries()
        ):
            return True

    return False


def has_valid_use(nam: Name) -> bool:
    # ICZN Art. 11.6.1: a name originally published as a synonym before 1961
    # is available from its original date if it was used as valid before 1961
    if nam.numeric_year() < 1961:
        if any(
            isinstance(tag, NameTag.ValidUse) and tag.source.numeric_year() < 1961
            for tag in nam.tags
        ):
            return True
        if any(
            not ce.rank.is_synonym
            and ce.article.numeric_year() < 1961
            and not any(
                isinstance(tag, ClassificationEntryTag.CECondition)
                and tag.status is NomenclatureStatus.not_used_as_valid
                for tag in ce.tags
            )
            for ce in nam.get_classification_entries()
        ):
            return True
    return False


def get_inherent_nomenclature_statuses_from_article(
    art: Article, *, nam: Name | None = None
) -> Iterable[NomenclatureStatus]:
    if art.has_tag(ArticleTag.UnavailableElectronic):
        yield NomenclatureStatus.unpublished_electronic
    if art.has_tag(ArticleTag.InPress):
        yield NomenclatureStatus.unpublished_pending
    if art.type is ArticleType.THESIS:
        yield NomenclatureStatus.unpublished_thesis
    if art.has_tag(ArticleTag.PlacedOnIndex) and not (
        nam is not None
        and (nam.group is Group.high or nam.has_name_tag(NameTag.Conserved))
    ):
        yield NomenclatureStatus.placed_on_index
    if art.has_tag(ArticleTag.InconsistentlyBinominal) and not (
        nam is not None
        and (nam.group is Group.high or nam.has_name_tag(NameTag.Conserved))
    ):
        yield NomenclatureStatus.inconsistently_binominal
    year = art.valid_numeric_year()
    if year is not None and year < 1757:
        yield NomenclatureStatus.before_1758


def get_inherent_nomenclature_statuses(nam: Name) -> Iterable[NomenclatureStatus]:
    if nam.original_citation is not None:
        yield from get_inherent_nomenclature_statuses_from_article(
            nam.original_citation, nam=nam
        )
    # Allow 1757 because of spiders
    elif nam.year is not None and nam.numeric_year() < 1757:
        yield NomenclatureStatus.before_1758
    if nam.original_rank is not None and nam.original_rank.is_synonym:
        yield NomenclatureStatus.not_used_as_valid
    if nam.numeric_year() > 1960 and nam.original_rank in (Rank.variety, Rank.form):
        yield NomenclatureStatus.variety_or_form
    if nam.original_rank in (Rank.aberratio, Rank.morph):
        yield NomenclatureStatus.infrasubspecific


def get_status_priorities() -> dict[NomenclatureStatus, int]:
    status_to_priority = {}
    i = 0
    for statuses in NomenclatureStatus.hierarchy():
        for status in statuses:
            status_to_priority[status] = i
            i += 1
    return status_to_priority


_priority_map = get_status_priorities()


def nomenclature_status_priority(status: NomenclatureStatus) -> int:
    return _priority_map[status]


def sort_nomenclature_statuses(
    statuses: Iterable[NomenclatureStatus],
) -> Sequence[NomenclatureStatus]:
    return sorted(statuses, key=lambda status: _priority_map[status])


def get_applicable_statuses(nam: Name) -> set[NomenclatureStatus]:
    applicable_from_tags = set(get_applicable_nomenclature_statuses_from_tags(nam))
    inherent = set(get_inherent_nomenclature_statuses(nam))
    return applicable_from_tags | inherent


def get_sorted_applicable_statuses(nam: Name) -> Sequence[NomenclatureStatus]:
    return sort_nomenclature_statuses(get_applicable_statuses(nam))


@LINT.add("expected_nomenclature_status")
def check_expected_nomenclature_status(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    """Check if the nomenclature status is as expected."""
    applicable_from_tags = set(get_applicable_nomenclature_statuses_from_tags(nam))
    if (
        NomenclatureStatus.infrasubspecific in applicable_from_tags
        and NomenclatureStatus.variety_or_form in applicable_from_tags
    ):
        yield "has both infrasubspecific and variety/form tags"

    inherent = set(get_inherent_nomenclature_statuses(nam))
    applicable = applicable_from_tags | inherent
    if NomenclatureStatus.not_used_as_valid in applicable:
        if has_valid_use(nam):
            applicable.remove(NomenclatureStatus.not_used_as_valid)
    expected_status = min(
        applicable,
        key=lambda status: _priority_map[status],
        default=NomenclatureStatus.available,
    )

    if nam.nomenclature_status is not expected_status:
        message = (
            f"has status {nam.nomenclature_status.name}, but expected"
            f" {expected_status.name}"
        )
        yield field_issue(message, nam, "nomenclature_status", expected_status)


_REPLACEMENT_TARGET_LINKS = (
    NameTag.NomenNovumFor,
    NameTag.JustifiedEmendationOf,
    NameTag.UnjustifiedEmendationOf,
    NameTag.IncorrectOriginalSpellingOf,
    NameTag.IncorrectSubsequentSpellingOf,
    NameTag.NameCombinationOf,
    NameTag.MandatoryChangeOf,
    NameTag.SubsequentUsageOf,
    NameTag.RerankingOf,
)
_REPLACEMENT_TARGET_LINK_STATUSES = {
    NomenclatureStatus.nomen_novum,
    NomenclatureStatus.justified_emendation,
    NomenclatureStatus.unjustified_emendation,
    NomenclatureStatus.incorrect_original_spelling,
    NomenclatureStatus.incorrect_subsequent_spelling,
    NomenclatureStatus.name_combination,
    NomenclatureStatus.mandatory_change,
    NomenclatureStatus.subsequent_usage,
    NomenclatureStatus.reranking,
}
_AVAILABLE_REPLACEMENT_TARGET_STATUSES = {
    NomenclatureStatus.available,
    NomenclatureStatus.preoccupied,
    NomenclatureStatus.as_emended,
    NomenclatureStatus.partially_suppressed,
    NomenclatureStatus.hybrid_name,
    NomenclatureStatus.art_13_nomen_oblitum,
    NomenclatureStatus.collective_group,
}


@LINT.add("nomen_novum_for_unavailable_name")
def check_nomen_novum_for_unavailable_name(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    """Review replacement targets under Arts. 12.2.3, 13.1.3, and 72.7.

    Availability may follow independently from a description or bibliographic
    reference (Arts. 72.4.2 and 72.4.4). Do not autofix status or type relationships.
    """
    # Retain historical NomenNovumFor tags on names already marked unavailable.
    # Only skip the name being linted; unavailable intermediate names must still
    # be reported when checking a later replacement.
    if nam.nomenclature_status not in (
        _REPLACEMENT_TARGET_LINK_STATUSES
        | _AVAILABLE_REPLACEMENT_TARGET_STATUSES
        | {NomenclatureStatus.variant, NomenclatureStatus.unpublished_pending}
    ):
        return
    targets = {tag.name for tag in nam.get_tags(nam.tags, NameTag.NomenNovumFor)}
    if len(targets) > 1:
        ids = ", ".join(
            str(item.id) for item in sorted(targets, key=lambda item: item.id)
        )
        yield f"has ambiguous NomenNovumFor targets: Names {ids}; review which name was replaced"
        return
    for target in targets:
        yield from _check_nomen_novum_target(nam, target)


def _check_nomen_novum_target(nam: Name, target: Name) -> Iterable[str]:
    chain = [nam]
    seen = {nam}
    while True:
        chain.append(target)
        path = " -> ".join(str(item.id) for item in chain)
        if target in seen:
            yield f"NomenNovumFor chain {path} contains a cycle; cannot establish an available replacement target"
            return
        seen.add(target)
        status = target.nomenclature_status
        label = target.corrected_original_name or target.root_name
        context = f"NomenNovumFor chain {path} reaches {label} ({status.name})"
        if status is NomenclatureStatus.fully_suppressed:
            yield f"{context}; review the Commission decision and its chronology before inferring availability or shared types"
            return
        if status in {
            NomenclatureStatus.variant,
            NomenclatureStatus.unpublished_pending,
        }:
            yield f"{context}; availability is unresolved, so review the publication and relationship before treating it as an available replacement target"
            return
        if (
            status
            not in _REPLACEMENT_TARGET_LINK_STATUSES
            | _AVAILABLE_REPLACEMENT_TARGET_STATUSES
        ):
            yield (
                f"{context}; replacement of a name marked unavailable does not itself establish availability or shared types. "
                "Review the original publication for an independent description, indication, or bibliographic reference and its type basis"
            )
            return
        # Never follow MisidentificationOf or UnavailableVersionOf. In particular,
        # do not assume an intermediate nomen_novum is independently available.
        targets = {
            tag.name
            for tag in target.tags
            if isinstance(tag, _REPLACEMENT_TARGET_LINKS)
        }
        if len(targets) > 1:
            ids = ", ".join(
                str(item.id) for item in sorted(targets, key=lambda item: item.id)
            )
            yield f"{context}; ambiguous replacement/variant links to Names {ids}; review which name was replaced"
            return
        if not targets:
            if status in _REPLACEMENT_TARGET_LINK_STATUSES:
                yield f"{context}; missing replacement/variant link, so cannot establish an available replacement target"
            return
        target = targets.pop()


@dataclass(frozen=True)
class ExpectedName:
    original_name: str | None
    corrected_original_name: str | None
    root_name: str
    base_ce: ClassificationEntry
    ces: list[ClassificationEntry]
    tags: list[NameTag]


@dataclass(frozen=True)
class ExistingVariant:
    name: Name
    has_mapped_ce: bool
    reasons: set[NameTagCons]
    syn_ces: list[ClassificationEntry]


REPLACEABLE_TAGS = (
    NameTag.NameCombinationOf,
    NameTag.IncorrectSubsequentSpellingOf,
    NameTag.RerankingOf,
)
REPLACEABLE_TAGS_SET = set(REPLACEABLE_TAGS)


def _update_replaceable_tags(
    tags: Sequence[NameTag], expected: ExpectedName
) -> list[NameTag] | None:
    new_tags: list[NameTag] = []
    made_change = False
    tags_to_add: set[NameTag] = set(expected.tags)
    for tag in tags:
        if isinstance(tag, REPLACEABLE_TAGS):
            if tag in tags_to_add:
                tags_to_add.remove(tag)
                new_tags.append(tag)
            else:
                for tag_to_add in set(tags_to_add):
                    if (
                        isinstance(tag_to_add, REPLACEABLE_TAGS)
                        and type(tag_to_add) is type(tag)
                        and tag_to_add.name == tag.name
                    ):
                        new_tags.append(tag)
                        tags_to_add.remove(tag_to_add)
                        break
                else:
                    # remove it
                    made_change = True
        else:
            new_tags.append(tag)
    if tags_to_add:
        new_tags.extend(tags_to_add)
        made_change = True
    if made_change:
        return new_tags
    return None


def _get_simplied_normalized_name(nam: Name) -> str | None:
    if nam.original_name is None or nam.corrected_original_name is None:
        return None
    if nam.original_rank is not None and nam.original_rank.is_synonym:
        if nam.original_parent is None:
            return nam.corrected_original_name
        if (
            nam.original_name == nam.root_name
            and nam.corrected_original_name
            == f"{nam.original_parent.root_name} {nam.root_name}"
        ):
            return nam.root_name
    return nam.corrected_original_name


def _get_extended_root_name_forms(nam: Name) -> set[str]:
    root_name_forms = set(nam.get_root_name_forms())
    if nam.corrected_original_name is not None:
        co_root_name = nam.corrected_original_name.split()[-1]
        root_name_forms.add(co_root_name)
    return root_name_forms


@LINT.add("status")
def check_correct_status(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.status.is_base_name() and nam != nam.taxon.base_name:
        yield f"is of status {nam.status!r} and should be base name of {nam.taxon}"
    if (
        nam.status is Status.valid
        and nam.taxon.parent is not None
        and nam.taxon.parent.base_name.status is not Status.valid
    ):
        yield (
            f"is valid, but parent {nam.taxon.parent} is of status"
            f" {nam.taxon.parent.base_name.status.name}"
        )
    if nam.status is Status.unavailable and nam.nomenclature_status in (
        NomenclatureStatus.available,
        NomenclatureStatus.nomen_novum,
        NomenclatureStatus.as_emended,
    ):
        yield (
            f"is marked as unavailable, but nomenclature_status is"
            f" {nam.nomenclature_status.name}"
        )


def _check_names_match(nam: Name, other: Name) -> Iterable[str]:
    if nam.author_tags != other.author_tags:
        yield f"authors do not match {other}"
    if nam.year != other.year:
        yield f"year does not match {other}"
    if nam.original_citation != other.original_citation:
        yield f"original_citation does not match {other}"
    if nam.verbatim_citation != other.verbatim_citation:
        yield f"verbatim_citation does not match {other}"
    if nam.citation_group != other.citation_group:
        yield f"citation_group does not match {other}"


def _check_as_emended_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.nomenclature_status not in (
        NomenclatureStatus.nomen_novum,
        NomenclatureStatus.preoccupied,
        NomenclatureStatus.as_emended,
    ):
        yield "expected status to be as_emended"
    as_emended_target = nam.get_tag_target(NameTag.AsEmendedBy)
    if as_emended_target is None:
        yield "as_emended without an AsEmendedBy tag"
        return
    if as_emended_target.taxon != nam.taxon:
        yield f"target {as_emended_target} does not belong to the same taxon"
    if as_emended_target.root_name != nam.root_name:
        yield (
            f"root name {nam.root_name} does not match target"
            f" {as_emended_target.root_name}"
        )
    if (
        as_emended_target.nomenclature_status
        is not NomenclatureStatus.justified_emendation
        and NomenclatureStatus.justified_emendation
        not in get_applicable_nomenclature_statuses_from_tags(as_emended_target)
    ):
        yield f"target {as_emended_target} is not a justified_emendation"
        return
    original = as_emended_target.get_tag_target(NameTag.JustifiedEmendationOf)
    if original is None:
        yield (
            f"as_emended target {as_emended_target} lacks a justified emendation tag"
        )
        return
    if original != nam:
        yield f"incorrect original spelling traces back to {original}, not this name"


@LINT.add("justified_emendation")
def check_justified_emendations(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Check for issues around justified emendations.

    Justified emendations are complex to handle because they involve multiple Names
    that are coupled together and require a very specific set of tags. See
    docs/name.md for an explanation of how these names should be organized.

    Some of the errors produced by this linter apply to a different name than the
    one that requires the change, or the same lint may be emitted multiple times for
    different names. This is to ensure we don't miss any issues while keeping the
    code relatively simple.

    """
    if nam.nomenclature_status is NomenclatureStatus.as_emended:
        yield from _check_as_emended_name(nam, cfg)
    elif nam.nomenclature_status is NomenclatureStatus.justified_emendation:
        target = nam.get_tag_target(NameTag.JustifiedEmendationOf)
        if target is None:
            yield "justified_emendation without a JustifiedEmendationOf tag"
            return
        if target.taxon != nam.taxon:
            yield f"target {target} does not belong to the same taxon"
        if (
            target.nomenclature_status is NomenclatureStatus.as_emended
            or target.get_tag_target(NameTag.AsEmendedBy)
        ):
            # Now we must have an JE/as_emended pair.
            if (
                target.corrected_original_name != None
                and target.corrected_original_name.split()[-1] == nam.root_name
            ):
                yield (
                    f"supposed incorrect spelling {target} has identical root"
                    f" name {nam.root_name}"
                )
            yield from _check_as_emended_name(target, cfg)
        elif nam.root_name != target.root_name:
            # Else it should be a justified emendation for something straightforward
            # (e.g., removing diacritics), so the root_name should match.
            # But the CON may not match exactly, because the species may have moved genera etc.
            yield f"root name {nam.root_name} does not match emended name {target}"
    elif nam.nomenclature_status is NomenclatureStatus.incorrect_original_spelling:
        ios_target = nam.get_tag_target(NameTag.IncorrectOriginalSpellingOf)
        if ios_target is None:
            yield "missing IncorrectOriginalSpellingOf tag"
            return
        # Incorrect original spellings are used where there are multiple spellings
        # in the original publication, and one is selected as valid. Then both names
        # should have the same author etc.
        yield from _check_names_match(nam, ios_target)


class Possibility(enum.Enum):
    yes = "yes"
    no = "no"
    maybe = "maybe"


def _contains_one_of_words(name: str, words: Iterable[str]) -> bool:
    return bool(re.search(rf"\b({'|'.join(words)})\b", name.lower()))


def _is_variety_or_form(nam: Name) -> bool:
    if nam.original_name is None:
        return False
    return _contains_one_of_words(
        nam.original_name, ("variety", "form", "var", "forma")
    ) or bool(re.search(r"(?<!\. )\b[vf]\. ", nam.original_name))


def should_be_infrasubspecific(nam: Name) -> Possibility:
    if nam.original_name is None or nam.corrected_original_name is None:
        return Possibility.maybe
    num_words = len(nam.corrected_original_name.split())
    match num_words:
        case 2:
            return Possibility.no
        case 3:
            # Art. 45.6.2
            if _contains_one_of_words(
                nam.original_name, ("aberration", "aber", "aberr", "morph")
            ):
                return Possibility.yes
            if nam.numeric_year() > 1960 and _is_variety_or_form(nam):
                return Possibility.yes
            return Possibility.maybe
        case 4:
            return Possibility.yes
        case _:
            return Possibility.maybe
    assert False, "unreachable"


@LINT.add("infrasubspecific")
def check_infrasubspecific(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.group is not Group.species:
        return
    if nam.original_rank in (
        Rank.variety,
        Rank.form,
        Rank.aberratio,
        Rank.morph,
        Rank.subvariety,
        Rank.natio,
    ):
        return
    status = should_be_infrasubspecific(nam)
    if status is not Possibility.yes:
        return
    if _is_variety_or_form(nam):
        if nam.has_name_tag(NameTag.VarietyOrForm):
            return
        message = "should be marked as variety or form"
        yield add_tag_issue(message, nam, NameTag.VarietyOrForm())
    else:
        if any(
            isinstance(tag, NameTag.Condition)
            and tag.status
            in (
                NomenclatureStatus.infrasubspecific,
                NomenclatureStatus.not_published_with_a_generic_name,
            )
            for tag in nam.tags
        ) or nam.has_name_tag(NameTag.VarietyOrForm):
            return
        message = "should be infrasubspecific but is not"
        yield add_tag_issue(
            message, nam, NameTag.Condition(NomenclatureStatus.infrasubspecific)
        )
