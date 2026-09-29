"""Lint steps for Names.

TODOs:
- For species-group names, disallow "other" and "unranked" as original ranks
- Require original_rank for species-group names with an original name

"""

import json
import re
from collections import defaultdict
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import TypeVar

import requests

from taxonomy import adt, getinput
from taxonomy.apis.zoobank import get_zoobank_data
from taxonomy.db import helpers
from taxonomy.db.constants import (
    AgeClass,
    ArticleKind,
    CommentKind,
    DateSource,
    Group,
    NameDataLevel,
    NamingConvention,
    NomenclatureStatus,
    OriginalCitationDataLevel,
    Rank,
    Status,
)
from taxonomy.db.models.article import ArticleTag, PresenceStatus
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import (
    add_tag_issue,
    field_fix,
    field_issue,
    fields_issue,
    fixes_issue,
)
from taxonomy.db.models.lint_types import LintIssue, LintResult
from taxonomy.db.models.name.name import ExperimentalLintCondition as Exp
from taxonomy.db.models.name.name import Name, NameComment, NameTag, TypeTag
from taxonomy.db.models.name.organ import CHECKED_ORGANS
from taxonomy.db.models.person import AuthorTag, PersonLevel
from taxonomy.db.models.taxon import Taxon

from .base import LINT
from .helpers import is_high_or_invalid_family, iter_parents, smallest_common_ancestor

T = TypeVar("T")
ADTT = TypeVar("ADTT", bound=adt.ADT)


def _smallest_high_group_ancestors(taxon: Taxon) -> Iterable[Taxon]:
    for ancestor in iter_parents(taxon):
        if taxon != ancestor:
            yield ancestor
        if ancestor.base_name.group is Group.high:
            return
    raise ValueError(f"{taxon} has no high-group ancestor")


@LINT.add("allocation")
def check_allocation(nam: Name, cfg: LintConfig) -> Iterable[str]:
    tag = nam.get_type_tag(TypeTag.TreatAsEquivalentTo)
    if tag is not None:
        if nam.taxon != tag.name.taxon:
            yield f"should be applied to {tag.name.taxon}, not {nam.taxon} (TreatAsEquivalentTo tag)"
        return
    if not is_high_or_invalid_family(nam):
        return
    if (
        nam.nomenclature_status is NomenclatureStatus.incorrect_subsequent_spelling
        or nam.nomenclature_status is NomenclatureStatus.nomen_novum
    ):
        return
    ce = nam.get_mapped_classification_entry()
    if ce is None:
        return
    if nam.has_type_tag(TypeTag.PhylogeneticDefinition):
        return
    children = list(ce.get_children())
    if not children:
        return
    child_names = [
        child.mapped_name for child in children if child.mapped_name is not None
    ]
    if not child_names:
        return
    ancestor = smallest_common_ancestor(child_names)
    allowed_taxa = [ancestor]
    if ancestor.base_name.group is not Group.high:
        allowed_taxa.extend(_smallest_high_group_ancestors(ancestor))
    if nam.taxon not in allowed_taxa:
        if len(allowed_taxa) == 1:
            allowed = str(allowed_taxa[0])
        else:
            allowed = " or ".join(str(taxon) for taxon in allowed_taxa)
        yield f"should be applied to {allowed}, not {nam.taxon} (based on allocation of child names {child_names})"


@LINT.add("dedupe_tags")
def dedupe_and_sort_tags(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    original_tags = list(nam.type_tags)
    organ_tags_to_merge = defaultdict(list)
    all_tags = set()
    for tag in nam.type_tags:
        if (
            isinstance(tag, TypeTag.Organ)
            and tag.organ in CHECKED_ORGANS
            and not tag.condition
        ):
            organ_tags_to_merge[tag.organ].append(tag)
        else:
            all_tags.add(tag)
    for organ, group in organ_tags_to_merge.items():
        detail = []
        for tag in group:
            if tag.detail:
                detail.append(tag.detail)
        all_tags.add(TypeTag.Organ(organ, detail=", ".join(detail) if detail else None))
    tags = sorted(all_tags)
    if tags != original_tags:
        if set(tags) != set(original_tags):
            print(f"changing tags for {nam}")
            getinput.print_diff(sorted(original_tags), tags)
        yield field_issue(
            "deduplicate and sort type tags", nam, "type_tags", tuple(tags)
        )


@LINT.add("redundant_fields")
def check_redundant_fields(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.nomenclature_status is not NomenclatureStatus.nomen_novum:
        return
    parent = nam.get_tag_target(NameTag.NomenNovumFor)
    for field in ("type_locality", "type_specimen", "collection", "species_type_kind"):
        value = getattr(nam, field)
        if value is None:
            continue
        can_autofix = parent is not None and getattr(parent, field) == value
        message = f"is a nomen novum and should not have the {field} field set ({can_autofix})"
        if can_autofix:
            yield field_issue(f"{message} (setting value to None)", nam, field, None)
        else:
            yield message


# disabled because it keeps timing out
@LINT.add("lsid", requires_network=True, disabled=True)
def check_for_lsid(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    # ICZN Art. 8.5.1: ZooBank is relevant to availability only starting in 2012
    if (
        nam.numeric_year() < 2012
        or nam.corrected_original_name is None
        or nam.original_citation is None
        or nam.nomenclature_status
        in (
            NomenclatureStatus.incorrect_subsequent_spelling,
            NomenclatureStatus.name_combination,
        )
        # Searching for this name consistently times out. The type
        # species (Lyra sherkana) is not in ZooBank, so probably the genus
        # isn't either.
        or nam.corrected_original_name == "Lyra"
    ):
        return
    try:
        zoobank_data_list = get_zoobank_data(nam.corrected_original_name)
    except requests.exceptions.HTTPError as e:
        print(f"Error retrieving ZooBank data: {e!r}")
        return
    if not zoobank_data_list:
        return
    type_tags = []
    art_tags = []
    art = nam.original_citation
    name_lsids = {tag.text for tag in nam.get_tags(nam.type_tags, TypeTag.LSID)}
    rejected_lsids = {
        tag.text for tag in nam.get_tags(nam.type_tags, TypeTag.RejectedLSID)
    }
    art_lsids = {tag.text for tag in art.get_tags(art.tags, ArticleTag.LSID)}
    for zoobank_data in zoobank_data_list:
        if zoobank_data.name_lsid in rejected_lsids:
            continue
        if zoobank_data.name_lsid not in name_lsids:
            type_tags.append(TypeTag.LSID(zoobank_data.name_lsid))
        if zoobank_data.citation_lsid and zoobank_data.citation_lsid not in art_lsids:
            art_tag = ArticleTag.LSID(
                zoobank_data.citation_lsid,
                present_in_article=PresenceStatus.to_be_determined,
            )
            art_tags.append(art_tag)
    if not type_tags and not art_tags:
        return
    for tag in type_tags:
        yield add_tag_issue(
            f"inferred ZooBank name LSID: {tag}", nam, tag, field="type_tags"
        )
    for tag in art_tags:
        yield add_tag_issue(f"inferred ZooBank article LSID: {tag}", art, tag)


@LINT.add("year")
def check_year(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.year is None:
        return
    if not helpers.is_valid_date(nam.year):
        yield f"has invalid year {nam.year!r}"
    if helpers.is_date_range(nam.year):
        yield "year is a range"


@LINT.add("year_matches")
def check_year_matches(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.original_citation is None:
        return

    if nam.year != nam.original_citation.year:
        if helpers.is_more_specific_date(nam.original_citation.year, nam.year):
            yield field_issue(
                f"fixing date {nam.year} -> {nam.original_citation.year}",
                nam,
                "year",
                nam.original_citation.year,
            )
        else:
            yield (
                f"year mismatch: {nam.year} (name) vs."
                f" {nam.original_citation.year} (article)"
            )


ATTRIBUTES_BY_GROUP = {
    "name_complex": (Group.genus,),
    "species_name_complex": (Group.species,),
    "type": (Group.family, Group.genus),
    "type_locality": (Group.species,),
    "type_specimen": (Group.species,),
    "collection": (Group.species,),
    "genus_type_kind": (Group.genus,),
    "species_type_kind": (Group.species,),
}


TYPE_TAGS_BY_GROUP: dict[type[TypeTag], tuple[Group, ...]] = {
    **dict.fromkeys(
        (
            TypeTag.Date,
            TypeTag.Gender,
            TypeTag.Age,
            TypeTag.Organ,
            TypeTag.Altitude,
            TypeTag.Coordinates,
            TypeTag.TypeLocality,
            TypeTag.StratigraphyDetail,
            TypeTag.Habitat,
            TypeTag.Host,
            TypeTag.LectotypeDesignation,
            TypeTag.NeotypeDesignation,
            TypeTag.SpecimenDetail,
            TypeTag.LocationDetail,
            TypeTag.Repository,
            TypeTag.ProbableRepository,
            TypeTag.CollectionDetail,
            TypeTag.CollectedBy,
            TypeTag.Involved,
            TypeTag.NoLocation,
            TypeTag.NoSpecimen,
            TypeTag.NoDate,
            TypeTag.NoCollector,
            TypeTag.NoOrgan,
            TypeTag.NoGender,
            TypeTag.NoAge,
            TypeTag.ImpreciseLocality,
            TypeTag.TypeSpecimenLink,
            TypeTag.FormerRepository,
            TypeTag.ExtraRepository,
            TypeTag.FutureRepository,
            TypeTag.TypeSpecimenLinkFor,
            TypeTag.GuessedRepository,
            TypeTag.InterpretedTypeLocality,
            TypeTag.InterpretedTypeSpecimen,
            TypeTag.AdditionalTypeSpecimen,
            TypeTag.OriginalTypification,
            TypeTag.PartialTypeLocality,
            TypeTag.TypeLocalityValidity,
        ),
        (Group.species,),
    ),
    **dict.fromkeys(
        (
            TypeTag.TypeDesignation,
            TypeTag.IncludedSpecies,
            TypeTag.GenusCoelebs,
            TypeTag.TypeSpeciesDetail,
            type(TypeTag.NoOriginalParent),
        ),
        (Group.genus,),
    ),
    TypeTag.CommissionTypeDesignation: (Group.genus, Group.family),
    TypeTag.InterpretedTypeTaxon: (Group.genus, Group.family),
}


@LINT.add("disallowed_attributes")
def check_disallowed_attributes(nam: Name, cfg: LintConfig) -> Iterable[str]:
    for field_name, groups in ATTRIBUTES_BY_GROUP.items():
        if nam.group not in groups:
            value = getattr(nam, field_name)
            if value is not None:
                yield f"should not have attribute {field_name} (value {value})"
    for tag in nam.type_tags:
        tag_groups = TYPE_TAGS_BY_GROUP.get(type(tag))
        if tag_groups is not None and nam.group not in tag_groups:
            allowed = ", ".join(group.name for group in tag_groups)
            yield (
                f"should not have type tag {type(tag).__name__} in group "
                f"{nam.group.name} (allowed groups: {allowed})"
            )


@LINT.add("autoset_original_rank")
def autoset_original_rank(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.original_rank is not None:
        return
    inferred = nam.infer_original_rank()
    if inferred is not None:
        yield field_issue(
            f"inferred original_rank to be {inferred!r} from {nam.original_name!r}",
            nam,
            "original_rank",
            inferred,
        )


@LINT.add("data_level")
def check_data_level(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not nam.taxon.is_high_coverage():
        return
    ocdl, ocdl_reason = nam.original_citation_data_level()
    ndl, ndl_reason = nam.name_data_level()
    match ocdl:
        case OriginalCitationDataLevel.no_citation:
            pass
        case OriginalCitationDataLevel.no_data:
            if ndl < NameDataLevel.missing_details_tags:
                yield f"has no data from original ({ocdl_reason}), but missing important fields: {ndl_reason}"
        case OriginalCitationDataLevel.some_data:
            if Exp(above_article_id=69_000).should_apply(nam, cfg):
                yield f"recent name has incomplete data from original ({ocdl_reason})"
            if ndl is NameDataLevel.missing_crucial_fields:
                yield f"has some data from original ({ocdl_reason}), but missing crucial data: {ndl_reason}"
        case OriginalCitationDataLevel.all_required_data:
            if ndl is NameDataLevel.missing_crucial_fields:
                yield f"has data from original, but missing crucial data: {ndl_reason}"


@LINT.add("citation_group")
def check_citation_group(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.citation_group is None or nam.year is None:
        return
    if message := nam.citation_group.is_year_in_range(nam.numeric_year()):
        yield message


_JG2015 = "{Mammalia Australia (Jackson & Groves 2015).pdf}"
_JG2015_RE = re.compile(rf"\[From {re.escape(_JG2015)}: [^\[\]]+ \[([A-Za-z\s\d]+)\]\]")
_JG2015_RE2 = re.compile(rf" \[([A-Za-z\s\d]+)\]\ \[from {re.escape(_JG2015)}\]")


@LINT.add("extract_date_from_verbatim")
def extract_date_from_verbatim(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.original_citation is None or not nam.data:
        return
    # Extract precise dates from references from Jackson & Groves (2015). This sometimes
    # produces incorrect results if the names aren't associated correctly. In that case,
    # edit the name that produced the bad tag to put "[error]" in its data field so the
    # regexes above don't match.
    try:
        verbatim = nam.get_data("verbatim_citation")
    except KeyError, TypeError, json.JSONDecodeError:
        return
    if isinstance(verbatim, str):
        verbatim = [verbatim]
    for option in verbatim:
        for regex in _JG2015_RE, _JG2015_RE2:
            match = regex.search(option)
            if not match:
                continue
            date = match.group(1)
            parsed = parse_date(date)
            if parsed is None:
                if not date.startswith("Published before "):
                    yield f"cannot parse date: {verbatim}"
            else:
                yield from _maybe_add_publication_date(nam, parsed, date, _JG2015, cfg)


def _maybe_add_publication_date(
    nam: Name, parsed: str, raw_date: str, source: str, cfg: LintConfig
) -> Iterator[LintResult]:
    article = nam.original_citation
    if article is None:
        return
    tag = ArticleTag.PublicationDate(
        source=DateSource.external, date=parsed, comment=f'"{raw_date}" {source}'
    )
    if tag in (article.tags or ()):
        return
    message = f'inferred date for {article} from raw date "{raw_date}": {parsed}'
    yield add_tag_issue(message, article, tag)


USNM_RGX = re.compile(
    r"""
    ,\s(ordered\spublished\s)?
    ((?P<day>\d{1,2})\s)?
    ((?P<month>[A-Z][a-z]+)\s)?
    (?P<year>\d{4})[a-z]?\.(\s|$)
    """,
    re.VERBOSE,
)
AMNH_RGX = re.compile(
    r"""
    ^([A-Za-z\.,"\s]+)\s\d+:\s?\d+(-\d+)?,\s
    (?P<month>[A-Z][a-z]+)\.?\s
    ((?P<day>\d+),\s?)?
    (?P<year>\d{4})\.
    """,
    re.VERBOSE,
)


@LINT.add("extract_date_from_structured_quote")
def extract_date_from_structured_quotes(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    if not nam.original_citation:
        return
    for comment in nam.comments.filter(
        NameComment.source.is_in(
            (
                34056,  # {Mammalia-USNM types (Fisher & Ludwig 2015).pdf}
                33916,  # {Anomaluromorpha, Hystricomorpha, Myomorpha-USNM types.pdf}
                29833,  # {Ferungulata-USNM types.pdf}
                15513,  # {Castorimorpha, Sciuromorpha-USNM types.pdf}
                9585,  # Mammalia-AMNH types (Lawrence 1993).pdf
            )
        ),
        NameComment.kind == CommentKind.structured_quote,
    ):
        if any(
            tag.comment is not None and comment.source.name in tag.comment
            for tag in nam.original_citation.get_tags(
                nam.original_citation.tags, ArticleTag.PublicationDate
            )
        ):
            continue

        cite = json.loads(comment.text)["verbatim_citation"]
        if comment.source.id == 9585:
            match = AMNH_RGX.search(cite)
        else:
            match = USNM_RGX.search(cite)
        if not match:
            if comment.source.id != 9585:
                yield (
                    f"cannot match verbatim citation (ref {nam.original_citation}):"
                    f" {cite!r}"
                )
            continue
        try:
            date = helpers.parse_date(
                match.group("year"), match.group("month"), match.group("day")
            )
        except ValueError as e:
            yield f"invalid date in {cite!r}: {e}"
            continue
        yield from _maybe_add_publication_date(
            nam, date, cite, f"{{{comment.source.name}}}", cfg
        )


def parse_date(date_str: str) -> str | None:
    for month in ("%b", "%B"):
        try:
            dt = datetime.strptime(date_str, f"{month} %Y").astimezone(UTC)
        except ValueError:
            pass
        else:
            return f"{dt.year}-{dt.month:02d}"
        for prefix in ("", "0"):
            try:
                dt = datetime.strptime(date_str, f"{prefix}%d {month} %Y").astimezone(
                    UTC
                )
            except ValueError:
                pass
            else:
                return f"{dt.year}-{dt.month:02d}-{dt.day:02d}"
    return None


@LINT.add("data")
def check_data(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not nam.data:
        return
    try:
        data = json.loads(nam.data)
    except json.JSONDecodeError:
        yield f"invalid data field: {nam.data!r}"
        return
    if not isinstance(data, dict):
        yield f"invalid data field: {nam.data!r}"
        return
    if "verbatim_citation" in data:
        if not isinstance(data["verbatim_citation"], (str, list)):
            yield f"invalid verbatim_citation data: {data['verbatim_citation']}"


@LINT.add("specific_authors")
def check_specific_authors(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not nam.original_citation and not nam.verbatim_citation:
        return
    for i, author in enumerate(nam.get_authors()):
        if (
            author.get_level() is not PersonLevel.family_name_only
            or author.naming_convention is not NamingConvention.unspecified
        ):
            continue
        if nam.original_citation is not None:
            yield (
                "has original citation, but has family name-only author"
                f" {author} (position {i})"
            )
            if cfg.interactive and not LINT.is_ignoring_lint(nam, "specific_authors"):
                author.edit_tag_sequence_on_object(
                    nam, "author_tags", AuthorTag.Author, "names"
                )
        elif nam.verbatim_citation is not None and helpers.simplify_string(
            author.family_name
        ) in helpers.simplify_string(nam.verbatim_citation):
            yield f"author {author} (position {i}) appears in verbatim citation"
            if cfg.interactive and not LINT.is_ignoring_lint(nam, "specific_authors"):
                author.edit_tag_sequence_on_object(
                    nam, "author_tags", AuthorTag.Author, "names"
                )


def has_accessible_original_citation(nam: Name) -> bool:
    return (
        nam.original_citation is not None
        and nam.original_citation.kind is not ArticleKind.no_copy
        and not nam.original_citation.has_tag(ArticleTag.NonOriginal)
    )


@LINT.add("required_fields")
def check_required_fields(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.verbatim_citation and not nam.citation_group:
        yield "has verbatim citation but no citation group"
    if has_accessible_original_citation(nam):
        citation = nam.original_citation
        assert citation is not None
        if nam.page_described is None and "page_described" in nam.get_required_fields():
            yield "has original citation but no page_described"
        if nam.original_name is None:
            yield "has original citation but no original_name"
        if nam.original_rank is None:
            yield "has original citation but no original_rank"
        if nam.author_tags is None:
            message = "has original citation but no author_tags"
            if citation.issupplement() and citation.parent is not None:
                authors = citation.parent.author_tags
            else:
                authors = citation.author_tags
            if authors is None:
                yield message
            else:
                yield field_issue(message, nam, "author_tags", authors)
        if nam.year is None:
            message = "has original citation but no year"
            yield field_issue(message, nam, "year", citation.year)
        if (
            nam.name_complex is None
            and nam.group is Group.genus
            and nam.nomenclature_status.requires_name_complex()
        ):
            yield "genus-group name with no name complex"
        if (
            nam.species_name_complex is None
            and nam.group is Group.species
            and nam.nomenclature_status.requires_name_complex()
        ):
            yield "species-group name with no species name complex"
    must_have_verbatim_reason = must_have_verbatim(nam)
    if must_have_verbatim_reason is not None:
        yield f"{must_have_verbatim_reason} must have verbatim citation"
    if nam.species_type_kind is None and nam.nomenclature_status.requires_type():
        if nam.type_specimen is not None:
            yield "has type_specimen but no species_type_kind"
        if nam.has_type_tag(TypeTag.Age) or nam.has_type_tag(TypeTag.Gender):
            yield "has type specimen age or gender but no species_type_kind"


def must_have_verbatim(nam: Name) -> str | None:
    if nam.original_citation is not None:
        return None
    if nam.verbatim_citation is not None:
        return None
    if nam.numeric_year() > 1970:
        return "recent name"
    if nam.nomenclature_status is NomenclatureStatus.subsequent_usage:
        return "subsequent usage"
    return None


@LINT.add("derived_tags")
def check_required_derived_tags(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not has_accessible_original_citation(nam) or not nam.taxon.is_high_coverage():
        return
    for tags, exp in nam.get_required_derived_tags():
        if not any(nam.has_type_tag(tag) for tag in tags) and exp.should_apply(
            nam, cfg
        ):
            yield f"missing required derived tags {tags}"


@LINT.add("synonym_group")
def check_synonym_group(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group not in (Group.genus, Group.species):
        return
    # Unavailable genus-group names may be synonyms of their parent taxon
    if (
        nam.group is Group.genus
        and not nam.can_be_valid_base_name()
        and nam.taxon.base_name.group is not Group.species
    ):
        return
    if nam.taxon.base_name.group is not nam.group:
        yield (
            f"taxon is of group {nam.taxon.base_name.group.name} but name is of group"
            f" {nam.group.name}"
        )


@LINT.add("composites")
def check_composites(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.status not in (Status.composite, Status.hybrid):
        return
    children = list(nam.taxon.get_children())
    if children:
        different_status = [
            child for child in children if child.base_name.status is not nam.status
        ]
        if different_status:
            yield (
                f"is of status {nam.status}, but has children of different status"
                f" {different_status}"
            )
    else:
        tags = list(nam.get_tags(nam.type_tags, TypeTag.PartialTaxon))
        if len(tags) < 2:
            yield (
                f"is of status {nam.status} and must have at least two PartialTaxon"
                f" tags (got {tags})"
            )


@LINT.add("move_to_lowest_rank")
def move_to_lowest_rank(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    query = Taxon.select_valid().filter(Taxon.base_name == nam)
    if query.count() < 2:
        return
    if nam.group is Group.high:
        yield "high-group names cannot be the base name of multiple taxa"
        return
    lowest, *ts = sorted(query, key=lambda t: t.rank)
    last_seen = lowest
    for t in ts:
        while last_seen is not None and last_seen != t:
            last_seen = last_seen.parent
        if last_seen is None:
            yield f"taxon {t} is not a parent of {lowest}"
            break
    if last_seen is None:
        return
    if nam.taxon != lowest:
        message = f"changing taxon of {nam} to {lowest}"
        yield field_issue(message, nam, "taxon", lowest)


@LINT.add_duplicate_finder(
    "duplicate_genus", query=Name.select_valid().filter(Name.group == Group.genus)
)
def duplicate_genus(name: Name) -> str:
    if name.original_citation is not None:
        citation = name.original_citation.name
    elif name.citation_group is not None:
        citation = name.citation_group.name
    else:
        citation = ""
    return f"{name.root_name} {name.taxonomic_authority()}, {name.year}, {citation}"


def _merge_name_issue(message: str, name: Name, target: Name) -> LintIssue:
    if name.status not in (Status.synonym, Status.dubious):
        return LintIssue(
            f"{message}; cannot autofix a Name with status {name.status.name}"
        )
    fixes = []
    for field in Name.fields():
        if field in {"id", "tags"}:
            continue
        source_value = getattr(name, field)
        target_value = getattr(target, field)
        if field == "type_tags" and source_value and target_value:
            merged_tags = (
                *target_value,
                *(
                    tag
                    for tag in source_value
                    if not isinstance(tag, TypeTag.AuthorityPageLink)
                ),
            )
            if merged_tags != tuple(target_value):
                fixes.append(field_fix(target, field, merged_tags))
        elif source_value not in (None, ()) and target_value in (None, ()):
            fixes.append(field_fix(target, field, source_value))
    return fixes_issue(
        message,
        *fixes,
        field_fix(name, "status", Status.redirect),
        field_fix(name, "target", target),
    )


def remove_duplicate_name(key: object, name: Name, others: list[Name]) -> LintResult:
    target = min(others, key=lambda candidate: candidate.id)
    message = f"merge duplicate {name} into {target} (key {key!r})"
    return _merge_name_issue(message, name, target)


@LINT.add_duplicate_finder(
    "duplicate_name",
    query=Name.select_valid().filter(Name.original_citation != None),
    fixer=remove_duplicate_name,
)
def duplicate_name(name: Name) -> tuple[object, ...]:
    assert name.original_citation is not None
    return (
        name.original_citation.id,
        name.original_name,
        name.corrected_original_name,
        name.page_described,
        name.original_rank,
    )


@LINT.add("original_rank")
def check_original_rank(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.original_rank is None:
        return
    if nam.original_rank is Rank.synonym:
        new_rank = helpers.GROUP_TO_SYNONYM_RANK[nam.group]
        message = f"changing original rank to {new_rank!r}"
        yield field_issue(message, nam, "original_rank", new_rank)
        return
    if nam.original_rank is Rank.other:
        if nam.group is Group.family:
            message = f"changing original rank to {Rank.other_family!r}"
            yield field_issue(message, nam, "original_rank", Rank.other_family)
            return
        elif nam.group is Group.genus:
            message = f"changing original rank to {Rank.other_subgeneric!r}"
            yield field_issue(message, nam, "original_rank", Rank.other_subgeneric)
            return
    if (
        nam.original_rank is Rank.informal
        and nam.group is Group.high
        and nam.taxon.group() is Group.species
    ):
        message = f"changing original rank to {Rank.informal_species!r} and group to {Group.species!r}"
        yield fields_issue(
            message,
            (nam, "original_rank", Rank.informal_species),
            (nam, "group", Group.species),
        )
    if (
        nam.corrected_original_name is not None
        and nam.nomenclature_status
        is not NomenclatureStatus.not_intended_as_a_scientific_name
    ):
        if nam.original_rank is Rank.species:
            num_words = len(nam.corrected_original_name.split())
            if num_words != 2:
                yield f"original rank {nam.original_rank!r} but corrected original name has {num_words} words"
        if nam.original_rank is Rank.subspecies:
            num_words = len(nam.corrected_original_name.split())
            if num_words != 3:
                yield f"original rank {nam.original_rank!r} but corrected original name has {num_words} words"
    group = helpers.group_of_rank(nam.original_rank)
    if group is not nam.group:
        yield f"original rank {nam.original_rank!r} is not in group {nam.group!r}"


@LINT.add("has_parent_species")
def check_has_parent_species(nam: Name, cfg: LintConfig) -> Iterable[str]:
    # The remaining known gaps are from 1840 through 1950. Apply this lint to
    # the completed ranges and to newly added names while that interval is
    # being worked through.
    if not Exp(before_year=1839, after_year=1935).should_apply(nam, cfg):
        return
    if nam.nomenclature_status in (
        NomenclatureStatus.not_intended_as_a_scientific_name,
        NomenclatureStatus.inconsistently_binominal,
    ):
        return
    art = nam.original_citation
    if art is None:
        return
    if not nam.taxon.is_high_coverage():
        return
    if nam.taxon.age is not AgeClass.extant:
        return
    if (
        nam.corrected_original_name is None
        or nam.corrected_original_name.count(" ") != 2
    ):
        return
    gen, sp, _ssp = nam.corrected_original_name.split(" ")
    species_name = f"{gen} {sp}"
    # TODO: also check that parent species is older than this subspecies
    existing = (
        Name.select_valid()
        .filter(
            Name.corrected_original_name == species_name,
            Name.original_rank == Rank.species,
        )
        .count()
    )
    if existing > 0:
        return
    yield f"missing parent species {species_name} for {nam}"
    if cfg.manual_mode:
        getinput.print_header(nam)
        art.display_names()
        print(f"{art} is the original citation of {nam}")
        art.edit()
        art.lint_object_list(art.new_names)
        art.lint_object_list(art.classification_entries)


@LINT.add("infer_reranking")
def infer_reranking(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.group is not Group.family:
        return
    if nam.type is None or nam.year is None or nam.original_name is None:
        return
    if nam.has_name_tag(NameTag.RerankingOf):
        return
    if not nam.can_be_valid_base_name():
        return
    my_type = nam.type.resolve_variant(misidentification=True)
    candidates = [
        other_nam
        for other_nam in nam.taxon.get_names().filter(Name.group == Group.family)
        if other_nam.type is not None
        and other_nam.type.resolve_variant(misidentification=True) == my_type
        and other_nam.can_be_valid_base_name()
        and other_nam != nam
        and other_nam.year is not None
    ]
    if not candidates:
        return
    best_candidate = min(
        candidates,
        key=lambda other_nam: (
            other_nam.get_date_object(),
            (
                -other_nam.original_rank.comparison_value
                if other_nam.original_rank is not None
                else 0
            ),
        ),
    )
    if best_candidate.get_date_object() >= nam.get_date_object():
        return
    tag = NameTag.RerankingOf(best_candidate)
    message = f"add RerankingOf tag: {tag}"
    yield add_tag_issue(message, nam, tag)


@LINT.add("must_have")
def enforce_must_have(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.citation_group is None:
        return
    if nam.original_citation is not None:
        return
    if nam.citation_group.must_have(nam.numeric_year()):
        yield f"{nam} is in {nam.citation_group}, but has no original_citation"
