"""Lint steps for Names.

TODOs:
- For species-group names, disallow "other" and "unranked" as original ranks
- Require original_rank for species-group names with an original name

"""

from __future__ import annotations

import enum
import functools
import itertools
import json
import pprint
import re
import subprocess
from collections import defaultdict
from collections.abc import Callable, Container, Generator, Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import cache
from typing import Generic, Protocol, Self, TypeVar, assert_never

import clirm
import Levenshtein
import requests

from taxonomy import adt, coordinates, getinput, urlparse
from taxonomy.apis import bhl, nominatim
from taxonomy.apis.zoobank import clean_lsid, get_zoobank_data, is_valid_lsid
from taxonomy.config import is_network_available
from taxonomy.db import helpers, models
from taxonomy.db.constants import (
    AgeClass,
    ArticleKind,
    ArticleType,
    CommentKind,
    DateSource,
    GenderArticle,
    Group,
    NameDataLevel,
    NamingConvention,
    NomenclatureStatus,
    OriginalCitationDataLevel,
    PhylogeneticDefinitionType,
    Rank,
    RegionKind,
    SpeciesBasis,
    SpeciesGroupType,
    SpeciesNameKind,
    SpecimenOrgan,
    Status,
    TypeSpeciesDesignation,
)
from taxonomy.db.models.article import Article, ArticleTag, PresenceStatus
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.classification_entry.ce import (
    ClassificationEntry,
    ClassificationEntryTag,
)
from taxonomy.db.models.collection import (
    BMNH_COLLECTION,
    MULTIPLE_COLLECTION,
    Collection,
)
from taxonomy.db.models.lint import IgnoreLint, Lint
from taxonomy.db.models.name_complex import (
    NameComplex,
    NameEnding,
    SpeciesNameComplex,
    SpeciesNameEnding,
    normalize_root_name_for_homonymy,
)
from taxonomy.db.models.person import AuthorTag, PersonLevel
from taxonomy.db.models.taxon import Taxon

from .guess_repository import get_most_likely_repository
from .name import (
    PREOCCUPIED_TAGS,
    STATUS_TO_TAG,
    LectotypeDesignationTerm,
    Name,
    NameComment,
    NameTag,
    NameTagCons,
    SelectionReason,
    TypeTag,
)
from .organ import CHECKED_ORGANS, ParsedOrgan, ParseException, parse_organ_detail
from .page import check_page, get_unique_page_text, parse_page_text
from .type_specimen import (
    BaseSpecimen,
    InformalSpecimen,
    InformalWithoutInstitution,
    SimpleSpecimen,
    SpecialSpecimen,
    Specimen,
    SpecimenRange,
    TripletSpecimen,
    parse_type_specimen,
    stringify_specimen_list,
    type_specimens_equal,
)

T = TypeVar("T")
ADTT = TypeVar("ADTT", bound=adt.ADT)


def remove_unused_ignores(nam: Name, unused: Container[str]) -> None:
    new_tags = []
    for tag in nam.type_tags:
        if isinstance(tag, TypeTag.IgnoreLintName) and tag.label in unused:
            print(f"{nam}: removing unused IgnoreLint tag: {tag}")
        else:
            new_tags.append(tag)
    nam.type_tags = new_tags  # type: ignore[assignment]


def get_ignores(nam: Name) -> Iterable[IgnoreLint]:
    return nam.get_tags(nam.type_tags, TypeTag.IgnoreLintName)


LINT = Lint(Name, get_ignores, remove_unused_ignores)


def replace_arg(tag: ADTT, arg: str, val: object) -> ADTT:
    kwargs = {**tag.__dict__, arg: val}
    return type(tag)(**kwargs)


def get_tag_fields_of_type(tag: adt.ADT, typ: type[T]) -> Iterable[tuple[str, T]]:
    tag_type = type(tag)
    for arg_name, arg_type in tag_type._attributes.items():
        if arg_type is typ:
            if (val := getattr(tag, arg_name)) is None:
                continue
            yield arg_name, val


UNIQUE_TAGS = (
    TypeTag.Altitude,
    TypeTag.Coordinates,
    TypeTag.Date,
    TypeTag.Gender,
    TypeTag.Age,
    TypeTag.DifferentAuthority,
    TypeTag.TextualOriginalRank,
    TypeTag.GenusCoelebs,
    TypeTag.TreatAsEquivalentTo,
    TypeTag.PhyloCodeNumber,
    TypeTag.PhylogeneticDefinition,
)
ORGAN_REPLACEMENTS = [
    (r"^both$", r"L, R"),
    (r"(^|, )partial ([LR])(?=$|, )", r"\1\2 part"),
    (r"(^|, )(proximal|distal|shaft|part) ([LR])(?=$|, )", r"\1\3 \2"),
    (r"^partial$", "part"),
    (r"^(L |R )?fragments$", r"\1parts"),
    (r"^(L |R )?fragment$", r"\1part"),
    (r"^(multiple|several|many|vertebrae)$", ">1"),
]


def specify_organ(
    organ: SpecimenOrgan, detail: str
) -> tuple[SpecimenOrgan, str | None] | None:
    if organ is SpecimenOrgan.pes:
        if detail == "phalanges":
            return SpecimenOrgan.phalanx_pes, ">1"
        elif detail == "phalanx":
            return SpecimenOrgan.phalanx_pes, "1"
        elif re.fullmatch(r"^(L |R )?Mt(I|II|III|IV|V)$", detail):
            return SpecimenOrgan.metatarsal, detail
        elif detail == "metatarsals":
            return SpecimenOrgan.metatarsal, ">1"
        elif detail == "metatarsal":
            return SpecimenOrgan.metatarsal, "1"
    elif organ is SpecimenOrgan.manus:
        if detail == "phalanges":
            return SpecimenOrgan.phalanx_manus, ">1"
        elif detail == "phalanx":
            return SpecimenOrgan.phalanx_manus, "1"
        elif re.fullmatch(r"^(L |R )?Mc(I|II|III|IV|V)$", detail):
            return SpecimenOrgan.metacarpal, detail
        elif detail == "metacarpals":
            return SpecimenOrgan.metacarpal, ">1"
        elif detail == "metacarpal":
            return SpecimenOrgan.metacarpal, "1"
    if organ is not SpecimenOrgan.in_alcohol:
        if detail.startswith(("L ", "R ")):
            new_detail, text = detail.split(" ", maxsplit=1)
        else:
            new_detail = None
            text = detail
        try:
            new_organ = SpecimenOrgan[text]
        except KeyError:
            pass
        else:
            return new_organ, new_detail
    if match := re.fullmatch(r"partial ([a-z]+)", detail):
        return organ, f"{match.group(1)} part"
    if match := re.fullmatch(r"([a-z]+) fragments", detail):
        return organ, f"{match.group(1)} parts"
    if match := re.fullmatch(r"([a-z]+) fragment", detail):
        return organ, f"{match.group(1)} part"
    if detail == "fragments":
        return organ, "parts"
    if detail == "fragment":
        return organ, "part"
    return None


def maybe_replace_tags(
    organ: SpecimenOrgan, detail: str, condition: str | None
) -> tuple[list[TypeTag.Organ], list[str]]:  # type: ignore[name-defined]
    new_detail: str | None
    if organ in (SpecimenOrgan.mandible, SpecimenOrgan.dentary):
        if " with " in detail:
            before, after = detail.split(" with ", maxsplit=1)
            new_organ = SpecimenOrgan.dentary
            rgx = r"[a-z][\da-z\-\?]*(, [a-z][\da-z\-\?]*)*"
            if match := re.fullmatch(
                "([LR]) (dentary|lower jaw|ramus|dentary fragment|mandible)", before
            ):
                side = match.group(1)
            elif before in ("dentary", "lower jaw", "ramus", "dentary fragment"):
                side = ""
            elif before == "mandible" and "L" in detail and "R" in detail:
                side = ""
                new_organ = SpecimenOrgan.mandible
                rgx = r"[LR][a-z][\da-z\-\?]*(, [LR][a-z][\da-z\-\?]*)*"
            else:
                side = None
            if side is not None:
                if re.fullmatch(rgx, after):
                    pieces = after.split(", ")
                    new_detail = ", ".join(side + piece for piece in pieces)
                    return [
                        TypeTag.Organ(new_organ, detail=new_detail, condition=condition)
                    ], []
    if organ in (SpecimenOrgan.maxilla, SpecimenOrgan.skull):
        if " with " in detail:
            before, after = detail.split(" with ", maxsplit=1)
            new_organ = SpecimenOrgan.maxilla
            if match := re.fullmatch("([LR]) (maxilla|upper jaw|palate)", before):
                side = match.group(1)
                if match.group(2) == "palate":
                    new_organ = SpecimenOrgan.palate
            elif before in ("maxilla", "upper jaw"):
                side = ""
            elif before == "palate":
                side = ""
                new_organ = SpecimenOrgan.palate
            else:
                side = None
            if side is not None:
                if re.fullmatch(r"[A-Z][\dA-Z\-\?]*(, [A-Z][\dA-Z\-\?]*)*", after):
                    pieces = after.split(", ")
                    new_detail = ", ".join(side + piece for piece in pieces)
                    return [
                        TypeTag.Organ(new_organ, detail=new_detail, condition=condition)
                    ], []
    if organ is SpecimenOrgan.skull:
        if " with " in detail:
            before, after = detail.split(" with ", maxsplit=1)
            if re.fullmatch(r"(partial |broken )?(skull|cranium)", before):
                if re.fullmatch(r"[A-Z][\dA-Z\-\?]*(, [A-Z][\dA-Z\-\?]*)*", after):
                    return [
                        TypeTag.Organ(
                            SpecimenOrgan.skull, detail=after, condition=condition
                        )
                    ], []
    parts = detail.split(", ")
    remaining_parts = []
    new_tags = []
    if len(parts) == 1 or (
        not condition and "L" not in parts and "R" not in parts and "(" not in detail
    ):
        for part in parts:
            new_pair = specify_organ(organ, part)
            if new_pair is None:
                remaining_parts.append(part)
            else:
                new_organ, new_detail = new_pair
                new_tags.append(
                    TypeTag.Organ(new_organ, detail=new_detail, condition=condition)
                )
    else:
        remaining_parts = parts
    return new_tags, remaining_parts


def check_organ_tag_with_parser(
    organ: SpecimenOrgan, detail: str | None
) -> Generator[str, None, str | None]:
    if detail is None or detail == "" or organ not in CHECKED_ORGANS:
        return detail
    try:
        parsed_list = parse_organ_detail(detail, organ)
    except ParseException as e:
        yield f"{e} while parsing {detail!r}"
        return detail
    for parsed in parsed_list:
        for issue in parsed.validate(organ):
            yield f"{issue} (from text {detail!r})"
    return ", ".join(
        str(parsed) for parsed in sorted(set(parsed_list), key=ParsedOrgan.sort_key)
    )


def check_organ_tag(tag: TypeTag.Organ) -> Generator[str, None, list[TypeTag.Organ]]:  # type: ignore[name-defined]
    if not tag.detail:
        return [tag]
    detail = tag.detail
    for rgx, replacement in ORGAN_REPLACEMENTS:
        detail = re.sub(rgx, replacement, detail)
    condition = tag.condition
    if not condition:
        if match := re.fullmatch(r"^([^\(\)]+) \(([^\(\)]+)\)", detail):
            detail = match.group(1)
            condition = match.group(2)
    organ = tag.organ
    new_tags, remaining_parts = maybe_replace_tags(organ, detail, condition)
    if not remaining_parts:
        return new_tags
    detail = ", ".join(remaining_parts)
    # Move museum numbers from "detail" to "condition"
    if (
        re.fullmatch(
            r"^[A-Z]{2,}(-[A-Z]+)? [A-Z]?(\d{2,}[\-\d/\.]*|\d(\.\d+)+)[a-z]?$", detail
        )
        and not condition
    ):
        condition = detail
        detail = None
    detail = yield from check_organ_tag_with_parser(organ, detail)
    new_tags.append(TypeTag.Organ(organ, detail=detail, condition=condition))
    return new_tags


class SelectionTag(Protocol):
    page: str | None
    page_link: str | None
    verbatim_citation: str | None
    citation_group: models.CitationGroup | None
    comment: str | None

    def replace(
        self,
        *,
        page: str | None = None,
        page_link: str | None = None,
        verbatim_citation: str | None = None,
        citation_group: models.CitationGroup | None = None,
        comment: str | None = None,
    ) -> Self:
        raise NotImplementedError


class TagWithPage(Protocol):
    page: str | None
    page_link: str | None
    comment: str | None

    def replace(
        self,
        *,
        page: str | None = None,
        page_link: str | None = None,
        comment: str | None = None,
    ) -> Self:
        raise NotImplementedError


def check_selection_tag[Tag: SelectionTag](
    tag: Tag, source: Article | None, cfg: LintConfig, owner: object
) -> Generator[str, None, Tag]:
    tag = yield from check_tag_with_page(tag, source, cfg, owner)
    if source is not None and (
        tag.verbatim_citation is not None or tag.citation_group is not None
    ):
        message = f"{tag} has redundant citation information"
        if cfg.autofix:
            tag = tag.replace(verbatim_citation=None, citation_group=None)
            print(f"{owner}: {message}")
        else:
            yield message
    if tag.verbatim_citation is not None and tag.citation_group is None:
        yield f"{tag} has verbatim_citation but no citation_group"
    if source is None and tag.verbatim_citation is None:
        yield f"{tag} has no source or verbatim_citation"
    return tag


def check_tag_with_page[Tag: TagWithPage](
    tag: Tag,
    source: Article | None,
    cfg: LintConfig,
    owner: object,
    *,
    allow_missing_page: bool = False,
) -> Generator[str, None, Tag]:
    if (
        source is not None
        and tag.page is not None
        and source.url is not None
        and tag.page_link is None
    ):
        page_described = get_unique_page_text(tag.page)[0]
        maybe_pair = infer_bhl_page_id(page_described, tag, source, cfg)
        if maybe_pair is not None:
            page_id, context = maybe_pair
            url = f"https://www.biodiversitylibrary.org/page/{page_id}"
            message = (
                f"inferred BHL page {page_id} from {context} for {tag} (add {url})"
            )
            if cfg.autofix:
                tag = tag.replace(page_link=url)
                print(f"{owner}: {message}")
            else:
                yield message
    if tag.page is None and tag.comment is not None:
        if match := re.fullmatch(r"pp?\. (\d+(-\d+)?(?:, \d+(-\d+)?)*)", tag.comment):
            page = match.group(1)
            message = f"extracted page {page} from comment in {tag}"
            if cfg.autofix:
                tag = tag.replace(page=page, comment=None)
                print(f"{owner}: {message}")
            else:
                yield message
        # mypy bug?
        elif match := re.search(r"^p\. (\d+(?:, \d+)*)\. ", tag.comment):  # type: ignore[arg-type]
            page = match.group(1)
            _, end_span = match.span()
            new_comment = tag.comment[end_span:]  # type: ignore[index]
            message = f"extracted page {page} from comment in {tag} (change comment to {new_comment!r})"
            if cfg.autofix:
                tag = tag.replace(page=page, comment=new_comment)
                print(f"{owner}: {message}")
            else:
                yield message
    if tag.page is not None:

        def set_page(page: str) -> None:
            nonlocal tag
            tag = tag.replace(page=page)

        yield from check_page(
            tag.page,
            set_page=set_page,
            obj=owner,
            cfg=cfg,
            get_raw_page_regex=(
                source.get_raw_page_regex if source is not None else None
            ),
        )
        if source is not None and tag.page is not None:
            yield from check_page_matches_citation(source, tag.page)

    if (
        not allow_missing_page
        and source is not None
        and tag.comment is not None
        and tag.page is None
    ):
        yield f"{tag} has source but no page"
    return tag


def _only_digits(text: str) -> str:
    return re.sub(r"[^0-9a-z]", "", text)


def _is_comparable_to_type(specimen_text: str, nam: Name) -> bool:
    if nam.type_specimen is None or nam.collection is None:
        return False
    if specimen_text.replace("ZISP", "ZIN") == nam.type_specimen:
        return True
    if nam.collection.label not in specimen_text:
        return False
    text = _only_digits(specimen_text)
    current_type = _only_digits(nam.type_specimen)
    if text == current_type:
        return True
    if nam.collection.label == "BMNH":
        current_type = current_type.replace("amm", "")
        if text == current_type:
            return True
        if current_type in ("18" + text, "19" + text):
            return True
    return False


type TagsByType = dict[type[TypeTag], list[TypeTag]]
type TypeTagChecker = Callable[
    [TypeTag, Name, LintConfig, TagsByType], Generator[str, None, list[TypeTag]]
]


def _check_links_in_type_tag(
    tag: TypeTag, nam: Name, cfg: LintConfig, tags_by_type: TagsByType
) -> Generator[str, None, list[TypeTag]]:
    for arg_name, art in get_tag_fields_of_type(tag, Article):
        if art.kind is ArticleKind.removed:
            yield f"bad article in tag {tag}"
        elif art.kind is ArticleKind.redirect:
            if art.parent is None or art.parent.should_skip():
                yield f"bad redirected article in tag {tag}"
            elif cfg.autofix:
                print(f"{nam} references a redirected Article in {tag} -> {art.parent}")
                tag = replace_arg(tag, arg_name, art.parent)
    for arg_name, tag_nam in get_tag_fields_of_type(tag, Name):
        if tag_nam.is_invalid():
            yield f"bad name in {arg_name} for tag {tag}"

    return [tag]


def _check_detail_type_tag(
    tag: TypeTag, nam: Name, cfg: LintConfig, tags_by_type: TagsByType
) -> Generator[str, None, list[TypeTag]]:
    if (
        isinstance(
            tag,
            (
                TypeTag.LocationDetail,
                TypeTag.SpecimenDetail,
                TypeTag.EtymologyDetail,
                TypeTag.TypeSpeciesDetail,
                TypeTag.CitationDetail,
            ),
        )
        and not tag.text
        and tag.source is None
    ):
        message = f"{tag} has no text and no source"
        if cfg.autofix:
            print(f"{nam}: {message}")
            return []
        else:
            yield message
    return [tag]


def _check_designation_type_tag(
    tag: TypeTag, nam: Name, cfg: LintConfig, tags_by_type: TagsByType
) -> Generator[str, None, list[TypeTag]]:
    if isinstance(
        tag,
        (
            TypeTag.LectotypeDesignation,
            TypeTag.NeotypeDesignation,
            TypeTag.TypeDesignation,
            TypeTag.CommissionTypeDesignation,
        ),
    ):
        if isinstance(tag, TypeTag.CommissionTypeDesignation):
            source = tag.opinion
        else:
            source = tag.optional_source
        tag = yield from check_selection_tag(tag, source, cfg, nam)
    return [tag]


def _check_all_type_tags(
    tag: TypeTag, nam: Name, cfg: LintConfig, by_type: TagsByType
) -> Generator[str, None, list[TypeTag]]:
    tags = []
    match tag:
        case (
            TypeTag.FormerRepository()
            | TypeTag.FutureRepository()
            | TypeTag.ExtraRepository()
        ):
            if tag.repository in nam.get_repositories():
                yield (
                    f"{tag.repository} is marked as a {type(tag).__name__}, but it is"
                    " a current repository"
                )

        case TypeTag.ProbableRepository() | TypeTag.GuessedRepository():
            if nam.collection is not None:
                message = f"has {tag} but collection is set to {nam.collection}"
                if cfg.autofix:
                    print(f"{nam}: {message}")
                    return []
                else:
                    yield message

        case TypeTag.CommissionTypeDesignation():
            if nam.type != tag.type:
                print(
                    f"{nam} has {nam.type} as its type, but the Commission has"
                    f" designated {tag.type}"
                )
                if cfg.autofix:
                    nam.type = tag.type
            if (
                nam.genus_type_kind
                != TypeSpeciesDesignation.designated_by_the_commission
            ):
                print(
                    f"{nam} has {nam.genus_type_kind}, but its type was set by the"
                    " Commission"
                )
                if cfg.autofix:
                    nam.genus_type_kind = (
                        TypeSpeciesDesignation.designated_by_the_commission
                    )

        case TypeTag.LectotypeDesignation():
            if tag.lectotype != nam.type_specimen and _is_comparable_to_type(
                tag.lectotype, nam
            ):
                message = f"in lectotype designation, change {tag.lectotype!r} to {nam.type_specimen!r}"
                if cfg.autofix:
                    tag = tag.replace(lectotype=nam.type_specimen)
                    print(f"{nam}: {message}")
                else:
                    yield message
            if tag.optional_source is None and tag.year is None:
                yield f"lectotype designation has no source or year: {tag}"
            validity = _is_lectotype_designation_valid(nam, tag)
            if validity is not None and tag.valid is not validity:
                message = f"lectotype designation validity should be {validity}, not {tag.valid}"
                if cfg.autofix:
                    tag = tag.replace(valid=validity)
                    print(f"{nam}: {message}")
                else:
                    yield message

        case TypeTag.NeotypeDesignation():
            if tag.neotype != nam.type_specimen and _is_comparable_to_type(
                tag.neotype, nam
            ):
                message = f"in neotype designation, change {tag.neotype!r} to {nam.type_specimen!r}"
                if cfg.autofix:
                    tag = tag.replace(neotype=nam.type_specimen)
                    print(f"{nam}: {message}")
                else:
                    yield message

        case TypeTag.IncludedSpecies():
            tag = yield from check_tag_with_page(
                tag, nam.original_citation, cfg, nam, allow_missing_page=True
            )
            if tag.classification_entry is not None:
                if tag.classification_entry.article != nam.original_citation:
                    yield f"{tag} has classification entry {tag.classification_entry} that does not match original citation {nam.original_citation}"
                if tag.classification_entry.mapped_name != tag.name:
                    yield f"{tag} has classification entry {tag.classification_entry} that does not match name {tag.name}"
            elif any(
                isinstance(other_tag, TypeTag.IncludedSpecies)
                and other_tag.classification_entry is not None
                for other_tag in by_type[TypeTag.IncludedSpecies]
            ):
                yield f"{tag} has no classification entry, but other IncludedSpecies tags do"
            if (
                tag.name.get_date_object() > nam.get_date_object()
                and not nam.has_type_tag(TypeTag.GenusCoelebs)
            ):
                yield f"{tag} has date {tag.name.get_date_object()} that is later than name date {nam.get_date_object()}"

        case TypeTag.Date():
            date = tag.date
            try:
                date = helpers.standardize_date(date)
            except ValueError:
                print(f"{nam} has date {tag.date}, which cannot be parsed")
                yield "unparseable date"
            if date is None:
                return []
            # TODO: add more tags here. Also consider requiring that the specimen details directly
            # support the derived tags; e.g., the SpecimenDetail tag should contain the year of the Date
            # tag.
            if TypeTag.SpecimenDetail not in by_type:
                yield "has Date tag but no SpecimenDetail tag"

        case TypeTag.Age():
            if TypeTag.SpecimenDetail not in by_type:
                yield "has Age tag but no SpecimenDetail tag"

        case TypeTag.Gender():
            if TypeTag.SpecimenDetail not in by_type:
                yield "has Gender tag but no SpecimenDetail tag"

        case TypeTag.Altitude():
            if (
                not re.match(r"^-?\d+([\-\.]\d+)?$", tag.altitude)
                or tag.altitude == "000"
            ):
                yield f"bad altitude tag {tag}"

        case TypeTag.LocationDetail():
            coords = helpers.extract_coordinates(tag.text)
            if coords and not any(
                isinstance(t, TypeTag.Coordinates) for t in nam.type_tags
            ):
                tags.append(TypeTag.Coordinates(coords[0], coords[1]))
                print(
                    f"{nam}: adding coordinates {tags[-1]} extracted from {tag.text!r}"
                )
            tag = yield from check_tag_with_page(
                tag, tag.source, cfg, nam, allow_missing_page=True
            )
            if is_empty_location_detail(tag.text):
                new_tag = TypeTag.NoLocation(source=tag.source)
                message = f"replace {tag} with {new_tag}"
                if cfg.autofix:
                    print(f"{nam}: {message}")
                    return [new_tag]
                else:
                    yield message

        case TypeTag.DefinitionDetail():
            # Phylonyms book PDF has "Tis" and "Te" for "This" and "The"
            text = re.sub(r"\bT(?=e\b|is\b)", "Th", tag.text)
            text = text.replace("defnition", "definition")
            tag = TypeTag.DefinitionDetail(text, tag.source)

        case TypeTag.Coordinates():
            try:
                lat, _ = helpers.standardize_coordinates(tag.latitude, is_latitude=True)
            except helpers.InvalidCoordinates as e:
                yield f"invalid latitude {tag.latitude}: {e}"
                lat = tag.latitude
            try:
                longitude, _ = helpers.standardize_coordinates(
                    tag.longitude, is_latitude=False
                )
            except helpers.InvalidCoordinates as e:
                yield f"invalid longitude {tag.longitude}: {e}"
                longitude = tag.longitude
            tag = TypeTag.Coordinates(lat, longitude)
            if TypeTag.LocationDetail not in by_type:
                yield "has Coordinates tag but no LocationDetail tag"

        case TypeTag.LSIDName():
            lsid = clean_lsid(tag.text)
            tag = TypeTag.LSIDName(lsid)
            if not is_valid_lsid(lsid):
                yield f"invalid LSID {lsid}"

        case TypeTag.TypeSpecimenLink():
            if not tag.url.startswith(("http://", "https://")):
                yield f"invalid type specimen URL {tag.url!r}"
            tag = TypeTag.TypeSpecimenLink(fix_type_specimen_link(tag.url))

        case TypeTag.TypeSpecimenLinkFor():
            if not tag.url.startswith(("http://", "https://")):
                yield f"invalid type specimen URL {tag.url!r}"
            try:
                specs = parse_type_specimen(tag.specimen)
            except ValueError as e:
                yield f"invalid type specimen {tag.specimen!r} in {tag}: {e}"
                new_spec = tag.specimen
            else:
                new_spec = stringify_specimen_list(specs)
            tag = TypeTag.TypeSpecimenLinkFor(
                fix_type_specimen_link(tag.url), new_spec, suffix=tag.suffix
            )

        case TypeTag.Organ():
            if tag.detail:
                return (yield from check_organ_tag(tag))

        case TypeTag.AuthorityPageLink():
            url = yield from check_page_link(
                tag_url=tag.url, tag_page=tag.page, page_described=nam.page_described
            )
            if match := re.fullmatch(
                r"(pl\. \d+) \(((?:unnumbered )?p\. \d+)\)", tag.page
            ):
                page = f"@{match.group(1)} {match.group(2)}"
            elif tag.page is not None:
                page = str(tag.page)
            else:
                page = ""
            tag = TypeTag.AuthorityPageLink(url, tag.confirmed, page)

        case TypeTag.CitationDetail():
            if tag.source is not None and tag.source == nam.original_citation:
                yield "replace CitationDetail with SourceDetail"
                tag = TypeTag.SourceDetail(tag.text, tag.source)

        case TypeTag.NoDate():
            if TypeTag.Date in by_type:
                yield "has NoDate tag but also has Date tag"
                return []

        case TypeTag.NoCollector():
            if TypeTag.CollectedBy in by_type:
                yield "has NoCollector tag but also has Collector tag"
                return []

        case TypeTag.NoAge():
            if TypeTag.Age in by_type:
                yield "has NoAge tag but also has Age tag"
                return []

        case TypeTag.NoGender():
            if TypeTag.Gender in by_type:
                yield "has NoGender tag but also has Gender tag"
                return []

        case TypeTag.NoOrgan():
            if TypeTag.Organ in by_type:
                yield "has NoOrgan tag but also has Organ tag"
                return []

        case TypeTag.NoLocation():
            if TypeTag.LocationDetail in by_type:
                witnesses = [
                    other_tag
                    for other_tag in by_type[TypeTag.LocationDetail]
                    if isinstance(other_tag, TypeTag.LocationDetail)
                    and other_tag.source == tag.source
                ]
                if witnesses:
                    yield f"has NoLocation tag but also has LocationDetail tag: {witnesses}"
                    return []

        case TypeTag.NoEtymology():
            if TypeTag.EtymologyDetail in by_type:
                witnesses = [
                    other_tag
                    for other_tag in by_type[TypeTag.EtymologyDetail]
                    if isinstance(other_tag, TypeTag.EtymologyDetail)
                    and other_tag.source == tag.source
                ]
                if witnesses:
                    yield f"has NoEtymology tag but also has EtymologyDetail tag: {witnesses}"
                    return []

        case TypeTag.NoSpecimen():
            if TypeTag.SpecimenDetail in by_type:
                witnesses = [
                    other_tag
                    for other_tag in by_type[TypeTag.SpecimenDetail]
                    if isinstance(other_tag, TypeTag.SpecimenDetail)
                    and other_tag.source == tag.source
                ]
                if witnesses:
                    yield f"has NoSpecimen tag but also has SpecimenDetail tag: {witnesses}"
                    return []

        case TypeTag.Altitude():
            if TypeTag.LocationDetail not in by_type:
                yield "has Altitude tag but no LocationDetail tag"

        case TypeTag.TextualOriginalRank():
            if nam.original_rank is None or not nam.original_rank.needs_textual_rank:
                yield f"has TextualOriginalRank tag but is of rank that does not need it ({nam.original_rank!r})"

        case TypeTag.OriginalTypification():
            if nam.group is not Group.species:
                yield "has OriginalTypification tag but is not a species-group name"
            witnesses = [
                other_tag
                for other_tag in by_type[TypeTag.OriginalTypification]
                if isinstance(other_tag, TypeTag.OriginalTypification)
                and other_tag != tag
                and other_tag.source == tag.source
            ]
            if witnesses:
                yield f"has multiple OriginalTypification tags from the same source: {witnesses}"
            if tag.source == nam.original_citation:
                match tag.basis:
                    case (
                        SpeciesBasis.implicit_holotype | SpeciesBasis.explicit_holotype
                    ):
                        if nam.species_type_kind not in (
                            SpeciesGroupType.holotype,
                            SpeciesGroupType.neotype,
                        ):
                            yield f"has {tag.basis!r} OriginalTypification, but species_type_kind is {nam.species_type_kind!r}"
                    case (
                        SpeciesBasis.explicit_syntypes | SpeciesBasis.implicit_syntypes
                    ):
                        if nam.species_type_kind not in (
                            SpeciesGroupType.syntypes,
                            SpeciesGroupType.neotype,
                            SpeciesGroupType.lectotype,
                        ):
                            yield f"has {tag.basis!r} OriginalTypification, but species_type_kind is {nam.species_type_kind!r}"

        case TypeTag.PhyloCodeNumber():
            if nam.group is Group.species:
                yield "has PhyloCodeNumber tag but is a species-group name"
            if TypeTag.DefinitionDetail not in by_type:
                yield "has PhyloCodeNumber tag but no DefinitionDetail tag"
            if TypeTag.PhylogeneticDefinition not in by_type:
                yield "has PhyloCodeNumber tag but no PhylogeneticDefinition tag"

        case TypeTag.PhylogeneticDefinition():
            # Note aspects of the PhyloCode we currently ignore:
            # - subtleties about the definition of "crown" clades in the presence of extinction. We
            #   just look at what's currently extant.
            # - subtleties about the use of species names versus specimens as specifiers. We only
            #   support species names.
            if TypeTag.DefinitionDetail not in by_type:
                yield "has PhylogeneticDefinition tag but no DefinitionDetail tag"
            else:
                matching_defns = [
                    defn_tag
                    for defn_tag in by_type[TypeTag.DefinitionDetail]
                    if defn_tag.source == tag.source
                ]
                if len(matching_defns) == 0:
                    yield f"has PhylogeneticDefinition tag but no DefinitionDetail tag with the same source ({tag.source})"
                elif len(matching_defns) > 1:
                    yield f"has PhylogeneticDefinition tag but multiple DefinitionDetail tags with the same source ({tag.source}): {matching_defns}"

            internal_specifiers = [
                tag.name for tag in by_type.get(TypeTag.InternalSpecifier, [])
            ]
            external_specifiers = [
                tag.name for tag in by_type.get(TypeTag.ExternalSpecifier, [])
            ]

            if tag.type is not PhylogeneticDefinitionType.other:
                if not internal_specifiers:
                    yield f"has {tag.type.name} PhylogeneticDefinition tag but no InternalSpecifier tag"
            if tag.type is PhylogeneticDefinitionType.maximum_clade:
                if not external_specifiers and TypeTag.MustBeExtinct not in by_type:
                    yield f"has {tag.type.name} PhylogeneticDefinition tag but no ExternalSpecifier tag"
            if tag.type is PhylogeneticDefinitionType.pan_clade:
                if len(internal_specifiers) != 1:
                    yield f"has {tag.type.name} PhylogeneticDefinition tag but not exactly one InternalSpecifier tag"
                elif external_specifiers:
                    yield f"has {tag.type.name} PhylogeneticDefinition tag but also has ExternalSpecifier tag(s)"
                else:
                    referent = internal_specifiers[0]
                    defn_type = referent.get_type_tag(TypeTag.PhylogeneticDefinition)
                    if defn_type is None:
                        yield f"has {tag.type.name} PhylogeneticDefinition tag but the internal specifier {referent} has no PhylogeneticDefinition tag"
                    elif defn_type.type not in (
                        PhylogeneticDefinitionType.minimum_crown_clade,
                        PhylogeneticDefinitionType.maximum_crown_clade,
                    ):
                        yield f"has {tag.type.name} PhylogeneticDefinition tag but the internal specifier {referent} is not defined as a crown clade"

            application = _check_phylogenetic_definition(
                nam,
                tag.type,
                internal_specifiers,
                external_specifiers,
                must_not_include=[
                    tag.name for tag in by_type.get(TypeTag.MustNotInclude, [])
                ],
                must_be_part_of=[
                    tag.name for tag in by_type.get(TypeTag.MustBePartOf, [])
                ],
                must_not_be_part_of=[
                    tag.name for tag in by_type.get(TypeTag.MustNotBePartOf, [])
                ],
                must_be_extinct=bool(by_type.get(TypeTag.MustBeExtinct, [])),
            )
            effective_taxon = _get_effective_taxon(nam)
            if application.is_applicable is False:
                if nam.status is not Status.synonym:
                    yield "has phylogenetic definition that does not apply, but is not a synonym"
            if (
                application.minimum_taxon is not None
                and application.maximum_taxon is not None
                and application.minimum_taxon == application.maximum_taxon
            ):
                if effective_taxon != application.minimum_taxon:
                    yield f"should be applied to {application.minimum_taxon}, but is applied to {effective_taxon}"
            elif application.minimum_taxon is not None:
                if not application.minimum_taxon.is_child_of(effective_taxon):
                    yield f"should be applied to a parent taxon of {application.minimum_taxon}, but is applied to {effective_taxon}"
            elif application.maximum_taxon is not None:
                if not effective_taxon.is_child_of(application.maximum_taxon):
                    yield f"should be applied to a child taxon of {application.maximum_taxon}, but is applied to {effective_taxon}"

        case TypeTag.InternalSpecifier() | TypeTag.ExternalSpecifier():
            if TypeTag.PhylogeneticDefinition not in by_type:
                yield f"has {type(tag).__name__} tag but no PhylogeneticDefinition tag"

        case TypeTag.TreatAsEquivalentTo():
            if not _is_high_or_invalid_family(nam):
                yield "has TreatAsEquivalentTo tag but is not a high-level or invalid family-group name"

    return [*tags, tag]


def _is_high_or_invalid_family(nam: Name) -> bool:
    match nam.group:
        case Group.high:
            return True
        case Group.family:
            return (
                nam.nomenclature_status
                is NomenclatureStatus.not_based_on_a_generic_name
            )
        case Group.genus | Group.species:
            return False
        case _:
            assert_never(nam.group)


def _all_parents(nam: Name) -> Iterable[Taxon]:
    yield from _iter_parents(nam.taxon)


def _iter_parents(taxon: Taxon) -> Iterable[Taxon]:
    seen: set[Taxon] = set()
    while True:
        yield taxon
        seen.add(taxon)
        if taxon.parent is None:
            break
        taxon = taxon.parent
        if taxon in seen:
            raise ValueError(f"cycle in taxon hierarchy involving {taxon}")


def _get_effective_taxon(nam: Name) -> Taxon:
    if nam.group is not Group.family or nam.original_rank is None:
        return nam.taxon
    resolved_nam = nam.resolve_variant()
    try:
        candidate = (
            Taxon.select_valid()
            .filter(Taxon.base_name == resolved_nam, Taxon.rank == nam.original_rank)
            .get()
        )
    except Taxon.DoesNotExist:
        return nam.taxon
    else:
        return candidate


EXCLUDED_TAXA = [6, 12]  # Nonorganic  # Legendary


def _smallest_common_ancestor(nams: Sequence[Name]) -> Taxon:
    if len(nams) < 1:
        raise ValueError("need at least one name")
    elif len(nams) == 1:
        return _get_effective_taxon(nams[0])
    parent_lists = [list(_all_parents(nam)) for nam in nams]
    if any(not parents for parents in parent_lists):
        raise ValueError("one of the names has no taxon")

    # If any of the taxa are not real, use the real ones
    non_excluded_lists = [
        parent_list
        for parent_list in parent_lists
        if not any(t.id in EXCLUDED_TAXA for t in parent_list)
    ]
    if non_excluded_lists:
        parent_lists = non_excluded_lists
    if len(parent_lists) == 1:
        return parent_lists[0][0]
    first, *rest = parent_lists
    for common_ancestor in first:
        if all(common_ancestor in parents for parents in rest):
            return common_ancestor
    raise ValueError("no common ancestor found")


def _largest_excluding(taxon: Taxon, exclude: Sequence[Name]) -> Taxon | None:
    if not exclude:
        return taxon
    parent_lists = [list(_all_parents(nam)) for nam in exclude]
    prev: Taxon | None = None
    for parent in _iter_parents(taxon):
        if any(parent in parents for parents in parent_lists):
            return prev
        prev = parent
    return prev


@dataclass(frozen=True, kw_only=True)
class DefinitionApplication:
    is_applicable: bool | None
    """If True, this name applies to a clade accepted in our classification. If False,
    it does not (and should not be a valid taxon). If None, we cannot determine."""
    minimum_taxon: Taxon | None
    maximum_taxon: Taxon | None
    """The name should be applied to a taxon that is at least minimum_taxon and at most
    maximum_taxon. Either may be None if we cannot determine it."""


def _get_maximum_clade(
    internal_specifiers: Sequence[Name], external_specifiers: Sequence[Name]
) -> tuple[bool, Taxon | None]:
    ancestor = _smallest_common_ancestor(internal_specifiers)
    parent_lists = [list(_all_parents(ext)) for ext in external_specifiers]
    applicable = []
    inapplicable = []
    for ext, parent_list in zip(external_specifiers, parent_lists, strict=True):
        if ancestor in parent_list:
            inapplicable.append(ext)
        else:
            applicable.append(ext)
    expected_taxon = _largest_excluding(ancestor, applicable)
    return not inapplicable and expected_taxon is not None, expected_taxon


def _check_phylogenetic_definition(
    nam: Name,
    defn_type: PhylogeneticDefinitionType,
    internal_specifiers: list[Name],
    external_specifiers: list[Name],
    *,
    must_not_include: list[Name],
    must_be_part_of: list[Name],
    must_not_be_part_of: list[Name],
    must_be_extinct: bool,
) -> DefinitionApplication:
    result = _check_phylogenetic_definition_inner(
        nam, defn_type, internal_specifiers, external_specifiers
    )
    if (
        not must_not_include
        and not must_be_part_of
        and not must_not_be_part_of
        and not must_be_extinct
    ):
        # Simple case: no conditions
        return result
    if result.minimum_taxon is None and result.maximum_taxon is None:
        # Can't narrow it down if we have no idea where to put the name
        return result
    if result.minimum_taxon is None:
        # This shouldn't happen but also makes it impossible to narrow
        return result

    if _is_applicable(
        result.minimum_taxon,
        must_not_include=must_not_include,
        must_be_part_of=must_be_part_of,
        must_not_be_part_of=must_not_be_part_of,
        must_be_extinct=must_be_extinct,
    ):
        new_minimum_taxon = result.minimum_taxon
    else:
        for parent in _iter_parents(result.minimum_taxon):
            if _is_applicable(
                parent,
                must_not_include=must_not_include,
                must_be_part_of=must_be_part_of,
                must_not_be_part_of=must_not_be_part_of,
                must_be_extinct=must_be_extinct,
            ):
                new_minimum_taxon = parent
                break
        else:
            return DefinitionApplication(
                is_applicable=False,
                minimum_taxon=result.minimum_taxon,
                maximum_taxon=result.maximum_taxon,
            )
    new_maximum_taxon = result.maximum_taxon
    last_seen = new_minimum_taxon
    for parent in _iter_parents(new_minimum_taxon):
        if not _is_applicable(
            parent,
            must_not_include=must_not_include,
            must_be_part_of=must_be_part_of,
            must_not_be_part_of=must_not_be_part_of,
            must_be_extinct=must_be_extinct,
        ):
            new_maximum_taxon = last_seen
            break
        last_seen = parent
    return DefinitionApplication(
        is_applicable=result.is_applicable,
        minimum_taxon=new_minimum_taxon,
        maximum_taxon=new_maximum_taxon,
    )


def _is_applicable(
    taxon: Taxon,
    *,
    must_not_include: list[Name],
    must_be_part_of: list[Name],
    must_not_be_part_of: list[Name],
    must_be_extinct: bool,
) -> bool:
    for mni in must_not_include:
        if taxon in _iter_parents(_get_effective_taxon(mni)):
            return False
    parents = list(_iter_parents(taxon))
    if must_be_part_of and not any(
        _get_effective_taxon(mbp) in parents for mbp in must_be_part_of
    ):
        return False
    for mnbp in must_not_be_part_of:
        if _get_effective_taxon(mnbp) in parents:
            return False
    if must_be_extinct and taxon.age is AgeClass.extant:
        return False
    return True


def _check_phylogenetic_definition_inner(
    nam: Name,
    defn_type: PhylogeneticDefinitionType,
    internal_specifiers: list[Name],
    external_specifiers: list[Name],
) -> DefinitionApplication:
    match defn_type:
        case PhylogeneticDefinitionType.other:
            return DefinitionApplication(
                is_applicable=None, minimum_taxon=None, maximum_taxon=None
            )  # cannot check
        case PhylogeneticDefinitionType.apomorphy:
            ancestor = _smallest_common_ancestor(internal_specifiers)
            if any(ancestor in _all_parents(ext) for ext in external_specifiers):
                return DefinitionApplication(
                    is_applicable=False, minimum_taxon=ancestor, maximum_taxon=None
                )
            else:
                # We only know it's definitely applicable if there is only one internal specifier and
                # no external specifiers. Otherwise, the apomorphy might not be homologous in the internal
                # specifiers, or might predate the external specifier.
                is_applicable = (
                    len(internal_specifiers) == 1 and not external_specifiers
                )
                if external_specifiers:
                    maximum_taxon = _largest_excluding(ancestor, external_specifiers)
                else:
                    maximum_taxon = None
                return DefinitionApplication(
                    is_applicable=is_applicable,
                    minimum_taxon=ancestor,
                    maximum_taxon=maximum_taxon,
                )
        case PhylogeneticDefinitionType.minimum_clade:
            ancestor = _smallest_common_ancestor(internal_specifiers)
            if any(ancestor in _all_parents(ext) for ext in external_specifiers):
                return DefinitionApplication(
                    is_applicable=False, minimum_taxon=ancestor, maximum_taxon=ancestor
                )
            else:
                return DefinitionApplication(
                    is_applicable=True, minimum_taxon=ancestor, maximum_taxon=ancestor
                )
        case PhylogeneticDefinitionType.maximum_clade:
            is_applicable, expected_taxon = _get_maximum_clade(
                internal_specifiers, external_specifiers
            )
            return DefinitionApplication(
                is_applicable=is_applicable,
                minimum_taxon=expected_taxon,
                maximum_taxon=expected_taxon,
            )
        case PhylogeneticDefinitionType.minimum_crown_clade:
            # Not clear how these are different from standard minimum-clade definitions.
            # Note that Ungulata has two extinct specifiers (Bos primigenius and Equus ferus).
            return _check_phylogenetic_definition_inner(
                nam,
                PhylogeneticDefinitionType.minimum_clade,
                internal_specifiers,
                external_specifiers,
            )
        case PhylogeneticDefinitionType.maximum_crown_clade:
            is_applicable, expected_taxon = _get_maximum_clade(
                internal_specifiers, external_specifiers
            )
            # We want the largest crown clade, i.e., the largest one that contains multiple extant children.
            if expected_taxon is None or expected_taxon.age is not AgeClass.extant:
                return DefinitionApplication(
                    is_applicable=False,
                    minimum_taxon=expected_taxon,
                    maximum_taxon=expected_taxon,
                )
            while expected_taxon is not None:
                all_children = list(expected_taxon.get_children())
                children = [
                    child for child in all_children if child.age is AgeClass.extant
                ]
                # TODO: This is wrong in some cases. Consider a monotypic (among extant taxa)
                # taxon (e.g., Chelodininae). If Chelodininae contains only extant Chelodina
                # and is defined as a maximum crown clade, we'll say that its name should be applied
                # to Chelodina. I tried to fix this with a special case for taxa with only one
                # child, but that instead caused trouble when it wanted Caniformia to be applied to
                # Pan-Caniformia (which has no other fossil members).
                if len(children) >= 2:
                    break
                if len(children) == 0:
                    # Shouldn't happen.
                    return DefinitionApplication(
                        is_applicable=None, minimum_taxon=None, maximum_taxon=None
                    )
                expected_taxon = children[0]
            return DefinitionApplication(
                is_applicable=is_applicable,
                minimum_taxon=expected_taxon,
                maximum_taxon=expected_taxon,
            )
        case PhylogeneticDefinitionType.maximum_total_clade:
            # This type is used for Amphibia in Phylonyms, and it's not clear to me how the
            # definition is different from a normal maximum-clade definition.
            return _check_phylogenetic_definition_inner(
                nam,
                PhylogeneticDefinitionType.maximum_clade,
                internal_specifiers,
                external_specifiers,
            )
        case PhylogeneticDefinitionType.pan_clade:
            # We start with the internal specifier, and then go up until we reach a taxon with
            # more than one extant child.
            if not internal_specifiers:
                return DefinitionApplication(
                    is_applicable=None, minimum_taxon=None, maximum_taxon=None
                )
            taxon = _get_effective_taxon(internal_specifiers[0])
            while True:
                if taxon.age is not AgeClass.extant:
                    return DefinitionApplication(
                        is_applicable=False, minimum_taxon=None, maximum_taxon=None
                    )
                parent = taxon.parent
                if parent is None:
                    return DefinitionApplication(
                        is_applicable=None, minimum_taxon=None, maximum_taxon=None
                    )
                children = [
                    child
                    for child in parent.get_children()
                    if child.age is AgeClass.extant
                ]
                if len(children) >= 2:
                    break
                if len(children) == 0:
                    return DefinitionApplication(
                        is_applicable=False, minimum_taxon=None, maximum_taxon=None
                    )
                taxon = parent
            return DefinitionApplication(
                is_applicable=True, minimum_taxon=taxon, maximum_taxon=taxon
            )
        case _:
            assert_never(defn_type)
    raise NotImplementedError(f"checking for {defn_type} not implemented")


def _get_original_typification(nam: Name) -> SpeciesBasis | None:
    for tag in nam.type_tags:
        if (
            isinstance(tag, TypeTag.OriginalTypification)
            and tag.source == nam.original_citation
        ):
            return tag.basis
    return None


def _get_lectotype_designation_year(tag: TypeTag.LectotypeDesignation) -> int | None:  # type: ignore[name-defined]
    if tag.optional_source is not None:
        return tag.optional_source.numeric_year()
    if tag.year is not None:
        return helpers.get_date_object(tag.year).year
    return None


def _is_lectotype_designation_valid(
    nam: Name, tag: TypeTag.LectotypeDesignation  # type: ignore[name-defined]
) -> bool | None:
    year = _get_lectotype_designation_year(tag)
    is_before_2000 = year is None or year < 2000
    # Art. 74.7
    if not is_before_2000:
        if tag.is_explicit_choice is None:
            return None
        if tag.is_explicit_choice is False:
            return False
        if tag.term is None:
            return None
        if tag.term is not LectotypeDesignationTerm.lectotype:
            return False
        return True
    typification = _get_original_typification(nam)
    # Art. 74.6
    if typification is SpeciesBasis.unclear and tag.is_assumption_of_monotypy is True:
        return True
    # Art. 74.5
    if tag.is_explicit_choice is True:
        return True
    if tag.term in (
        LectotypeDesignationTerm.lectotype,
        LectotypeDesignationTerm.the_type,
    ):
        return True
    if tag.term is LectotypeDesignationTerm.holotype:
        return False
    return None


def is_empty_location_detail(text: str) -> bool:
    return text.replace(".", "") in (
        "[No locality given]",
        "[Plate only]",
        "[None given]",
        "[No locality]",
        "[No explicit locality]",
    )


TYPE_TAG_CHECKERS: list[TypeTagChecker] = [
    _check_links_in_type_tag,
    _check_detail_type_tag,
    _check_designation_type_tag,
    _check_all_type_tags,
]


@LINT.add("type_tags")
def check_type_tags_for_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not nam.type_tags:
        return
    original_tags = list(nam.type_tags)
    tags = list(original_tags)
    by_type: dict[type[TypeTag], list[TypeTag]] = {}
    for tag in original_tags:
        by_type.setdefault(type(tag), []).append(tag)
    for tag_type, tags_of_type in by_type.items():
        if tag_type in UNIQUE_TAGS and len(tags_of_type) > 1:
            yield f"has multiple tags of type {tag_type}: {tags_of_type}"

    for checker in TYPE_TAG_CHECKERS:
        new_tags = []
        for tag in tags:
            new_tags += yield from checker(tag, nam, cfg, by_type)
        tags = new_tags

    if nam.collection is not None and nam.collection.id == MULTIPLE_COLLECTION:
        repos = by_type.get(TypeTag.Repository, [])
        if len(repos) < 2:
            yield (
                "name with collection 'multiple' must have multiple Repository tags:"
                f" {repos}"
            )
    elif TypeTag.Repository in by_type:
        yield f"name may not have Repository tags: {by_type[TypeTag.Repository]}"

    if tags != original_tags:
        if set(tags) != set(original_tags):
            print(f"changing tags for {nam}")
            getinput.print_diff(sorted(original_tags), tags)
        if cfg.autofix:
            nam.type_tags = tags  # type: ignore[assignment]


def _smallest_high_group_ancestors(taxon: Taxon) -> Iterable[Taxon]:
    for ancestor in _iter_parents(taxon):
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
    if not _is_high_or_invalid_family(nam):
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
    ancestor = _smallest_common_ancestor(child_names)
    allowed_taxa = [ancestor]
    if ancestor.base_name.group is not Group.high:
        allowed_taxa.extend(_smallest_high_group_ancestors(ancestor))
    if nam.taxon not in allowed_taxa:
        if len(allowed_taxa) == 1:
            allowed = str(allowed_taxa[0])
        else:
            allowed = " or ".join(str(taxon) for taxon in allowed_taxa)
        yield f"should be applied to {allowed}, not {nam.taxon} (based on allocation of child names {child_names})"


def check_page_link(
    tag_url: str, tag_page: str, page_described: str | None
) -> Generator[str, None, str]:
    url = urlparse.parse_url(tag_url)
    if isinstance(
        url,
        (
            urlparse.BhlItem,
            urlparse.BhlBibliography,
            urlparse.BhlItem,
            urlparse.GoogleBooksVolume,
        ),
    ):
        yield f"invalid authority page link {url!r}"
    for message in url.lint():
        yield f"page link {tag_url}: {message}"
    if page_described is not None and tag_page != "NA":
        allowed_pages = get_unique_page_text(page_described)
        if tag_page not in allowed_pages:
            yield (
                f"authority page link {tag_url} for page {tag_page!r}"
                f" does not match any pages in page_described ({allowed_pages})"
            )
    return str(url)


@LINT.add("dedupe_tags")
def dedupe_and_sort_tags(nam: Name, cfg: LintConfig) -> Iterable[str]:
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
        if cfg.autofix:
            nam.type_tags = tags  # type: ignore[assignment]
    return []


def fix_type_specimen_link(url: str) -> str:
    if url.startswith(
        (
            "http://arctos.database.museum/",
            "http://researcharchive.calacademy.org/",
            "http://ucmpdb.berkeley.edu/cgi/",
        )
    ):
        return url.replace("http://", "https://")
    return url


def make_point(tag: TypeTag.Coordinates) -> coordinates.Point | None:  # type: ignore[name-defined]
    try:
        _, lat = helpers.standardize_coordinates(tag.latitude, is_latitude=True)
        _, lon = helpers.standardize_coordinates(tag.longitude, is_latitude=False)
    except helpers.InvalidCoordinates:
        return None
    return coordinates.Point(lon, lat)


@LINT.add("coordinates")
def check_coordinates(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_locality is None:
        return
    for tag in nam.get_tags(nam.type_tags, TypeTag.Coordinates):
        point = make_point(tag)
        if point is None:
            continue  # reported elsewhere

        tl_region = nam.type_locality.region
        tl_country = tl_region.parent_of_kind(RegionKind.country)
        if tl_country is None:
            continue
        polygon_path = coordinates.get_path(tl_country.name)
        if polygon_path is not None and coordinates.is_in_polygon(point, polygon_path):
            continue
        osm_country = nominatim.get_openstreetmap_country(point)
        if osm_country is None:
            yield f"cannot place coordinates {point} in any country (expected {tl_country.name})"
            continue
        our_country = tl_country.name
        if osm_country == our_country:
            continue
        our_country = nominatim.HESP_COUNTRY_TO_OSM_COUNTRY.get(
            our_country, our_country
        )
        if our_country == osm_country:
            continue
        yield f"coordinates {point} are in {osm_country}, not {tl_country.name}"


@LINT.add("type_locality_strict")
def check_type_locality_strict(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group is not Group.species or not is_valid_mammal(nam):
        return
    if nam.has_type_tag(TypeTag.InterpretedTypeLocality):
        return
    if "type_locality" not in nam.get_required_fields():
        return
    # will have to deal with this separately
    if nam.original_citation is None:
        return
    if nam.type_locality is None:
        yield "missing type locality"
    if not cfg.experimental:
        return
    original_localities = [
        tag
        for tag in nam.type_tags
        if isinstance(tag, TypeTag.LocationDetail)
        and tag.source == nam.original_citation
    ]
    if not original_localities:
        yield "missing original locality detail"


@LINT.add("type_designation_strict")
def check_type_designation_strict(nam: Name, cfg: LintConfig) -> Iterable[str]:
    # Move to check_type_designation below if this is fixed for all names
    if not is_valid_mammal(nam) or nam.has_type_tag(TypeTag.InterpretedTypeSpecimen):
        return
    match nam.genus_type_kind:
        case TypeSpeciesDesignation.subsequent_designation:
            if not any(
                tag.type == nam.type
                for tag in nam.get_tags(nam.type_tags, TypeTag.TypeDesignation)
            ):
                yield "missing a reference for type species designation"
        case TypeSpeciesDesignation.subsequent_monotypy:
            if not any(
                tag.type == nam.type
                for tag in nam.get_tags(nam.type_tags, TypeTag.TypeDesignation)
            ):
                yield "missing a reference for type species designation"

    match nam.species_type_kind:
        case SpeciesGroupType.lectotype:
            if nam.type_specimen is None:
                yield "type specimen is not set"
                return
            if not any(
                type_specimens_equal(tag.lectotype, nam.type_specimen)
                for tag in nam.get_tags(nam.type_tags, TypeTag.LectotypeDesignation)
            ):
                yield "missing a reference for lectotype designation"
        case SpeciesGroupType.neotype:
            if nam.type_specimen is None:
                yield "type specimen is not set"
                return
            if not any(
                type_specimens_equal(tag.neotype, nam.type_specimen)
                for tag in nam.get_tags(nam.type_tags, TypeTag.NeotypeDesignation)
            ):
                yield "missing a reference for neotype designation"
        case None:
            if (
                cfg.experimental
                and nam.original_citation is not None
                and nam.group is Group.species
                and "type_specimen" in nam.get_required_fields()
            ):
                yield "missing type specimen"


@LINT.add("type_designation")
def check_type_designation(nam: Name, cfg: LintConfig) -> Iterable[str]:
    match nam.genus_type_kind:
        case TypeSpeciesDesignation.designated_by_the_commission:
            tag = nam.get_type_tag(TypeTag.CommissionTypeDesignation)
            if tag is None:
                yield "type species is set to designated_by_the_commission, but missing CommissionTypeDesignation tag"

    match nam.species_type_kind:
        case SpeciesGroupType.lectotype:
            if (
                nam.type_specimen is not None
                and nam.has_type_tag(TypeTag.LectotypeDesignation)
                and not any(
                    type_specimens_equal(tag.lectotype, nam.type_specimen)
                    for tag in nam.get_tags(nam.type_tags, TypeTag.LectotypeDesignation)
                )
            ):
                yield "missing a reference for lectotype designation"
        case SpeciesGroupType.neotype:
            if (
                nam.type_specimen is not None
                and nam.has_type_tag(TypeTag.NeotypeDesignation)
                and not any(
                    type_specimens_equal(tag.neotype, nam.type_specimen)
                    for tag in nam.get_tags(nam.type_tags, TypeTag.NeotypeDesignation)
                )
            ):
                yield "missing a reference for neotype designation"


class RepositoryKind(enum.Enum):
    repository = 1
    former = 2
    extra = 3
    future = 4


def _get_repositories(nam: Name) -> set[tuple[RepositoryKind, Collection]]:
    repos = set()
    if nam.collection is not None and nam.collection.id != MULTIPLE_COLLECTION:
        repos.add((RepositoryKind.repository, nam.collection))
    repo: Collection
    for tag in nam.type_tags:
        match tag:
            case TypeTag.Repository(repo):
                repos.add((RepositoryKind.repository, repo))
            case TypeTag.FormerRepository(repo):
                repos.add((RepositoryKind.former, repo))
            case TypeTag.ExtraRepository(repo):
                repos.add((RepositoryKind.extra, repo))
            case TypeTag.FutureRepository(repo):
                repos.add((RepositoryKind.future, repo))
    return repos


@LINT.add("type_specimen_order")
def check_type_specimen_order(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError:
        return  # reported elsewhere
    expected_text = stringify_specimen_list(specs)
    if nam.type_specimen == expected_text:
        return
    message = (
        f"Incorrectly formatted type specimen: got {nam.type_specimen!r}, expected"
        f" {expected_text!r}"
    )
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.type_specimen = expected_text
    else:
        yield message


@LINT.add("type_specimen")
def check_type_specimen(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError as e:
        yield f"cannot parse type specimen string {nam.type_specimen}: {e}"
        return
    for spec in specs:
        if isinstance(spec, SpecimenRange):
            yield from _check_specimen(spec.start, nam)
            yield from _check_specimen(spec.end, nam)
        else:
            yield from _check_specimen(spec, nam)


def _validate_specimen(
    spec: BaseSpecimen,
    possible_repos: set[tuple[RepositoryKind, Collection]],
    allowed_kinds: set[RepositoryKind],
) -> str | None:
    messages = []
    for kind, repo in possible_repos:
        if kind not in allowed_kinds:
            continue
        message = repo.validate_specimen(spec)
        if message is None:
            return None
        messages.append(message)
    return "; ".join(messages)


def _check_specimen(spec: Specimen, nam: Name) -> Iterable[str]:
    repos = _get_repositories(nam)
    if message := _validate_specimen(spec.base, repos, {RepositoryKind.repository}):
        yield f"{spec.base.stringify()!r} does not match: {message}"
    for former in spec.former_texts:
        if isinstance(former, InformalWithoutInstitution):
            continue
        if message := _validate_specimen(
            former, repos, {RepositoryKind.repository, RepositoryKind.former}
        ):
            yield (
                f"former specimen reference {former!r} does not match any collection"
                f" ({message})"
            )
    for future in spec.future_texts:
        if message := _validate_specimen(
            future, repos, {RepositoryKind.repository, RepositoryKind.future}
        ):
            yield (
                f"future specimen reference {future!r} does not match any collection"
                f" ({message})"
            )
    for extra in spec.extra_texts:
        if message := _validate_specimen(
            extra, repos, {RepositoryKind.repository, RepositoryKind.extra}
        ):
            yield (
                f"extra specimen reference {extra!r} does not match any collection"
                f" ({message})"
            )


_BMNH_REGEXES = [
    (r"^BMNH ([MR])[\- \.]*(\d+[a-z]?)$", r"BMNH \1 \2"),  # M1234 -> M 1234
    (r"^BMNH (\d)(\.\d+\.\d+\.\d+)$", r"BMNH 190\1\2"),  # 2.1.1.1 -> 1902.1.1.1
    (r"^BMNH ([4-9]\d)(\.\d+\.\d+\.\d+)$", r"BMNH 18\1\2"),  # 55.1.1.1 -> 1855.1.1.1
    (r"^BMNH ([1-2]\d)(\.\d+\.\d+\.\d+)$", r"BMNH 19\1\2"),  # 11.1.1.1 -> 1911.1.1.1
    (r"^BMNH (\d{4,5})$", r"BMNH OR \1"),  # BMNH 12345 -> BMNH OR 12345
    (r"^BMNH (3[0-5])(\.\d+\.\d+\.\d+)$", r"BMNH 19\1\2"),  # 33.1.1.1 -> 1933.1.1.1
    (r"^BMNH (3[7-9])(\.\d+\.\d+\.\d+)$", r"BMNH 19\1\2"),  # 38.1.1.1 -> 1838.1.1.1
]


@LINT.add("bmnh_types")
def check_bmnh_type_specimens(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Fix some simple issues with BMNH type specimens."""
    if nam.type_specimen is None:
        return
    if nam.collection is None or nam.collection.id != BMNH_COLLECTION:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError:
        return  # other check will complain
    for spec in specs:
        if not isinstance(spec, Specimen) or not isinstance(spec.base, SimpleSpecimen):
            continue
        text = spec.base.text
        if not text.startswith("BMNH"):
            continue
        new_spec = clean_up_bmnh_type(text)
        if new_spec != text:
            message = f"replace {text!r} with {new_spec!r}"
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.type_specimen = nam.type_specimen.replace(text, new_spec)
            else:
                yield message


def get_all_type_specimen_texts(nam: Name) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    for spec in parse_type_specimen(nam.type_specimen):
        if isinstance(spec, Specimen):
            yield from _get_all_type_specimen_texts_from_specimen(spec)
        elif isinstance(spec, SpecimenRange):
            yield from _get_all_type_specimen_texts_from_specimen(spec.start)
            yield from _get_all_type_specimen_texts_from_specimen(spec.end)


def _get_all_type_specimen_texts_from_specimen(spec: Specimen) -> Iterable[str]:
    if isinstance(spec.base, SimpleSpecimen):
        yield spec.base.text
    elif isinstance(spec.base, TripletSpecimen):
        yield spec.base.stringify()


@LINT.add("type_specimen_link")
def check_must_have_type_specimen_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    # TODO: cover ExtraRepository, FormerRepository here
    # After replacing all TypeSpecimenLink tags. Then we should be able to associate every TypeSpecimenLinkFor tag with some part of the type_specimen text.
    if nam.collection is None or not nam.collection.must_have_specimen_links(nam):
        return
    if nam.type_specimen is None:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError:
        return  # other check will complain

    if nam.collection.id == MULTIPLE_COLLECTION:
        collections = [
            tag.repository for tag in nam.get_tags(nam.type_tags, TypeTag.Repository)
        ]
    else:
        collections = [nam.collection]

    actual_specimens = {
        tag.specimen
        for tag in nam.get_tags(nam.type_tags, TypeTag.TypeSpecimenLinkFor)
        if any(coll.is_valid_specimen_link(tag.url) for coll in collections)
    }

    for spec in specs:
        if isinstance(spec, SpecimenRange):
            yield f"type specimen range is not supported for links: {spec}"
            continue
        if isinstance(spec.base, (SpecialSpecimen, InformalSpecimen)):
            continue
        text = spec.base.stringify()
        if text not in actual_specimens:
            yield (
                f"missing type specimen link for {text!r} in {nam.type_specimen!r}. "
                "Add a TypeSpecimenLinkFor tag with the correct URL."
            )


@LINT.add("duplicate_type_specimen_links")
def check_duplicate_type_specimen_links(nam: Name, cfg: LintConfig) -> Iterable[str]:
    tags_with_specimens = {
        tag.url for tag in nam.type_tags if isinstance(tag, TypeTag.TypeSpecimenLinkFor)
    }
    if not tags_with_specimens:
        return
    new_tags = tuple(
        tag
        for tag in nam.type_tags
        if not isinstance(tag, TypeTag.TypeSpecimenLink)
        or tag.url not in tags_with_specimens
    )
    if nam.type_tags != new_tags:
        removed = set(nam.type_tags) - set(new_tags)
        message = f"remove redundant tags: {removed}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.type_tags = new_tags  # type: ignore[assignment]
        else:
            yield message


@LINT.add("replace_simple_type_specimen_link")
def replace_simple_type_specimen_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    if "," in nam.type_specimen or "(" in nam.type_specimen:
        return
    num_ts_links = sum(
        isinstance(tag, TypeTag.TypeSpecimenLink) for tag in nam.type_tags
    )
    if num_ts_links != 1:
        return
    new_tags = tuple(
        (
            TypeTag.TypeSpecimenLinkFor(tag.url, nam.type_specimen)
            if isinstance(tag, TypeTag.TypeSpecimenLink)
            and nam.type_specimen is not None
            else tag
        )
        for tag in nam.type_tags
    )
    message = "replace TypeSpecimenLink tag with TypeSpecimenLinkFor"
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.type_tags = new_tags  # type: ignore[assignment]
    else:
        yield message


def _parse_specimen_detail(text: str) -> dict[str, str] | None:
    match = re.fullmatch(r"(.*) \[at (https?://.*)\]", text)
    if match is None:
        return None
    rest = match.group(1)
    url = match.group(2)
    out = {"at": url}
    for piece in rest.split(" ... "):
        match = re.fullmatch(r"\[([^\]]+)\] (.*)", piece)
        if match:
            out[match.group(1)] = match.group(2)
    return out


@LINT.add("replace_type_specimen_link")
def replace_type_specimen_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    if not any(isinstance(tag, TypeTag.TypeSpecimenLink) for tag in nam.type_tags):
        return
    from_spec_details: dict[str, str] = {}
    for tag in nam.type_tags:
        if not isinstance(tag, TypeTag.SpecimenDetail):
            continue
        parsed = _parse_specimen_detail(tag.text)
        if not parsed:
            continue
        url = fix_type_specimen_link(parsed["at"])
        if "data.nhm.ac.uk" in url:
            catno = parsed["catalogNumber"]
            catno = (
                "BMNH "
                + catno.replace("NHMUK", "")
                .replace("ZD", "")
                .replace("GMCM", "")
                .replace("GERM", "")
                .strip()
            )
            catno = clean_up_bmnh_type(catno).replace("BMNH ", "BMNH Mammals ")
            catno = re.sub(r"\.\[[a-zA-Z]\]$", "", catno)
            catno = re.sub(r"\.([a-z])$", r"\1", catno)
        elif "coldb.mnhn.fr" in url:
            code = url.split("/")[-2].upper()
            catno = f"MNHN-{code}-{parsed['catalogNumber']}"
        elif "institutioncode" in parsed and "catalognumber" in parsed:
            catno = f"{parsed['institutioncode']} {parsed['catalognumber']}"
            catno = (
                catno.replace("UCMP ", "UCMP:V:")
                .replace("FMNH ", "FMNH Mammals ")
                .replace("UCLA ", "UCLA Mammals ")
                .replace("UF ", "UF:VP:")
            )
            catno = re.sub(r"^([A-Z]+) \1", r"\1", catno)
        else:
            continue
        from_spec_details[url] = catno

    new_tags = []
    messages = []
    possible_types = set(get_possible_type_specimens(nam))
    for tag in nam.type_tags:
        if not isinstance(tag, TypeTag.TypeSpecimenLink):
            new_tags.append(tag)
            continue
        specimen = None
        if tag.url in from_spec_details:
            specimen = from_spec_details[tag.url]
        elif tag.url.startswith("https://mczbase.mcz.harvard.edu/guid/"):
            specimen = tag.url.removeprefix("https://mczbase.mcz.harvard.edu/guid/")
        elif match := re.fullmatch(
            r"https?://portal\.vertnet\.org/o/ucla/mammals\?id=urn-catalog-ucla-mammals-(\d+)",
            tag.url,
        ):
            specimen = f"UCLA Mammals {match.group(1)}"
        elif match := re.fullmatch(
            r"http://coldb\.mnhn\.fr/catalognumber/mnhn/([a-z]+)/([a-z\-]+\d+)([a-z])",
            tag.url,
        ):
            specimen = f"MNHN-{match.group(1).upper()}-{match.group(2).upper()}{match.group(3)}"
        elif match := re.fullmatch(
            r"http://portal\.vertnet\.org/o/amnh/mammals\?id=urn-catalog-amnh-mammals-([a-z\d\-]+)",
            tag.url,
        ):
            specimen = f"AMNH {match.group(1).upper()}"

        if specimen is not None and specimen in possible_types:
            new_tag = TypeTag.TypeSpecimenLinkFor(tag.url, specimen)
            messages.append(f"{tag} -> {new_tag}")
        else:
            if specimen is not None:
                print(
                    f"Reject {specimen!r} (not in {possible_types} for {nam}, URL"
                    f" {tag.url})"
                )
            yield f"replace TypeSpecimenLink tag: {tag}"
            new_tag = tag
        new_tags.append(new_tag)

    if not messages:
        return
    message = (
        f"replace TypeSpecimenLink tag with TypeSpecimenLinkFor: {', '.join(messages)}"
    )
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.type_tags = new_tags  # type: ignore[assignment]
    else:
        yield message


def get_possible_type_specimens(nam: Name) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    for spec in parse_type_specimen(nam.type_specimen):
        match spec:
            case SpecimenRange(start=start, end=end):
                yield from _get_possible_type_specimens_from_specimen(start)
                yield from _get_possible_type_specimens_from_specimen(end)
            case Specimen():
                yield from _get_possible_type_specimens_from_specimen(spec)


def _get_possible_type_specimens_from_specimen(spec: Specimen) -> Iterable[str]:
    yield spec.base.stringify()
    for former in spec.former_texts:
        yield former.stringify()
    for future in spec.future_texts:
        yield future.stringify()
    for extra in spec.extra_texts:
        yield extra.stringify()


def clean_up_bmnh_type(text: str) -> str:
    for rgx, replacement in _BMNH_REGEXES:
        text = re.sub(rgx, replacement, text)
    return text


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
            my_original = nam.original_parent.resolve_name()
        if senior_name.original_parent is None:
            senior_original = None
        else:
            senior_original = senior_name.original_parent.resolve_name()
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
            my_genus = _get_parent(nam)
            senior_genus = _get_parent(senior_name)
            if my_genus != senior_genus:
                yield (
                    f"{nam} is marked as a secondary homonym of {senior_name}, but is not currently placed in the same genus"
                )
        elif isinstance(tag, NameTag.PreoccupiedBy):
            if my_original is not None and my_original == senior_original:
                new_tag = NameTag.PrimaryHomonymOf(tag.name, comment=tag.comment)
            elif _get_parent(nam) == _get_parent(senior_name):
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
) -> Generator[str, None, NameTag | None]:
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
    if not tag.name.nomenclature_status.can_preoccupy():
        if (
            target := tag.name.get_tag_target(NameTag.UnavailableVersionOf)
        ) and nam.year > target.year:
            yield f"tag {tag} should instead point to available name {target}"
            new_tag = type(tag)(target, comment=tag.comment)
    if (
        nam.taxon != tag.name.taxon
        and not isinstance(tag, NameTag.MisidentificationOf)
        and nam.get_tag_target(NameTag.MisidentificationOf) is None
    ):
        yield f"{nam} is not assigned to the same name as {tag.name}"
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
        if cfg.autofix and isinstance(tag, NameTag.NameCombinationOf):
            print(f"{nam}: changing NameCombinationOf to SubsequentUsageOf")
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
            new_tag = type(tag)(new_target, comment=tag.comment)
        yield message
    return new_tag


def _check_all_tags(
    tag: NameTag, nam: Name, cfg: LintConfig
) -> Generator[str, None, NameTag | None]:
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
                if cfg.autofix and tag.name.species_name_complex is not None:
                    print(f"{nam}: {message}")
                    nam.species_name_complex = tag.name.species_name_complex
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
    [NameTag, Name, LintConfig], Generator[str, None, NameTag | None]
]

TAG_CHECKERS: list[TagChecker] = [
    _check_preoccupation_tag,
    _check_variant_tag,
    _check_all_tags,
]


@LINT.add("tags")
def check_tags_for_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
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
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.tags = new_tags  # type: ignore[assignment]
        else:
            yield message


def _get_parent(nam: Name, rank: Rank = Rank.genus) -> Taxon | None:
    try:
        return nam.taxon.parent_of_rank(rank)
    except ValueError:
        return None


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


@functools.cache
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
) -> list[NomenclatureStatus]:
    return sorted(statuses, key=lambda status: _priority_map[status])


def get_applicable_statuses(nam: Name) -> set[NomenclatureStatus]:
    applicable_from_tags = set(get_applicable_nomenclature_statuses_from_tags(nam))
    inherent = set(get_inherent_nomenclature_statuses(nam))
    return applicable_from_tags | inherent


def get_sorted_applicable_statuses(nam: Name) -> list[NomenclatureStatus]:
    return sort_nomenclature_statuses(get_applicable_statuses(nam))


@LINT.add("expected_nomenclature_status")
def check_expected_nomenclature_status(nam: Name, cfg: LintConfig) -> Iterable[str]:
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
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.nomenclature_status = expected_status
        else:
            yield message


@LINT.add("redundant_fields")
def check_redundant_fields(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.nomenclature_status is not NomenclatureStatus.nomen_novum:
        return
    parent = nam.get_tag_target(NameTag.NomenNovumFor)
    for field in ("type_locality", "type_specimen", "collection", "species_type_kind"):
        value = getattr(nam, field)
        if value is None:
            continue
        can_autofix = parent is not None and getattr(parent, field) == value
        message = f"is a nomen novum and should not have the {field} field set ({can_autofix})"
        if cfg.autofix and can_autofix:
            print(f"{nam}: {message} (setting value to None)")
            setattr(nam, field, None)
        else:
            yield message


# disabled because it keeps timing out
@LINT.add("lsid", requires_network=True, disabled=True)
def check_for_lsid(nam: Name, cfg: LintConfig) -> Iterable[str]:
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
    name_lsids = {tag.text for tag in nam.get_tags(nam.type_tags, TypeTag.LSIDName)}
    rejected_lsids = {
        tag.text for tag in nam.get_tags(nam.type_tags, TypeTag.RejectedLSIDName)
    }
    art_lsids = {tag.text for tag in art.get_tags(art.tags, ArticleTag.LSIDArticle)}
    for zoobank_data in zoobank_data_list:
        if zoobank_data.name_lsid in rejected_lsids:
            continue
        if zoobank_data.name_lsid not in name_lsids:
            type_tags.append(TypeTag.LSIDName(zoobank_data.name_lsid))
        if zoobank_data.citation_lsid and zoobank_data.citation_lsid not in art_lsids:
            art_tag = ArticleTag.LSIDArticle(
                zoobank_data.citation_lsid,
                present_in_article=PresenceStatus.to_be_determined,
            )
            art_tags.append(art_tag)
    if not type_tags and not art_tags:
        return
    message = f"Inferred ZooBank data: {type_tags}, {art_tags}"
    if cfg.autofix:
        print(f"{nam}: {message}")
        for tag in type_tags:
            nam.add_type_tag(tag)
        for tag in art_tags:
            nam.original_citation.add_tag(tag)
    else:
        yield message


@LINT.add("year")
def check_year(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.year is None:
        return
    if not helpers.is_valid_date(nam.year):
        yield f"has invalid year {nam.year!r}"
    if helpers.is_date_range(nam.year):
        yield "year is a range"


@LINT.add("year_matches")
def check_year_matches(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_citation is None:
        return

    if nam.year != nam.original_citation.year:
        if cfg.autofix and helpers.is_more_specific_date(
            nam.original_citation.year, nam.year
        ):
            print(f"{nam}: fixing date {nam.year} -> {nam.original_citation.year}")
            nam.year = nam.original_citation.year
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


@LINT.add("disallowed_attributes")
def check_disallowed_attributes(nam: Name, cfg: LintConfig) -> Iterable[str]:
    for field_name, groups in ATTRIBUTES_BY_GROUP.items():
        if nam.group not in groups:
            value = getattr(nam, field_name)
            if value is not None:
                yield f"should not have attribute {field_name} (value {value})"


def _make_con_messsage(nam: Name, text: str) -> str:
    return f"corrected original name {nam.corrected_original_name!r} {text}"


CON_REGEX = re.compile(r"^[A-Z][a-z]+( [a-z]+){0,3}$")
CON_GENUS_FAMILY_REGEX = re.compile(r"^[A-Z][a-z]+$")
CON_HIGH_REGEX = re.compile(r"^(Pan-|Apo-|Zoo-)?[A-Z][a-z]+$")


@LINT.add("corrected_original_name")
def check_corrected_original_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Check that corrected_original_names are correct."""
    if nam.corrected_original_name is None:
        return
    if nam.nomenclature_status.permissive_corrected_original_name():
        return
    inferred = nam.infer_corrected_original_name()
    if (
        inferred is not None
        and inferred != nam.corrected_original_name
        and inferred.replace(" ", "") != nam.corrected_original_name.replace(" ", "")
    ):
        yield _make_con_messsage(
            nam,
            f"inferred name {inferred!r} does not match current name"
            f" {nam.corrected_original_name!r}",
        )
    if nam.group is Group.species:
        con_regex = CON_REGEX
    elif nam.group is Group.high:
        con_regex = CON_HIGH_REGEX
    else:
        con_regex = CON_GENUS_FAMILY_REGEX
    if not con_regex.fullmatch(nam.corrected_original_name):
        yield _make_con_messsage(nam, "contains unexpected characters")
        return
    if (
        nam.original_name is not None
        and nam.original_name != nam.corrected_original_name
        and nam.original_name.count(" ") == nam.corrected_original_name.count(" ")
        and con_regex.fullmatch(nam.original_name)
    ):
        yield _make_con_messsage(
            nam, f"is different from original name {nam.original_name!r}"
        )
    if nam.group in (Group.high, Group.genus):
        if " " in nam.corrected_original_name:
            yield _make_con_messsage(nam, "contains whitespace")
        elif nam.corrected_original_name != nam.root_name:
            emended = nam.get_tag_target(NameTag.AsEmendedBy)
            if emended is not None and emended.root_name == nam.root_name:
                return
            yield _make_con_messsage(nam, f"does not match root_name {nam.root_name!r}")
    elif nam.group is Group.species:
        parts = nam.corrected_original_name.split(" ")
        if len(parts) not in (2, 3, 4):
            yield _make_con_messsage(nam, "is not a valid species or subspecies name")
        elif parts[-1] != nam.root_name:
            emended = nam.get_tag_target(NameTag.AsEmendedBy)
            if emended is not None and emended.root_name == nam.root_name:
                return
            if nam.species_name_complex is not None:
                try:
                    forms = list(nam.species_name_complex.get_forms(nam.root_name))
                except ValueError as e:
                    yield _make_con_messsage(nam, f"has invalid name complex: {e!r}")
                    return
                if parts[-1] in forms:
                    return
            yield _make_con_messsage(nam, f"does not match root_name {nam.root_name!r}")


def _make_rn_message(nam: Name, text: str) -> str:
    return f"root name {nam.root_name!r} {text}"


@LINT.add("root_name")
def check_root_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Check that root_names are correct."""
    if nam.nomenclature_status.permissive_corrected_original_name():
        return
    match nam.group:
        case Group.high:
            if not re.match(CON_HIGH_REGEX, nam.root_name):
                yield _make_rn_message(nam, "contains unexpected characters")
        case Group.genus | Group.family:
            if not re.match(CON_GENUS_FAMILY_REGEX, nam.root_name):
                yield _make_rn_message(nam, "contains unexpected characters")
        case Group.species:
            if not re.match(r"^[a-z]+$", nam.root_name):
                yield _make_rn_message(nam, "contains unexpected characters")
                return
            yield from _check_species_name_gender(nam, cfg)
        case _:
            assert_never(nam.group)


def _check_rn_matches_original(
    nam: Name, corrected_original_name: str, cfg: LintConfig, reason: str
) -> Iterable[str]:
    con_root = corrected_original_name.split()[-1]
    if con_root == nam.root_name:
        return
    message = _make_con_messsage(
        nam, f"does not match root_name {nam.root_name!r} ({reason})"
    )
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.root_name = con_root
    else:
        yield message


def _check_species_name_gender(nam: Name, cfg: LintConfig) -> Iterable[str]:
    emending_name = nam.get_tag_target(NameTag.AsEmendedBy)
    if emending_name is not None:
        corrected_original_name = emending_name.corrected_original_name
    else:
        corrected_original_name = nam.corrected_original_name
    if corrected_original_name is not None:
        con_root = corrected_original_name.split()[-1]
    else:
        con_root = None
    # For nomina dubia we always follow the original name
    if nam.status in (Status.nomen_dubium, Status.species_inquirenda):
        if corrected_original_name is not None:
            yield from _check_rn_matches_original(
                nam, corrected_original_name, cfg, nam.status.name
            )
        return
    # If there is no name complex, the root_name should match exactly
    if nam.species_name_complex is None:
        if corrected_original_name is not None:
            yield from _check_rn_matches_original(
                nam, corrected_original_name, cfg, "no name complex"
            )
        return
    if nam.species_name_complex.kind is not SpeciesNameKind.adjective:
        if corrected_original_name is not None:
            yield from _check_rn_matches_original(
                nam, corrected_original_name, cfg, "not an adjective"
            )
        return
    if nam.species_name_complex.is_invariant_adjective():
        if corrected_original_name is not None:
            yield from _check_rn_matches_original(
                nam, corrected_original_name, cfg, "invariant adjective"
            )
        return
    if not nam.nomenclature_status.can_preoccupy() and nam.nomenclature_status not in (
        NomenclatureStatus.subsequent_usage,
        NomenclatureStatus.name_combination,
    ):
        if nam.corrected_original_name is None:
            return
        expected_form = nam.corrected_original_name.split()[-1]
        motivation = f"to match corrected original name {nam.corrected_original_name!r}"
        rn_message = "for original name"
    else:
        # Now we have an adjective that needs to agree in gender with its genus, so we
        # have to find the genus. But first we check whether the name even makes sense.
        try:
            forms = list(nam.species_name_complex.get_forms(nam.root_name))
        except ValueError as e:
            yield _make_con_messsage(nam, f"has invalid name complex: {e!r}")
            return
        if con_root is not None and con_root not in forms:
            yield _make_con_messsage(nam, f"does not match root_name {nam.root_name!r}")
            return

        taxon = nam.taxon
        genus = taxon.get_current_genus()
        if genus is None or genus.name_complex is None:
            return

        genus_gender = genus.name_complex.gender
        expected_form = nam.species_name_complex.get_form(nam.root_name, genus_gender)
        motivation = f"to agree in gender with {genus_gender.name} genus {genus} ({{n#{genus.id}}})"
        rn_message = f"for {genus_gender.name} genus {genus}"
    if expected_form != nam.root_name:
        message = _make_rn_message(
            nam, f"does not match expected form {expected_form!r} {rn_message}"
        )
        if cfg.autofix:
            print(f"{nam}: {message}")
            comment = (
                f"Name changed from {nam.root_name!r} to {expected_form!r}{motivation}"
            )
            nam.add_static_comment(CommentKind.automatic_change, comment)
            nam.root_name = expected_form
        else:
            yield message


@LINT.add("family_root_name")
def check_family_root_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group is not Group.family:
        return
    if set(get_applicable_nomenclature_statuses_from_tags(nam)) & {
        NomenclatureStatus.not_based_on_a_generic_name,
        NomenclatureStatus.not_intended_as_a_scientific_name,
    }:
        if nam.root_name != nam.corrected_original_name:
            yield _make_rn_message(nam, "does not match corrected original name")
        return
    if nam.original_rank is None:
        return
    expected_suffix = helpers.SUFFIXES.get(nam.original_rank)
    if expected_suffix is not None and not nam.root_name.endswith(expected_suffix):
        yield _make_rn_message(
            nam, f"does not match original rank ending -{expected_suffix}"
        )
    if nam.type is None:
        return
    resolved_type = nam.type.resolve_variant()
    try:
        stem_name = resolved_type.get_stem()
    except ValueError:
        yield f"{resolved_type} has bad name complex: {resolved_type.name_complex}"
        return
    if stem_name is None:
        yield f"type {resolved_type} has no stem"
        return
    if expected_suffix is None:
        expected_suffix = ""
    expected_root_name = stem_name + expected_suffix
    if nam.root_name == expected_root_name:
        return
    if stem_name.endswith("id"):
        # The Code allows eliding -id- from the stem.
        if nam.root_name == stem_name.removesuffix("id") + expected_suffix:
            return
    if nam.has_type_tag(TypeTag.IncorrectGrammar):
        return
    yield _make_rn_message(nam, f"does not match expected stem {expected_root_name!r}")
    if nam.root_name == stem_name:
        message = f"Autofixing root name: {nam.root_name} -> {expected_root_name}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.root_name = expected_root_name
        else:
            yield message
    elif stem_name.endswith("id") and nam.root_name == stem_name.removesuffix("id"):
        message = f"Autofixing root name: {nam.root_name} -> {expected_root_name}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.root_name = stem_name.removesuffix("id") + expected_suffix
        else:
            yield message


@LINT.add("type_taxon")
def correct_type_taxon(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Moves names to child taxa if the type allows it."""
    if nam.group not in (Group.genus, Group.family):
        return
    if nam.type is None:
        return
    if nam.taxon == nam.type.taxon:
        return
    expected_taxon = nam.type.taxon.parent
    while (
        expected_taxon is not None
        and expected_taxon.base_name.group != nam.group
        and expected_taxon != nam.taxon
    ):
        expected_taxon = expected_taxon.parent
    if expected_taxon is None:
        return
    if nam.taxon != expected_taxon:
        message = f"expected taxon to be {expected_taxon} not {nam.taxon}"
        if cfg.autofix and expected_taxon.is_child_of(nam.taxon):
            print(f"{nam}: {message}")
            nam.taxon = expected_taxon
        else:
            yield message


@LINT.add("type")
def check_type(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Checks for the type taxon."""
    if nam.type is None:
        return
    if not nam.type.taxon.is_child_of(nam.taxon):
        yield f"type {nam.type} is not a child of {nam.taxon}"

    if (
        (target := nam.type.get_tag_target(NameTag.UnavailableVersionOf)) is not None
        and nam.get_date_object() >= target.get_date_object()
        and not nam.has_name_tag(NameTag.UnavailableVersionOf)
    ):
        message = f"type is an unavailable version of {target}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.type = target
        else:
            yield message

    match nam.group:
        case Group.family:
            if nam.type.group is not Group.genus:
                yield f"type {nam.type} is in group {nam.type.group!r}, not genus"
        case Group.genus:
            if nam.type.group is not Group.species:
                yield f"type {nam.type} is in group {nam.type.group!r}, not species"


@LINT.add("infer_family_group_type")
def infer_family_group_type(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if (
        nam.group is not Group.family
        or nam.type is not None
        or "type" not in nam.get_required_fields()
    ):
        return
    stem = nam.get_family_group_stem()
    possible_types = [
        child_nam
        for child_nam in nam.taxon.all_names()
        if child_nam.group is Group.genus
        and child_nam.name_complex is not None
        and child_nam.safe_get_stem() == stem
    ]
    if len(possible_types) != 1:
        if cfg.verbose:
            print(f"{nam}: could not infer type for family {nam.root_name}")
        return
    message = f"inferred type {possible_types[0]} for family {nam}"
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.type = possible_types[0]
    else:
        yield message


@LINT.add("name_complex")
def check_name_complex(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if (
        nam.name_complex is not None
        and nam.original_citation is not None
        and nam.name_complex.code_article is GenderArticle.assumed
    ):
        yield "'assumed' name_complex for name with original_citation"
    try:
        nam.get_stem()
    except ValueError as e:
        yield f"bad name complex {nam.name_complex}: {e}"
    if (
        nam.species_name_complex is not None
        and nam.species_name_complex.kind is SpeciesNameKind.adjective
    ):
        try:
            nam.species_name_complex.get_stem_from_name(nam.root_name)
        except ValueError as e:
            yield f"has invalid name complex {nam.species_name_complex}: {e!r}"


@LINT.add("verbatim")
def clean_up_verbatim(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.verbatim_citation is not None and nam.original_citation is not None:
        message = (
            f"cleaning up verbatim citation: {nam.original_citation.name},"
            f" {nam.verbatim_citation}"
        )
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_data(
                "verbatim_citation", nam.verbatim_citation, concat_duplicate=True
            )
            nam.verbatim_citation = None
        else:
            yield message
    if nam.citation_group is not None and nam.original_citation is not None:
        message = (
            f"cleaning up citation group: {nam.original_citation.name},"
            f" {nam.citation_group}"
        )
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.citation_group = None
        else:
            yield message


@LINT.add("verbatim_to_citation_detail")
def verbatim_to_citation_detail(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.verbatim_citation is None:
        return
    for whole_match, source, text in re.findall(
        r"(\[From \{([^}]+)\}: ([^\]]+)\])", nam.verbatim_citation
    ):
        try:
            source_art = (
                Article.select().filter(Article.name == source).get().resolve_redirect()
            )
        except clirm.DoesNotExist:
            continue
        tag = TypeTag.CitationDetail(text, source_art)
        message = (
            f"converting verbatim citation to citation detail: {whole_match} -> {tag}"
        )
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_type_tag(tag)
            nam.verbatim_citation = nam.verbatim_citation.replace(whole_match, "")
        else:
            yield message
    match = re.fullmatch(
        r"([^\]]+(?: \[[^\[\]\{\}]+\])?|[^{]+) \[from \{([^\}]+)\}\]",
        nam.verbatim_citation,
    )
    if match:
        text, source = match.groups()
        try:
            source_art = (
                Article.select().filter(Article.name == source).get().resolve_redirect()
            )
        except clirm.DoesNotExist:
            pass
        else:
            tag = TypeTag.CitationDetail(text, source_art)
            message = f"converting verbatim citation to citation detail: {nam.verbatim_citation} -> {tag}"
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.add_type_tag(tag)
                nam.verbatim_citation = None
            else:
                yield message

    # if nam.verbatim_citation is not None and "{" in nam.verbatim_citation:
    #     yield f"unhandled verbatim citation: {nam.verbatim_citation}"


@LINT.add("verbatim_from_tags")
def verbatim_citation_from_tags(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.verbatim_citation:
        return
    if "verbatim_citation" not in nam.get_required_fields():
        return
    tags = list(nam.get_tags(nam.type_tags, TypeTag.CitationDetail))
    if not tags:
        return
    longest = max(tags, key=lambda tag: len(tag.text))
    message = f"setting verbatim citation from tag {longest}"
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.verbatim_citation = longest.text
    else:
        yield message


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


@LINT.add("autoset_original_rank")
def autoset_original_rank(nam: Name, cfg: LintConfig) -> Iterable[str]:
    nam.autoset_original_rank(dry_run=not cfg.autofix)
    return []


@LINT.add("corrected_original_name")
def autoset_corrected_original_name(
    nam: Name, cfg: LintConfig, *, aggressive: bool = False
) -> Iterable[str]:
    if nam.original_name is None or nam.corrected_original_name is not None:
        return
    if "corrected_original_name" not in nam.get_required_fields():
        return
    inferred = nam.infer_corrected_original_name(aggressive=aggressive)
    if inferred:
        message = (
            f"inferred corrected_original_name to be {inferred!r} from"
            f" {nam.original_name!r}"
        )
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.corrected_original_name = inferred
        else:
            yield message
    else:
        yield f"could not infer corrected original name from {nam.original_name!r}"


@LINT.add("data_level")
def check_data_level(nam: Name, cfg: LintConfig) -> Iterable[str]:
    ocdl, ocdl_reason = nam.original_citation_data_level()
    ndl, ndl_reason = nam.name_data_level()
    match ocdl:
        case OriginalCitationDataLevel.no_citation:
            pass
        case OriginalCitationDataLevel.no_data:
            if ndl < NameDataLevel.missing_details_tags and not (
                nam.nomenclature_status is NomenclatureStatus.name_combination
                and nam.original_citation is not None
                and _is_msw3(nam.original_citation)
            ):
                yield f"has no data from original ({ocdl_reason}), but missing important fields: {ndl_reason}"
        case OriginalCitationDataLevel.some_data:
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


@LINT.add("matches_citation")
def check_matches_citation(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_citation is None or nam.page_described is None:
        return
    art = nam.original_citation
    yield from check_page_matches_citation(art, nam.page_described)


def check_page_matches_citation(art: Article, page_text: str) -> Iterable[str]:
    if art.type not in (ArticleType.JOURNAL, ArticleType.CHAPTER, ArticleType.PART):
        return
    start_page = art.numeric_start_page()
    end_page = art.numeric_end_page()
    if not start_page or not end_page:
        return
    page_range = range(start_page, end_page + 1)
    for page in parse_page_text(page_text):
        if page.is_raw:
            continue
        try:
            numeric_page = int(page.text)
        except ValueError:
            continue
        if numeric_page not in page_range:
            yield f"{page_text} is not in {start_page}–{end_page} for {art}"


@LINT.add("no_page_ranges")
def no_page_ranges(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.page_described is None:
        return
    # Only applicable if there is a citation. This ensures we check
    # if the range is appropriate when adding a citation.
    if nam.original_citation is None:
        return
    for part in parse_page_text(nam.page_described):
        if not part.is_raw and re.fullmatch(r"[0-9]+-[0-9]+", part.text):
            # Ranges should only be used in very rare cases (e.g., where the
            # name itself literally extends across multiple pages). Enforce
            # an explicit IgnoreLintName in such cases.
            yield f"page_described contains range: {part}"


@LINT.add("infer_page_described")
def infer_page_described(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if (
        nam.page_described is not None
        or nam.verbatim_citation is None
        or nam.original_citation is not None
    ):
        return
    cite = nam.verbatim_citation
    while True:
        new_cite = re.sub(r"\[[^\[\]]+\]", " ", cite).strip().rstrip(".")
        new_cite = re.sub(r"\([A-Za-z ]+\)", " ", new_cite).strip().rstrip(".")
        # remove trailing date
        new_cite = re.sub(
            r", (\d{1,2} )?([A-Z][a-z][a-z][a-z]?\.?\s)?1[789]\d{2}[a-e]?$",
            "",
            new_cite,
        ).strip()
        # remove trailing "fig." or "pl."
        new_cite = re.sub(r", ?(pl|pls|fig|figs|pi)\. ?\d[\d\-]*\.?$", "", new_cite)
        # remove trailing "fig." or "pl."
        new_cite = re.sub(r", \d+ ?(pl|pls|fig|figs|pi)\.?$", "", new_cite)
        if cite == new_cite:
            break
        cite = new_cite
    if match := re.search(r"(?:\bp ?\.|\bS\.|:)\s*(\d{1,4})\.?$", cite):
        page = match.group(1)
    elif match := re.search(
        r"(?:\bp\.|pp\.|\bS\.|:|,)\s*(\d{1,4}) ?[-–e] ?(\d{1,4})\.?$", cite
    ):
        page = f"{match.group(1)}-{match.group(2)}"
    else:
        return
    message = f"infer page {page!r} from {nam.verbatim_citation!r}"
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.page_described = page
    else:
        yield message


@LINT.add("page_described")
def check_page_described(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.page_described is None:
        return
    # For now ignore names without a citation
    if nam.original_citation is None:
        return

    def set_page(page_described: str) -> None:
        nam.page_described = page_described

    yield from check_page(
        nam.page_described,
        set_page=set_page,
        obj=nam,
        cfg=cfg,
        get_raw_page_regex=nam.original_citation.get_raw_page_regex,
    )


_JG2015 = "{Mammalia Australia (Jackson & Groves 2015).pdf}"
_JG2015_RE = re.compile(rf"\[From {re.escape(_JG2015)}: [^\[\]]+ \[([A-Za-z\s\d]+)\]\]")
_JG2015_RE2 = re.compile(rf" \[([A-Za-z\s\d]+)\]\ \[from {re.escape(_JG2015)}\]")


@LINT.add("extract_date_from_verbatim")
def extract_date_from_verbatim(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_citation is None or not nam.data:
        return
    # Extract precise dates from references from Jackson & Groves (2015). This sometimes
    # produces incorrect results if the names aren't associated correctly. In that case,
    # edit the name that produced the bad tag to put "[error]" in its data field so the
    # regexes above don't match.
    try:
        verbatim = nam.get_data("verbatim_citation")
    except (KeyError, TypeError, json.JSONDecodeError):
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
) -> Iterator[str]:
    article = nam.original_citation
    if article is None:
        return
    tag = ArticleTag.PublicationDate(
        source=DateSource.external, date=parsed, comment=f'"{raw_date}" {source}'
    )
    if tag in (article.tags or ()):
        return
    message = f'inferred date for {article} from raw date "{raw_date}": {parsed}'
    if cfg.autofix:
        print(f"{nam}: {message}")
        article.add_tag(tag)
    else:
        yield message


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
def extract_date_from_structured_quotes(nam: Name, cfg: LintConfig) -> Iterable[str]:
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


@LINT.add("required_fields")
def check_required_fields(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.verbatim_citation and not nam.citation_group:
        yield "has verbatim citation but no citation group"
    if (
        nam.original_citation
        and not nam.page_described
        and nam.original_citation.kind is not ArticleKind.no_copy
        and not nam.original_citation.has_tag(ArticleTag.NonOriginal)
        and not (
            nam.nomenclature_status is NomenclatureStatus.name_combination
            and _is_msw3(nam.original_citation)
        )
        and "page_described" in nam.get_required_fields()
    ):
        yield "has original citation but no page_described"
    if (
        nam.numeric_year() > 1970
        and not nam.verbatim_citation
        and not nam.original_citation
    ):
        yield "recent name must have verbatim citation"
    if nam.species_type_kind is None and nam.nomenclature_status.requires_type():
        if nam.type_specimen is not None:
            yield "has type_specimen but no species_type_kind"
        if nam.has_type_tag(TypeTag.Age) or nam.has_type_tag(TypeTag.Gender):
            yield "has type specimen age or gender but no species_type_kind"


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


@dataclass
class PossibleHomonym:
    exact_name_match: bool = False
    fuzzy_name_match: bool = False
    same_original_genus: bool = False
    same_current_genus: bool = False
    edit_distance: int | None = None
    related_genus: bool = False

    def is_current_homonym(self) -> bool:
        return (self.same_original_genus or self.same_current_genus) and (
            self.exact_name_match or self.fuzzy_name_match
        )


def get_possible_homonyms(
    genus_name: str, root_name: str, sc: SpeciesNameComplex | None = None
) -> tuple[Iterable[Name], Iterable[tuple[Name, PossibleHomonym]]]:
    name_dict: dict[Name, PossibleHomonym] = defaultdict(PossibleHomonym)
    normalized_root_name = normalize_root_name_for_homonymy(root_name, sc)
    genera = {
        genus.resolve_name()
        for genus in Name.select_valid().filter(
            Name.group == Group.genus, Name.root_name == genus_name
        )
    }
    for genus in genera:
        primary_nonfuzzy = _get_primary_names_of_genus_and_variants(genus, fuzzy=False)
        for nam in primary_nonfuzzy.get(root_name, []):
            name_dict[nam].exact_name_match = True
            name_dict[nam].same_original_genus = True
        for other_root_name, nams in primary_nonfuzzy.items():
            distance = Levenshtein.distance(root_name, other_root_name)
            if distance <= 2:
                for nam in nams:
                    name_dict[nam].same_original_genus = True
                    name_dict[nam].edit_distance = distance
        primary_fuzzy = _get_primary_names_of_genus_and_variants(genus, fuzzy=True)
        for nam in primary_fuzzy.get(normalized_root_name, []):
            name_dict[nam].fuzzy_name_match = True
            name_dict[nam].same_original_genus = True
        taxon = _get_parent(genus)
        if taxon is not None:
            secondary_nonfuzzy = _get_secondary_names_of_genus(taxon, fuzzy=False)
            for nam in secondary_nonfuzzy.get(root_name, []):
                name_dict[nam].exact_name_match = True
                name_dict[nam].same_current_genus = True
            for other_root_name, nams in secondary_nonfuzzy.items():
                distance = Levenshtein.distance(root_name, other_root_name)
                if distance <= 2:
                    for nam in nams:
                        name_dict[nam].same_current_genus = True
                        name_dict[nam].edit_distance = distance

            secondary_fuzzy = _get_secondary_names_of_genus(taxon, fuzzy=True)
            for nam in secondary_fuzzy.get(normalized_root_name, []):
                name_dict[nam].fuzzy_name_match = True
                name_dict[nam].same_current_genus = True

            if taxon.parent is not None:
                for related_genus in taxon.parent.get_children().filter(
                    Taxon.rank == Rank.genus
                ):
                    secondary_nonfuzzy = _get_secondary_names_of_genus(
                        related_genus, fuzzy=False
                    )
                    for other_root_name, nams in secondary_nonfuzzy.items():
                        distance = Levenshtein.distance(root_name, other_root_name)
                        if distance <= 2:
                            for nam in nams:
                                name_dict[nam].related_genus = True
                                name_dict[nam].edit_distance = distance

    return genera, name_dict.items()


def _clear_homonym_caches() -> None:
    _get_primary_names_of_genus.cache_clear()
    _get_secondary_names_of_genus.cache_clear()


@LINT.add("species_secondary_homonym", clear_caches=_clear_homonym_caches)
def check_species_group_secondary_homonyms(nam: Name, cfg: LintConfig) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.secondary_homonymy, fuzzy=False, cfg=cfg
    )


@LINT.add("species_primary_homonym")
def check_species_group_primary_homonyms(nam: Name, cfg: LintConfig) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.primary_homonymy, fuzzy=False, cfg=cfg
    )


@LINT.add("species_mixed_homonym", disabled=True)
def check_species_group_mixed_homonyms(nam: Name, cfg: LintConfig) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.mixed_homonymy, fuzzy=False, cfg=cfg
    )


@LINT.add("species_reverse_mixed_homonym", disabled=True)
def check_species_group_reverse_mixed_homonyms(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.reverse_mixed_homonymy, fuzzy=False, cfg=cfg
    )


@LINT.add("species_fuzzy_secondary_homonym")
def check_species_group_fuzzy_secondary_homonyms(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.secondary_homonymy, fuzzy=True, cfg=cfg
    )


@LINT.add("species_fuzzy_primary_homonym")
def check_species_group_fuzzy_primary_homonyms(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.primary_homonymy, fuzzy=True, cfg=cfg
    )


@LINT.add("species_fuzzy_mixed_homonym", disabled=True)
def check_species_group_fuzzy_mixed_homonyms(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.mixed_homonymy, fuzzy=True, cfg=cfg
    )


@LINT.add("species_fuzzy_reverse_mixed_homonym", disabled=True)
def check_species_group_fuzzy_reverse_mixed_homonyms(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    yield from _check_species_group_homonyms(
        nam, reason=SelectionReason.reverse_mixed_homonymy, fuzzy=True, cfg=cfg
    )


@LINT.add("genus_homonym")
def check_genus_group_homonyms(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group is not Group.genus:
        return
    if not nam.can_preoccupy():
        return
    possible_homonyms = list(
        Name.select_valid().filter(
            Name.root_name == nam.root_name, Name.group == Group.genus
        )
    )
    yield from _check_homonym_list(nam, possible_homonyms, cfg=cfg)


def _check_species_group_homonyms(
    nam: Name, *, reason: SelectionReason, fuzzy: bool, cfg: LintConfig
) -> Iterable[str]:
    if nam.group is not Group.species:
        return
    if not nam.can_preoccupy():
        return
    # TODO: pyanalyze otherwise thinks it may be uninitialized
    name_dict: dict[str, list[Name]] = {}
    match reason:
        case SelectionReason.primary_homonymy:
            genus = nam.original_parent
            if genus is None:
                return
            genus = genus.resolve_name()
            name_dict = _get_primary_names_of_genus_and_variants(genus, fuzzy=fuzzy)
        case SelectionReason.secondary_homonymy:
            genus = _get_parent(nam)
            if genus is None:
                return
            name_dict = _get_secondary_names_of_genus(genus, fuzzy=fuzzy)
        case SelectionReason.mixed_homonymy:
            genus = _get_parent(nam)
            if genus is None:
                return
            genus = genus.base_name.resolve_name()
            name_dict = _get_primary_names_of_genus_and_variants(genus, fuzzy=fuzzy)
        case SelectionReason.reverse_mixed_homonymy:
            genus = nam.original_parent
            if genus is None:
                return
            name_dict = _get_secondary_names_of_genus(genus.taxon, fuzzy=fuzzy)
        case SelectionReason.synonymy:
            return
        case _:
            assert_never(reason)
    root = (
        nam.get_normalized_root_name_for_homonymy()
        if fuzzy
        else nam.get_normalized_root_name()
    )
    possible_homonyms = name_dict.get(root, [])
    yield from _check_homonym_list(
        nam, possible_homonyms, reason=reason, fuzzy=fuzzy, cfg=cfg
    )


def _check_homonym_list(
    nam: Name,
    possible_homonyms: Sequence[Name],
    *,
    reason: SelectionReason = SelectionReason.primary_homonymy,
    fuzzy: bool = False,
    cfg: LintConfig,
) -> Iterable[str]:
    relevant_names = [
        other_nam
        for other_nam in possible_homonyms
        if nam != other_nam and other_nam.can_preoccupy()
        # TODO: not nam.has_priority_over(other_nam)
        and other_nam.has_priority_over(nam)
    ]
    if fuzzy:
        # Exclude non-fuzzy matches, the regular check will catch them.
        relevant_names = [
            other_nam
            for other_nam in relevant_names
            if other_nam.get_normalized_root_name() != nam.get_normalized_root_name()
        ]

    if not relevant_names:
        return
    relevant_tags: tuple[type[NameTag], ...] = (
        NameTag.PreoccupiedBy,
        NameTag.UnjustifiedEmendationOf,
        NameTag.JustifiedEmendationOf,
        NameTag.PrimaryHomonymOf,
        NameTag.SecondaryHomonymOf,
        NameTag.VariantOf,
        NameTag.IncorrectSubsequentSpellingOf,
        NameTag.SubsequentUsageOf,
        NameTag.MisidentificationOf,
        NameTag.NameCombinationOf,
        NameTag.AsEmendedBy,
        NameTag.RerankingOf,
    )
    if fuzzy:
        # Allow ignoring preoccupation only for fuzzy matches
        relevant_tags += (NameTag.IgnorePreoccupationBy,)
    already_variant_of = {
        tag.name for tag in nam.tags if isinstance(tag, relevant_tags)
    }

    for senior_homonym in relevant_names:
        # Ignore secondary homonyms that are also primary homonyms
        if (
            reason is SelectionReason.secondary_homonymy
            and nam.original_parent is not None
            and senior_homonym.original_parent == nam.original_parent
        ):
            continue

        # Ignore nomina oblita
        if any(
            isinstance(tag, NameTag.NomenOblitum) and tag.name == nam
            for tag in senior_homonym.tags
        ):
            continue

        # Already marked as a homonym
        if senior_homonym in already_variant_of:
            continue

        # Marked as needing resolution through First Reviser action
        if nam.get_date_object() == senior_homonym.get_date_object() and any(
            isinstance(tag, NameTag.NeedsPrioritySelection)
            and tag.reason == reason
            and tag.over == senior_homonym
            for tag in nam.tags
        ):
            continue

        if cfg.interactive:
            getinput.print_header(nam)
            print(f"{nam}: preoccupied by {senior_homonym}")
            if getinput.yes_no("Accept preoccupation? "):
                if nam.group is Group.species:
                    if reason is SelectionReason.primary_homonymy:
                        tag = NameTag.PrimaryHomonymOf
                    else:
                        tag = NameTag.SecondaryHomonymOf
                else:
                    tag = NameTag.PreoccupiedBy
                nam.preoccupied_by(senior_homonym, tag=tag)
            elif (
                nam.get_date_object() == senior_homonym.get_date_object()
                and getinput.yes_no("Mark as needing First Reviser selection instead? ")
            ):
                nam.add_tag(
                    NameTag.NeedsPrioritySelection(over=senior_homonym, reason=reason)
                )
            elif getinput.yes_no("Mark as subsequent usage instead? "):
                nam.add_tag(NameTag.SubsequentUsageOf(senior_homonym))
        yield f"preoccupied by {senior_homonym}"


def _get_primary_names_of_genus_and_variants(
    genus: Name, *, fuzzy: bool = False
) -> dict[str, list[Name]]:
    all_genera = {genus}
    stack = [genus]
    while stack:
        current = stack.pop()
        for nam in itertools.chain(
            current.get_derived_field("variants") or (),
            current.get_derived_field("unjustified_emendations") or (),
            current.get_derived_field("justified_emendations") or (),
            current.get_derived_field("incorrect_original_spellings") or (),
            current.get_derived_field("subsequent_usages") or (),
            current.get_derived_field("mandatory_changes") or (),
            current.get_derived_field("incorrect_subsequent_spellings") or (),
        ):
            if nam not in all_genera:
                all_genera.add(nam)
                stack.append(nam)
    if len(all_genera) == 1:
        return _get_primary_names_of_genus(genus, fuzzy=fuzzy)
    root_name_to_names: dict[str, list[Name]] = {}
    for nam in all_genera:
        for root_name, names in _get_primary_names_of_genus(nam, fuzzy=fuzzy).items():
            root_name_to_names.setdefault(root_name, []).extend(names)
    return root_name_to_names


@functools.lru_cache(maxsize=8192)
def _get_primary_names_of_genus(
    genus: Name, *, fuzzy: bool = False
) -> dict[str, list[Name]]:
    root_name_to_names: dict[str, list[Name]] = {}
    for nam in Name.add_validity_check(genus.original_children):
        if nam.group is Group.species and nam.year is not None:
            if fuzzy:
                root_name = nam.get_normalized_root_name_for_homonymy()
            else:
                root_name = nam.get_normalized_root_name()
            root_name_to_names.setdefault(root_name, []).append(nam)
    return root_name_to_names


@functools.lru_cache(maxsize=8192)
def _get_secondary_names_of_genus(
    genus: Taxon, *, fuzzy: bool = False
) -> dict[str, list[Name]]:
    root_name_to_names: dict[str, list[Name]] = {}
    for nam in genus.all_names():
        if nam.group is Group.species and nam.year is not None:
            if fuzzy:
                root_name = nam.get_normalized_root_name_for_homonymy()
            else:
                root_name = nam.get_normalized_root_name()
            root_name_to_names.setdefault(root_name, []).append(nam)
    return root_name_to_names


def should_require_subgenus_original_parent(nam: Name) -> bool:
    if nam.original_rank not in (Rank.subgenus, Rank.other_subgeneric):
        return True
    # Should ideally be set for all names, but let's lock in the progress already made
    if nam.numeric_year() > 1909:
        return True
    if nam.original_citation is not None and nam.original_citation.id > 60332:
        return True
    return False


def resolve_usage(nam: Name, *, resolve_unavailable_version_of: bool) -> Name:
    if target := nam.get_tag_target(NameTag.SubsequentUsageOf):
        return resolve_usage(
            target, resolve_unavailable_version_of=resolve_unavailable_version_of
        )
    if target := nam.get_tag_target(NameTag.MisidentificationOf):
        return resolve_usage(
            target, resolve_unavailable_version_of=resolve_unavailable_version_of
        )
    if target := nam.get_tag_target(NameTag.NameCombinationOf):
        return resolve_usage(
            target, resolve_unavailable_version_of=resolve_unavailable_version_of
        )
    if target := nam.get_tag_target(NameTag.RerankingOf):
        return resolve_usage(
            target, resolve_unavailable_version_of=resolve_unavailable_version_of
        )
    if resolve_unavailable_version_of:
        if target := nam.get_tag_target(NameTag.UnavailableVersionOf):
            return resolve_usage(
                target, resolve_unavailable_version_of=resolve_unavailable_version_of
            )
    return nam


@LINT.add("check_original_parent")
def check_original_parent(nam: Name, cfg: LintConfig) -> Iterable[str]:
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
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.original_parent = alternative
            else:
                yield message

    if (
        target := nam.original_parent.get_tag_target(NameTag.UnavailableVersionOf)
    ) is not None and nam.get_date_object() >= target.get_date_object():
        message = f"original_parent is an unavailable version of {target}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.original_parent = target
        else:
            yield message

    if nam.original_parent.nomenclature_status in (
        NomenclatureStatus.subsequent_usage,
        NomenclatureStatus.misidentification,
    ):
        target = resolve_usage(
            nam.original_parent, resolve_unavailable_version_of=False
        )
        message = f"original_parent is a subsequent usage or misidentification: {nam.original_parent} (change to {target})"
        if cfg.autofix and nam.original_parent != target:
            print(f"{nam}: {message}")
            nam.original_parent = target
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
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.original_parent = alternatives[0]
            else:
                yield message

    if nam.group is Group.species and nam.corrected_original_name is not None:
        original_genus, *_ = nam.corrected_original_name.split()
        # corrected_original_name is for the case where the genus name got a justified emendation
        if original_genus not in (
            nam.original_parent.root_name,
            nam.original_parent.corrected_original_name,
        ):
            yield (
                f"original_parent {nam.original_parent} does not match corrected"
                f" original name {nam.corrected_original_name}"
            )


@LINT.add("infer_original_parent")
def infer_original_parent(nam: Name, cfg: LintConfig) -> Iterable[str]:
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
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.original_parent = original_parent
        else:
            yield message
    elif (nam.group is Group.species) and nam.corrected_original_name is not None:
        candidates = _get_inferred_original_parent(nam)
        if len(candidates) != 1:
            return
        message = f"inferred original_parent to be {candidates} from {nam.corrected_original_name}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.original_parent = candidates[0]
        else:
            yield message


def _get_inferred_original_parent(nam: Name) -> list[Name]:
    if nam.corrected_original_name is None:
        return []
    original_genus, *_ = nam.corrected_original_name.split()

    # Is it still in the same genus? If so, use that genus.
    current_genus = _get_parent(nam)
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
    current_order = _get_parent(nam, Rank.order)
    same_order_genera = [
        genus
        for genus in possible_genera
        if _get_parent(genus, Rank.order) == current_order
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
def check_infrasubspecific(nam: Name, cfg: LintConfig) -> Iterable[str]:
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
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_tag(NameTag.VarietyOrForm())
        else:
            yield message
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
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_tag(NameTag.Condition(NomenclatureStatus.infrasubspecific))
        else:
            yield message


def _autoset_original_citation_url(nam: Name) -> None:
    if nam.original_citation is None:
        return
    nam.display()
    nam.original_citation.display()
    nam.open_url()
    url = getinput.get_line(
        "URL: ", callbacks=nam.original_citation.get_adt_callbacks()
    )
    if not url:
        return
    nam.original_citation.set_or_replace_url(url)


@LINT.add("authority_page_link", requires_network=True)
def check_must_have_authority_page_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if (
        nam.original_citation is None
        or not nam.original_citation.has_bhl_link_with_pages()
    ):
        return
    pages_with_links = _get_pages_with_links(nam)
    for page in get_unique_page_text(nam.page_described):
        if page not in pages_with_links:
            yield f"must have authority page link for {page}"


@LINT.add("check_bhl_page", requires_network=True)
def check_bhl_page(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_citation is None:
        return
    wrong_bhl_pages = nam.original_citation.has_tag(ArticleTag.BHLWrongPageNumbers)
    for tag in nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink):
        parsed = urlparse.parse_url(tag.url)
        if not isinstance(parsed, urlparse.BhlPage):
            continue
        if wrong_bhl_pages:
            page_metadata = bhl.get_page_metadata(parsed.page_id)
            try:
                page_number = page_metadata["PageNumbers"][0]["Number"]
            except LookupError:
                pass
            else:
                if page_number == tag.page:
                    yield f"page number {tag.page} matches BHL data for {tag.url}, but {nam.original_citation} is marked as having wrong page numbers"
        if nam.original_citation.url is None:
            yield f"name has BHL page, but original citation has no URL: {nam.original_citation}"
            if cfg.interactive:
                nam.original_citation.format(quiet=True)
                if nam.original_citation.url is None:
                    _autoset_original_citation_url(nam)
            continue
        parsed_url = urlparse.parse_url(nam.original_citation.url)
        if not isinstance(parsed_url, urlparse.BhlUrl):
            yield f"name has BHL page, but citation has non-BHL URL {nam.original_citation.url}"
            if cfg.interactive:
                _autoset_original_citation_url(nam)
            continue
        yield from _check_bhl_item_matches(nam, tag, cfg)
        yield from _check_bhl_bibliography_matches(nam, tag, cfg)


def _check_bhl_item_matches(
    nam: Name,
    tag: TypeTag.AuthorityPageLink,  # type: ignore[name-defined]
    cfg: LintConfig,
) -> Iterable[str]:
    item_id = bhl.get_bhl_item_from_url(tag.url)
    if item_id is None:
        yield f"cannot find BHL item for {tag.url}"
        return
    if nam.original_citation is None or nam.original_citation.url is None:
        return
    citation_item_ids = list(nam.original_citation.get_possible_bhl_item_ids())
    if not citation_item_ids:
        return
    if item_id not in citation_item_ids:
        yield f"BHL item mismatch: {item_id} (name) not in {citation_item_ids} (citation)"
        replacement = [
            page
            for page in get_candidate_bhl_pages(nam, verbose=False)
            if page.item_id in citation_item_ids and page.is_confident
        ]
        if len(replacement) == 1:
            new_tag = TypeTag.AuthorityPageLink(
                url=replacement[0].page_url, confirmed=True, page=tag.page
            )
            yield from _replace_page_link(nam, tag, new_tag, cfg)
            return
        if nam.original_citation.url is not None and nam.page_described is not None:
            parsed_url = urlparse.parse_url(nam.original_citation.url)
            if isinstance(parsed_url, urlparse.BhlPart):
                pages = bhl.get_possible_pages_from_part(
                    parsed_url.part_id, nam.page_described
                )
                if len(pages) == 1:
                    new_tag = TypeTag.AuthorityPageLink(
                        url=str(urlparse.BhlPage(pages[0])),
                        confirmed=True,
                        page=nam.page_described,
                    )
                    yield from _replace_page_link(nam, tag, new_tag, cfg)
                    return
        if cfg.manual_mode:
            nam.display()
            nam.original_citation.display()
            nam.open_url()
            nam.edit_until_clean()


def _replace_page_link(
    nam: Name, existing_tag: TypeTag, new_tag: TypeTag, cfg: LintConfig
) -> Iterable[str]:
    message = f"replace {existing_tag} with {new_tag}"
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.type_tags = [  # type: ignore[assignment]
            (tag if tag != existing_tag else new_tag) for tag in nam.type_tags
        ]
    else:
        yield message


def _check_bhl_bibliography_matches(
    nam: Name,
    tag: TypeTag.AuthorityPageLink,  # type: ignore[name-defined]
    cfg: LintConfig,
) -> Iterable[str]:
    bibliography_id = bhl.get_bhl_bibliography_from_url(tag.url)
    if bibliography_id is None:
        if not bhl.is_item_missing_bibliography(tag.url):
            yield f"cannot find BHL bibliography for {tag.url}"
        return
    if nam.original_citation is None or nam.original_citation.url is None:
        return
    citation_biblio_ids = list(
        nam.original_citation.get_possible_bhl_bibliography_ids()
    )
    if bibliography_id not in citation_biblio_ids:
        yield f"BHL item mismatch: {bibliography_id} (name) not in {citation_biblio_ids} (citation)"
        replacement = [
            page
            for page in get_candidate_bhl_pages(nam, verbose=False)
            if page.item_id in citation_biblio_ids and page.is_confident
        ]
        if len(replacement) == 1:
            message = f"replace {tag.url} with {replacement[0].page_url}"
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.type_tags = [  # type: ignore[assignment]
                    (
                        existing_tag
                        if existing_tag != tag
                        else TypeTag.AuthorityPageLink(
                            url=replacement[0].page_url, confirmed=True, page=tag.page
                        )
                    )
                    for existing_tag in nam.type_tags
                ]
            else:
                yield message


def _get_pages_with_links(nam: Name) -> set[str]:
    return {tag.page for tag in nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink)}


def _should_look_for_page_links(nam: Name) -> bool:
    if not nam.page_described:
        return False
    pages = get_unique_page_text(nam.page_described)
    pages_with_links = _get_pages_with_links(nam)
    return not all(page in pages_with_links for page in pages)


def _maybe_add_bhl_page(
    nam: Name, cfg: LintConfig, page_obj: bhl.PossiblePage
) -> Iterable[str]:
    message = f"inferred BHL page {page_obj}"
    if cfg.autofix:
        print(f"{nam}: {message}")
        tag = TypeTag.AuthorityPageLink(
            url=page_obj.page_url, confirmed=True, page=str(page_obj.page_number)
        )
        nam.add_type_tag(tag)
    else:
        yield message
    print(page_obj.page_url)


@LINT.add("infer_bhl_page", requires_network=True)
def infer_bhl_page(
    nam: Name, cfg: LintConfig = LintConfig(autofix=False, interactive=False)
) -> Iterable[str]:
    if not _should_look_for_page_links(nam):
        if cfg.verbose:
            print(f"{nam}: Skip because no page or enough tags")
        return
    confident_candidates = [
        page
        for page in get_candidate_bhl_pages(nam, verbose=cfg.verbose)
        if page.is_confident
    ]
    for _, group_iter in itertools.groupby(
        confident_candidates, lambda page: page.page_number
    ):
        group = list(group_iter)
        if len(group) == 1:
            yield from _maybe_add_bhl_page(nam, cfg, group[0])
        else:
            if cfg.verbose or cfg.manual_mode:
                print(f"Reject for {nam} because multiple pages with name:")
                for page_obj in group:
                    print(page_obj.page_url)
            if cfg.manual_mode:
                nam.display()
                if nam.original_citation is not None:
                    nam.original_citation.display()
                for page_obj in group:
                    if not _should_look_for_page_links(nam):
                        break
                    print(page_obj.page_url)
                    subprocess.check_call(["open", page_obj.page_url])
                    if getinput.yes_no("confirm? ", callbacks=nam.get_adt_callbacks()):
                        yield from _maybe_add_bhl_page(nam, cfg, page_obj)
                        break


def get_candidate_bhl_pages(
    nam: Name, *, verbose: bool = False
) -> Iterable[bhl.PossiblePage]:
    if nam.page_described is None or nam.year is None:
        if verbose:
            print(f"{nam}: Skip because no page or year")
        return
    tags = list(nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink))
    known_pages = [
        parsed_url.page_id
        for tag in tags
        if isinstance((parsed_url := urlparse.parse_url(tag.url)), urlparse.BhlPage)
    ]
    year = nam.numeric_year()
    contains_text: list[str] = []
    if nam.original_name is not None:
        contains_text.append(nam.original_name)
    if nam.corrected_original_name is not None:
        contains_text.append(nam.corrected_original_name)
    if not contains_text:
        return
    if nam.original_citation is not None:
        known_item_id = nam.original_citation.get_bhl_item_id()
    else:
        known_item_id = None
    if known_item_id is None:
        cg = nam.get_citation_group()
        if cg is None:
            if verbose:
                print(f"{nam}: Skip because no citation group")
            return
        title_ids = cg.get_bhl_title_ids()
        if not title_ids:
            if verbose:
                print(f"{nam}: Skip because citation group has no BHLBibliography tag")
            return
    else:
        title_ids = []

    pages = get_unique_page_text(nam.page_described)
    for page in pages:
        possible_pages = list(
            bhl.find_possible_pages(
                title_ids,
                year=year,
                start_page=page,
                contains_text=contains_text,
                known_item_id=known_item_id,
            )
        )
        possible_pages = [
            page for page in possible_pages if page.page_id not in known_pages
        ]
        confident_pages = [page for page in possible_pages if page.is_confident]
        if not confident_pages:
            if verbose:
                print(f"Reject for {nam} because no confident pages")
                for page_obj in possible_pages:
                    print(page_obj.page_url)
            yield from possible_pages
        else:
            yield from confident_pages


def maybe_infer_page_from_other_name(
    *,
    cfg: LintConfig,
    other_nam: object,
    url: str,
    my_page: str,
    their_page: str,
    is_same_page: bool,
) -> int | None:
    parsed = urlparse.parse_url(url)
    if not isinstance(parsed, urlparse.BhlPage):
        if cfg.verbose:
            print(f"{other_nam}: {url} is not a BHL page URL")
        return None
    existing_page_id = parsed.page_id
    if my_page == their_page and is_same_page:
        return existing_page_id
    if not my_page.isnumeric() or not their_page.isnumeric():
        if cfg.verbose:
            print(f"{other_nam}: {my_page} or {their_page} is not numeric")
        return None
    diff = int(my_page) - int(their_page)
    page_metadata = bhl.get_page_metadata(existing_page_id)
    item_id = int(page_metadata["ItemID"])
    item_metadata = bhl.get_item_metadata(item_id)
    if item_metadata is None:
        if cfg.verbose:
            print(f"{other_nam}: no metadata for item {item_id}")
        return None
    page_mapping = bhl.get_page_id_to_index(item_id)
    existing_page_idx = page_mapping.get(existing_page_id)
    if existing_page_idx is None:
        if cfg.verbose:
            print(f"{other_nam}: no index for page {existing_page_id}")
        return None
    expected_page_idx = existing_page_idx + diff
    if not (0 <= expected_page_idx < len(item_metadata["Pages"])):
        if cfg.verbose:
            print(f"{other_nam}: {expected_page_idx} is out of range")
        return None
    inferred_page_id = item_metadata["Pages"][expected_page_idx]["PageID"]
    if diff > 0:
        start = existing_page_id
        end = inferred_page_id
    else:
        start = inferred_page_id
        end = existing_page_id
    if not bhl.is_contiguous_range(
        item_id, start, end, page_mapping, allow_unnumbered=False, verbose=cfg.verbose
    ):
        if cfg.verbose:
            print(
                f"{other_nam}: {existing_page_id} and {inferred_page_id} are not"
                " contiguous"
            )
        return None
    possible_pages = bhl.get_possible_pages(item_id, my_page)
    if inferred_page_id not in possible_pages:
        if cfg.verbose:
            print(
                f"{other_nam}: {inferred_page_id} not in possible pages {possible_pages}"
            )
        return None
    return inferred_page_id


@LINT.add("infer_page_from_other_names")
def infer_page_from_other_names(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not _should_look_for_page_links(nam):
        if cfg.verbose:
            print(f"{nam}: not looking for BHL URL")
        return
    if nam.page_described is None:
        if cfg.verbose:
            print(f"{nam}: no page described")
        return
    if nam.original_citation is None:
        if cfg.verbose:
            print(f"{nam}: no original citation")
        return
    pages = get_unique_page_text(nam.page_described)
    for page in pages:
        other_new_names = [
            nam
            for nam in nam.original_citation.get_new_names().filter(
                Name.page_described.contains(page)
            )
            if nam.has_type_tag(TypeTag.AuthorityPageLink)
        ]
        if not other_new_names:
            if cfg.verbose:
                print(f"{nam}: {page}: no other new names")
            return
        inferred_pages: set[str] = set()
        for other_nam in other_new_names:
            for tag in other_nam.get_tags(
                other_nam.type_tags, TypeTag.AuthorityPageLink
            ):
                if tag.page == page:
                    inferred_pages.add(tag.url)
        if len(inferred_pages) != 1:
            if cfg.verbose:
                print(
                    f"{nam}: no single inferred page from other names ({inferred_pages})"
                )
            continue
        (url,) = inferred_pages
        tag = TypeTag.AuthorityPageLink(url=url, confirmed=True, page=page)
        if tag in nam.type_tags:
            if cfg.verbose:
                print(f"{nam}: already has {tag}")
            continue
        message = f"inferred URL {url} from other names (add {tag})"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_type_tag(tag)
        else:
            yield message


@LINT.add("infer_bhl_page_from_other_names", requires_network=True)
def infer_bhl_page_from_other_names(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not _should_look_for_page_links(nam):
        if cfg.verbose:
            print(f"{nam}: not looking for BHL URL")
        return
    if nam.page_described is None:
        if cfg.verbose:
            print(f"{nam}: no page described")
        return
    if nam.original_citation is None:
        if cfg.verbose:
            print(f"{nam}: no original citation")
        return
    # Just so we don't waste effort adding incorrect pages before the link has been
    # confirmed on the article.
    if not nam.original_citation.has_bhl_link():
        if cfg.verbose:
            print(f"{nam}: original citation has no BHL link")
        return
    pages = get_unique_page_text(nam.page_described)
    for page in pages:
        other_new_names = [
            nam
            for nam in nam.original_citation.get_new_names().filter(
                Name.page_described.contains(page)
            )
            if nam.has_type_tag(TypeTag.AuthorityPageLink)
        ]
        if not other_new_names:
            if cfg.verbose:
                print(f"{nam}: no other new names")
            return
        inferred_pages: set[int] = set()
        for other_nam in other_new_names:
            for tag in other_nam.get_tags(
                other_nam.type_tags, TypeTag.AuthorityPageLink
            ):
                inferred_page = maybe_infer_page_from_other_name(
                    cfg=cfg,
                    other_nam=other_nam,
                    my_page=page,
                    their_page=tag.page,
                    is_same_page=tag.page == page,
                    url=tag.url,
                )
                if inferred_page is not None:
                    inferred_pages.add(inferred_page)
        if len(inferred_pages) != 1:
            if cfg.verbose:
                print(
                    f"{nam}: no single inferred page from other names ({inferred_pages})"
                )
            continue
        (inferred_page_id,) = inferred_pages
        tag = TypeTag.AuthorityPageLink(
            url=f"https://www.biodiversitylibrary.org/page/{inferred_page_id}",
            confirmed=True,
            page=page,
        )
        if tag in nam.type_tags:
            if cfg.verbose:
                print(f"{nam}: already has {tag}")
            continue
        message = f"inferred BHL page {inferred_page_id} from other names (add {tag})"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_type_tag(tag)
        else:
            yield message


@LINT.add("bhl_page_from_classification_entries", requires_network=True)
def infer_bhl_page_from_classification_entries(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    if not _should_look_for_page_links(nam):
        return
    ce = nam.get_mapped_classification_entry()
    if ce is None or ce.article != nam.original_citation:
        return
    new_tags = [
        TypeTag.AuthorityPageLink(url=tag.url, confirmed=True, page=tag.page)
        for tag in ce.tags
        if isinstance(tag, ClassificationEntryTag.PageLink)
    ]
    if not new_tags:
        return
    message = f"inferred BHL page from classification entry {ce}: {new_tags}"
    if cfg.autofix:
        print(f"{nam}: {message}")
        for tag in new_tags:
            nam.add_type_tag(tag)
    else:
        yield message


@LINT.add("bhl_page_from_article", requires_network=True)
def infer_bhl_page_from_article(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not _should_look_for_page_links(nam):
        if cfg.verbose:
            print(f"{nam}: not looking for BHL URL")
        return
    if nam.page_described is None:
        if cfg.verbose:
            print(f"{nam}: no page described")
        return
    art = nam.original_citation
    if art is None or art.url is None:
        if cfg.verbose:
            print(f"{nam}: no original citation or URL")
        return
    pages_with_links = _get_pages_with_links(nam)
    for page_described in get_unique_page_text(nam.page_described):
        if page_described in pages_with_links:
            continue
        maybe_pair = infer_bhl_page_id(page_described, nam, art, cfg)
        if maybe_pair is not None:
            page_id, message = maybe_pair
            tag = TypeTag.AuthorityPageLink(
                url=f"https://www.biodiversitylibrary.org/page/{page_id}",
                confirmed=True,
                page=page_described,
            )
            message = f"inferred BHL page {page_id} from {message} (add {tag})"
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.add_type_tag(tag)
            else:
                yield message


def infer_bhl_page_id(
    page: str, obj: object, art: Article, cfg: LintConfig
) -> tuple[int, str] | None:
    if art.id == 9306 and page.isnumeric():
        # {Marsupialia, Monotremata-British Museum.pdf}. BHL version doesn't have
        # page numbers but we can interpolate them.
        numeric_page = int(page)
        # page 1 through 20 = 37986429 - 37986410 (in decreasing order)
        if 1 <= numeric_page <= 20:
            page_id = 37986429 - (numeric_page - 1)
            return page_id, "interpolated from known range"
        # page 21 through 53 = 37986377 - 37986409
        if 21 <= numeric_page <= 53:
            page_id = 37986377 + (numeric_page - 21)
            return page_id, "interpolated from known range"
        # page 54 through 401 (end) = 37986448 - 37986795
        if 54 <= numeric_page <= 401:
            page_id = 37986448 + (numeric_page - 54)
            return page_id, "interpolated from known range"
    elif art.id == 35530 and page.isnumeric():
        # {Mammalia-in BMNH (Gray 1843).pdf}. BHL has wrong page numbers.
        # The copy in BHL always has a pair of odd, even pages (e.g. 1 and 2),
        # then two blank pages. In addition, there's an extra pair of pages
        # after page 6.
        numeric_page = int(page)
        # page 1 = 53729558
        # page 6 = 53729567
        # page 7 = 53729572
        # page 214 = 53729985
        if 1 <= numeric_page <= 6:
            is_odd = (numeric_page % 2) == 1
            if is_odd:
                page_id = 53729558 + ((numeric_page - 1) // 2) * 4
            else:
                page_id = 53729558 + ((numeric_page - 2) // 2) * 4 + 1
            return page_id, "corrected from known BHL page numbering error"
        if 7 <= numeric_page <= 214:
            is_odd = (numeric_page % 2) == 1
            if is_odd:
                page_id = 53729560 + ((numeric_page - 1) // 2) * 4
            else:
                page_id = 53729560 + ((numeric_page - 2) // 2) * 4 + 1
            return page_id, "corrected from known BHL page numbering error"
    if not is_network_available():
        return None
    if art.url is None:
        return None
    match urlparse.parse_url(art.url):
        case urlparse.BhlPage(start_page_id):
            if art.start_page is not None and art.start_page == art.end_page == page:
                return start_page_id, "article citation where start_page == end_page"
            # TODO: where the link is a BhlPage but the article is a book and so there's no
            # end_page, we can try harder.
            return _infer_bhl_page_from_article_page(obj, art, cfg, start_page_id, page)
        case urlparse.BhlItem(item_id):
            pages = bhl.get_possible_pages(item_id, page)
            if len(pages) != 1:
                if cfg.verbose:
                    print(f"{obj}: no single page for {item_id} {pages}")
                return None
            return pages[0], "item citation"
        case urlparse.BhlPart(part_id):
            pages = bhl.get_possible_pages_from_part(part_id, page)
            if len(pages) != 1:
                if cfg.verbose:
                    print(f"{obj}: no single page for {part_id} {pages} (using {page})")
                return None
            return pages[0], "part citation"
    return None


def _infer_bhl_page_from_article_page(
    obj: object, art: Article, cfg: LintConfig, start_page_id: int, page_described: str
) -> tuple[int, str] | None:
    if art.end_page is None:
        if cfg.verbose:
            print(f"{obj}: no citation")
        return None
    start_page_metadata = bhl.get_page_metadata(start_page_id)
    if not start_page_metadata:
        if cfg.verbose:
            print(f"{obj}: no metadata for start page")
        return None
    item_id = int(start_page_metadata["ItemID"])
    possible_end_pages = bhl.get_possible_pages(item_id, art.end_page)
    page_mapping = bhl.get_page_id_to_index(item_id)
    possible_end_pages = [
        page
        for page in possible_end_pages
        if bhl.is_contiguous_range(item_id, start_page_id, page, page_mapping)
    ]
    possible_pages = bhl.get_possible_pages(item_id, page_described)
    possible_pages = [
        page
        for page in possible_pages
        if bhl.is_contiguous_range(item_id, start_page_id, page, page_mapping)
    ]
    if len(possible_pages) != len(possible_end_pages):
        if cfg.verbose:
            print(
                f"{obj}: different number of possible description and end pages"
                f" {possible_pages} {possible_end_pages}"
            )
        return None
    if not possible_pages:
        if cfg.verbose:
            print(f"{obj}: no possible pages in item {item_id}")
        return None
    for possible_page, possible_end_page in zip(
        possible_pages, possible_end_pages, strict=True
    ):
        if not bhl.is_contiguous_range(
            item_id, start_page_id, possible_page, page_mapping
        ):
            if cfg.verbose:
                print(
                    f"{obj}: page {possible_page} is not contiguous with start page {start_page_id}"
                )
            continue
        if not bhl.is_contiguous_range(
            item_id, possible_page, possible_end_page, page_mapping
        ):
            if cfg.verbose:
                print(
                    f"{obj}: end page {possible_end_page} is not contiguous with {possible_page}"
                )
            continue

        return possible_page, "page citation"
    return None


@LINT.add("move_to_lowest_rank")
def move_to_lowest_rank(nam: Name, cfg: LintConfig) -> Iterable[str]:
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
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.taxon = lowest
        else:
            yield message


@LINT.add("infer_original_name")
def infer_original_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_name is not None:
        return
    if nam.group not in (Group.genus, Group.high):
        return
    message = f"inferred original name from root name {nam.root_name}"
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.original_name = nam.root_name
    else:
        yield message


@cache
def get_name_complex_finder() -> Callable[[str], tuple[NameComplex, str] | None]:
    endings_tree: SuffixTree[NameEnding] = SuffixTree()
    for ending in NameEnding.select_valid():
        endings_tree.add(ending.ending, ending)

    def finder(root_name: str) -> tuple[NameComplex, str] | None:
        endings = list(endings_tree.lookup(root_name))
        if not endings:
            return None
        inferred = max(endings, key=lambda e: -len(e.ending)).name_complex
        return inferred, f"matches endings {endings}"

    return finder


@LINT.add("infer_name_complex", clear_caches=get_name_complex_finder.cache_clear)
def infer_name_complex(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.name_complex is not None:
        return
    if nam.group is not Group.genus:
        return
    if not nam.nomenclature_status.requires_name_complex():
        return
    finder = get_name_complex_finder()
    result = finder(nam.root_name)
    if result is None:
        return
    nc, reason = result
    message = f"inferred name complex {nc} from root name {nam.root_name} ({reason})"
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.name_complex = nc
    else:
        yield message


NameComplex.creation_event.on(lambda _: get_name_complex_finder.cache_clear())
NameEnding.creation_event.on(lambda _: get_name_complex_finder.cache_clear())
NameComplex.save_event.on(lambda _: get_name_complex_finder.cache_clear())
NameEnding.save_event.on(lambda _: get_name_complex_finder.cache_clear())


@cache
def get_species_name_complex_finder() -> (
    Callable[[str], tuple[SpeciesNameComplex, str] | None]
):
    endings_tree: SuffixTree[SpeciesNameEnding] = SuffixTree()
    full_names: dict[str, tuple[SpeciesNameComplex, str]] = {}
    for ending in SpeciesNameEnding.select_valid():
        for form in ending.name_complex.get_forms(ending.ending):
            if ending.full_name_only:
                full_names[form] = (ending.name_complex, f"matches ending {ending}")
            else:
                endings_tree.add(form, ending)
    for snc in SpeciesNameComplex.filter(
        SpeciesNameComplex.kind == SpeciesNameKind.adjective
    ):
        for form in snc.get_forms(snc.stem):
            full_names[form] = (snc, "matches a form of the stem")

    def finder(root_name: str) -> tuple[SpeciesNameComplex, str] | None:
        if root_name in full_names:
            return full_names[root_name]
        endings = list(endings_tree.lookup(root_name))
        if not endings:
            return None
        inferred = max(endings, key=lambda e: -len(e.ending)).name_complex
        return inferred, f"matches endings {endings}"

    return finder


SpeciesNameComplex.creation_event.on(
    lambda _: get_species_name_complex_finder.cache_clear()
)
SpeciesNameEnding.creation_event.on(
    lambda _: get_species_name_complex_finder.cache_clear()
)
SpeciesNameComplex.save_event.on(
    lambda _: get_species_name_complex_finder.cache_clear()
)
SpeciesNameEnding.save_event.on(lambda _: get_species_name_complex_finder.cache_clear())


@LINT.add(
    "infer_species_name_complex",
    clear_caches=get_species_name_complex_finder.cache_clear,
)
def infer_species_name_complex(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.species_name_complex is not None:
        return
    if nam.group is not Group.species:
        return
    finder = get_species_name_complex_finder()
    result = finder(nam.root_name)
    if result is None:
        return
    snc, reason = result
    message = (
        f"inferred species name complex {snc} from root name {nam.root_name} ({reason})"
    )
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.species_name_complex = snc
    else:
        yield message


class SuffixTree(Generic[T]):
    def __init__(self) -> None:
        self.children: dict[str, SuffixTree[T]] = defaultdict(SuffixTree)
        self.values: list[T] = []

    def add(self, key: str, value: T) -> None:
        self._add(iter(reversed(key)), value)

    def count(self) -> int:
        return len(self.values) + sum(child.count() for child in self.children.values())

    def lookup(self, key: str) -> Iterable[T]:
        yield from self._lookup(iter(reversed(key)))

    def _add(self, key: Iterator[str], value: T) -> None:
        try:
            char = next(key)
        except StopIteration:
            self.values.append(value)
        else:
            self.children[char]._add(key, value)

    def _lookup(self, key: Iterator[str]) -> Iterable[T]:
        yield from self.values
        try:
            char = next(key)
        except StopIteration:
            pass
        else:
            if char in self.children:
                yield from self.children[char]._lookup(key)


_checked_root_names: set[str] = set()


@LINT.add("infer_species_name_complex", clear_caches=_checked_root_names.clear)
def infer_species_name_complex_from_other_names(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    if nam.group is not Group.species:
        return
    # Name combinations inherit their name complex from their parent, let's ignore them here
    if nam.nomenclature_status is NomenclatureStatus.name_combination:
        return
    # Fast path
    if nam.species_name_complex is not None and nam.root_name in _checked_root_names:
        return
    other_names = Name.select_valid().filter(
        Name.root_name == nam.root_name,
        Name.species_name_complex != None,
        Name.nomenclature_status != NomenclatureStatus.name_combination,
    )
    sc_to_nams: dict[SpeciesNameComplex, list[Name]] = {}
    for other_name in other_names:
        if other_name.species_name_complex is None:
            continue
        sc_to_nams.setdefault(other_name.species_name_complex, []).append(nam)

    if len(sc_to_nams) == 1:
        _checked_root_names.add(nam.root_name)
        if nam.species_name_complex is None:
            sc = next(iter(sc_to_nams))
            message = f"inferred species name complex {sc} from other names"
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.species_name_complex = sc
            else:
                yield message
    else:
        if nam.corrected_original_name is None:
            return
        root_from_con = nam.corrected_original_name.split()[-1]
        relevant_names = [
            other_name
            for other_name in other_names
            if other_name.corrected_original_name is not None
            and other_name.corrected_original_name.split()[-1] == root_from_con
        ]
        if not relevant_names:
            return
        relevant_names = sorted(
            relevant_names, key=lambda nam: (nam.get_date_object(), nam.id)
        )
        earliest = relevant_names[0]
        if earliest.species_name_complex != nam.species_name_complex:
            yield f"species name complex {nam.species_name_complex} does not match that of {earliest} ({earliest.species_name_complex})"


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


def remove_duplicates(key: object, names: list[Name], cfg: LintConfig) -> None:
    if len(names) < 2:
        return
    names = sorted(names, key=lambda nam: nam.id)
    print(f"Removing duplicates for {key}")
    for name in names[1:]:
        print(f"Remove name: {name}")
        if cfg.autofix:
            if cfg.interactive and getinput.yes_no("Remove? "):
                name.merge(names[0])


@LINT.add_duplicate_finder(
    "duplicate_name",
    query=Name.select_valid().filter(Name.original_citation != None),
    fixer=remove_duplicates,
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


@LINT.add("guess_repository")
def guess_repository(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.collection is not None:
        return
    if nam.group is not Group.species:
        return
    if "collection" not in nam.get_required_fields():
        return
    if nam.has_type_tag(TypeTag.ProbableRepository):
        expected_tag = None
    else:
        result = get_most_likely_repository(nam)
        if result is None:
            expected_tag = None
        else:
            repo, score = result
            expected_tag = TypeTag.GuessedRepository(repo, score)
    current_tags = list(nam.get_tags(nam.type_tags, TypeTag.GuessedRepository))
    # TODO: fix pyanalyze
    message = ""
    new_tags = []
    match (bool(current_tags), bool(expected_tag)):
        case (True, True):
            current_tag, *_ = current_tags
            if current_tag == expected_tag:
                return
            if (
                expected_tag is not None
                and current_tag.repository == expected_tag.repository
                and abs(current_tag.score - expected_tag.score) < 0.05
            ):
                return
            message = (
                f"changing inferred repository from {current_tag} to {expected_tag}"
            )
            new_tags = [
                tag if tag != current_tag else expected_tag for tag in nam.type_tags
            ]
        case (True, False):
            message = f"removing inferred repository {current_tags[0]}"
            new_tags = [tag for tag in nam.type_tags if tag != current_tags[0]]
        case (False, True):
            message = f"inferred repository {expected_tag}"
            new_tags = [*nam.type_tags, expected_tag]
        case (False, False):
            return

    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.type_tags = new_tags  # type: ignore[assignment]
    else:
        getinput.print_diff(nam.type_tags, new_tags)
        yield message


def _maybe_add_name_variant(
    nam: Name,
    nomenclature_status: NomenclatureStatus,
    ce: ClassificationEntry,
    cfg: LintConfig,
) -> Iterable[str]:
    message = f"adding {nomenclature_status.name} based on {ce}"
    corrected_name = ce.get_corrected_name()
    if corrected_name is None or not cfg.autofix or not cfg.interactive:
        should_autofix = False
    elif nomenclature_status is NomenclatureStatus.name_combination:
        should_autofix = True
    else:
        should_autofix = getinput.yes_no(
            f"{nam}: add {nomenclature_status!r} based on {ce}? ",
            callbacks=nam.get_adt_callbacks(),
        )
    if should_autofix:
        print(f"{nam}: {message}")
        assert corrected_name is not None
        new_name = nam.add_variant(
            corrected_name.split()[-1],
            status=nomenclature_status,
            paper=ce.article,
            page_described=ce.page,
            original_name=ce.name,
            interactive=False,
        )
        if new_name is not None:
            new_name.corrected_original_name = corrected_name
            new_name.original_rank = ce.rank
            ce.mapped_name = new_name
            new_name.format()
            if cfg.interactive:
                new_name.edit_until_clean()
    else:
        yield message


def take_over_name(nam: Name, ce: ClassificationEntry, cfg: LintConfig) -> None:
    nam.original_citation = ce.article
    nam.page_described = ce.page
    nam.original_name = ce.name
    nam.copy_authors()
    nam.copy_year()
    nam.type_tags = [  # type: ignore[assignment]
        tag for tag in nam.type_tags if not isinstance(tag, TypeTag.AuthorityPageLink)
    ]
    nam.format()
    if cfg.interactive:
        nam.edit_until_clean()


def maybe_take_over_name(
    nam: Name, ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    if (
        nam.get_mapped_classification_entry() is not None
        and nam.nomenclature_status
        in (
            NomenclatureStatus.name_combination,
            NomenclatureStatus.incorrect_subsequent_spelling,
        )
    ):
        message = f"changing original citation of {nam} to {ce.article}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            take_over_name(nam, ce, cfg)
        else:
            yield message
    elif should_report_unreplaceable_name(nam):
        yield f"replace name {nam} with {ce}"


def _is_msw3(art: Article) -> bool:
    return art.id == 9291 or (art.parent is not None and art.parent.id == 9291)


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
def infer_name_variants(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group is not Group.species:
        return
    ces: Iterable[ClassificationEntry] = nam.classification_entries
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
            message = f"add {expected_ce} as a name variant"
            if cfg.autofix:
                print(f"{nam}: {message}")
                expected_ce.add_incorrect_subsequent_spelling(nam)
            else:
                yield message


def _is_iss_from_synonym_without_full_name(nam: Name) -> bool:
    ce = nam.get_mapped_classification_entry()
    return ce is not None and ce.is_synonym_without_full_name()


def _infer_name_variants_of_status(
    nam: Name,
    cfg: LintConfig,
    expected_name_variants: list[ClassificationEntry],
    nomenclature_status: NomenclatureStatus,
) -> Iterable[str]:
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
                yield from _maybe_add_name_variant(nam, nomenclature_status, ce, cfg)
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
                    if cfg.autofix and can_replace is None:
                        print(f"{duplicate}: {message}")
                        duplicate.merge(existing[0], copy_fields=False)
                    elif should_report_unreplaceable_name(duplicate):
                        yield f"{message} (cannot replace because of: {can_replace})"


@LINT.add("duplicate_variants")
def check_duplicate_variants(nam: Name, cfg: LintConfig) -> Iterable[str]:
    nomenclature_status = nam.nomenclature_status
    if nomenclature_status not in (
        NomenclatureStatus.name_combination,
        NomenclatureStatus.incorrect_subsequent_spelling,
    ):
        return
    if nam.original_rank is Rank.synonym:
        return
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
    earlier = sorted(
        [
            dupe
            for dupe in dupes
            if dupe.get_date_object() < nam.get_date_object()
            and dupe.resolve_variant() == base
        ],
        key=lambda dupe: (dupe.get_date_object(), dupe.id),
    )
    if earlier:
        message = f"remove because of earlier names with status {nomenclature_status.name}: {', '.join(str(dupe) for dupe in earlier)}"
        can_replace = can_replace_name(nam)
        if cfg.autofix and can_replace is None:
            print(f"{nam}: {message}")
            nam.merge(earlier[0], copy_fields=False)
        elif should_report_unreplaceable_name(nam):
            yield f"{message} (cannot replace because of: {can_replace})"


def can_replace_name(nam: Name) -> str | None:
    for tag in nam.type_tags:
        if isinstance(tag, TypeTag.AuthorityPageLink):
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
            continue
        return f"tag {tag}"
    if nam.get_mapped_classification_entry() is None:
        return "has no classification entries"
    return None


def should_report_unreplaceable_name(nam: Name) -> bool:
    if nam.original_citation is None:
        return False
    return (
        nam.id > 100_000
        or nam.nomenclature_status is NomenclatureStatus.name_combination
        or (nam.group is Group.species and nam.numeric_year() > 1950)
    )


@LINT.add("mark_incorrect_subsequent_spelling_as_name_combination")
def mark_incorrect_subsequent_spelling_as_name_combination(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    if nam.nomenclature_status != NomenclatureStatus.incorrect_subsequent_spelling:
        return
    target = nam.get_tag_target(NameTag.IncorrectSubsequentSpellingOf)
    existing = [
        nam
        for nam in Name.select_valid().filter(
            Name.root_name == nam.root_name,
            Name.group == nam.group,
            Name.taxon == nam.taxon,
        )
        if nam.get_tag_target(NameTag.IncorrectSubsequentSpellingOf) == target
    ]
    if not existing:
        return
    existing = sorted(existing, key=lambda nam: (nam.get_date_object(), nam.id))
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
            new_tags = [
                tag
                for tag in nam.tags
                if not isinstance(
                    tag, (NameTag.SubsequentUsageOf, NameTag.NameCombinationOf)
                )
            ] + [expected_tag]
            getinput.print_diff(nam.tags, new_tags)
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.tags = new_tags  # type: ignore[assignment]
            else:
                yield message


@LINT.add("family_group_subsequent_usage")
def mark_family_group_subsequent_usage(nam: Name, cfg: LintConfig) -> Iterable[str]:
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
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.add_tag(tag)
    else:
        yield message


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


# This should replace a number of other linters that manipulate name combinations
# and misspellings, but it still gets some things wrong and I'm not sure it's possible
# to get all the edge cases right with this approach.
@LINT.add("determine_name_variants", disabled=True)
def determine_name_variants(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group is not Group.species:
        return
    if nam.resolve_variant() != nam:
        return

    root_name_forms = _get_extended_root_name_forms(nam)
    # First find all the names and CEs that are variants of this name
    nams_with_reasons: list[ExistingVariant] = []
    ces: list[ClassificationEntry] = []
    con_to_existing_names: dict[str, list[ExistingVariant]] = defaultdict(list)
    root_name_to_existing_names: dict[str, ExistingVariant] = {}
    for syn in nam.taxon.get_names():
        variant_base, reason = syn.resolve_variant_with_reason()
        if variant_base != nam:
            continue
        simplified_normalized = _get_simplied_normalized_name(syn)
        has_mapped_ces = syn.get_mapped_classification_entry() is not None
        syn_ces = list(syn.classification_entries)
        ev = ExistingVariant(syn, has_mapped_ces, reason, syn_ces)
        nams_with_reasons.append(ev)
        ces += syn_ces
        if simplified_normalized is not None:
            con_to_existing_names[simplified_normalized].append(ev)
        for root_name in _get_extended_root_name_forms(syn):
            if root_name not in root_name_to_existing_names:
                root_name_to_existing_names[root_name] = ev
            elif name_combination_name_sort_key(
                ev.name
            ) < name_combination_name_sort_key(
                root_name_to_existing_names[root_name].name
            ):
                root_name_to_existing_names[root_name] = ev

    # Then figure out what names should exist based on the CEs
    ces_by_name: dict[str, list[ClassificationEntry]] = defaultdict(list)
    ces_by_root_name: dict[str, list[ClassificationEntry]] = defaultdict(list)
    bare_synonym_ces: dict[str, list[ClassificationEntry]] = defaultdict(list)
    for ce in ces:
        corrected_name = ce.get_corrected_name()
        if corrected_name is None:
            continue
        if ce.is_synonym_without_full_name():
            bare_synonym_ces[corrected_name].append(ce)
        else:
            ces_by_name[corrected_name].append(ce)
        root_name = corrected_name.split()[-1]
        ces_by_root_name[root_name].append(ce)

    root_name_to_oldest_ce = {
        root_name: min(ces, key=lambda ce: _name_variant_article_sort_key(ce.article))
        for root_name, ces in ces_by_root_name.items()
    }

    con_to_expected_name: dict[str | None, ExpectedName] = {}
    for con, ce_list in ces_by_name.items():
        base_ce = min(
            ce_list, key=lambda ce: _name_variant_article_sort_key(ce.article)
        )
        root_name = con.split()[-1]
        tags = []
        if root_name not in root_name_forms:
            tags.append(NameTag.IncorrectSubsequentSpellingOf(nam))
        if root_name in root_name_to_existing_names:
            oldest_root = root_name_to_existing_names[root_name]
            if base_ce.get_corrected_name() != oldest_root.name.corrected_original_name:
                tags.append(NameTag.NameCombinationOf(oldest_root.name))
        con_to_expected_name[con] = ExpectedName(
            original_name=base_ce.name,
            corrected_original_name=con,
            root_name=root_name,
            base_ce=base_ce,
            ces=list(ce_list),
            tags=tags,
        )
    for root_name, ce_list in bare_synonym_ces.items():
        base_ce = min(
            ce_list, key=lambda ce: _name_variant_article_sort_key(ce.article)
        )
        if root_name in root_name_forms:
            if nam.corrected_original_name in con_to_expected_name:
                con_to_expected_name[nam.corrected_original_name].ces.extend(ce_list)
            else:
                con_to_expected_name[nam.corrected_original_name] = ExpectedName(
                    original_name=nam.original_name,
                    corrected_original_name=nam.corrected_original_name,
                    root_name=root_name,
                    base_ce=base_ce,
                    ces=list(ce_list),
                    tags=[],
                )
        elif root_name in root_name_to_existing_names and (
            name_combination_name_sort_key(root_name_to_existing_names[root_name].name)
            < _name_variant_article_sort_key(base_ce.article)
            or root_name_to_existing_names[root_name].name.original_citation
            == base_ce.article
        ):
            maybe_con = _get_simplied_normalized_name(
                root_name_to_existing_names[root_name].name
            )
            if maybe_con is None:
                continue
            if maybe_con in con_to_expected_name:
                con_to_expected_name[maybe_con].ces.extend(ce_list)
            else:
                con_to_expected_name[maybe_con] = ExpectedName(
                    original_name=base_ce.name,
                    corrected_original_name=root_name_to_existing_names[
                        root_name
                    ].name.corrected_original_name,
                    root_name=root_name,
                    base_ce=base_ce,
                    ces=list(ce_list),
                    tags=[NameTag.IncorrectSubsequentSpellingOf(nam)],
                )
        elif base_ce == root_name_to_oldest_ce[root_name]:
            maybe_con = base_ce.get_name_to_use_as_normalized_original_name()
            if maybe_con is None:
                continue
            con_to_expected_name[root_name] = ExpectedName(
                original_name=base_ce.name,
                corrected_original_name=maybe_con,
                root_name=root_name,
                base_ce=base_ce,
                ces=list(ce_list),
                tags=[NameTag.IncorrectSubsequentSpellingOf(nam)],
            )
        else:
            base_con = root_name_to_oldest_ce[
                root_name
            ].get_name_to_use_as_normalized_original_name()
            if base_con is None:
                continue
            con_to_expected_name[base_con].ces.extend(ce_list)

    # Then line up the names
    if cfg.verbose:
        pprint.pp(con_to_expected_name)

    for maybe_con, expected_name in con_to_expected_name.items():
        if maybe_con is None:
            # must be the original name
            assert expected_name.root_name in root_name_forms, expected_name
            for ce in expected_name.ces:
                if ce.mapped_name != nam:
                    message = f"{ce}: change mapped name from {ce.mapped_name} to {nam}"
                    if cfg.autofix:
                        print(f"{nam}: {message}")
                        ce.mapped_name = nam
                    else:
                        yield message
        elif maybe_con not in con_to_existing_names:
            yield from _add_name_variant(expected_name, nam, cfg)
        else:
            existing = con_to_existing_names[maybe_con]
            replaceable, others = helpers.sift(
                existing, lambda en: en.reasons <= REPLACEABLE_TAGS_SET
            )
            if replaceable:
                replaceable = sorted(
                    replaceable, key=lambda en: name_combination_name_sort_key(en.name)
                )
                to_use, *redundant_names = replaceable

                for redundant_name in redundant_names:
                    message = f"remove redundant name {redundant_name.name}"
                    cannot_replace_reason = can_replace_name(redundant_name.name)
                    if cfg.autofix and cannot_replace_reason is None:
                        print(f"{redundant_name.name}: {message}")
                        redundant_name.name.redirect(to_use.name)
                    else:
                        yield message + f" (cannot replace because of: {cannot_replace_reason})"
                if to_use.name.original_citation == expected_name.base_ce.article:
                    new_tags = _update_replaceable_tags(to_use.name.tags, expected_name)
                    if new_tags is not None:
                        getinput.print_diff(to_use.name.tags, new_tags)
                        message = f"update tags of {to_use.name}"
                        if cfg.autofix:
                            print(f"{to_use.name}: {message}")
                            to_use.name.tags = new_tags  # type: ignore[assignment]
                        else:
                            yield message
                elif (
                    expected_name.base_ce.get_date_object()
                    < to_use.name.get_date_object()
                ):
                    yield from maybe_take_over_name(
                        to_use.name, expected_name.base_ce, cfg
                    )
            else:
                oldest = min(
                    others, key=lambda en: name_combination_name_sort_key(en.name)
                )
                if (
                    oldest.name.get_date_object()
                    <= expected_name.base_ce.get_date_object()
                ):
                    for ce in expected_name.ces:
                        if ce.mapped_name != oldest.name:
                            message = f"{ce}: change mapped name from {ce.mapped_name} to {oldest.name}"
                            if cfg.autofix:
                                print(f"{nam}: {message}")
                                ce.mapped_name = oldest.name
                            else:
                                yield message
                else:
                    yield from _add_name_variant(expected_name, nam, cfg)

    # We don't remove additional names and instead rely on
    # logic to enforce that every name is mapped to a CE if the article
    # has any CEs.


def _add_name_variant(
    expected_name: ExpectedName, nam: Name, cfg: LintConfig
) -> Iterable[str]:
    message = f"add name variant for {expected_name}"
    if cfg.autofix:
        print(f"{nam}: {message}")
        new_name = nam.taxon.syn_from_paper(
            root_name=expected_name.root_name,
            paper=expected_name.base_ce.article,
            page_described=expected_name.base_ce.page,
            original_name=expected_name.original_name,
            corrected_original_name=expected_name.corrected_original_name,
            group=Group.species,
            interactive=False,
        )
        if new_name is not None:
            new_name.tags = expected_name.tags
            new_name.original_rank = expected_name.base_ce.rank
            for ce in expected_name.ces:
                print(f"{ce}: change mapped name from {ce.mapped_name} to {new_name}")
                ce.mapped_name = new_name
            new_name.format()
            if cfg.interactive:
                new_name.edit_until_clean()
    else:
        yield message


@LINT.add("infer_included_species")
def infer_included_species(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group is not Group.genus:
        return
    if nam.original_citation is None:
        return
    ce = nam.get_mapped_classification_entry()
    if ce is None or ce.rank.is_synonym:
        return
    current_included_species_with_ces = {
        tag.name: tag.classification_entry
        for tag in nam.type_tags
        if isinstance(tag, TypeTag.IncludedSpecies) and tag.classification_entry
    }
    included = ce.get_children_of_rank(Rank.species)
    for child_ce in included:
        if child_ce.mapped_name is None:
            continue
        if (
            child_ce.mapped_name in current_included_species_with_ces
            and current_included_species_with_ces[child_ce.mapped_name] == child_ce
        ):
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
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_type_tag(tag)
        else:
            yield message


@LINT.add("duplicate_included_species")
def check_duplicate_included_species(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group is not Group.genus:
        return
    all_included: dict[  # type: ignore[name-defined]
        Name, tuple[list[TypeTag.IncludedSpecies], list[TypeTag.IncludedSpecies]]
    ] = defaultdict(lambda: ([], []))
    for tag in nam.type_tags:
        if isinstance(tag, TypeTag.IncludedSpecies):
            key_name = tag.name
            tag_target = key_name.get_tag_target(NameTag.NameCombinationOf)
            if tag_target is not None:
                key_name = tag_target
            tag_target = key_name.get_tag_target(NameTag.SubsequentUsageOf)
            if tag_target is not None:
                key_name = tag_target
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
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.type_tags = [tag for tag in nam.type_tags if tag not in tags_to_remove]  # type: ignore[assignment]
        else:
            yield message


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


@LINT.add("infer_tags_from_mapped_entries")
def infer_tags_from_mapped_entries(nam: Name, cfg: LintConfig) -> Iterable[str]:
    # if nam.group is not Group.species:
    #     return
    ces = list(nam.classification_entries)
    if not ces:
        return
    tag_name = nam.resolve_variant()
    for ce in ces:
        if nam.group is Group.species:
            location = ce.type_locality
            if location and not any(
                tag.source == ce.article
                for tag in tag_name.get_tags(tag_name.type_tags, TypeTag.LocationDetail)
            ):
                tag = TypeTag.LocationDetail(location, ce.article)
                message = f"adding location detail from {ce} to {tag_name}: {tag}"
                if cfg.autofix:
                    print(f"{tag_name}: {message}")
                    tag_name.add_type_tag(tag)
                else:
                    yield message
            type_specimen = None
            for tag in ce.tags:
                if isinstance(tag, ClassificationEntryTag.TypeSpecimenData):
                    type_specimen = tag.text
                    break
            if type_specimen is not None and not any(
                tag.source == ce.article
                for tag in tag_name.get_tags(tag_name.type_tags, TypeTag.SpecimenDetail)
            ):
                tag = TypeTag.SpecimenDetail(type_specimen, ce.article)
                message = f"adding specimen detail from {ce} to {tag_name}: {tag}"
                if cfg.autofix:
                    print(f"{tag_name}: {message}")
                    tag_name.add_type_tag(tag)
                else:
                    yield message
        if (
            tag_name.original_citation is None
            and ce.citation is not None
            and (nam.group is not Group.family or nam.year == ce.year)
        ):
            tag = TypeTag.CitationDetail(ce.citation, ce.article)
            if tag not in tag_name.type_tags:
                message = f"adding verbatim citation from {ce} to {nam}: {tag}"
                if cfg.autofix:
                    print(f"{nam}: {message}")
                    tag_name.add_type_tag(tag)
                else:
                    yield message

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
                if cfg.autofix:
                    print(f"{nam}: {message}")
                    nam.add_type_tag(expected_tag)
                else:
                    yield message


@LINT.add("remove_redundant_name")
def remove_redundant_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.nomenclature_status is not NomenclatureStatus.subsequent_usage:
        return
    target = nam.get_tag_target(NameTag.SubsequentUsageOf)
    if target is None:
        return  # other linters will complain
    has_ces = nam.get_mapped_classification_entry() is not None
    cannot_replace_reason = can_replace_name(nam)
    if cannot_replace_reason is None:
        message = f"remove redundant name {nam} by redirecting to {target}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.redirect(target)
        else:
            yield message
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
        elif (
            nam.numeric_year() > 2000
            or nam.numeric_year() < 1800
            or nam.group is Group.high
            or nam.id > 100_000
        ):
            yield "subsequent usage should be replaced"
        elif cfg.experimental:
            yield "subsequent usage should be replaced"


def has_classification(art: Article) -> bool:
    return any(art.get_classification_entries()) and not art.has_tag(
        ArticleTag.PartialClassification
    )


@LINT.add("must_have_ce")
def check_must_have_ce(nam: Name, cfg: LintConfig) -> Iterable[str]:
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
                if cfg.autofix:
                    print(f"{nam}: {message}")
                    ce.mapped_name = nam
                else:
                    yield message
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
) -> Iterable[str]:
    ce = nam.get_mapped_classification_entry()
    if ce is None:
        return
    if ce.name != nam.original_name:
        yield f"mapped to {ce}, but {ce.name=} != {nam.original_name=}"
        if nam.original_name is None or can_transform(
            ce.name, nam.original_name, _ALLOWED_TRANSFORMS
        ):
            message = f"changing original name to {ce.name}"
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.original_name = ce.name
            else:
                yield message
    expected_con = ce.get_name_to_use_as_normalized_original_name()
    if expected_con != nam.corrected_original_name:
        yield f"mapped to {ce}, but {expected_con} != {nam.corrected_original_name}"
    if ce.page != nam.page_described:
        yield f"mapped to {ce}, but {ce.page=} != {nam.page_described=}"
        if cfg.autofix:
            if nam.page_described is None:
                print(f"{nam}: inferred page {ce.page}")
                nam.page_described = ce.page
            elif set(parse_page_text(nam.page_described)) < set(
                parse_page_text(ce.page)
            ):
                print(f"{nam}: extended page from {nam.page_described} to {ce.page}")
                nam.page_described = ce.page
    yield from _check_matching_original_parent(nam, ce)
    if nam.original_rank is not ce.rank:
        yield f"mapped to {ce}, but {ce.rank=!r} != {nam.original_rank=!r}"
        if nam.original_rank is None or (
            nam.status is Status.synonym and ce.rank.is_synonym
        ):
            message = f"inferred rank {ce.rank!r} from {ce}"
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.original_rank = ce.rank
            else:
                yield message
    elif ce.rank.needs_textual_rank:
        ce_tags = list(ce.get_tags(ce.tags, ClassificationEntryTag.TextualRank))
        if ce_tags:
            nam_tags = list(nam.get_tags(nam.type_tags, TypeTag.TextualOriginalRank))
            if not nam_tags:
                tag = TypeTag.TextualOriginalRank(ce_tags[0].text)
                message = f"inferred textual rank from {ce}: {tag}"
                if cfg.autofix:
                    print(f"{nam}: {message}")
                    nam.add_type_tag(tag)
                else:
                    yield message
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
        message = f"mapped {ce} has conditions {new_conditions}; add to name"
        if cfg.autofix:
            print(f"{nam}: {message}")
            for tag in new_conditions:
                nam.add_tag(NameTag.Condition(tag.status, comment=tag.comment))
        else:
            yield message

    for tag in ce.tags:
        if isinstance(tag, ClassificationEntryTag.LSIDCE):
            new_tag = TypeTag.LSIDName(tag.text)
            if new_tag not in nam.type_tags:
                message = f"adding LSID from {ce} to {nam}: {new_tag}"
                if cfg.autofix:
                    print(f"{nam}: {message}")
                    nam.add_type_tag(new_tag)
                else:
                    yield message


def _check_matching_original_parent(
    nam: Name, ce: ClassificationEntry
) -> Iterable[str]:
    if nam.original_parent is None or ce.rank.is_synonym:
        return
    ce_parent = ce.parent_of_rank(Rank.genus)
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


@LINT.add("original_rank")
def check_original_rank(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_rank is None:
        return
    if nam.original_rank is Rank.synonym:
        new_rank = helpers.GROUP_TO_SYNONYM_RANK[nam.group]
        message = f"changing original rank to {new_rank!r}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.original_rank = new_rank
        else:
            yield message
        return
    if nam.original_rank is Rank.other:
        if nam.group is Group.family:
            message = f"changing original rank to {Rank.other_family!r}"
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.original_rank = Rank.other_family
            else:
                yield message
            return
        elif nam.group is Group.genus:
            message = f"changing original rank to {Rank.other_subgeneric!r}"
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.original_rank = Rank.other_subgeneric
            else:
                yield message
            return
    if (
        nam.original_rank is Rank.informal
        and nam.group is Group.high
        and nam.taxon.group() is Group.species
    ):
        message = f"changing original rank to {Rank.informal_species!r} and group to {Group.species!r}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.original_rank = Rank.informal_species
            nam.group = Group.species
        else:
            yield message
    group = helpers.group_of_rank(nam.original_rank)
    if group is not nam.group:
        yield f"original rank {nam.original_rank!r} is not in group {nam.group!r}"


@LINT.add("infer_unavalailable_version")
def infer_unavailable_version(nam: Name, cfg: LintConfig) -> Iterable[str]:
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
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.add_tag(tag)
    else:
        yield message


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


@LINT.add("has_parent_species", disabled=True)
def check_has_parent_species(nam: Name, cfg: LintConfig) -> Iterable[str]:
    # TODO: check other subspecific ranks
    if nam.original_rank not in (Rank.subspecies, Rank.variety):
        return
    if nam.nomenclature_status is not NomenclatureStatus.available:
        return
    art = nam.original_citation
    if art is None:
        return
    if (
        nam.corrected_original_name is None
        or nam.corrected_original_name.count(" ") != 2
    ):
        return
    gen, sp, ssp = nam.corrected_original_name.split(" ")
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


@LINT.add("unique_type_locality")
def check_unique_type_locality(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_locality is None or nam.original_citation is None:
        return
    if nam.status is not Status.valid:
        return
    if nam.has_type_tag(TypeTag.InterpretedTypeLocality):
        return
    if nam.taxon.age not in (AgeClass.extant, AgeClass.recently_extinct):
        return
    original_localities = [
        tag
        for tag in nam.type_tags
        if isinstance(tag, TypeTag.LocationDetail)
        and tag.source == nam.original_citation
    ]
    if len(original_localities) > 1:
        message = f"multiple original localities for {nam}:\n"
        for tag in original_localities:
            message += f"  {tag.text}\n"
        yield message


def is_valid_mammal(nam: Name) -> bool:
    if nam.status is not Status.valid:
        if nam.nomenclature_status is NomenclatureStatus.preoccupied:
            valid_name = nam.taxon.base_name
            if (
                valid_name.nomenclature_status is NomenclatureStatus.nomen_novum
                and valid_name.get_tag_target(NameTag.NomenNovumFor) == nam
            ):
                return is_valid_mammal(valid_name)
        return False
    taxon = nam.taxon
    if taxon.age not in (AgeClass.extant, AgeClass.recently_extinct):
        return False
    if taxon.rank is Rank.subspecies and not taxon.is_nominate_subspecies():
        return False
    return taxon.get_derived_field("class_").valid_name == "Mammalia"


@LINT.add("infer_reranking")
def infer_reranking(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group is not Group.family:
        return
    if nam.type is None or nam.year is None or nam.original_name is None:
        return
    if nam.has_name_tag(NameTag.RerankingOf):
        return
    if not nam.can_be_valid_base_name():
        return
    my_type = nam.type.resolve_name()
    candidates = [
        other_nam
        for other_nam in nam.taxon.get_names().filter(Name.group == Group.family)
        if other_nam.type is not None
        and other_nam.type.resolve_name() == my_type
        and other_nam.can_be_valid_base_name()
        and other_nam != nam
        and other_nam.year is not None
    ]
    if not candidates:
        return
    best_candidate = min(
        candidates,
        key=lambda other_nam: (
            other_nam.valid_numeric_year(),
            (
                -other_nam.original_rank.comparison_value
                if other_nam.original_rank is not None
                else 0
            ),
        ),
    )
    if best_candidate.valid_numeric_year() >= nam.valid_numeric_year():
        return
    tag = NameTag.RerankingOf(best_candidate)
    message = f"add RerankingOf tag: {tag}"
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.add_tag(tag)
    else:
        yield message
