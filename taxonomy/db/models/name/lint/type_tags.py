import re
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, Self, TypeVar, assert_never

from taxonomy import adt, getinput
from taxonomy.apis.zoobank import clean_lsid, is_valid_lsid
from taxonomy.db import coordinate_lint, helpers, models
from taxonomy.db.constants import (
    AgeClass,
    ArticleKind,
    Group,
    PhylogeneticDefinitionType,
    Rank,
    SpeciesBasis,
    SpeciesGroupType,
    SpecimenOrgan,
    Status,
    TypeSpeciesDesignation,
)
from taxonomy.db.models.article import Article
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.classification_entry.ce import DETAIL_METADATA_FIELDS
from taxonomy.db.models.collection import MULTIPLE_COLLECTION
from taxonomy.db.models.lint import field_issue
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name.name import (
    DETAIL_TAG_TO_CE_TAG,
    SOURCE_TAGS,
    LectotypeDesignationTerm,
    Name,
    TypeTag,
)
from taxonomy.db.models.name.organ import (
    CHECKED_ORGANS,
    ParsedOrgan,
    ParseException,
    parse_organ_detail,
)
from taxonomy.db.models.name.page import (
    check_page,
    check_page_link,
    get_unique_page_text,
)
from taxonomy.db.models.name.type_specimen import (
    parse_type_specimen,
    stringify_specimen_list,
)
from taxonomy.db.models.taxon import Taxon

from .base import LINT
from .helpers import (
    all_parents,
    fix_type_specimen_link,
    get_effective_taxon,
    has_classification,
    is_high_or_invalid_family,
    iter_parents,
    smallest_common_ancestor,
)
from .page_described import check_page_matches_citation
from .page_link import infer_bhl_page_id

T = TypeVar("T")
ADTT = TypeVar("ADTT", bound=adt.ADT)


def replace_arg(tag: ADTT, arg: str, val: object) -> ADTT:
    return adt.replace(tag, **{arg: val})


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
) -> Generator[LintResult, None, Tag]:
    tag = yield from check_tag_with_page(tag, source, cfg, owner)
    if source is not None and (
        tag.verbatim_citation is not None or tag.citation_group is not None
    ):
        tag = tag.replace(verbatim_citation=None, citation_group=None)
    if tag.verbatim_citation is not None and tag.citation_group is None:
        yield f"{tag} has verbatim_citation but no citation_group"
    if source is None and tag.verbatim_citation is None:
        yield f"{tag} has no source or verbatim_citation"
    if (
        source is None
        and tag.citation_group is not None
        and tag.citation_group.must_have(2100)  # no year available, be aggressive
    ):
        yield f"{tag} has citation_group {tag.citation_group} but no source"
    return tag


def check_tag_with_page[Tag: TagWithPage](
    tag: Tag,
    source: Article | None,
    cfg: LintConfig,
    owner: object,
    *,
    allow_missing_page: bool = False,
) -> Generator[LintResult, None, Tag]:
    if (
        source is not None
        and tag.page is not None
        and source.url is not None
        and tag.page_link is None
    ):
        page_described = get_unique_page_text(tag.page)[0]
        maybe_pair = infer_bhl_page_id(page_described, tag, source, cfg)
        if maybe_pair is not None:
            page_id, _context = maybe_pair
            url = f"https://www.biodiversitylibrary.org/page/{page_id}"
            tag = tag.replace(page_link=url)
    if tag.page is None and tag.comment is not None:
        if match := re.fullmatch(r"pp?\. (\d+(-\d+)?(?:, \d+(-\d+)?)*)", tag.comment):
            page = match.group(1)
            tag = tag.replace(page=page, comment=None)
        # mypy bug?
        elif match := re.search(r"^p\. (\d+(?:, \d+)*)\. ", tag.comment):  # type: ignore[arg-type]
            page = match.group(1)
            _, end_span = match.span()
            new_comment = tag.comment[end_span:]  # type: ignore[index]
            tag = tag.replace(page=page, comment=new_comment)
    if tag.page is not None:
        new_page = yield from check_page(
            tag.page,
            get_raw_page_regex=(
                source.get_raw_page_regex if source is not None else None
            ),
        )
        if new_page != tag.page:
            tag = tag.replace(page=new_page)
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


type TagsByType = Mapping[type[TypeTag], Sequence[TypeTag]]
type TypeTagChecker = Callable[
    [TypeTag, Name, LintConfig, TagsByType],
    Generator[LintResult, None, Sequence[TypeTag]],
]


def _check_links_in_type_tag(
    tag: TypeTag, nam: Name, cfg: LintConfig, tags_by_type: TagsByType
) -> Generator[LintResult, None, Sequence[TypeTag]]:
    for arg_name, art in get_tag_fields_of_type(tag, Article):
        if art.kind is ArticleKind.removed:
            yield f"bad article in tag {tag}"
        elif art.kind is ArticleKind.redirect:
            if art.parent is None or art.parent.should_skip():
                yield f"bad redirected article in tag {tag}"
            else:
                tag = replace_arg(tag, arg_name, art.parent)
    for arg_name, tag_nam in get_tag_fields_of_type(tag, Name):
        if tag_nam.is_invalid():
            yield f"bad name in {arg_name} for tag {tag}"

    return [tag]


def _check_detail_type_tag(
    tag: TypeTag, nam: Name, cfg: LintConfig, tags_by_type: TagsByType
) -> Generator[LintResult, None, Sequence[TypeTag]]:
    if not isinstance(tag, SOURCE_TAGS):
        return [tag]
    if not tag.text and tag.source is None:
        return []
    if tag.classification_entry is None and not isinstance(tag, TypeTag.LocationDetail):
        for other in tags_by_type.get(type(tag), ()):
            if (
                other is not tag
                and other.source == tag.source
                and other.text == tag.text
                and other.classification_entry is not None
                and all(
                    getattr(tag, field) in (None, getattr(other, field))
                    for field in DETAIL_METADATA_FIELDS
                )
            ):
                yield f"remove redundant tag {tag} (obviated by {other})"
                return []
    if tag.classification_entry is not None:
        ce = tag.classification_entry
        if ce.article != tag.source:
            yield f"{tag} has classification entry {ce} that does not match source {tag.source}"
        if (
            ce.mapped_name is not None
            and ce.mapped_name.resolve_variant() != nam
            and ce.mapped_name != nam
        ):
            yield f"{tag} has classification entry {ce} that does not match name {nam}"
        if isinstance(tag, TypeTag.LocationDetail):
            if ce.type_locality != tag.text:
                yield f"{tag} has classification entry {ce} whose type locality {ce.type_locality!r} does not match text"
        else:
            ce_tag_type = DETAIL_TAG_TO_CE_TAG[type(tag)]
            matching = [
                other
                for other in ce.get_tags(ce.tags, ce_tag_type)
                if other.text == tag.text
            ]
            if not matching:
                yield f"{tag} has classification entry {ce} whose {ce_tag_type.__name__} does not match text"
            elif len(matching) == 1 and ce.article == tag.source:
                for field in DETAIL_METADATA_FIELDS:
                    expected = getattr(matching[0], field)
                    current = getattr(tag, field)
                    if expected is not None:
                        if current is None:
                            tag = tag.replace(**{field: expected})
                        elif current != expected:
                            yield f"{tag} has {field} {current!r} that does not match classification entry {ce}: {expected!r}"
    elif cfg.experimental and has_classification(tag.source):
        yield f"{tag} has no classification entry, but source {tag.source} has some"
    tag = yield from check_tag_with_page(
        tag, tag.source, cfg, nam, allow_missing_page=True
    )
    return [tag]


def _check_designation_type_tag(
    tag: TypeTag, nam: Name, cfg: LintConfig, tags_by_type: TagsByType
) -> Generator[LintResult, None, Sequence[TypeTag]]:
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
) -> Generator[LintResult, None, Sequence[TypeTag]]:
    tags: list[TypeTag] = []
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
                return []

        case TypeTag.CommissionTypeDesignation():
            if nam.type != tag.type:
                yield field_issue(
                    f"Commission designated type {tag.type}", nam, "type", tag.type
                )
            if (
                nam.genus_type_kind
                != TypeSpeciesDesignation.designated_by_the_commission
            ):
                yield field_issue(
                    "set genus type kind from Commission designation",
                    nam,
                    "genus_type_kind",
                    TypeSpeciesDesignation.designated_by_the_commission,
                )

        case TypeTag.LectotypeDesignation():
            if tag.lectotype != nam.type_specimen and _is_comparable_to_type(
                tag.lectotype, nam
            ):
                tag = tag.replace(lectotype=nam.type_specimen)
            if tag.optional_source is None and tag.year is None:
                yield f"lectotype designation has no source or year: {tag}"
            validity = _is_lectotype_designation_valid(nam, tag)
            if validity is not None and tag.valid is not validity:
                tag = tag.replace(valid=validity)

        case TypeTag.NeotypeDesignation():
            if tag.neotype != nam.type_specimen and _is_comparable_to_type(
                tag.neotype, nam
            ):
                tag = tag.replace(neotype=nam.type_specimen)

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
            if is_empty_location_detail(tag.text):
                new_tag = TypeTag.NoLocation(source=tag.source)
                return [new_tag]

            obviating_location_tag: TypeTag.LocationDetail | None = None  # type: ignore[name-defined]
            for other_tag in by_type[TypeTag.LocationDetail]:
                if (
                    isinstance(other_tag, TypeTag.LocationDetail)
                    and other_tag is not tag
                    and other_tag.source == tag.source
                    and other_tag.text == tag.text
                ):
                    obviating_location_tag = other_tag
                    break
            if obviating_location_tag is not None:
                yield f"merge tags {tag} and {obviating_location_tag}"
                return [
                    TypeTag.LocationDetail(
                        text=tag.text,
                        source=tag.source,
                        comment=(
                            tag.comment
                            if tag.comment is not None
                            else obviating_location_tag.comment
                        ),
                        page=(
                            tag.page
                            if tag.page is not None
                            else obviating_location_tag.page
                        ),
                        translation=(
                            tag.translation
                            if tag.translation is not None
                            else obviating_location_tag.translation
                        ),
                        page_link=(
                            tag.page_link
                            if tag.page_link is not None
                            else obviating_location_tag.page_link
                        ),
                        classification_entry=(
                            tag.classification_entry
                            if (
                                tag.classification_entry is not None
                                and not (
                                    tag.classification_entry.rank is Rank.subspecies
                                    and tag.classification_entry.parent
                                    == obviating_location_tag.classification_entry
                                )
                            )
                            else obviating_location_tag.classification_entry
                        ),
                    )
                ]

        case TypeTag.DefinitionDetail():
            # Phylonyms book PDF has "Tis" and "Te" for "This" and "The"
            text = re.sub(r"\bT(?=e\b|is\b)", "Th", tag.text)
            text = text.replace("defnition", "definition")
            tag = tag.replace(text=text)

        case TypeTag.Coordinates():
            try:
                lat, _ = coordinate_lint.standardize_coordinate_interval(
                    tag.latitude, is_latitude=True
                )
            except helpers.InvalidCoordinates as e:
                yield f"invalid latitude {tag.latitude}: {e}"
                lat = tag.latitude
            try:
                longitude, _ = coordinate_lint.standardize_coordinate_interval(
                    tag.longitude, is_latitude=False
                )
            except helpers.InvalidCoordinates as e:
                yield f"invalid longitude {tag.longitude}: {e}"
                longitude = tag.longitude
            tag = TypeTag.Coordinates(lat, longitude)
            if TypeTag.LocationDetail not in by_type:
                yield "has Coordinates tag but no LocationDetail tag"

        case TypeTag.LSID():
            lsid = clean_lsid(tag.text)
            tag = TypeTag.LSID(lsid)
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
                tag.name
                for tag in by_type.get(TypeTag.InternalSpecifier, [])
                if isinstance(tag, TypeTag.InternalSpecifier)
            ]
            external_specifiers = [
                tag.name
                for tag in by_type.get(TypeTag.ExternalSpecifier, [])
                if isinstance(tag, TypeTag.ExternalSpecifier)
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
                    tag.name
                    for tag in by_type.get(TypeTag.MustNotInclude, [])
                    if isinstance(tag, TypeTag.MustNotInclude)
                ],
                must_be_part_of=[
                    tag.name
                    for tag in by_type.get(TypeTag.MustBePartOf, [])
                    if isinstance(tag, TypeTag.MustBePartOf)
                ],
                must_not_be_part_of=[
                    tag.name
                    for tag in by_type.get(TypeTag.MustNotBePartOf, [])
                    if isinstance(tag, TypeTag.MustNotBePartOf)
                ],
                must_be_extinct=bool(by_type.get(TypeTag.MustBeExtinct, [])),
            )
            effective_taxon = get_effective_taxon(nam)
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
            if not is_high_or_invalid_family(nam):
                yield "has TreatAsEquivalentTo tag but is not a high-level or invalid family-group name"

        case TypeTag.StructuredVerbatimCitation():
            if nam.original_citation is not None:
                yield "has StructuredVerbatimCitation tag but also has original_citation"
                return []

    return [*tags, tag]


def _largest_excluding(taxon: Taxon, exclude: Sequence[Name]) -> Taxon | None:
    if not exclude:
        return taxon
    parent_lists = [list(all_parents(nam)) for nam in exclude]
    prev: Taxon | None = None
    for parent in iter_parents(taxon):
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
    ancestor = smallest_common_ancestor(internal_specifiers)
    parent_lists = [list(all_parents(ext)) for ext in external_specifiers]
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

    new_minimum_taxon = None
    if _is_applicable(
        result.minimum_taxon,
        must_not_include=must_not_include,
        must_be_part_of=must_be_part_of,
        must_not_be_part_of=must_not_be_part_of,
        must_be_extinct=must_be_extinct,
    ):
        new_minimum_taxon = result.minimum_taxon
    else:
        for parent in iter_parents(result.minimum_taxon):
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
    assert new_minimum_taxon is not None
    new_maximum_taxon = result.maximum_taxon
    last_seen = new_minimum_taxon
    for parent in iter_parents(new_minimum_taxon):
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
        if taxon in iter_parents(get_effective_taxon(mni)):
            return False
    parents = list(iter_parents(taxon))
    if must_be_part_of and not any(
        get_effective_taxon(mbp) in parents for mbp in must_be_part_of
    ):
        return False
    for mnbp in must_not_be_part_of:
        if get_effective_taxon(mnbp) in parents:
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
            ancestor = smallest_common_ancestor(internal_specifiers)
            if any(ancestor in all_parents(ext) for ext in external_specifiers):
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
            ancestor = smallest_common_ancestor(internal_specifiers)
            if any(ancestor in all_parents(ext) for ext in external_specifiers):
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
            taxon = get_effective_taxon(internal_specifiers[0])
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
def check_type_tags_for_name(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
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
        new_tags: list[TypeTag] = []
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
        yield field_issue("changing type tags", nam, "type_tags", tuple(tags))


def _get_collection_year(date_text: str) -> int | None:
    try:
        standardized = helpers.standardize_date(date_text)
    except ValueError:
        return None
    if standardized is None or standardized.startswith("<"):
        return None
    match = re.search(r"\b(\d{4})$", standardized)
    if match is None:
        return None
    return int(match.group(1))


@LINT.add("collector_lifespan")
def check_collector_lifespan(nam: Name, cfg: LintConfig) -> Iterable[str]:
    date_tags = list(nam.get_tags(nam.type_tags, TypeTag.Date))
    collector_tags = list(nam.get_tags(nam.type_tags, TypeTag.CollectedBy))
    if not date_tags or not collector_tags:
        return
    for collector_tag in collector_tags:
        death = collector_tag.person.death
        if death is None:
            continue
        try:
            death_year = int(death)
        except ValueError:
            continue
        for date_tag in date_tags:
            collection_year = _get_collection_year(date_tag.date)
            if collection_year is not None and collection_year > death_year:
                yield (
                    f"collector {collector_tag.person} died in {death}, before collection"
                    f" date {date_tag.date}"
                )
