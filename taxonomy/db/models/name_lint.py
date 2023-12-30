"""

Lint steps for Names.

"""

import enum
import functools
import json
import re
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TypeVar

import requests

from ... import adt, getinput
from ...apis.zoobank import clean_lsid, get_zoobank_data, is_valid_lsid
from .. import helpers
from ..constants import (
    ArticleKind,
    ArticleType,
    CommentKind,
    DateSource,
    FillDataLevel,
    Group,
    NamingConvention,
    NomenclatureStatus,
    SpeciesGroupType,
    SpeciesNameKind,
    SpecimenOrgan,
    Status,
    TypeSpeciesDesignation,
)
from .article import Article, ArticleTag, PresenceStatus
from .base import LintConfig
from .collection import Collection, CollectionTag
from .name import STATUS_TO_TAG, Name, NameComment, NameTag, TypeTag
from .person import AuthorTag, PersonLevel

T = TypeVar("T")
ADTT = TypeVar("ADTT", bound=adt.ADT)

Linter = Callable[[Name, LintConfig], Iterable[str]]
IgnorableLinter = Callable[[Name, LintConfig], Generator[str, None, set[str]]]

LINTERS = []
DISABLED_LINTERS = []


def get_ignored_lints(nam: Name) -> set[str]:
    tags = nam.get_tags(nam.type_tags, TypeTag.IgnoreLintName)
    return {tag.label for tag in tags}


def make_linter(
    label: str, *, disabled: bool = False
) -> Callable[[Linter], IgnorableLinter]:
    def decorator(linter: Linter) -> IgnorableLinter:
        @functools.wraps(linter)
        def wrapper(
            nam: Name, cfg: LintConfig, **kwargs: Any
        ) -> Generator[str, None, set[str]]:
            # static analysis: ignore[incompatible_call]
            issues = list(linter(nam, cfg, **kwargs))
            if not issues:
                return set()
            ignored_lints = get_ignored_lints(nam)
            if label in ignored_lints:
                return {label}
            for issue in issues:
                yield f"{nam}: {issue} [{label}]"
            return set()

        if disabled:
            DISABLED_LINTERS.append(wrapper)
        else:
            LINTERS.append(wrapper)
        return wrapper

    return decorator


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
)
ORGAN_REPLACEMENTS = [
    (r"(^|, | with )right(\b|$)", r"\1R"),
    (r"(^|, | with )left(\b|$)", r"\1L"),
    (r"(^|, | with )L (?=[A-Z]\d)", r"\1L"),
    (r"(^|, | with )R (?=[A-Z]\d)", r"\1R"),
    (r"M(\d) or M(\d)", r"M\1/\2"),
    (r"m(\d) or m(\d)", r"m\1/\2"),
    (r"^both$", r"L, R"),
    (r"(^|, )partial ([LR])(?=$|, )", r"\1\2 part"),
    (r"(^|, )(proximal|distal|shaft|part) ([LR])(?=$|, )", r"\1\3 \2"),
    (r"^partial$", "part"),
    (r"^(L |R )?fragments$", r"\1parts"),
    (r"^(L |R )?fragment$", r"\1part"),
    (r"^(multiple|several|many)$", ">1"),
    (r"(^|, )(L|R) (\??[A-Za-z]{1,2}(\??\d|$))", r"\1\2\3"),  # remove space after L/R in tooth
    (r"^([LR]?)([A-Za-z]\d)\?", r"\1?\2"),
]

LONG_BONES = (
    SpecimenOrgan.fibula,
    SpecimenOrgan.radius,
    SpecimenOrgan.ulna,
    SpecimenOrgan.tibia,
    SpecimenOrgan.femur,
    SpecimenOrgan.humerus,
)
PROXIMAL_DISTAL_ORGANS = {
    *LONG_BONES,
    SpecimenOrgan.metapodial,
    SpecimenOrgan.carpometacarpal,
    SpecimenOrgan.tarsometatarsus,
    SpecimenOrgan.clavicle,
    SpecimenOrgan.tibiotarsus,
    SpecimenOrgan.metacarpal,
    SpecimenOrgan.metatarsal,
    SpecimenOrgan.pubis,
    SpecimenOrgan.ischium,
    SpecimenOrgan.ilium,
    SpecimenOrgan.coracoid,
    SpecimenOrgan.scapula,
    SpecimenOrgan.pelvis,
    SpecimenOrgan.calcaneum,
    SpecimenOrgan.astragalus,
}
PAIRED_ORGANS = {
    *PROXIMAL_DISTAL_ORGANS,
    SpecimenOrgan.furcula,
    SpecimenOrgan.petrosal,
    SpecimenOrgan.patella,
    SpecimenOrgan.hyoid,
    SpecimenOrgan.carpal,
    SpecimenOrgan.scapulocoracoid,
    SpecimenOrgan.girdle,
    SpecimenOrgan.antler,
    SpecimenOrgan.limb,
    SpecimenOrgan.prepubis,
    SpecimenOrgan.predentary,
}
COUNTED_ORGANS = {
    SpecimenOrgan.carpal,
    SpecimenOrgan.metapodial,
    SpecimenOrgan.metacarpal,
    SpecimenOrgan.metatarsal,
    SpecimenOrgan.limb,
    SpecimenOrgan.tooth,
    SpecimenOrgan.gastralia,
}
TOOTHED_ORGANS = {
    SpecimenOrgan.dentary,
    SpecimenOrgan.palate,
    SpecimenOrgan.tooth,
    SpecimenOrgan.mandible,
    SpecimenOrgan.maxilla,
    SpecimenOrgan.premaxilla,
    SpecimenOrgan.skull,
}
CHECKED_ORGANS = {
    *PAIRED_ORGANS,
    *COUNTED_ORGANS,
    *PROXIMAL_DISTAL_ORGANS,
    *TOOTHED_ORGANS,
    SpecimenOrgan.egg,
    SpecimenOrgan.gastralia,
    SpecimenOrgan.interclavicle,
    SpecimenOrgan.baculum,
    SpecimenOrgan.caudal_tube,
    SpecimenOrgan.skeleton,
    SpecimenOrgan.pelvis,
}
# TODO: carpal, phalanx_manus, frontlet, phalanx_pes, other, phalanx, sternum, horn_core (split up?), manus, osteoderm, pes, vertebra, postcranial_skeleton, shell, rib
# Maybe leave alone: tissue_sample, whole_animal, in_alcohol, skin
# Other possible improvements:
# - Parse teeth into a dataclass and then restringify to make the format
#   more consistent
# - Sort the comma-separated parts
# - Unify multiple organ tags that use the same organ and have no comment
# - Rename the "condition" tag to "comment"
# - Restrict some organs to specific taxonomic groups (antler, frontlet, predentary, horn_core, shell)


def remove_prefix(text: str, prefixes: tuple[str, ...]) -> str:
    if text in prefixes:
        return ""
    for prefix in prefixes:
        text = text.removeprefix(prefix + " ")
    return text

def remove_suffix(text: str, suffixes: tuple[str, ...]) -> str:
    if text in suffixes:
        return ""
    for suffix in suffixes:
        text = text.removesuffix(" " + suffix)
    return text

TOOTH_REGEX = re.compile(
    r"""
        (
            (?P<category>[A-Za-z]+)
            (/(?P<category_alt>[A-Za-z]+))?
        )?
        \??
        (
            (?P<number>\d+)
            (/(?P<number_alt>\d+))?
        )?
        (?P<full_alt>/[A-Za-z]\d+)?
    """,
    re.VERBOSE,
)
CATEGORIES = ['i', 'c', 'p', 'm']
ALLOWED_CATEGORIES = {*CATEGORIES, "di", "dc", "dp", "mf", "a", "if", "pmf", "maxillary", "mandibular"}


def check_tooth_text(text: str, organ: SpecimenOrgan) -> Iterable[str]:
    text = text.removeprefix("L").removeprefix("R").removeprefix("?")
    if not text:
        return
    if "-" in text:
        parts = text.split("-", maxsplit=1)
    else:
        parts = [text]
    if organ in (SpecimenOrgan.mandible, SpecimenOrgan.dentary, SpecimenOrgan.maxilla, SpecimenOrgan.premaxilla, SpecimenOrgan.skull, SpecimenOrgan.palate):
        if text.strip() in ("edentulous", "complete"):
            return
    if organ in (SpecimenOrgan.mandible, SpecimenOrgan.dentary):
        if text == "symphysis":
            return
    if organ in (SpecimenOrgan.mandible, SpecimenOrgan.dentary, SpecimenOrgan.maxilla, SpecimenOrgan.premaxilla, SpecimenOrgan.tooth, SpecimenOrgan.skull):
        match = re.fullmatch(r" ?(?P<count>\d+)( (?P<category>[A-Za-z/]+))?", text)
        if match:
            category = match.group("category")
            if category is None or all(part in ALLOWED_CATEGORIES for part in category.lower().split("/")):
                return
    if organ in (SpecimenOrgan.mandible, SpecimenOrgan.skull):
        # allow arbirary text
        if re.fullmatch(r"[a-z]+( [a-z]+)?", text.strip()):
            return
    allow_uppers = organ not in (SpecimenOrgan.mandible, SpecimenOrgan.dentary)
    allow_lowers = organ not in (SpecimenOrgan.maxilla, SpecimenOrgan.premaxilla, SpecimenOrgan.skull, SpecimenOrgan.palate)
    first_tooth_category: int | None = None
    first_tooth_position: int | None = None
    for i, part in enumerate(parts):
        match = TOOTH_REGEX.fullmatch(part)
        if not match:
            yield f"cannot parse tooth {part!r}"
            continue
        if "-" in text and (match.group("category_alt") or match.group("number_alt") or match.group("full_alt")):
            yield f"cannot use / in a range of tooth positions"
        if i == 0 and not match.group("category"):
            yield "category may not be omitted in first part of range"
        category = match.group("category")
        if category and category.lower() not in ALLOWED_CATEGORIES:
            yield f"disallowed category {category!r}"
        category_alt = match.group("category_alt")
        if category_alt and category_alt.lower() not in ALLOWED_CATEGORIES:
            yield f"disallowed category {category_alt!r}"
        if not allow_uppers:
            if category and category.isupper():
                yield f"tooth is from upper jaw: {category}"
            if category_alt and category_alt.isupper():
                yield f"tooth is from upper jaw: {category}"
        if not allow_lowers:
            if category and category.islower():
                yield f"tooth is from lower jaw: {category}"
            if category_alt and category_alt.islower():
                yield f"tooth is from lower jaw: {category}"
        try:
            tooth_category = CATEGORIES.index(category.lower())
        except (AttributeError, ValueError):
            tooth_category = None
        else:
            if i == 0:
                first_tooth_category = tooth_category
            elif first_tooth_category is not None:
                if tooth_category < first_tooth_category:
                    yield f"invalid range from {CATEGORIES[first_tooth_category]} to {CATEGORIES[tooth_category]}"
        if number := match.group("number"):
            try:
                tooth_position = int(number)
            except ValueError:
                continue
            else:
                if i == 0:
                    first_tooth_position = tooth_position
                elif first_tooth_category is not None and (tooth_category is None or first_tooth_category == tooth_category) and first_tooth_position is not None:
                    if tooth_position < first_tooth_position:
                        yield f"invalid range from {first_tooth_position} to {tooth_position}"


def check_single_organ_detail(organ: SpecimenOrgan, detail: str) -> Iterable[str]:
    if organ not in CHECKED_ORGANS:
        return
    original_detail = detail
    detail = detail.removeprefix("?")
    detail = remove_suffix(detail, ("part", "parts"))
    if organ in COUNTED_ORGANS:
        detail = re.sub(r"^[>~]?\d+(?= |$)", "", detail).lstrip()
    if organ in PAIRED_ORGANS:
        detail = remove_prefix(detail, ("L", "R"))
    if organ in PROXIMAL_DISTAL_ORGANS:
        detail = remove_prefix(detail, ("proximal", "distal"))
    if organ in LONG_BONES:
        detail = remove_prefix(detail, ("shaft",))
    if organ is SpecimenOrgan.girdle:
        detail = remove_prefix(detail, ("pelvic", "pectoral"))
    if organ is SpecimenOrgan.limb:
        detail = remove_prefix(detail, ("fore", "hind"))
    if organ is SpecimenOrgan.metacarpal:
        detail = re.sub(r"^Mc(I|II|III|IV|V)$", "", detail)
    if organ is SpecimenOrgan.metatarsal:
        detail = re.sub(r"^Mt(I|II|III|IV|V)$", "", detail)
    if organ in TOOTHED_ORGANS:
        yield from check_tooth_text(detail, organ)
        return
    if detail:
        if detail == original_detail:
            yield f"organ of type {organ!r} has disallowed detail text {detail!r}"
        else:
            yield (
                f"organ of type {organ!r} has disallowed detail text"
                f" {detail!r} (remaining from {original_detail!r})"
            )


def check_organ_detail(organ: SpecimenOrgan, detail: str) -> Iterable[str]:
    for part in detail.split(", "):
        yield from check_single_organ_detail(organ, part)


def specify_organ(organ: SpecimenOrgan, detail: str) -> tuple[SpecimenOrgan, str] | None:
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
            new_detail = ""
            text = detail
        try:
            new_organ = SpecimenOrgan[text]
        except KeyError:
            pass
        else:
            return new_organ, new_detail
    return None


def maybe_replace_tags(organ: SpecimenOrgan, detail: str, condition: str) -> tuple[list[TypeTag.Organ], list[str]]:
    if organ in (SpecimenOrgan.mandible, SpecimenOrgan.dentary):
        if " with " in detail:
            before, after = detail.split(" with ", maxsplit=1)
            new_organ = SpecimenOrgan.dentary
            rgx = r"[a-z][\da-z\-\?]*(, [a-z][\da-z\-\?]*)*"
            if match := re.fullmatch("([LR]) (dentary|lower jaw|ramus|dentary fragment|mandible)", before):
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
                    return [TypeTag.Organ(new_organ, new_detail, condition)], []
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
                    return [TypeTag.Organ(new_organ, new_detail, condition)], []
    if organ is SpecimenOrgan.skull:
        if " with " in detail:
            before, after = detail.split(" with ", maxsplit=1)
            if re.fullmatch(r"(partial |broken )?(skull|cranium)", before):
                if re.fullmatch(r"[A-Z][\dA-Z\-\?]*(, [A-Z][\dA-Z\-\?]*)*", after):
                    return [TypeTag.Organ(SpecimenOrgan.skull, after, condition)], []
    parts = detail.split(", ")
    remaining_parts = []
    new_tags = []
    if len(parts) == 1 or (not condition and "L" not in parts and "R" not in parts and "(" not in detail):
        for part in parts:
            new_pair = specify_organ(organ, part)
            if new_pair is None:
                remaining_parts.append(part)
            else:
                new_organ, new_detail = new_pair
                new_tags.append(TypeTag.Organ(new_organ, new_detail, condition))
    else:
        remaining_parts = parts
    return new_tags, remaining_parts


def check_organ_tag(tag: TypeTag.Organ) -> Generator[str, None, list[TypeTag.Organ]]:
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
    if re.fullmatch(r"^[A-Z]{2,}(-[A-Z]+)? [A-Z]?(\d{2,}[\-\d/\.]*|\d(\.\d+)+)[a-z]?$", detail) and not condition:
        condition = detail
        detail = ""
    yield from check_organ_detail(organ, detail)
    new_tags.append(TypeTag.Organ(organ, detail, condition))
    return new_tags


@make_linter("type_tags")
def check_type_tags_for_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not nam.type_tags:
        return
    tags: list[TypeTag] = []
    original_tags = list(nam.type_tags)
    by_type: dict[type[TypeTag], list[TypeTag]] = {}
    for tag in original_tags:
        by_type.setdefault(type(tag), []).append(tag)
        for arg_name, art in get_tag_fields_of_type(tag, Article):
            if art.kind is ArticleKind.removed:
                print(f"{nam} references a removed Article in {tag}")
                yield f"bad article in tag {tag}"
            elif art.kind is ArticleKind.redirect:
                print(f"{nam} references a redirected Article in {tag} -> {art.parent}")
                if art.parent is None or art.parent.should_skip():
                    yield f"bad redirected article in tag {tag}"
                elif cfg.autofix:
                    tag = replace_arg(tag, arg_name, art.parent)
        for arg_name, tag_nam in get_tag_fields_of_type(tag, Name):
            if tag_nam.is_invalid():
                print(
                    f"{nam} references a removed Name in argument {arg_name} to {tag}"
                )
                yield f"bad name in tag {tag}"
        for arg_name, coll in get_tag_fields_of_type(tag, Collection):
            if coll.must_use_children():
                yield f"must use child collection in argument {arg_name} to {tag}"

        if isinstance(
            tag,
            (
                TypeTag.FormerRepository,
                TypeTag.FutureRepository,
                TypeTag.ExtraRepository,
            ),
        ):
            if tag.repository in nam.get_repositories():
                yield (
                    f"{tag.repository} is marked as a {type(tag).__name__}, but it is"
                    " the current repository"
                )

        if (
            isinstance(
                tag,
                (
                    TypeTag.LocationDetail,
                    TypeTag.SpecimenDetail,
                    TypeTag.EtymologyDetail,
                    TypeTag.TypeSpeciesDetail,
                ),
            )
            and tag.text == ""
            and tag.source is None
        ):
            message = f"{tag} has no text and no source"
            if cfg.autofix:
                print(f"{nam}: {message}")
                continue
            else:
                yield message

        if isinstance(tag, TypeTag.ProbableRepository) and nam.collection is not None:
            message = f"has {tag} but colllection is set to {nam.collection}"
            if cfg.autofix:
                print(f"{nam}: {message}")
                continue
            else:
                yield message

        if isinstance(tag, TypeTag.CommissionTypeDesignation):
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
                        TypeSpeciesDesignation.designated_by_the_commission  # type: ignore
                    )
            tags.append(tag)
        elif isinstance(tag, TypeTag.Date):
            date = tag.date
            try:
                date = helpers.standardize_date(date)
            except ValueError:
                print(f"{nam} has date {tag.date}, which cannot be parsed")
                yield "unparseable date"
            if date is None:
                continue
            tags.append(TypeTag.Date(date))
        elif isinstance(tag, TypeTag.Altitude):
            if (
                not re.match(r"^-?\d+([\-\.]\d+)?$", tag.altitude)
                or tag.altitude == "000"
            ):
                print(f"{nam} has altitude {tag}, which cannot be parsed")
                yield f"bad altitude tag {tag}"
            tags.append(tag)
        elif isinstance(tag, TypeTag.LocationDetail):
            coords = helpers.extract_coordinates(tag.text)
            if coords and not any(
                isinstance(t, TypeTag.Coordinates) for t in original_tags
            ):
                tags.append(TypeTag.Coordinates(coords[0], coords[1]))
                print(
                    f"{nam}: adding coordinates {tags[-1]} extracted from {tag.text!r}"
                )
            tags.append(tag)
        elif isinstance(tag, TypeTag.Coordinates):
            try:
                lat = helpers.standardize_coordinates(tag.latitude, is_latitude=True)
            except helpers.InvalidCoordinates as e:
                print(f"{nam} has invalid latitude {tag.latitude}: {e}")
                yield f"invalid latitude {tag.latitude}"
                lat = tag.latitude
            try:
                longitude = helpers.standardize_coordinates(
                    tag.longitude, is_latitude=False
                )
            except helpers.InvalidCoordinates as e:
                print(f"{nam} has invalid longitude {tag.longitude}: {e}")
                yield f"invalid longitude {tag.longitude}"
                longitude = tag.longitude
            tags.append(TypeTag.Coordinates(lat, longitude))
        elif isinstance(tag, TypeTag.LSIDName):
            lsid = clean_lsid(tag.text)
            tags.append(TypeTag.LSIDName(lsid))
            if not is_valid_lsid(lsid):
                yield f"invalid LSID {lsid}"
        elif isinstance(tag, TypeTag.TypeSpecimenLink):
            if not tag.url.startswith(("http://", "https://")):
                yield f"invalid type specimen URL {tag.url!r}"
            tags.append(TypeTag.TypeSpecimenLink(fix_type_specimen_link(tag.url)))
        elif isinstance(tag, TypeTag.TypeSpecimenLinkFor):
            if not tag.url.startswith(("http://", "https://")):
                yield f"invalid type specimen URL {tag.url!r}"
            tags.append(
                TypeTag.TypeSpecimenLinkFor(
                    fix_type_specimen_link(tag.url), tag.specimen
                )
            )
        elif isinstance(tag, TypeTag.Organ) and tag.detail:
            new_tags = yield from check_organ_tag(tag)
            tags += new_tags
        else:
            tags.append(tag)
        # TODO: for lectotype and subsequent designations, ensure the earliest valid one is used.

    for tag_type, tags_of_type in by_type.items():
        if tag_type in UNIQUE_TAGS and len(tags_of_type) > 1:
            yield f"has multiple tags of type {tag_type}: {tags_of_type}"
    if nam.collection is not None and nam.collection.id == MULTIPLE_ID:
        repos = by_type.get(TypeTag.Repository, [])
        if len(repos) < 2:
            yield (
                "name with collection 'multiple' must have multiple Repository tags:"
                f" {repos}"
            )
    elif TypeTag.Repository in by_type:
        yield f"name may not have Repository tags: {by_type[TypeTag.Repository]}"

    tags = sorted(set(tags))
    if tags != original_tags:
        if set(tags) != set(original_tags):
            print(f"changing tags for {nam}")
            getinput.print_diff(sorted(original_tags), tags)
        if cfg.autofix:
            nam.type_tags = tags  # type: ignore


def fix_type_specimen_link(url: str) -> str:
    if url.startswith((
        "http://arctos.database.museum/",
        "http://researcharchive.calacademy.org/",
        "http://ucmpdb.berkeley.edu/cgi/",
    )):
        return url.replace("http://", "https://")
    return url


@make_linter("type_designations", disabled=True)
def check_type_designations_present(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.genus_type_kind is TypeSpeciesDesignation.subsequent_designation:
        if not any(
            tag.type == nam.type
            for tag in nam.get_tags(nam.type_tags, TypeTag.TypeDesignation)
        ):
            yield "missing a reference for type species designation"
    if (
        nam.species_type_kind is SpeciesGroupType.lectotype
        and nam.type_specimen is not None
    ):
        if not any(
            tag.lectotype == nam.type_specimen
            for tag in nam.get_tags(nam.type_tags, TypeTag.LectotypeDesignation)
        ):
            yield "missing a reference for lectotype designation"
    if (
        nam.species_type_kind is SpeciesGroupType.neotype
        and nam.type_specimen is not None
    ):
        if not any(
            tag.neotype == nam.type_specimen
            for tag in nam.get_tags(nam.type_tags, TypeTag.NeotypeDesignation)
        ):
            yield "missing a reference for neotype designation"


MULTIPLE_ID = 366
BMNH_MAMMALS_ID = 1471
BMNH_COLLECTION_ID = 5


class RepositoryKind(enum.Enum):
    repository = 1
    former = 2
    extra = 3
    future = 4


def _get_repositories(nam: Name) -> set[tuple[RepositoryKind, Collection]]:
    repos = set()
    if nam.collection is not None and nam.collection.id != MULTIPLE_ID:
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


@dataclass
class Specimen:
    text: str
    comment: str | None = None
    future_texts: Sequence[str] = field(default_factory=list)
    extra_texts: Sequence[str] = field(default_factory=list)
    former_texts: Sequence[str] = field(default_factory=list)

    def stringify(self) -> str:
        text = self.text
        if self.comment is not None:
            text += f" ({self.comment}!)"
        text += "".join(f" (=> {future})" for future in sorted(self.future_texts))
        text += "".join(f" (+ {extra})" for extra in sorted(self.extra_texts))
        text += "".join(f" (= {former})" for former in sorted(self.former_texts))
        return text

    def sort_key(self) -> tuple[object, ...]:
        numeric_match = re.fullmatch(r"(.+) (\d+)", self.text)
        return (
            1,
            numeric_match is not None,
            (
                (numeric_match.group(1), int(numeric_match.group(2)))
                if numeric_match is not None
                else self.text
            ),
            self.comment or "",
            tuple(sorted(self.future_texts)),
            tuple(sorted(self.extra_texts)),
            tuple(sorted(self.former_texts)),
        )


@dataclass
class SpecialSpecimen:
    collection: str
    label: str
    comment: str | None = None
    former_texts: Sequence[str] = field(default_factory=list)

    def stringify(self) -> str:
        text = f"{self.collection} ({self.label})"
        if self.comment is not None:
            text += f" ({self.comment}!)"
        text += "".join(f" (= {former})" for former in sorted(self.former_texts))
        return text

    def sort_key(self) -> tuple[object, ...]:
        return (
            2,
            self.collection,
            self.label,
            self.comment or "",
            tuple(sorted(self.former_texts)),
        )


@dataclass
class SpecimenRange:
    start: Specimen
    end: Specimen

    def stringify(self) -> str:
        return f"{self.start.stringify()} through {self.end.stringify()}"

    def sort_key(self) -> tuple[object, ...]:
        return (0, self.start.sort_key(), self.end.sort_key())


AnySpecimen = Specimen | SpecialSpecimen | SpecimenRange


def _split_type_spec_string(text: str) -> Iterable[str]:
    current: list[str] = []
    parens = 0
    just_saw_comma = False
    for i, c in enumerate(text):
        if just_saw_comma:
            just_saw_comma = False
            if c != " ":
                raise ValueError(f"expected space at position {i}, not {c!r}")
            continue
        if c == "(":
            parens += 1
        elif c == ")":
            parens -= 1
        elif c == "," and parens == 0:
            just_saw_comma = True
            if not current:
                raise ValueError(f"unexpected comma at position {i}")
            yield "".join(current)
            current.clear()
            continue
        current.append(c)
    if current:
        yield "".join(current)
    else:
        raise ValueError("comma at end of string")
    if parens != 0:
        raise ValueError("unbalanced parentheses")


@make_linter("type_specimen_order")
def check_type_specimen_order(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError:
        return  # reported elsewhere
    expected_text = ", ".join(
        spec.stringify() for spec in sorted(specs, key=lambda spec: spec.sort_key())
    )
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


def parse_type_specimen(text: str) -> list[AnySpecimen]:
    specs: list[AnySpecimen] = []
    for chunk in _split_type_spec_string(text):
        if " through " in chunk:
            left, right = chunk.split(" through ", maxsplit=1)
            left_spec = _parse_single_specimen(left)
            if not isinstance(left_spec, Specimen):
                raise ValueError(
                    f"range must contain a simple specimen, not {left_spec}"
                )
            right_spec = _parse_single_specimen(right)
            if not isinstance(right_spec, Specimen):
                raise ValueError(
                    f"range must contain a simple specimen, not {right_spec}"
                )
            specs.append(SpecimenRange(left_spec, right_spec))
        else:
            specs.append(_parse_single_specimen(chunk))
    return specs


_SPECIAL_SUFFIXES = (
    " (unnumbered)",
    " (no number given)",
    " (no numbers given)",
    " (lost)",
)


def _parse_single_specimen(text: str) -> Specimen | SpecialSpecimen:
    formers: list[str] = []
    futures: list[str] = []
    extras: list[str] = []
    comment = None
    while text.endswith(")"):
        if text.endswith(_SPECIAL_SUFFIXES):
            text, end = text.rsplit(" (", maxsplit=1)
            if futures or extras:
                raise ValueError(
                    f"Special specimen {text} cannot have future or extra location"
                )
            return SpecialSpecimen(
                text, end.removesuffix(")"), comment=comment, former_texts=formers
            )
        elif " (" in text:
            text, end = text.rsplit(" (", maxsplit=1)
            tail = end.removesuffix(")")
            if tail.endswith("!"):
                if comment is not None:
                    raise ValueError(f"cannot have two comments in {text}")
                comment = tail.rstrip("!")
            elif tail.startswith("=>"):
                futures.append(tail.removeprefix("=>").strip())
            elif tail.startswith("+"):
                extras.append(tail.removeprefix("+").strip())
            elif tail.startswith("="):
                formers.append(tail.removeprefix("=").strip())
            else:
                raise ValueError(f"invalid parenthesized text in {text!r}")
        else:
            raise ValueError(f"invalid parenthesized text in {text!r}")
    return Specimen(
        text,
        comment=comment,
        former_texts=formers,
        future_texts=futures,
        extra_texts=extras,
    )


@make_linter("type_specimen")
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
        elif isinstance(spec, SpecialSpecimen):
            continue
        else:
            yield from _check_specimen(spec, nam)


def _validate_specimen(
    text: str, repos: set[tuple[RepositoryKind, Collection]], kinds: set[RepositoryKind]
) -> str | None:
    messages = []
    for kind, repo in repos:
        if kind not in kinds:
            continue
        message = repo.validate_specimen(text)
        if message is None:
            return None
        messages.append(message)
    return "; ".join(messages)


def _check_specimen(spec: Specimen, nam: Name) -> Iterable[str]:
    repos = _get_repositories(nam)
    if message := _validate_specimen(spec.text, repos, {RepositoryKind.repository}):
        yield f"{spec.text!r} does not match: {message}"
    for former in spec.former_texts:
        if former.startswith('"') and former.endswith('"'):
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


@make_linter("bmnh_types")
def check_bmnh_type_specimens(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Fix some simple issues with BMNH type specimens."""
    if nam.type_specimen is None:
        return
    if nam.collection is None or nam.collection.id != BMNH_COLLECTION_ID:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError:
        return  # other check will complain
    for spec in specs:
        if not isinstance(spec, Specimen):
            continue
        if not spec.text.startswith("BMNH"):
            continue
        new_spec = clean_up_bmnh_type(spec.text)
        if new_spec != spec.text:
            message = f"replace {spec.text!r} with {new_spec!r}"
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.type_specimen = nam.type_specimen.replace(spec.text, new_spec)
            else:
                yield message


def get_all_type_specimen_texts(nam: Name) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    for spec in parse_type_specimen(nam.type_specimen):
        if isinstance(spec, Specimen):
            yield spec.text
        elif isinstance(spec, SpecimenRange):
            yield spec.start.text
            yield spec.end.text


@make_linter("child_collection")
def check_general_collection(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.collection is None:
        return
    if not nam.collection.must_use_children():
        return
    if nam.type_specimen is not None:
        for tag in nam.collection.tags:
            if (
                isinstance(tag, CollectionTag.ChildRule)
                and all(
                    re.fullmatch(tag.regex, spec)
                    for spec in get_all_type_specimen_texts(nam)
                )
                and (tag.age is None or nam.taxon.age == tag.age)
                and (tag.taxon is None or nam.taxon.is_child_of(tag.taxon))
            ):
                message = f"should use collection {tag.collection} based on rule {tag}"
                if cfg.autofix:
                    print(f"{nam}: {message}")
                    nam.collection = tag.collection
                else:
                    yield message
                return
    yield f"should use child collection of {nam.collection}"


@make_linter("type_specimen_link")
def check_must_have_type_specimen_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    # TODO: cover multiple, ExtraRepository etc. here
    # After replacing all TypeSpecimenLink tags. Then we should be able to associate every TypeSpecimenLinkFor tag with some part of the type_specimen text.
    if nam.collection is None or not nam.collection.must_have_specimen_links(nam):
        return
    if nam.type_specimen is None:
        return
    num_expected = sum(
        not isinstance(spec, SpecialSpecimen)
        for spec in parse_type_specimen(nam.type_specimen)
    )
    num_actual = sum(
        isinstance(tag, (TypeTag.TypeSpecimenLink, TypeTag.TypeSpecimenLinkFor))
        and nam.collection is not None
        and nam.collection.is_valid_specimen_link(tag.url)
        for tag in nam.type_tags
    )
    if num_actual < num_expected:
        yield (
            f"has {num_actual} type specimen links, but expected at least"
            f" {num_expected}"
        )


@make_linter("duplicate_type_specimen_links")
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
            nam.type_tags = new_tags  # type: ignore
        else:
            yield message


@make_linter("replace_simple_type_specimen_link")
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
        nam.type_tags = new_tags  # type: ignore
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


@make_linter("replace_type_specimen_link")
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
        nam.type_tags = new_tags  # type: ignore
    else:
        yield message


def get_possible_type_specimens(nam: Name) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    for spec in parse_type_specimen(nam.type_specimen):
        match spec:
            case SpecialSpecimen(
                collection=collection, label=label, former_texts=former_texts
            ):
                yield f"{collection} ({label})"
                yield from former_texts
            case SpecimenRange(start=start, end=end):
                yield from _get_possible_type_specimens_from_specimen(start)
                yield from _get_possible_type_specimens_from_specimen(end)
            case Specimen():
                yield from _get_possible_type_specimens_from_specimen(spec)


def _get_possible_type_specimens_from_specimen(spec: Specimen) -> Iterable[str]:
    yield spec.text
    yield from spec.former_texts
    yield from spec.future_texts
    yield from spec.extra_texts


def clean_up_bmnh_type(text: str) -> str:
    for rgx, replacement in _BMNH_REGEXES:
        text = re.sub(rgx, replacement, text)
    return text


@make_linter("tags")
def check_tags_for_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Looks at all tags set on names and applies related changes."""
    try:
        tags = nam.tags
    except Exception:
        yield "could not deserialize tags"
        return
    if not tags:
        return

    status_to_priority = {}
    for priority, statuses in enumerate(NomenclatureStatus.hierarchy()):
        for status in statuses:
            status_to_priority[status] = priority

    def maybe_adjust_status(nam: Name, status: NomenclatureStatus, tag: object) -> None:
        current_priority = status_to_priority[nam.nomenclature_status]
        new_priority = status_to_priority[status]
        if current_priority > new_priority:
            comment = (
                f"Status automatically changed from {nam.nomenclature_status.name} to"
                f" {status.name} because of {tag}"
            )
            print(f"changing status of {nam} and adding comment {comment!r}")
            if cfg.autofix:
                nam.add_static_comment(CommentKind.automatic_change, comment)
                nam.nomenclature_status = status  # type: ignore

    for tag in tags:
        if isinstance(tag, NameTag.PreoccupiedBy):
            maybe_adjust_status(nam, NomenclatureStatus.preoccupied, tag)
            senior_name = tag.name
            if nam.group != senior_name.group:
                yield (
                    f"is of a different group than supposed senior name {senior_name}"
                )
            if senior_name.nomenclature_status is NomenclatureStatus.subsequent_usage:
                for senior_name_tag in senior_name.get_tags(
                    senior_name.tags, NameTag.SubsequentUsageOf
                ):
                    senior_name = senior_name_tag.name
            if senior_name.nomenclature_status is NomenclatureStatus.name_combination:
                for senior_name_tag in senior_name.get_tags(
                    senior_name.tags, NameTag.NameCombinationOf
                ):
                    senior_name = senior_name_tag.name
            if nam.get_date_object() < senior_name.get_date_object():
                yield f"predates supposed senior name {senior_name}"
            # TODO apply this check to species too by handling gender endings correctly.
            if nam.group is not Group.species:
                if nam.root_name != tag.name.root_name:
                    yield (
                        "has a different root name than supposed senior name"
                        f" {senior_name}"
                    )
        elif isinstance(
            tag,
            (
                NameTag.UnjustifiedEmendationOf,
                NameTag.IncorrectSubsequentSpellingOf,
                NameTag.VariantOf,
                NameTag.NomenNovumFor,
                NameTag.JustifiedEmendationOf,
                NameTag.SubsequentUsageOf,
                NameTag.NameCombinationOf,
            ),
        ):
            for status, tag_cls in STATUS_TO_TAG.items():
                if isinstance(tag, tag_cls):
                    maybe_adjust_status(nam, status, tag)
            if nam.get_date_object() < tag.name.get_date_object():
                yield f"predates supposed original name {tag.name}"
            if not isinstance(tag, NameTag.SubsequentUsageOf):
                if nam.taxon != tag.name.taxon:
                    yield f"{nam} is not assigned to the same name as {tag.name}"
        elif isinstance(tag, NameTag.PartiallySuppressedBy):
            maybe_adjust_status(nam, NomenclatureStatus.partially_suppressed, tag)
        elif isinstance(tag, NameTag.FullySuppressedBy):
            maybe_adjust_status(nam, NomenclatureStatus.fully_suppressed, tag)
        elif isinstance(tag, NameTag.Conserved):
            if nam.nomenclature_status not in (
                NomenclatureStatus.available,
                NomenclatureStatus.as_emended,
                NomenclatureStatus.nomen_novum,
                NomenclatureStatus.preoccupied,
            ):
                yield f"{nam} is on the Official List, but is not marked as available."
        # haven't handled TakesPriorityOf, NomenOblitum, MandatoryChangeOf


@make_linter("required_tags")
def check_required_tags(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.nomenclature_status not in STATUS_TO_TAG:
        return
    tag_cls = STATUS_TO_TAG[nam.nomenclature_status]
    tags = list(nam.get_tags(nam.tags, tag_cls))
    if not tags:
        yield (f"has status {nam.nomenclature_status.name} but no corresponding tag")


@make_linter("lsid")
def check_for_lsid(nam: Name, cfg: LintConfig) -> Iterable[str]:
    # ICZN Art. 8.5.1: ZooBank is relevant to availability only starting in 2012
    if (
        nam.numeric_year() < 2012
        or nam.corrected_original_name is None
        or nam.original_citation is None
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


@make_linter("year")
def check_year(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.year is None:
        return
    if not helpers.is_valid_date(nam.year):
        yield f"has invalid year {nam.year!r}"
    if helpers.is_date_range(nam.year):
        yield "year is a range"


@make_linter("year_matches")
def check_year_matches(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_citation is None:
        return

    if nam.original_citation.has_tag(
        ArticleTag.UnavailableElectronic
    ) and nam.nomenclature_status not in (
        NomenclatureStatus.unpublished_electronic,
        NomenclatureStatus.unpublished_pending,
    ):
        yield (
            "was published in unavailable electronic-only publication and should be"
            " marked as unpublished_electronic"
        )
        if cfg.autofix:
            nam.nomenclature_status = NomenclatureStatus.unpublished_electronic  # type: ignore
    elif nam.nomenclature_status in (
        NomenclatureStatus.unpublished_electronic,
        NomenclatureStatus.unpublished_pending,
    ) and not nam.original_citation.has_tag(ArticleTag.UnavailableElectronic):
        yield (
            "is marked as unpublished_electronic, but original citation is not marked"
            " accordingly"
        )

    if (
        nam.original_citation.type is ArticleType.THESIS
        and nam.nomenclature_status is not NomenclatureStatus.unpublished_thesis
    ):
        yield "was published in a thesis and should be marked as unpublished_thesis"
        if cfg.autofix:
            nam.nomenclature_status = NomenclatureStatus.unpublished_thesis  # type: ignore
    elif (
        nam.nomenclature_status is NomenclatureStatus.unpublished_thesis
        and nam.original_citation.type is not ArticleType.THESIS
    ):
        yield "is marked as unpublished_thesis, but was not published in a thesis"

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


@make_linter("disallowed_attributes")
def check_disallowed_attributes(nam: Name, cfg: LintConfig) -> Iterable[str]:
    for field_name, groups in ATTRIBUTES_BY_GROUP.items():
        if nam.group not in groups:
            value = getattr(nam, field_name)
            if value is not None:
                yield f"should not have attribute {field_name} (value {value})"
    if (
        nam.species_name_complex is not None
        and not nam.nomenclature_status.requires_name_complex()
    ):
        message = (
            f"is of status {nam.nomenclature_status.name} and should not have a"
            " name complex"
        )
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_data(
                "species_name_complex",
                nam.species_name_complex.id,
                concat_duplicate=True,
            )
            nam.species_name_complex = None
        else:
            yield message


def _make_con_messsage(nam: Name, text: str) -> str:
    return f"corrected original name {nam.corrected_original_name!r} {text}"


@make_linter("corrected_original_name")
def check_corrected_original_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Check that corrected_original_names are correct."""
    if nam.corrected_original_name is None:
        return
    if nam.nomenclature_status.permissive_corrected_original_name():
        return
    inferred = nam.infer_corrected_original_name()
    if inferred is not None and inferred != nam.corrected_original_name:
        yield _make_con_messsage(
            nam,
            f"inferred name {inferred!r} does not match current name"
            f" {nam.corrected_original_name!r}",
        )
    if not re.match(r"^[A-Z][a-z ]+$", nam.corrected_original_name):
        yield _make_con_messsage(nam, "contains unexpected characters")
        return
    if nam.group in (Group.high, Group.genus):
        if " " in nam.corrected_original_name:
            yield _make_con_messsage(nam, "contains whitespace")
        elif nam.corrected_original_name != nam.root_name:
            emended = nam.get_tag_target(NameTag.AsEmendedBy)
            if emended is not None and emended.root_name == nam.root_name:
                return
            yield _make_con_messsage(nam, f"does not match root_name {nam.root_name!r}")
    elif nam.group is Group.family:
        if nam.nomenclature_status is NomenclatureStatus.not_based_on_a_generic_name:
            possibilities = {
                f"{nam.root_name}{suffix}" for suffix in helpers.VALID_SUFFIXES
            }
            if nam.corrected_original_name not in {nam.root_name} | possibilities:
                yield _make_con_messsage(
                    nam, f"does not match root_name {nam.root_name!r}"
                )
        elif not nam.corrected_original_name.endswith(tuple(helpers.VALID_SUFFIXES)):
            yield _make_con_messsage(
                nam, "does not end with a valid family-group suffix"
            )
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


@make_linter("root_name")
def check_root_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Check that root_names are correct."""
    if nam.nomenclature_status.permissive_corrected_original_name():
        return
    if nam.group in (Group.high, Group.genus, Group.family):
        if not re.match(r"^[A-Z][a-z]+$", nam.root_name):
            yield _make_rn_message(nam, "contains unexpected characters")
    elif nam.group is Group.species:
        if not re.match(r"^[a-z]+$", nam.root_name):
            yield _make_rn_message(nam, "contains unexpected characters")
            return
        yield from _check_species_name_gender(nam, cfg)


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
    if corrected_original_name is None:
        return
    con_root = corrected_original_name.split()[-1]
    # For nomina dubia we always follow the original name
    if nam.status in (Status.nomen_dubium, Status.species_inquirenda):
        yield from _check_rn_matches_original(
            nam, corrected_original_name, cfg, nam.status.name
        )
        return
    # If there is no name complex, the root_name should match exactly
    if nam.species_name_complex is None:
        yield from _check_rn_matches_original(
            nam, corrected_original_name, cfg, "no name complex"
        )
        return
    if nam.species_name_complex.kind is not SpeciesNameKind.adjective:
        yield from _check_rn_matches_original(
            nam, corrected_original_name, cfg, "not an adjective"
        )
        return
    if nam.species_name_complex.is_invariant_adjective():
        yield from _check_rn_matches_original(
            nam, corrected_original_name, cfg, "invariant adjective"
        )
        return
    # Now we have an adjective that needs to agree in gender with its genus, so we
    # have to find the genus. But first we check whether the name even makes sense.
    try:
        forms = list(nam.species_name_complex.get_forms(nam.root_name))
    except ValueError as e:
        yield _make_con_messsage(nam, f"has invalid name complex: {e!r}")
        return
    if con_root not in forms:
        yield _make_con_messsage(nam, f"does not match root_name {nam.root_name!r}")
        return

    taxon = nam.taxon
    genus = taxon.get_logical_genus() or taxon.get_nominal_genus()
    if genus is None or genus.name_complex is None:
        return

    genus_gender = genus.name_complex.gender
    expected_form = nam.species_name_complex.get_form(con_root, genus_gender)
    if expected_form != nam.root_name:
        message = _make_rn_message(
            nam,
            f"does not match expected form {expected_form!r} for"
            f" {genus_gender.name} genus {genus}",
        )
        if cfg.autofix:
            print(f"{nam}: {message}")
            comment = (
                f"Name changed from {nam.root_name!r} to {expected_form!r} to agree in"
                f" gender with {genus_gender.name} genus {genus} ({{n#{genus.id}}})"
            )
            nam.add_static_comment(CommentKind.automatic_change, comment)
            nam.root_name = expected_form
        else:
            yield message


@make_linter("family_root_name")
def check_family_root_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group is not Group.family or nam.type is None:
        return
    if nam.is_unavailable():
        return
    try:
        stem_name = nam.type.get_stem()
    except ValueError:
        yield f"{nam.type} has bad name complex: {nam.type.name_complex}"
        return
    if stem_name is None:
        return
    if nam.root_name == stem_name:
        return
    if nam.root_name + "id" == stem_name:
        # The Code allows eliding -id- from the stem.
        return
    for stripped in helpers.name_with_suffixes_removed(nam.root_name):
        if stripped == stem_name or stripped + "i" == stem_name:
            print(f"{nam}: Autocorrecting root name: {nam.root_name} -> {stem_name}")
            if cfg.autofix:
                nam.root_name = stem_name
            break
    if nam.root_name != stem_name:
        if nam.has_type_tag(TypeTag.IncorrectGrammar):
            return
        yield f"Stem mismatch: {nam.root_name} vs. {stem_name}"


@make_linter("type_taxon")
def correct_type_taxon(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Check that a name's type belongs to a child of the name's taxon."""
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


@make_linter("verbatim")
def clean_up_verbatim(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.verbatim_type is not None and (
        nam.type is not None or "type" not in nam.get_required_fields()
    ):
        message = f"cleaning up verbatim type: {nam.type}, {nam.verbatim_type}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_data("verbatim_type", nam.verbatim_type, concat_duplicate=True)
            nam.verbatim_type = None
        else:
            yield message
    if (
        nam.group is Group.species
        and nam.verbatim_type is not None
        and nam.type_specimen is not None
    ):
        message = f"cleaning up verbatim type: {nam.type_specimen}, {nam.verbatim_type}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_data("verbatim_type", nam.verbatim_type, concat_duplicate=True)
            nam.verbatim_type = None
        else:
            yield message
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


@make_linter("status")
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
            f" {nam.taxon.parent.base_name.status}"
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


@make_linter("justified_emendation")
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
        else:
            # Else it should be a justified emendation for something straightforward
            # (e.g., removing diacritics), so the root_name should match.
            # But the CON may not match exactly, because the species may have moved genera etc.
            if nam.root_name != target.root_name:
                yield (
                    f"root name {nam.root_name} does not match emended name {target}"
                )
    elif nam.nomenclature_status is NomenclatureStatus.incorrect_original_spelling:
        ios_target = nam.get_tag_target(NameTag.IncorrectOriginalSpellingOf)
        if ios_target is None:
            yield "missing IncorrectOriginalSpellingOf tag"
            return
        # Incorrect original spellings are used where there are multiple spellings
        # in the original publication, and one is selected as valid. Then both names
        # should have the same author etc.
        yield from _check_names_match(nam, ios_target)


@make_linter("autoset_original_rank")
def autoset_original_rank(nam: Name, cfg: LintConfig) -> Iterable[str]:
    nam.autoset_original_rank(dry_run=not cfg.autofix)
    return []


@make_linter("corrected_original_name")
def autoset_corrected_original_name(
    nam: Name, cfg: LintConfig, aggressive: bool = False
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
        yield (f"could not infer corrected original name from {nam.original_name!r}")


@make_linter("fill_data_level")
def check_fill_data_level(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_citation is None:
        return
    level, reason = nam.fill_data_level()
    if level > FillDataLevel.missing_required_fields:
        return
    if (
        nam.original_citation.has_tag(ArticleTag.NeedsTranslation)
        or nam.original_citation.is_non_original()
    ):
        return
    yield f"missing basic data: {reason}"


@make_linter("citation_group")
def check_citation_group(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.citation_group is None or nam.year is None:
        return
    if message := nam.citation_group.is_year_in_range(nam.numeric_year()):
        yield message


@make_linter("matches_citation")
def check_matches_citation(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_citation is None or nam.page_described is None:
        return
    art = nam.original_citation
    if art.type not in (ArticleType.JOURNAL, ArticleType.CHAPTER, ArticleType.PART):
        return
    start_page = art.numeric_start_page()
    end_page = art.numeric_end_page()
    if not start_page or not end_page:
        return
    page_range = range(start_page, end_page + 1)
    for page in extract_pages(nam.page_described):
        try:
            numeric_page = int(page)
        except ValueError:
            continue
        if numeric_page not in page_range:
            yield (f"{nam.page_described} is not in {start_page}–{end_page} for {art}")


def extract_pages(page_described: str) -> Iterable[str]:
    page_described = re.sub(r" \([^\)]+\)(?=, |$)", "", page_described)
    parts = page_described.split(", ")
    for part in parts:
        part = re.sub(r" \[as [0-9]+\]$", "", part)
        yield part


@make_linter("page_described")
def check_page_described(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.page_described is None:
        return
    # For now ignore names without a citation
    if nam.original_citation is None:
        return
    for part in extract_pages(nam.page_described):
        if part.isdecimal() and part.isascii():
            continue
        if re.fullmatch(r"[0-9]+-[0-9]+", part):
            continue
        if part.startswith("pl. "):
            number = part.removeprefix("pl. ")
            if helpers.is_valid_roman_numeral(number):
                continue
            if re.fullmatch(r"[A-Z]?[0-9]+[A-Za-z]*", number):
                continue
        if helpers.is_valid_roman_numeral(part):
            continue
        # Pretty common to see "S40" or "40A"
        if re.fullmatch(r"[A-Z]?[0-9]+[A-Z]?", part):
            continue
        if part in (
            "unnumbered",
            "cover",
            "foldout",
            "erratum",
            "addenda",
            "table of contents",
        ):
            continue
        yield f"invalid part {part!r} in {nam.page_described!r}"


_JG2015 = "{Australia (Jackson & Groves 2015).pdf}"
_JG2015_RE = re.compile(rf"\[From {re.escape(_JG2015)}: [^\[\]]+ \[([A-Za-z\s\d]+)\]\]")
_JG2015_RE2 = re.compile(rf" \[([A-Za-z\s\d]+)\]\ \[from {re.escape(_JG2015)}\]")


@make_linter("extract_date_from_verbatim")
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
        DateSource.external, parsed, f'"{raw_date}" {source}'
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


@make_linter("extract_date_from_structured_quote")
def extract_date_from_structured_quotes(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not nam.original_citation:
        return
    for comment in nam.comments.filter(
        NameComment.source
        << (
            34056,  # {Mammalia-USNM types (Fisher & Ludwig 2015).pdf}
            33916,  # {Anomaluromorpha, Hystricomorpha, Myomorpha-USNM types.pdf}
            29833,  # {Ferungulata-USNM types.pdf}
            15513,  # {Castorimorpha, Sciuromorpha-USNM types.pdf}
            9585,  # Mammalia-AMNH types (Lawrence 1993).pdf
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
            dt = datetime.strptime(date_str, f"{month} %Y")
        except ValueError:
            pass
        else:
            return f"{dt.year}-{dt.month:02d}"
        for prefix in ("", "0"):
            try:
                dt = datetime.strptime(date_str, f"{prefix}%d {month} %Y")
            except ValueError:
                pass
            else:
                return f"{dt.year}-{dt.month:02d}-{dt.day:02d}"
    return None


@make_linter("data")
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


@make_linter("specific_authors")
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
            if cfg.interactive and "specific_authors" not in get_ignored_lints(nam):
                author.edit_tag_sequence_on_object(
                    nam, "author_tags", AuthorTag.Author, "names"
                )
        elif nam.verbatim_citation is not None and helpers.simplify_string(
            author.family_name
        ) in helpers.simplify_string(nam.verbatim_citation):
            yield f"author {author} (position {i}) appears in verbatim citation"
            if cfg.interactive and "specific_authors" not in get_ignored_lints(nam):
                author.edit_tag_sequence_on_object(
                    nam, "author_tags", AuthorTag.Author, "names"
                )


@make_linter("required_fields")
def check_required_fields(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.verbatim_citation and not nam.citation_group:
        yield "has verbatim citation but no citation group"
    if (
        nam.original_citation
        and not nam.page_described
        and not nam.original_citation.is_non_original()
    ):
        yield "has original citation but no page_described"
    if (
        nam.numeric_year() > 1970
        and not nam.verbatim_citation
        and not nam.original_citation
    ):
        yield "recent name must have verbatim citation"
    if nam.type_specimen is not None and nam.species_type_kind is None:
        yield "has type_specimen but no species_type_kind"


@make_linter("synonym_group")
def check_synonym_group(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group not in (Group.genus, Group.species):
        return
    if nam.taxon.base_name.group is not nam.group:
        yield (
            f"taxon is of group {nam.taxon.base_name.group.name} but name is of group"
            f" {nam.group.name}"
        )


@make_linter("composites")
def check_composites(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.status not in (Status.composite, Status.hybrid):
        return
    children = list(nam.taxon.children)
    if children:
        different_status = [
            child for child in children if child.status is not nam.status
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


def run_linters(
    nam: Name, cfg: LintConfig, *, include_disabled: bool = False
) -> Iterable[str]:
    if include_disabled:
        linters = [*LINTERS, *DISABLED_LINTERS]
    else:
        linters = [*LINTERS]

    used_ignores = set()
    for linter in linters:
        used_ignores |= yield from linter(nam, cfg)
    actual_ignores = get_ignored_lints(nam)
    unused = actual_ignores - used_ignores
    if unused:
        if cfg.autofix:
            tags = nam.type_tags or ()
            new_tags = []
            for tag in tags:
                if isinstance(tag, TypeTag.IgnoreLintName) and tag.label in unused:
                    print(f"{nam}: removing unused IgnoreLint tag: {tag}")
                else:
                    new_tags.append(tag)
            nam.type_tags = new_tags  # type: ignore
        else:
            yield f"{nam}: has unused IgnoreLint tags {', '.join(unused)}"
