"""

Lint steps for Names.

"""

from __future__ import annotations

import enum
import functools
import itertools
import json
import re
import traceback
from collections import defaultdict
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from dataclasses import replace
from datetime import datetime
from typing import Any, TypeVar

import requests

from taxonomy.apis import bhl

from .... import adt, getinput
from ....apis.zoobank import clean_lsid, get_zoobank_data, is_valid_lsid
from ... import helpers
from ...constants import (
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
    Rank,
    SpeciesGroupType,
    SpeciesNameKind,
    SpecimenOrgan,
    Status,
    TypeSpeciesDesignation,
)
from ..article import Article, ArticleTag, PresenceStatus
from ..base import LintConfig
from ..collection import BMNH_COLLECTION, MULTIPLE_COLLECTION, Collection
from ..person import AuthorTag, PersonLevel
from ..taxon import Taxon
from .name import PREOCCUPIED_TAGS, STATUS_TO_TAG, Name, NameComment, NameTag, TypeTag
from .organ import CHECKED_ORGANS, ParsedOrgan, ParseException, parse_organ_detail
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
)

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
            ignored_lints = get_ignored_lints(nam)
            if label in ignored_lints:
                cfg = replace(cfg, interactive=False)
            try:
                # static analysis: ignore[incompatible_call]
                issues = list(linter(nam, cfg, **kwargs))
            except Exception as e:
                traceback.print_exc()
                yield f"{nam}: error running {label} linter: {e}"
                return set()
            if not issues:
                return set()
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
) -> tuple[SpecimenOrgan, str] | None:
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
    if len(parts) == 1 or (
        not condition and "L" not in parts and "R" not in parts and "(" not in detail
    ):
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


def check_organ_tag_with_parser(
    organ: SpecimenOrgan, detail: str
) -> Generator[str, None, str]:
    if organ not in CHECKED_ORGANS:
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
        detail = ""
    detail = yield from check_organ_tag_with_parser(organ, detail)
    new_tags.append(TypeTag.Organ(organ, detail, condition))
    return new_tags


ALLOWED_AUTHORITY_PAGE_LINK_TYPES = {
    bhl.UrlType.bhl_page,
    bhl.UrlType.google_books,
    bhl.UrlType.archive_org,
}


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
            try:
                specs = parse_type_specimen(tag.specimen)
            except ValueError as e:
                yield f"invalid type specimen {tag.specimen!r} in {tag}: {e}"
                new_spec = tag.specimen
            else:
                new_spec = stringify_specimen_list(specs)
            tags.append(
                TypeTag.TypeSpecimenLinkFor(fix_type_specimen_link(tag.url), new_spec)
            )
        elif isinstance(tag, TypeTag.Organ) and tag.detail:
            new_tags = yield from check_organ_tag(tag)
            tags += new_tags
        elif isinstance(tag, TypeTag.AuthorityPageLink):
            url = bhl.parse_possible_bhl_url(tag.url)
            if url.url_type not in ALLOWED_AUTHORITY_PAGE_LINK_TYPES:
                yield f"invalid authority page link {url!r}"
            if nam.page_described is not None:
                allowed_pages = list(extract_pages(nam.page_described))
                if tag.page not in allowed_pages:
                    yield f"authority page link {tag.url} for page {tag.page!r}" f" does not match any pages in page_described ({allowed_pages})"
            tags.append(
                TypeTag.AuthorityPageLink(str(url), tag.confirmed, str(tag.page))
            )
        else:
            tags.append(tag)
        # TODO: for lectotype and subsequent designations, ensure the earliest valid one is used.

    for tag_type, tags_of_type in by_type.items():
        if tag_type in UNIQUE_TAGS and len(tags_of_type) > 1:
            yield f"has multiple tags of type {tag_type}: {tags_of_type}"
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
            nam.type_tags = tags  # type: ignore


@make_linter("dedupe_tags")
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
        all_tags.add(TypeTag.Organ(organ, ", ".join(detail), ""))
    tags = sorted(all_tags)
    if tags != original_tags:
        if set(tags) != set(original_tags):
            print(f"changing tags for {nam}")
            getinput.print_diff(sorted(original_tags), tags)
        if cfg.autofix:
            nam.type_tags = tags  # type: ignore
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


@make_linter("type_specimen_order")
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


@make_linter("bmnh_types")
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


@make_linter("type_specimen_link")
def check_must_have_type_specimen_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    # TODO: cover multiple, ExtraRepository etc. here
    # After replacing all TypeSpecimenLink tags. Then we should be able to associate every TypeSpecimenLinkFor tag with some part of the type_specimen text.
    if nam.collection is None or not nam.collection.must_have_specimen_links(nam):
        return
    if nam.type_specimen is None:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError:
        return  # other check will complain
    num_expected = sum(
        not (
            isinstance(spec, Specimen)
            and isinstance(spec.base, (SpecialSpecimen, InformalSpecimen))
        )
        for spec in specs
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


def _check_preoccupation_tag(tag: NameTag, nam: Name) -> Generator[str, None, NameTag]:
    senior_name = tag.name
    new_tag = tag
    if nam.group != senior_name.group:
        yield f"is of a different group than supposed senior name {senior_name}"
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
    if nam.group is Group.species:
        if nam.original_parent is None:
            my_original = None
        else:
            my_original = _resolve_original_parent(nam.original_parent, depth=0)
        if senior_name.original_parent is None:
            senior_original = None
        else:
            senior_original = _resolve_original_parent(
                senior_name.original_parent, depth=0
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
                new_tag = NameTag.PrimaryHomonymOf(tag.name, tag.comment)
            my_genus = _get_parent(nam)
            senior_genus = _get_parent(senior_name)
            if my_genus != senior_genus:
                yield (
                    f"{nam} is marked as a secondary homonym of {senior_name}, but is not currently placed in the same genus"
                )
        elif isinstance(tag, NameTag.PreoccupiedBy):
            if my_original is not None and my_original == senior_original:
                new_tag = NameTag.PrimaryHomonymOf(tag.name, tag.comment)
            elif _get_parent(nam) == _get_parent(senior_name):
                new_tag = NameTag.SecondaryHomonymOf(tag.name, tag.comment)
    else:
        if isinstance(tag, (NameTag.PrimaryHomonymOf, NameTag.SecondaryHomonymOf)):
            yield f"{nam} is not a species-group name, but uses {type(tag).__name__} tag"
            new_tag = NameTag.PreoccupiedBy(tag.name, tag.comment)
    if (
        nam.get_normalized_root_name_for_homonymy()
        != senior_name.get_normalized_root_name_for_homonymy()
    ):
        yield ("has a different root name than supposed senior name" f" {senior_name}")
    if not senior_name.can_preoccupy():
        yield f"senior name {senior_name} is not available"
    return new_tag


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

    new_tags = []
    for tag in tags:
        new_tag: NameTag | None = tag
        if isinstance(tag, PREOCCUPIED_TAGS):
            new_tag = yield from _check_preoccupation_tag(tag, nam)
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
            if nam.get_date_object() < tag.name.get_date_object():
                yield f"predates supposed original name {tag.name}"
            if isinstance(tag, NameTag.SubsequentUsageOf):
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
                    new_tag = NameTag.NameCombinationOf(tag.name, tag.comment)
            else:
                if nam.taxon != tag.name.taxon:
                    yield f"{nam} is not assigned to the same name as {tag.name}"
            if isinstance(tag, NameTag.VariantOf) and nam.original_citation is not None:
                # should be specified to unjustified emendation or incorrect subsequent spelling
                yield f"{nam} is marked as a variant, but has an original citation"

        elif isinstance(tag, NameTag.Conserved):
            if nam.nomenclature_status not in (
                NomenclatureStatus.available,
                NomenclatureStatus.as_emended,
                NomenclatureStatus.nomen_novum,
                NomenclatureStatus.preoccupied,
            ):
                yield f"{nam} is on the Official List, but is not marked as available."

        elif isinstance(tag, NameTag.Condition):
            if tag.status in get_inherent_nomenclature_statuses(nam):
                yield f"has redundant Condition tag for {tag.status.name}"
                if not tag.comment:
                    new_tag = None
            else:
                statuses_from_tags = get_applicable_nomenclature_statuses_from_tags(
                    nam, exclude_condition=True
                )
                if tag.status in statuses_from_tags:
                    yield f"has Condition tag for {tag.status.name}, but already has a more specific tag"
                    if not tag.comment:
                        new_tag = None
                if (
                    tag.status is NomenclatureStatus.infrasubspecific
                    and NomenclatureStatus.variety_or_form in statuses_from_tags
                ):
                    yield "is marked as infrasubspecific, but also as variety or form"
                    if not tag.comment:
                        new_tag = None

                if tag.status is NomenclatureStatus.variety_or_form:
                    new_tag = NameTag.VarietyOrForm(tag.comment)
                elif tag.status is NomenclatureStatus.not_used_as_valid:
                    new_tag = NameTag.NotUsedAsValid(tag.comment)

                if tag.status is NomenclatureStatus.infrasubspecific:
                    possibility = should_be_infrasubspecific(nam)
                    if possibility is Possibility.no:
                        yield "is marked as infrasubspecific, but should not be"

            # TODO: lint against other statuses that have their own tag
        elif isinstance(tag, NameTag.VarietyOrForm):
            possibility = should_be_infrasubspecific(nam)
            if possibility is Possibility.no:
                yield "is marked as a variety or form, but should not be"

        # haven't handled TakesPriorityOf, NomenOblitum, MandatoryChangeOf
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
            yield tag.status
        elif isinstance(tag, PREOCCUPIED_TAGS):
            yield NomenclatureStatus.preoccupied
        elif isinstance(tag, NameTag.VarietyOrForm):
            # ICZN Art. 45.6.4.1: a name originally published as a variety or form before 1961
            # is available from its original date if it was used as valid before 1985
            if nam.numeric_year() < 1961 and any(
                isinstance(tag, NameTag.ValidUse) and tag.source.numeric_year() < 1985
                for tag in nam.tags
            ):
                continue
            yield NomenclatureStatus.variety_or_form
        elif isinstance(tag, NameTag.NotUsedAsValid):
            # ICZN Art. 11.6.1: a name originally published as a synonym before 1961
            # is available from its original date if it was used as valid before 1961
            if nam.numeric_year() < 1961 and any(
                isinstance(tag, NameTag.ValidUse) and tag.source.numeric_year() < 1961
                for tag in nam.tags
            ):
                continue
            yield NomenclatureStatus.not_used_as_valid
        elif type(tag) in TAG_TO_STATUS:
            yield TAG_TO_STATUS[type(tag)]


def get_inherent_nomenclature_statuses(nam: Name) -> Iterable[NomenclatureStatus]:
    # Allow 1757 because of spiders
    if nam.year is not None and nam.numeric_year() < 1757:
        yield NomenclatureStatus.before_1758
    if nam.original_citation is not None:
        if nam.original_citation.has_tag(ArticleTag.UnavailableElectronic):
            yield NomenclatureStatus.unpublished_electronic
        if nam.original_citation.has_tag(ArticleTag.InPress):
            yield NomenclatureStatus.unpublished_pending
        if nam.original_citation.type is ArticleType.THESIS:
            yield NomenclatureStatus.unpublished_thesis


@functools.cache
def _get_status_priorities() -> dict[NomenclatureStatus, int]:
    status_to_priority = {}
    i = 0
    for statuses in NomenclatureStatus.hierarchy():
        for status in statuses:
            status_to_priority[status] = i
            i += 1
    return status_to_priority


@make_linter("expected_nomenclature_status")
def check_expected_nomenclature_status(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Check if the nomenclature status is as expected."""
    priority_map = _get_status_priorities()
    applicable_from_tags = set(get_applicable_nomenclature_statuses_from_tags(nam))
    if (
        NomenclatureStatus.infrasubspecific in applicable_from_tags
        and NomenclatureStatus.variety_or_form in applicable_from_tags
    ):
        yield "has both infrasubspecific and variety/form tags"

    inherent = set(get_inherent_nomenclature_statuses(nam))
    applicable = applicable_from_tags | inherent
    expected_status = min(
        applicable,
        key=lambda status: priority_map[status],
        default=NomenclatureStatus.available,
    )

    if nam.nomenclature_status is not expected_status:
        message = (
            f"has status {nam.nomenclature_status.name}, but expected"
            f" {expected_status.name}"
        )
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.nomenclature_status = expected_status  # type: ignore[assignment]
        else:
            yield message


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


def _make_con_messsage(nam: Name, text: str) -> str:
    return f"corrected original name {nam.corrected_original_name!r} {text}"


CON_REGEX = re.compile(r"^[A-Z][a-z]+( [a-z]+){0,3}$")


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
    if not CON_REGEX.fullmatch(nam.corrected_original_name):
        yield _make_con_messsage(nam, "contains unexpected characters")
        return
    if (
        nam.original_name is not None
        and nam.group is not Group.family
        and nam.original_name != nam.corrected_original_name
        and nam.original_name.count(" ") == nam.corrected_original_name.count(" ")
        and CON_REGEX.fullmatch(nam.original_name)
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


@make_linter("type_is_child")
def check_type_is_child(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Checks that the type taxon is a child of the name's taxon."""
    if nam.type is None:
        return
    if nam.type.taxon.is_child_of(nam.taxon):
        return
    yield f"type {nam.type} is not a child of {nam.taxon}"


@make_linter("infer_family_group_type")
def infer_family_group_type(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if (
        nam.group is not Group.family
        or nam.type is not None
        or "type" not in nam.get_required_fields()
    ):
        return
    possible_types = [
        child_nam
        for child_nam in nam.taxon.all_names()
        if child_nam.group is Group.genus
        and child_nam.name_complex is not None
        and child_nam.safe_get_stem() == nam.root_name
    ]
    if len(possible_types) != 1:
        return
    message = f"inferred type {possible_types[0]} for family {nam}"
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.type = possible_types[0]
    else:
        yield message


@make_linter("name_complex")
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
        return


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
            f" {nam.taxon.parent.base_name.status.name}"
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


@make_linter("data_level")
def check_data_level(nam: Name, cfg: LintConfig) -> Iterable[str]:
    ocdl, ocdl_reason = nam.original_citation_data_level()
    ndl, ndl_reason = nam.name_data_level()
    match ocdl:
        case OriginalCitationDataLevel.no_citation:
            pass
        case OriginalCitationDataLevel.no_data:
            if ndl < NameDataLevel.missing_details_tags:
                yield f"has no data from original ({ocdl_reason}), but missing important fields: {ndl_reason}"
        case OriginalCitationDataLevel.some_data:
            if ndl is NameDataLevel.missing_crucial_fields:
                yield f"has some data from original ({ocdl_reason}), but missing crucial data: {ndl_reason}"
        case OriginalCitationDataLevel.all_required_data:
            if ndl is NameDataLevel.missing_crucial_fields:
                yield f"has data from original, but missing crucial data: {ndl_reason}"


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


@make_linter("no_page_ranges")
def no_page_ranges(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.page_described is None:
        return
    # Only applicable if there is a citation. This ensures we check
    # if the range is appropriate when adding a citation.
    if nam.original_citation is None:
        return
    for part in extract_pages(nam.page_described):
        if re.fullmatch(r"[0-9]+-[0-9]+", part):
            # Ranges should only be used in very rare cases (e.g., where the
            # name itself literally extends across multiple pages). Enforce
            # an explicit IgnoreLintName in such cases.
            yield f"page_describes contains range: {part}"


@make_linter("infer_page_described")
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
            if re.fullmatch(r"[A-Z]?-?[0-9]+[A-Za-z]*", number):
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


@make_linter("composites")
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


# TODO:
# - Add homonymy check for genus-group names
# - Remove/lint against NotPreoccupiedBy if it *is* preoccupied
@make_linter("species_secondary_homonym")
def check_species_group_secondary_homonyms(nam: Name, cfg: LintConfig) -> Iterable[str]:
    yield from _check_species_group_homonyms(nam, primary=False, fuzzy=False, cfg=cfg)


@make_linter("species_primary_homonym")
def check_species_group_primary_homonyms(nam: Name, cfg: LintConfig) -> Iterable[str]:
    yield from _check_species_group_homonyms(nam, primary=True, fuzzy=False, cfg=cfg)


@make_linter("species_fuzzy_secondary_homonym")
def check_species_group_fuzzy_secondary_homonyms(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    yield from _check_species_group_homonyms(nam, primary=False, fuzzy=True, cfg=cfg)


@make_linter("species_fuzzy_primary_homonym")
def check_species_group_fuzzy_primary_homonyms(
    nam: Name, cfg: LintConfig
) -> Iterable[str]:
    yield from _check_species_group_homonyms(nam, primary=True, fuzzy=True, cfg=cfg)


@make_linter("genus_homonym")
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
    nam: Name, *, primary: bool, fuzzy: bool, cfg: LintConfig
) -> Iterable[str]:
    if nam.group is not Group.species:
        return
    if not nam.can_preoccupy():
        return
    if primary:
        genus = nam.original_parent
        if genus is None:
            return
        genus = _resolve_original_parent(genus, depth=0)
        name_dict = _get_primary_names_of_genus_and_variants(genus, fuzzy=fuzzy)
    else:
        genus = _get_parent(nam)
        if genus is None:
            return
        name_dict = _get_secondary_names_of_genus(genus, fuzzy=fuzzy)
    root = (
        nam.get_normalized_root_name_for_homonymy()
        if fuzzy
        else nam.get_normalized_root_name()
    )
    possible_homonyms = name_dict.get(root, [])
    yield from _check_homonym_list(
        nam, possible_homonyms, primary=primary, fuzzy=fuzzy, cfg=cfg
    )


def _check_homonym_list(
    nam: Name,
    possible_homonyms: Sequence[Name],
    *,
    primary: bool = True,
    fuzzy: bool = False,
    cfg: LintConfig,
) -> Iterable[str]:
    relevant_names = [
        other_nam
        for other_nam in possible_homonyms
        if nam != other_nam
        and other_nam.can_preoccupy()
        and other_nam.get_date_object() < nam.get_date_object()
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
    already_variant_of = {
        tag.name
        for tag in nam.tags
        if isinstance(
            tag,
            (
                NameTag.PreoccupiedBy,
                NameTag.UnjustifiedEmendationOf,
                NameTag.JustifiedEmendationOf,
                NameTag.PrimaryHomonymOf,
                NameTag.SecondaryHomonymOf,
                NameTag.NotPreoccupiedBy,
                NameTag.VariantOf,
                NameTag.IncorrectSubsequentSpellingOf,
                NameTag.SubsequentUsageOf,
                NameTag.NameCombinationOf,
            ),
        )
    }

    for senior_homonym in relevant_names:
        # Ignore secondary homonyms that are also primary homonyms
        if (
            not primary
            and nam.original_parent is not None
            and senior_homonym.original_parent == nam.original_parent
        ):
            continue

        # Already marked as a homonym
        if senior_homonym in already_variant_of:
            continue

        if cfg.interactive:
            getinput.print_header(nam)
            print(f"{nam}: preoccupied by {senior_homonym}")
            if getinput.yes_no("Accept preoccupation? "):
                if nam.group is Group.species:
                    if primary:
                        tag = NameTag.PrimaryHomonymOf
                    else:
                        tag = NameTag.SecondaryHomonymOf
                else:
                    tag = NameTag.PreoccupiedBy
                nam.preoccupied_by(senior_homonym, tag=tag)
            elif getinput.yes_no("Mark as subsequent usage instead? "):
                nam.add_tag(NameTag.SubsequentUsageOf(senior_homonym, ""))
        yield f"preoccupied by {senior_homonym}"


def _resolve_original_parent(nam: Name, *, depth: int) -> Name:
    if depth > 10:
        raise ValueError(f"too deep: {nam}")
    for tag in nam.tags:
        if isinstance(
            tag,
            (
                NameTag.VariantOf,
                NameTag.UnjustifiedEmendationOf,
                NameTag.JustifiedEmendationOf,
                NameTag.IncorrectOriginalSpellingOf,
                NameTag.SubsequentUsageOf,
                NameTag.MandatoryChangeOf,
                NameTag.IncorrectSubsequentSpellingOf,
            ),
        ):
            return _resolve_original_parent(tag.name, depth=depth + 1)
    return nam


def _get_primary_names_of_genus_and_variants(
    genus: Name, fuzzy: bool = False
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
    genus: Name, fuzzy: bool = False
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
    genus: Taxon, fuzzy: bool = False
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


@make_linter("check_original_parent")
def check_original_parent(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_parent is None:
        return
    if nam.group is not Group.species:
        yield "original_parent should only be set for species-group names"
        return
    if nam.original_parent.group is not Group.genus:
        yield f"original_parent is not a genus: {nam.original_parent}"
    if nam.corrected_original_name is None:
        return
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


@make_linter("infer_original_parent")
def infer_original_parent(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if (
        nam.group is not Group.species
        or nam.original_parent is not None
        or not nam.nomenclature_status.requires_original_parent()
    ):
        return
    if nam.corrected_original_name is None:
        return
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
        nam.original_name, ("variety", "form", "var", "forma", "v", "f")
    )


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


@make_linter("infrasubspecific")
def check_infrasubspecific(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group is not Group.species:
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
            nam.add_tag(NameTag.VarietyOrForm(""))
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
            nam.add_tag(NameTag.Condition(NomenclatureStatus.infrasubspecific, ""))
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


@make_linter("authority_page_link")
def check_must_have_authority_page_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.has_type_tag(TypeTag.AuthorityPageLink):
        return
    if nam.original_citation is None or not nam.original_citation.has_bhl_link():
        return
    yield "must have authority page link"


@make_linter("check_bhl_page")
def check_bhl_page(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_citation is None:
        return
    for tag in nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink):
        parsed = bhl.parse_possible_bhl_url(tag.url)
        if parsed.url_type is not bhl.UrlType.bhl_page:
            continue
        if nam.original_citation.url is None:
            yield f"name has BHL page, but original citation has no URL: {nam.original_citation}"
            if cfg.interactive:
                nam.original_citation.format(quiet=True)
                if nam.original_citation.url is None:
                    _autoset_original_citation_url(nam)
            continue
        parsed_url = bhl.parse_possible_bhl_url(nam.original_citation.url)
        if not parsed_url.is_bhl():
            yield f"name has BHL page, but citation has non-BHL URL {nam.original_citation.url}"
            if cfg.interactive:
                _autoset_original_citation_url(nam)
            continue
        yield from _check_bhl_item_matches(nam, tag, cfg)
        yield from _check_bhl_bibliography_matches(nam, tag, cfg)


def _check_bhl_item_matches(
    nam: Name,
    tag: TypeTag.AuthorityPageLink,  # type:ignore[name-defined]
    cfg: LintConfig,
) -> Iterable[str]:
    item_id = bhl.get_bhl_item_from_url(tag.url)
    if item_id is None:
        yield f"cannot find BHL item for {tag.url}"
        return
    if nam.original_citation is None or nam.original_citation.url is None:
        return
    citation_item_ids = list(_get_possible_bhl_item_ids(nam.original_citation))
    if not citation_item_ids:
        return
    if item_id not in citation_item_ids:
        # TODO: Ignore this for now; too noisy. In the future we can add more infrastructure and try
        # to automatically replace the URL in this case.
        # yield f"BHL item mismatch: {item_id} (name) != {citation_item_id} (citation)"
        replacement = [
            page
            for page in get_candidate_bhl_pages(nam, verbose=False)
            if page.item_id in citation_item_ids and page.is_confident
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
                            replacement[0].page_url, True, tag.page
                        )
                    )
                    for existing_tag in nam.type_tags
                ]
            else:
                yield message


def _get_possible_bhl_item_ids(art: Article) -> Iterable[int]:
    if art.url is not None:
        item_id = bhl.get_bhl_item_from_url(art.url)
        if item_id is not None:
            yield item_id
    for tag in art.tags:
        if isinstance(tag, ArticleTag.AlternativeURL):
            item_id = bhl.get_bhl_item_from_url(tag.url)
            if item_id is not None:
                yield item_id


def _check_bhl_bibliography_matches(
    nam: Name,
    tag: TypeTag.AuthorityPageLink,  # type:ignore[name-defined]
    cfg: LintConfig,
) -> Iterable[str]:
    bibliography_id = bhl.get_bhl_bibliography_from_url(tag.url)
    if bibliography_id is None:
        yield f"cannot find BHL bibliograohy for {tag.url}"
        return
    if nam.original_citation is None or nam.original_citation.url is None:
        return
    citation_biblio_ids = list(
        _get_possible_bhl_bibliography_ids(nam.original_citation)
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
                            replacement[0].page_url, True, tag.page
                        )
                    )
                    for existing_tag in nam.type_tags
                ]
            else:
                yield message


def _get_possible_bhl_bibliography_ids(art: Article) -> Iterable[int]:
    if art.url is not None:
        bibliography_id = bhl.get_bhl_bibliography_from_url(art.url)
        if bibliography_id is not None:
            yield bibliography_id
    for tag in art.tags:
        if isinstance(tag, ArticleTag.AlternativeURL):
            bibliography_id = bhl.get_bhl_bibliography_from_url(tag.url)
            if bibliography_id is not None:
                yield bibliography_id


def _should_look_for_page_links(nam: Name) -> bool:
    if not nam.page_described:
        return False
    pages = list(extract_pages(nam.page_described))
    tags = list(nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink))
    if len(tags) >= len(pages):
        return False
    return True


@make_linter("infer_bhl_page")
def infer_bhl_page(
    nam: Name,
    cfg: LintConfig = LintConfig(autofix=False, interactive=False),
    verbose: bool = False,
) -> Iterable[str]:
    if not _should_look_for_page_links(nam):
        if verbose:
            print(f"{nam}: Skip because no page or enough tags")
        return
    confident_candidates = [
        page
        for page in get_candidate_bhl_pages(nam, verbose=verbose)
        if page.is_confident
    ]
    for _, group_iter in itertools.groupby(
        confident_candidates, lambda page: page.page_number
    ):
        group = list(group_iter)
        if len(group) == 1:
            page_obj = group[0]
            message = f"inferred BHL page {page_obj}"
            if cfg.autofix:
                print(f"{nam}: {message}")
                tag = TypeTag.AuthorityPageLink(
                    page_obj.page_url, True, str(page_obj.page_number)
                )
                nam.add_type_tag(tag)
            else:
                yield message
            print(page_obj.page_url)
        elif verbose:
            print(f"Reject for {nam} because multiple pages with name:")
            for page_obj in group:
                print(page_obj.page_url)


def get_candidate_bhl_pages(
    nam: Name, verbose: bool = False
) -> Iterable[bhl.PossiblePage]:
    if nam.page_described is None or nam.year is None:
        if verbose:
            print(f"{nam}: Skip because no page or year")
        return
    tags = list(nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink))
    known_pages = [
        int(parsed_url.payload)
        for tag in tags
        if (parsed_url := bhl.parse_possible_bhl_url(tag.url)).url_type
        is bhl.UrlType.bhl_page
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

    pages = list(extract_pages(nam.page_described))
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


@make_linter("infer_bhl_page_from_other_names")
def infer_bhl_page_from_other_names(
    nam: Name, cfg: LintConfig, verbose: bool = False
) -> Iterable[str]:
    if not _should_look_for_page_links(nam):
        if verbose:
            print(f"{nam}: not looking for BHL URL")
        return
    if nam.page_described is None:
        if verbose:
            print(f"{nam}: no page described")
        return
    if nam.original_citation is None:
        if verbose:
            print(f"{nam}: no original citation")
        return
    # Just so we don't waste effort adding incorrect pages before the link has been
    # confirmed on the article.
    if not nam.original_citation.has_bhl_link():
        if verbose:
            print(f"{nam}: original citation has no BHL link")
        return
    pages = list(extract_pages(nam.page_described))
    if len(pages) != 1:
        if verbose:
            print(f"{nam}: no single page from {nam.page_described}")
        return
    (page,) = pages
    other_new_names = [
        nam
        for nam in nam.original_citation.new_names
        if nam.has_type_tag(TypeTag.AuthorityPageLink)
    ]
    if not other_new_names:
        if verbose:
            print(f"{nam}: no other new names")
        return
    inferred_pages: set[int] = set()
    for other_nam in other_new_names:
        for tag in other_nam.get_tags(other_nam.type_tags, TypeTag.AuthorityPageLink):
            parsed = bhl.parse_possible_bhl_url(tag.url)
            if parsed.url_type is not bhl.UrlType.bhl_page:
                if verbose:
                    print(f"{other_nam}: {tag} is not a BHL page URL")
                continue
            existing_page_id = int(parsed.payload)
            if tag.page == page and nam.page_described == other_nam.page_described:
                inferred_pages.add(existing_page_id)
            if not tag.page.isnumeric() or not page.isnumeric():
                if verbose:
                    print(f"{other_nam}: {tag.page} or {page} is not numeric")
                continue
            diff = int(page) - int(tag.page)
            page_metadata = bhl.get_page_metadata(existing_page_id)
            item_id = int(page_metadata["ItemID"])
            item_metadata = bhl.get_item_metadata(item_id)
            if item_metadata is None:
                if verbose:
                    print(f"{other_nam}: no metadata for item {item_id}")
                continue
            page_mapping = bhl.get_page_id_to_index(item_id)
            existing_page_idx = page_mapping.get(existing_page_id)
            if existing_page_idx is None:
                if verbose:
                    print(f"{other_nam}: no index for page {existing_page_id}")
                continue
            expected_page_idx = existing_page_idx + diff
            if not (0 <= expected_page_idx < len(item_metadata["Pages"])):
                if verbose:
                    print(f"{other_nam}: {expected_page_idx} is out of range")
                continue
            inferred_page_id = item_metadata["Pages"][expected_page_idx]["PageID"]
            if diff > 0:
                start = existing_page_id
                end = inferred_page_id
            else:
                start = inferred_page_id
                end = existing_page_id
            if not bhl.is_contigous_range(
                item_id,
                start,
                end,
                page_mapping,
                allow_unnumbered=False,
                verbose=verbose,
            ):
                if verbose:
                    print(
                        f"{other_nam}: {existing_page_id} and {inferred_page_id} are not"
                        " contiguous"
                    )
                continue
            possible_pages = bhl.get_possible_pages(item_id, page)
            if inferred_page_id not in possible_pages:
                if verbose:
                    print(
                        f"{other_nam}: {inferred_page_id} not in possible pages {possible_pages}"
                    )
                continue
            inferred_pages.add(inferred_page_id)
    if len(inferred_pages) != 1:
        if verbose:
            print(f"{nam}: no single inferred page from other names ({inferred_pages})")
        return
    (inferred_page_id,) = inferred_pages
    tag = TypeTag.AuthorityPageLink(
        f"https://www.biodiversitylibrary.org/page/{inferred_page_id}", True, page
    )
    message = f"inferred BHL page {inferred_page_id} from other names (add {tag})"
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.add_type_tag(tag)
    else:
        yield message


@make_linter("bhl_page_from_article")
def infer_bhl_page_from_article(
    nam: Name, cfg: LintConfig, verbose: bool = False
) -> Iterable[str]:
    if not _should_look_for_page_links(nam):
        if verbose:
            print(f"{nam}: not looking for BHL URL")
        return
    if nam.page_described is None:
        if verbose:
            print(f"{nam}: no page described")
        return
    if nam.original_citation is None or nam.original_citation.url is None:
        if verbose:
            print(f"{nam}: no original citation or URL")
        return
    parsed = bhl.parse_possible_bhl_url(nam.original_citation.url)
    for page_described in extract_pages(nam.page_described):
        if any(
            isinstance(tag, TypeTag.AuthorityPageLink) and tag.page == page_described
            for tag in nam.type_tags
        ):
            continue
        match parsed.url_type:
            case bhl.UrlType.bhl_page:
                start_page_id = int(parsed.payload)
                yield from _infer_bhl_page_from_article_page(
                    nam, cfg, start_page_id, page_described, verbose
                )
            case bhl.UrlType.bhl_item:
                item_id = int(parsed.payload)
                yield from _infer_bhl_page_from_item(
                    nam, cfg, item_id, page_described, verbose
                )
            case bhl.UrlType.bhl_part:
                part_id = int(parsed.payload)
                yield from _infer_bhl_page_from_part(
                    nam, cfg, part_id, page_described, verbose
                )
            case _:
                if verbose:
                    print(f"{nam}: not a BHL page URL")


def _infer_bhl_page_from_part(
    nam: Name, cfg: LintConfig, part_id: int, page_described: str, verbose: bool
) -> Iterable[str]:
    pages = bhl.get_possible_pages_from_part(part_id, page_described)
    if len(pages) != 1:
        if verbose:
            print(
                f"{nam}: no single page for {part_id} {pages} (using {page_described})"
            )
        return

    tag = TypeTag.AuthorityPageLink(
        f"https://www.biodiversitylibrary.org/page/{pages[0]}", True, page_described
    )
    message = f"inferred BHL page {pages[0]} from part citation (add {tag})"
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.add_type_tag(tag)
    else:
        yield message


def _infer_bhl_page_from_item(
    nam: Name, cfg: LintConfig, item_id: int, page_described: str, verbose: bool
) -> Iterable[str]:
    pages = bhl.get_possible_pages(item_id, page_described)
    if len(pages) != 1:
        if verbose:
            print(f"{nam}: no single page for {item_id} {pages}")
        return

    tag = TypeTag.AuthorityPageLink(
        f"https://www.biodiversitylibrary.org/page/{pages[0]}", True, page_described
    )
    message = f"inferred BHL page {pages[0]} from item citation (add {tag})"
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.add_type_tag(tag)
    else:
        yield message


def _infer_bhl_page_from_article_page(
    nam: Name, cfg: LintConfig, start_page_id: int, page_described: str, verbose: bool
) -> Iterable[str]:
    if nam.original_citation is None or nam.original_citation.end_page is None:
        if verbose:
            print(f"{nam}: no citation")
        return
    start_page_metadata = bhl.get_page_metadata(start_page_id)
    if not start_page_metadata:
        if verbose:
            print(f"{nam}: no metadata for start page")
        return
    item_id = int(start_page_metadata["ItemID"])
    possible_end_pages = bhl.get_possible_pages(item_id, nam.original_citation.end_page)
    page_mapping = bhl.get_page_id_to_index(item_id)
    possible_end_pages = [
        page
        for page in possible_end_pages
        if bhl.is_contigous_range(item_id, start_page_id, page, page_mapping)
    ]
    possible_pages = bhl.get_possible_pages(item_id, page_described)
    possible_pages = [
        page
        for page in possible_pages
        if bhl.is_contigous_range(item_id, start_page_id, page, page_mapping)
    ]
    if len(possible_pages) != len(possible_end_pages):
        if verbose:
            print(
                f"{nam}: different number of possible description and end pages"
                f" {possible_pages} {possible_end_pages}"
            )
        return
    if not possible_pages:
        if verbose:
            print(f"{nam}: no possible pages in item {item_id}")
        return
    for possible_page, possible_end_page in zip(
        possible_pages, possible_end_pages, strict=True
    ):
        if not bhl.is_contigous_range(
            item_id, start_page_id, possible_page, page_mapping
        ):
            if verbose:
                print(
                    f"{nam}: page {possible_page} is not contiguous with start page {start_page_id}"
                )
            continue
        if not bhl.is_contigous_range(
            item_id, possible_page, possible_end_page, page_mapping
        ):
            if verbose:
                print(
                    f"{nam}: end page {possible_end_page} is not contiguous with {possible_page}"
                )
            continue

        tag = TypeTag.AuthorityPageLink(
            f"https://www.biodiversitylibrary.org/page/{possible_page}",
            True,
            page_described,
        )
        message = f"inferred BHL page {possible_page} from page citation (add {tag})"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_type_tag(tag)
        else:
            yield message


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
