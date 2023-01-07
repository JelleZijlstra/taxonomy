"""

Lint steps for Names.

"""
from collections.abc import Iterable, Callable
import re
from typing import TypeVar
from .name import Name, NameTag, TypeTag, STATUS_TO_TAG
from .article import Article
from ..constants import (
    ArticleKind,
    TypeSpeciesDesignation,
    CommentKind,
    Group,
    NomenclatureStatus,
    Status,
    SpeciesGroupType,
)
from .. import helpers
from ... import adt, getinput

T = TypeVar("T")

Linter = Callable[[Name, bool], Iterable[str]]


def replace_arg(tag: adt.ADT, arg: str, val: object) -> adt.ADT:
    kwargs = {**tag.__dict__, arg: val}
    return type(tag)(**kwargs)


def extract_id(text: str, call_sign: str) -> int | None:
    rgx = rf"{call_sign}#(\d+)"
    if match := re.search(rgx, text):
        return int(match.group(1))
    return None


def is_name_removed(nam: Name) -> bool | Name:
    """Is a name removed?

    Return False if not, True if yes but we don't know what it was merged
    into, and a Name if it was merged into something else.

    """
    if nam.status is not Status.removed:
        return False
    for comment in nam.comments:
        if comment.kind is CommentKind.removal:
            if id := extract_id(comment.text, "N"):
                replacement = Name.get(id=id)
                if replacement.status is not Status.removed:
                    return replacement
                else:
                    print(f"Ignoring {replacement} as it is itself removed")
    return True


def get_tag_fields_of_type(tag: adt.ADT, typ: type[T]) -> Iterable[tuple[str, T]]:
    tag_type = type(tag)
    for arg_name, arg_type in tag_type._attributes.items():
        if arg_type is typ:
            if (val := getattr(tag, arg_name)) is None:
                continue
            yield arg_name, val


def check_type_tags_for_name(nam: Name, autofix: bool) -> Iterable[str]:
    if not nam.type_tags:
        return
    tags: list[TypeTag] = []
    original_tags = list(nam.type_tags)
    for tag in original_tags:
        for arg_name, art in get_tag_fields_of_type(tag, Article):
            if art.kind is ArticleKind.removed:
                print(f"{nam} references a removed Article in {tag}")
                yield f"bad article in tag {tag}"
            elif art.kind is ArticleKind.redirect:
                print(f"{nam} references a redirected Article in {tag} -> {art.parent}")
                if art.parent is None or art.parent.should_skip():
                    yield f"bad redirected article in tag {tag}"
                elif autofix:
                    tag = replace_arg(tag, arg_name, art.parent)
        for arg_name, tag_nam in get_tag_fields_of_type(tag, Name):
            result = is_name_removed(tag_nam)
            if isinstance(result, Name):
                print(f"{nam} references a merged name")
                if autofix:
                    tag = replace_arg(tag, arg_name, result)
            elif result:
                print(f"{nam} references a removed Name in {tag}")
                yield f"bad name in tag {tag}"

        if isinstance(tag, TypeTag.CommissionTypeDesignation):
            if nam.type != tag.type:
                print(
                    f"{nam} has {nam.type} as its type, but the Commission has designated {tag.type}"
                )
                if autofix:
                    nam.type = tag.type
            if (
                nam.genus_type_kind
                != TypeSpeciesDesignation.designated_by_the_commission
            ):
                print(
                    f"{nam} has {nam.genus_type_kind}, but its type was set by the Commission"
                )
                if autofix:
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
        else:
            tags.append(tag)
        # TODO: for lectotype and subsequent designations, ensure the earliest valid one is used.
    tags = sorted(set(tags))
    if tags != original_tags:
        if set(tags) != set(original_tags):
            print(f"changing tags for {nam}")
            getinput.print_diff(sorted(original_tags), tags)
        if autofix:
            nam.type_tags = tags  # type: ignore


def check_type_designations_present(nam: Name, autofix: bool = True) -> Iterable[str]:
    if nam.genus_type_kind is TypeSpeciesDesignation.subsequent_designation:
        if not any(
            tag.type == nam.type
            for tag in nam.get_tags(nam.type_tags, TypeTag.TypeDesignation)
        ):
            yield f"{nam}: missing a reference for type species designation"
    if (
        nam.species_type_kind is SpeciesGroupType.lectotype
        and nam.type_specimen is not None
    ):
        if not any(
            tag.lectotype == nam.type_specimen
            for tag in nam.get_tags(nam.type_tags, TypeTag.LectotypeDesignation)
        ):
            yield f"{nam}: missing a reference for lectotype designation"
    if (
        nam.species_type_kind is SpeciesGroupType.neotype
        and nam.type_specimen is not None
    ):
        if not any(
            tag.neotype == nam.type_specimen
            for tag in nam.get_tags(nam.type_tags, TypeTag.NeotypeDesignation)
        ):
            yield f"{nam}: missing a reference for neotype designation"


def check_tags_for_name(nam: Name, autofix: bool) -> Iterable[str]:
    """Looks at all tags set on names and applies related changes."""
    try:
        tags = nam.tags
    except Exception:
        yield f"{nam}: could not deserialize tags"
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
            comment = f"Status automatically changed from {nam.nomenclature_status.name} to {status.name} because of {tag}"
            print(f"changing status of {nam} and adding comment {comment!r}")
            if autofix:
                nam.add_static_comment(CommentKind.automatic_change, comment)
                nam.nomenclature_status = status  # type: ignore

    for tag in tags:
        if isinstance(tag, NameTag.PreoccupiedBy):
            maybe_adjust_status(nam, NomenclatureStatus.preoccupied, tag)
            senior_name = tag.name
            if nam.group != senior_name.group:
                yield (
                    f"{nam}: is of a different group than supposed senior name {senior_name}"
                )
            if senior_name.nomenclature_status is NomenclatureStatus.subsequent_usage:
                for senior_name_tag in senior_name.get_tags(
                    senior_name.tags, NameTag.SubsequentUsageOf
                ):
                    senior_name = senior_name_tag.name
            if nam.effective_year() < senior_name.effective_year():
                yield f"{nam}: predates supposed senior name {senior_name}"
            # TODO apply this check to species too by handling gender endings correctly.
            if nam.group is not Group.species:
                if nam.root_name != tag.name.root_name:
                    yield (
                        f"{nam}: has a different root name than supposed senior name {senior_name}"
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
            ),
        ):
            for status, tag_cls in STATUS_TO_TAG.items():
                if isinstance(tag, tag_cls):
                    maybe_adjust_status(nam, status, tag)
            if nam.effective_year() < tag.name.effective_year():
                yield f"{nam}: predates supposed original name {tag.name}"
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


def check_required_tags(nam: Name, autofix: bool = True) -> Iterable[str]:
    if nam.nomenclature_status not in STATUS_TO_TAG:
        return
    tag_cls = STATUS_TO_TAG[nam.nomenclature_status]
    tags = list(nam.get_tags(nam.tags, tag_cls))
    if not tags:
        yield f"{nam}: has status {nam.nomenclature_status.name} but no corresponding tag"


_single_year = re.compile(r"^\d{4}$")
_multi_year = re.compile(r"^\d{4}-\d{4}$")


def check_year(nam: Name, autofix: bool = True) -> Iterable[str]:
    if (
        nam.year is None
        or nam.year == "in press"
        or _single_year.match(nam.year)
        or _multi_year.match(nam.year)
    ):
        return
    yield f"{nam}: has invalid year {nam.year!r}"


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


def check_disallowed_attributes(nam: Name, autofix: bool = True) -> Iterable[str]:
    for field, groups in ATTRIBUTES_BY_GROUP.items():
        if nam.group not in groups:
            value = getattr(nam, field)
            if value is not None:
                yield f"{nam}: should not have attribute {field} (value {value})"


def _make_con_messsage(nam: Name, text: str) -> str:
    return f"{nam}: corrected original name {nam.corrected_original_name!r} {text}"


def check_corrected_original_name(nam: Name, autofix: bool = True) -> Iterable[str]:
    """Check that corrected_original_names are correct."""
    if nam.corrected_original_name is None:
        return
    if nam.nomenclature_status.permissive_corrected_original_name():
        return
    inferred = nam.infer_corrected_original_name()
    if inferred is not None and inferred != nam.corrected_original_name:
        yield _make_con_messsage(
            nam,
            f"inferred name {inferred!r} does not match current name {nam.corrected_original_name!r}",
        )
    if not re.match(r"^[A-Z][a-z ]+$", nam.corrected_original_name):
        yield _make_con_messsage(nam, "contains unexpected characters")
        return
    if nam.group in (Group.high, Group.genus):
        if " " in nam.corrected_original_name:
            yield _make_con_messsage(nam, "contains whitespace")
        elif nam.corrected_original_name != nam.root_name:
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
    return f"{nam}: root name {nam.root_name!r} {text}"


def check_root_name(nam: Name, autofix: bool = True) -> Iterable[str]:
    """Check that root_names are correct."""
    if nam.nomenclature_status.permissive_corrected_original_name():
        return
    if nam.group in (Group.high, Group.genus, Group.family):
        if not re.match(r"^[A-Z][a-z]+$", nam.root_name):
            yield _make_rn_message(nam, "contains unexpected characters")
    elif nam.group is Group.species:
        if not re.match(r"^[a-z]+$", nam.root_name):
            yield _make_rn_message(nam, "contains unexpected characters")


def check_family_root_name(nam: Name, autofix: bool = True) -> Iterable[str]:
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
            print(f"Autocorrecting root name: {nam.root_name} -> {stem_name}")
            if autofix:
                nam.root_name = stem_name
            break
    if nam.root_name != stem_name:
        if nam.has_type_tag(TypeTag.IncorrectGrammar):
            return
        yield f"{nam}: Stem mismatch: {nam.root_name} vs. {stem_name}"


def correct_type_taxon(nam: Name, autofix: bool = True) -> Iterable[str]:
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
        message = f"{nam}: expected taxon to be {expected_taxon} not {nam.taxon}"
        if autofix and expected_taxon.is_child_of(nam.taxon):
            print(message)
            nam.taxon = expected_taxon
        else:
            yield message


def clean_up_verbatim(nam: Name, autofix: bool = True) -> Iterable[str]:
    if (
        nam.group in (Group.family, Group.genus)
        and nam.verbatim_type is not None
        and (nam.type is not None or "type" not in nam.get_required_fields())
    ):
        message = f"{nam}: cleaning up verbatim type: {nam.type}, {nam.verbatim_type}"
        if autofix:
            print(message)
            nam.add_data("verbatim_type", nam.verbatim_type, concat_duplicate=True)
            nam.verbatim_type = None
        else:
            yield message
    if (
        nam.group is Group.species
        and nam.verbatim_type is not None
        and nam.type_specimen is not None
    ):
        message = f"{nam}: {nam.type_specimen}, {nam.verbatim_type}"
        if autofix:
            print(message)
            nam.add_data("verbatim_type", nam.verbatim_type, concat_duplicate=True)
            nam.verbatim_type = None
        else:
            yield message
    if nam.verbatim_citation is not None and nam.original_citation is not None:
        message = f"{nam}: {nam.original_citation.name}, {nam.verbatim_citation}"
        if autofix:
            print(message)
            nam.add_data(
                "verbatim_citation", nam.verbatim_citation, concat_duplicate=True
            )
            nam.verbatim_citation = None
        else:
            yield message
    if nam.citation_group is not None and nam.original_citation is not None:
        message = f"{nam}: {nam.original_citation.name}, {nam.citation_group}"
        if autofix:
            print(message)
            nam.citation_group = None
        else:
            yield message


def check_correct_status(nam: Name, autofix: bool = True) -> Iterable[str]:
    if nam.status.is_base_name() and nam != nam.taxon.base_name:
        yield f"{nam}: is of status {nam.status!r} and should be base name of {nam.taxon}"


LINTERS: list[Linter] = [
    check_type_tags_for_name,
    check_required_tags,
    check_tags_for_name,
    check_year,
    check_disallowed_attributes,
    check_corrected_original_name,
    check_root_name,
    check_family_root_name,
    correct_type_taxon,
    clean_up_verbatim,
    check_correct_status,
]
DISABLED_LINTERS: list[Linter] = [
    check_type_designations_present,  # too many missing (about 580)
]
