"""

Lint steps for Names.

"""
from collections.abc import Iterable
import re
from typing import TypeVar
from .name import Name, TypeTag
from .article import Article
from ..constants import ArticleKind, TypeSpeciesDesignation, CommentKind, Status
from .. import helpers
from ... import adt, getinput

T = TypeVar("T")


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


def check_type_tags_for_name(nam: Name, dry_run: bool = False) -> Iterable[str]:
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
                elif not dry_run:
                    tag = replace_arg(tag, arg_name, art.parent)
        for arg_name, tag_nam in get_tag_fields_of_type(tag, Name):
            result = is_name_removed(tag_nam)
            if isinstance(result, Name):
                print(f"{nam} references a merged name")
                if not dry_run:
                    tag = replace_arg(tag, arg_name, result)
            elif result:
                print(f"{nam} references a removed Name in {tag}")
                yield f"bad name in tag {tag}"

        if isinstance(tag, TypeTag.CommissionTypeDesignation):
            if nam.type != tag.type:
                print(
                    f"{nam} has {nam.type} as its type, but the Commission has designated {tag.type}"
                )
                if not dry_run:
                    nam.type = tag.type
            if (
                nam.genus_type_kind
                != TypeSpeciesDesignation.designated_by_the_commission
            ):
                print(
                    f"{nam} has {nam.genus_type_kind}, but its type was set by the Commission"
                )
                if not dry_run:
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
        if not dry_run:
            nam.type_tags = tags  # type: ignore


LINTERS = [
    check_type_tags_for_name,
]
