import csv
import functools
import re
import sys
from collections import Counter
from collections.abc import Iterable
from typing import IO

from taxonomy import getinput
from taxonomy.db import constants
from taxonomy.db.models import Article, Collection, Name, TypeTag

from .lib import DATA_DIR, get_type_specimens

INTERACTIVE = True


@functools.cache
def mnhn_mammals() -> Collection:
    coll = Collection.getter("label")("MNHN (ZM)")
    assert coll is not None
    return coll


@functools.cache
def mnhn() -> Collection:
    coll = Collection.getter("label")("MNHN")
    assert coll is not None
    return coll


@functools.cache
def mnhn_f() -> Collection:
    coll = Collection.getter("label")("MNHN (F)")
    assert coll is not None
    return coll


def get_hesp_data() -> dict[str, list[Name]]:
    return get_type_specimens(mnhn())


def get_bmnh_db() -> Iterable[dict[str, str]]:
    seen = set()
    for path in (
        "mnhn-mammalia-holotypes.csv",
        "mnhn-mammalia-types-1.csv",
        "mnhn-mammalia-types-2.csv",
        "mnhn-fossils-1.csv",
        "mnhn-fossils-2.csv",
        "mnhn-fossils-3.csv",
        "mnhn-fossils-4.csv",
        "mnhn-fossils-5.csv",
    ):
        with (DATA_DIR / path).open() as f:
            rows = csv.DictReader(f)
            for row in rows:
                if row["catalogNumber"] in seen:
                    continue
                seen.add(row["catalogNumber"])
                yield row


def find_name(original_name: str, authority: str, year: str) -> Name | None:
    query = Name.filter(Name.corrected_original_name == original_name)
    num_year = int(year)
    nams = [
        nam
        for nam in query
        if nam.taxonomic_authority().casefold() == authority.casefold()
        and abs(nam.numeric_year() - num_year) <= 2
    ]
    if len(nams) == 1:
        return nams[0]
    return None


_NAME_REGEX = re.compile(
    r"""
    (?P<original_name>[A-Z][a-z]+(\s\([A-Z][a-z]+\))?\s[a-z]+(\s[a-z]+)?)
    \s+
    (?P<authority>[A-Z]\D+)
    (?P<year>\d{4})
    \.?
    """,
    re.VERBOSE,
)


def split_name(text: str) -> tuple[str, str, str] | None:
    text = text.strip().replace("(", "").replace(")", "")
    match = _NAME_REGEX.fullmatch(text)
    if match is None:
        return None
    name = match.group("original_name")
    name = re.sub(r" \([^\)]+\)", " ", name)
    name = re.sub(r"\s+", " ", name)
    name = name.strip()
    authority = match.group("authority").strip().strip(",")
    authority = authority.replace(" and ", " & ")
    authority = re.sub(r"\s+", " ", authority)
    return name, authority, match.group("year")


@functools.cache
def get_source() -> Article:
    art = Article.getter("name")("MNHN-catalog")
    assert art is not None
    return art


DETAIL_COLUMNS = [
    "catalogNumber",
    "recordedBy",
    "eventDate",
    "itemType",
    "previousIdentifications",
    "typeStatus",
]
LOCALITY_COLUMNS = ["country", "locality"]


def get_tags(row: dict[str, str]) -> Iterable[TypeTag]:
    locality = " ... ".join(
        f"{row[column]}" for column in LOCALITY_COLUMNS if row[column]
    )
    if "[" in locality:
        locality += " [brackets original]"
    if locality:
        yield TypeTag.LocationDetail(locality, get_source())
    text = " ... ".join(
        f"[{column}] {row[column]}" for column in DETAIL_COLUMNS if row[column]
    )
    text += f" [at {row['URI']}]"
    yield TypeTag.SpecimenDetail(text, get_source())
    yield TypeTag.TypeSpecimenLink(row["URI"])


def handle_interactively(
    nam: Name, row: dict[str, str], cat_num: str, exclude_list: IO[str]
) -> None:
    if not INTERACTIVE:
        return
    getinput.print_header(nam)
    tags = list(get_tags(row))
    nam.display()
    print(f"{cat_num}: mapped to {nam}, {nam.type_specimen}, {nam.collection}, {tags}")
    nam.edit()
    if nam.type_specimen and cat_num not in nam.type_specimen:
        if getinput.yes_no("exclude? "):
            exclude_list.write(f"{cat_num}\n")


def extract_name_and_status(row: dict[str, str]) -> tuple[str, str] | None:
    if row["typeStatus"] and not row["previousIdentifications"]:
        return row["typeStatus"], row["scientificName"]
    for name in row["previousIdentifications"].split(";"):
        if not name.endswith("type"):
            continue
        sci_name, status = name.rsplit(" ", maxsplit=1)
        return status, sci_name
    return None


def can_replace_collection(coll: Collection | None) -> bool:
    return coll is None or coll in (mnhn(), mnhn_mammals(), mnhn_f())


def can_replace_type_secimen(text: str | None, row: dict[str, str]) -> bool:
    if text is None:
        return True
    text = text.lower().replace(".", "-").replace(" ", "")
    cat_no = row["catalogNumber"].lower()
    cat_no = cat_no.removeprefix("mo-").removeprefix("ac-")
    if "," in text or "=" in text:
        return False
    if cat_no in text:
        return True
    return False


def maybe_get(exclude_list: IO[str], cat_num: str) -> Name | None:
    if INTERACTIVE:
        nam = Name.getter(None).get_one("name> ")
        if nam is None:
            if getinput.yes_no("add to exclude? "):
                exclude_list.write(f"{cat_num}\n")
            return None
        nam.display()
        if not getinput.yes_no("confirm? "):
            return None
        return nam
    else:
        return None


def handle_cannot_find(
    row: dict[str, str], cat_num: str, exclude_list: IO[str], *, dry_run: bool
) -> str:
    maybe_name = extract_name_and_status(row)
    if maybe_name is None:
        return "cannot extract name"
    type_status, scientific_name = maybe_name
    type_status = type_status.strip("?")
    if should_exclude_type(type_status):
        return "not a type"
    parsed = split_name(scientific_name)
    if parsed is None:
        if INTERACTIVE:
            for key, value in row.items():
                if value:
                    print(f"{key}: {value}")
        print("cannot parse:", row["previousIdentifications"], row["scientificName"])
        if INTERACTIVE:
            nam = Name.getter(None).get_one("name> ")
            if nam is None:
                if getinput.yes_no("add to exclude? "):
                    exclude_list.write(f"{cat_num}\n")
                return "cannot parse"
            nam.display()
            if not getinput.yes_no("confirm? "):
                return "cannot parse"
        else:
            return "cannot parse"
    else:
        name, authority, year = parsed
        nam = find_name(name, authority, year)
    if nam is None:
        print(f"cannot find {name} {authority}, {year} ({cat_num}, {type_status})")
        nam = maybe_get(exclude_list, cat_num)
        if nam is None:
            return "cannot find name"
    tags = list(get_tags(row))
    if (
        nam.nomenclature_status not in constants.REQUIRES_TYPE
        and nam.nomenclature_status is not constants.NomenclatureStatus.nomen_nudum
        and nam.nomenclature_status
        is not constants.NomenclatureStatus.not_used_as_valid
    ):
        nam.display()
        print(
            f"{cat_num} {row['typeStatus']}: mapped to {nam}, {nam.type_specimen},"
            f" {nam.collection}, {tags}"
        )
        new_nam = maybe_get(exclude_list, cat_num)
        if new_nam is None:
            return f"has status {nam.nomenclature_status.name}"
        nam = new_nam
    if not can_replace_collection(nam.collection) or not can_replace_type_secimen(
        nam.type_specimen, row
    ):
        handle_interactively(nam, row, cat_num, exclude_list)
        return "matched with existing type"
    print("matched:", cat_num, nam)
    print(
        f"{cat_num} {type_status}: mapped to {nam}, {nam.type_specimen},"
        f" {nam.collection}, {tags}"
    )
    if nam.type_specimen is not None:
        print(f"! replace type {nam.type_specimen} -> {cat_num}")
    if not dry_run:
        for tag in tags:
            nam.add_type_tag(tag)
        if can_replace_type_secimen(nam.type_specimen, row):
            nam.type_specimen = cat_num
        if can_replace_collection(nam.collection):
            nam.collection = get_collection(row)
        if nam.species_type_kind is None:
            type_kind = get_type_kind(type_status)
            if type_kind is not None:
                nam.species_type_kind = type_kind
    return "matched"


def get_collection(row: dict[str, str]) -> Collection:
    if row["collectionCode"] == "F":
        return mnhn_f()
    elif row["collectionCode"] == "ZM":
        return mnhn_mammals()
    else:
        raise ValueError(row["collectionCode"])


def get_type_kind(type_status: str) -> constants.SpeciesGroupType | None:
    type_status = type_status.lower().replace("é", "e")
    try:
        return constants.SpeciesGroupType[type_status]
    except KeyError:
        pass
    if "syntype" in type_status or "cotype" in type_status:
        return constants.SpeciesGroupType.syntypes
    if type_status == "type":
        return constants.SpeciesGroupType.holotype
    return None


def is_close_enough(nam: Name, row: dict[str, str]) -> bool | None:
    """Is this close enough for a match?

    True = yes, accept it
    False = absolutely not, ignore it
    None = ask the user

    """
    if nam.corrected_original_name is None:
        return False
    det = row["determinationNames"]
    if nam.corrected_original_name in det:
        return True
    if nam.original_name is not None and nam.original_name.lower() in det.lower():
        return True
    parts = nam.corrected_original_name.split()
    genus = parts[0]
    epithet = parts[-1]
    if (
        epithet[:-1] in det
        or nam.taxon.parent_of_rank(constants.Rank.species).base_name.root_name in det
    ):
        if (
            genus in det
            or nam.taxon.parent_of_rank(constants.Rank.genus).valid_name in det
        ):
            return True
    if any(isinstance(tag, TypeTag.TypeSpecimenLink) for tag in nam.type_tags):
        return False
    return None


def print_row(row: dict[str, str]) -> None:
    print("-----")
    for k, v in row.items():
        if k in (
            "nomenclaturalCode",
            "collectionCode",
            "family",
            "genus",
            "countryCode",
        ):
            continue
        if v:
            print(f"{k}: {v}")


def should_exclude_type(type_status: str) -> bool:
    type_status = type_status.lower().strip("?").replace("é", "e")
    return type_status in (
        "paratype",
        "paralectotype",
        "topotype",
        "neoparatype",
        "allotype",
        "figure",
        "cite",
    )


def main(*, dry_run: bool = True) -> None:
    hesp_data = get_hesp_data()
    total = cannot_find = added_tag = tag_present = excluded_count = excluded_status = 0
    statuses: Counter[str] = Counter()
    exclude_file = DATA_DIR / "mnhn-exclude.txt"
    excluded = {line.strip() for line in exclude_file.read_text().splitlines()}
    with exclude_file.open("a") as exclude_f:
        for row in getinput.print_every_n(get_bmnh_db(), label="specimens", n=100):
            if should_exclude_type(row["typeStatus"]) or row["itemType"] == "moulage":
                excluded_status += 1
                continue
            cat_num = f"MNHN-{row['collectionCode']}-{row['catalogNumber']}"
            total += 1
            if cat_num in excluded:
                excluded_count += 1
                continue
            if cat_num not in hesp_data:
                result = handle_cannot_find(row, cat_num, exclude_f, dry_run=dry_run)
                statuses[result] += 1
                if result == "not a type":
                    excluded_status += 1
                else:
                    cannot_find += 1
                continue
            for nam in hesp_data[cat_num]:
                tag = TypeTag.TypeSpecimenLink(row["URI"])
                if tag in nam.type_tags:
                    tag_present += 1
                    continue
                print(f"{nam}: add URL {tag}")
                added_tag += 1
                if not dry_run:
                    for tag in get_tags(row):
                        print(f"{nam}: add tag {tag}")
                        nam.add_type_tag(tag)
                    expected = get_collection(row)
                    if nam.collection != expected:
                        print(f"{nam}: set collection")
                        nam.collection = expected
    print(
        f"total = {total}, cannot_find = {cannot_find}, tag_present = {tag_present},"
        f" excluded_status = {excluded_status}, excluded = {excluded_count}, added_tag"
        f" = {added_tag}, statuses = {statuses}"
    )


if __name__ == "__main__":
    try:
        main(dry_run="--dry-run" in sys.argv)
    except getinput.StopException:
        print("Stopped!")
