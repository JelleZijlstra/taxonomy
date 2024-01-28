import csv
import functools
import re
import string
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable
from typing import IO, TypedDict

from taxonomy import getinput
from taxonomy.db import constants
from taxonomy.db.models import Article, Collection, Name, TypeTag, name_lint

from .lib import DATA_DIR

INTERACTIVE = False

Row = TypedDict(
    "Row",
    {
        "id": str,
        "occurrenceID": str,
        "catalogNumber": str,
        "basisOfRecord": str,
        "collectionCode": str,
        "typeStatus": str,
        "genus": str,
        "subgenus": str,
        "specificEpithet": str,
        "infraspecificEpithet": str,
        "scientificName": str,
        "authorshipVerbatim": str,
        "taxonRank": str,
        "kingdom": str,
        "phylum": str,
        "class": str,
        "order": str,
        "family": str,
        "higherClassification": str,
        "identifiedBy": str,
        "dateIdentified": str,
        "nomenclaturalCode": str,
        "individualCount": str,
        "lifeStage": str,
        "sex": str,
        "Associated Taxa": str,
        "preparations": str,
        "latitudeDecimal": str,
        "longitudeDecimal": str,
        "geodeticDatum": str,
        "coordinateUncertaintyInMeters": str,
        "verbatimCoordinates": str,
        "continent": str,
        "country": str,
        "provinceState": str,
        "island": str,
        "locality": str,
        "city": str,
        "habitat": str,
        "altitude": str,
        "depth": str,
        "recordedBy": str,
        "eventDate": str,
        "verbatimEventDate": str,
        "otherCatalogNumbers": str,
        "recordNumber": str,
        "informationWithheld": str,
        "institutionID": str,
        "rightsHolder": str,
        "license": str,
        "modified": str,
    },
)


@functools.cache
def collection_by_label(label: str) -> Collection:
    coll = Collection.getter("label")(label)
    assert coll is not None
    return coll


def all_rmnh_collections() -> list[Collection]:
    return [
        collection_by_label("RMNH"),
        collection_by_label("RMNH (Mammalia)"),
        collection_by_label("RMNH (Amphibia and Reptilia)"),
        collection_by_label("ZMA"),
    ]


def fossil_collections() -> list[Collection]:
    return [collection_by_label("RMNH"), collection_by_label("RGM")]


def get_hesp_data(fossil: bool) -> dict[str, list[Name]]:
    output = defaultdict(list)
    for coll in fossil_collections() if fossil else all_rmnh_collections():
        for nam in coll.type_specimens:
            if nam.type_specimen is None:
                continue
            for spec in name_lint.parse_type_specimen(nam.type_specimen):
                if isinstance(spec, name_lint.Specimen):
                    output[spec.text.replace("RGM ", "RGM.")].append(nam)
    multiple = collection_by_label("multiple")
    for nam in multiple.type_specimens:
        if nam.type_specimen is None:
            continue
        try:
            for spec in name_lint.parse_type_specimen(nam.type_specimen):
                if isinstance(spec, name_lint.Specimen) and spec.text.startswith(
                    ("RMNH", "RGM")
                ):
                    output[spec.text].append(nam)
        except ValueError as e:
            print(f"failed to parse {nam} due to {e!r}")
    return output


def get_rmnh_db(fossil: bool, herps: bool) -> Iterable[Row]:
    for path in (
        (
            "rmnh-paleontology.csv"
            if fossil
            else ("rmnh-herps.csv" if herps else "rmnh-mammalia.csv")
        ),
    ):
        with (DATA_DIR / path).open() as f:
            rows: Iterable[Row] = csv.DictReader(f)  # type: ignore
            yield from sorted(
                rows, key=lambda row: (row["typeStatus"], row["scientificName"])
            )


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
    art = Article.getter("name")("RMNH-catalog")
    assert art is not None
    return art


DETAIL_COLUMNS = [
    "catalogNumber",
    "lifeStage",
    "recordedBy",
    "eventDate",
    "verbatimEventDate",
    "sex",
    "preparations",
    "typeStatus",
]
LOCALITY_COLUMNS = ["country", "island", "locality", "altitude"]


def get_tags(row: Row) -> Iterable[TypeTag]:
    locality = " ... ".join(
        f"{row[column]}" for column in LOCALITY_COLUMNS if row[column]  # type: ignore
    )
    if "[" in locality:
        locality += " [brackets original]"
    if locality:
        yield TypeTag.LocationDetail(locality, get_source())
    text = " ... ".join(
        f"[{column}] {row[column]}" for column in DETAIL_COLUMNS if row[column]  # type: ignore
    )
    text += f" [at {row['occurrenceID']}]"
    yield TypeTag.SpecimenDetail(text, get_source())
    yield TypeTag.TypeSpecimenLink(row["occurrenceID"])
    if row["typeStatus"] != "syntype":
        if life_stage := parse_life_stage(row["lifeStage"]):
            yield TypeTag.Age(life_stage)
        if sex := parse_sex(row["sex"]):
            yield TypeTag.Gender(sex)


def parse_life_stage(text: str) -> constants.SpecimenAge | None:
    match text:
        case "adult" | "oud":
            return constants.SpecimenAge.adult
        case "juvnile" | "neonate" | "embryo":
            return constants.SpecimenAge.juvenile
        case "subadult" | "immature":
            return constants.SpecimenAge.subadult
        case _:
            return None


def parse_sex(text: str) -> constants.SpecimenGender | None:
    try:
        return constants.SpecimenGender[text]
    except KeyError:
        return None


def handle_interactively(
    nam: Name, row: Row, cat_num: str, exclude_list: IO[str]
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


def can_replace_collection(coll: Collection | None, fossil: bool) -> bool:
    return coll is None or coll in (
        fossil_collections() if fossil else all_rmnh_collections()
    )


def can_replace_type_secimen(text: str | None, row: Row) -> bool:
    if text is None:
        return True
    text = text.lower().replace(".mam.", "").replace(" ", "").replace(".", "")
    cat_no = (
        row["catalogNumber"]
        .lower()
        .replace(".mam.", "")
        .replace(" ", "")
        .replace(".", "")
    )
    if "," in text or "=" in text:
        return False
    if cat_no in text:
        return True
    if cat_no in (text + "a", text + "b"):
        return True
    return False


SUFFIXES = set(string.ascii_lowercase) | set(string.digits)


def can_add_to_type_specimen(nam: Name, row: Row) -> bool:
    if nam.type_specimen is None:
        return False
    match nam.species_type_kind:
        case constants.SpeciesGroupType.holotype | constants.SpeciesGroupType.lectotype:
            parts = name_lint.parse_type_specimen(nam.type_specimen)
            if not all(
                isinstance(part, name_lint.Specimen)
                and part.text.startswith(("RMNH.", "ZMA.", "RGM."))
                for part in parts
            ):
                return False
            texts = [
                part.text for part in parts if isinstance(part, name_lint.Specimen)
            ]
            cat_num = row["catalogNumber"]
            return (
                all(
                    text[:-1] == cat_num[:-1] and text[-1] in SUFFIXES for text in texts
                )
                and cat_num[-1] in SUFFIXES
            )
        case constants.SpeciesGroupType.syntypes:
            return True
        case _:
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
    row: Row, cat_num: str, exclude_list: IO[str], dry_run: bool, fossil: bool
) -> str:
    parsed = split_name(row["scientificName"])
    if parsed is None:
        if INTERACTIVE:
            print_row(row)
        print("cannot parse:", row["scientificName"])
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
            print(
                f"cannot find {name} {authority}, {year} ({cat_num},"
                f" {row['typeStatus']})"
            )
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
    if not can_replace_collection(nam.collection, fossil) or (
        not can_replace_type_secimen(nam.type_specimen, row)
        and not can_add_to_type_specimen(nam, row)
    ):
        handle_interactively(nam, row, cat_num, exclude_list)
        return "matched with existing type"
    print("matched:", cat_num, nam)
    print(
        f"{cat_num} {row['typeStatus']}: mapped to {nam}, {nam.type_specimen},"
        f" {nam.collection}, {tags}"
    )
    if nam.type_specimen is not None:
        print(f"! replace type {nam.type_specimen} -> {cat_num}")
    for tag in tags:
        print(f"{nam}: add tag {tag}")
        if not dry_run:
            nam.add_type_tag(tag)
    if can_replace_type_secimen(nam.type_specimen, row):
        print(f"{nam}: set type {nam.type_specimen!r} -> {cat_num!r}")
        if not dry_run:
            nam.type_specimen = cat_num
    elif can_add_to_type_specimen(nam, row):
        new_type = f"{nam.type_specimen}, {cat_num}"
        print(f"{nam}: set type {nam.type_specimen!r} -> {new_type!r}")
        if not dry_run:
            nam.type_specimen = new_type
    if can_replace_collection(nam.collection, fossil):
        coll = get_collection(row)
        if coll != nam.collection:
            print(f"{nam}: set collection {nam.collection!r} -> {coll!r}")
            if not dry_run:
                nam.collection = coll
    if nam.species_type_kind is None:
        type_kind = get_type_kind(row["typeStatus"])
        if type_kind is not None:
            print(f"{nam}: set type kind to {type_kind}")
            if not dry_run:
                nam.species_type_kind = type_kind
    return "matched"


def get_collection(row: Row) -> Collection:
    match row["collectionCode"]:
        case "Mammalia":
            return collection_by_label("RMNH (Mammalia)")
        case "Micro Vertebrates" | "Macro Vertebrates" | "Paleontology Vertebrates":
            return collection_by_label("RGM")
        case "Amphibia and Reptilia":
            return collection_by_label("RMNH (Amphibia and Reptilia)")
        case _:
            raise ValueError(row["collectionCode"])


def get_type_kind(type_status: str) -> constants.SpeciesGroupType | None:
    try:
        return constants.SpeciesGroupType[type_status]
    except KeyError:
        pass
    match type_status:
        case "syntype":
            return constants.SpeciesGroupType.syntypes
        case "type":
            return constants.SpeciesGroupType.holotype
    return None


def print_row(row: Row) -> None:
    print("-----")
    for k, v in row.items():
        if k in (
            "nomenclaturalCode",
            "collectionCode",
            "family",
            "genus",
            "countryCode",
            "specificEpithet",
            "basisOfRecord",
            "taxonRank",
            "geodeticDatum",
            "institutionID",
            "rightsHolder",
            "license",
            "modified",
        ):
            continue
        if v:
            print(f"{k}: {v}")


def main(dry_run: bool = True, fossil: bool = False, herps: bool = False) -> None:
    hesp_data = get_hesp_data(fossil)
    total = cannot_find = added_tag = tag_present = excluded_count = excluded_status = 0
    statuses: Counter[str] = Counter()
    exclude_file = DATA_DIR / "rmnh-exclude.txt"
    excluded = {line.strip() for line in exclude_file.read_text().splitlines()}
    with exclude_file.open("a") as exclude_f:
        for row in getinput.print_every_n(
            get_rmnh_db(fossil, herps), label="specimens", n=1000
        ):
            type_status = get_type_kind(row["typeStatus"])
            not_a_type = type_status is None or (
                row["class"] != "Mammalia" and row["higherClassification"] != "Mammalia"
            )
            cat_num = row["catalogNumber"]
            total += 1
            if cat_num in excluded:
                excluded_count += 1
                continue
            if cat_num not in hesp_data:
                if not_a_type:
                    excluded_status += 1
                    continue
                result = handle_cannot_find(row, cat_num, exclude_f, dry_run, fossil)
                statuses[result] += 1
                cannot_find += 1
                continue
            for nam in hesp_data[cat_num]:
                tag = TypeTag.TypeSpecimenLink(row["occurrenceID"])
                if tag in nam.type_tags:
                    tag_present += 1
                    continue
                print(f"{nam}: add URL {tag} (identified as {row['scientificName']})")
                added_tag += 1
                if not dry_run:
                    for tag in get_tags(row):
                        print(f"{nam}: add tag {tag}")
                        nam.add_type_tag(tag)
                    expected = get_collection(row)
                    if (
                        nam.collection != expected
                        and nam.collection != collection_by_label("multiple")
                    ):
                        print(f"{nam}: set collection")
                        nam.collection = expected
    print(
        f"total = {total}, cannot_find = {cannot_find}, tag_present = {tag_present},"
        f" excluded_status = {excluded_status}, excluded = {excluded_count}, added_tag"
        f" = {added_tag}, statuses = {statuses}"
    )


if __name__ == "__main__":
    try:
        main(
            dry_run="--dry-run" in sys.argv,
            fossil="--fossil" in sys.argv,
            herps="--herps" in sys.argv,
        )
    except getinput.StopException:
        print("Stopped!")
