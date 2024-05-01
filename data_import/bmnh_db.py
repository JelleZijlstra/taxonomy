import csv
import functools
import re
import subprocess
import sys
from collections import Counter
from collections.abc import Iterable
from typing import IO

from taxonomy import getinput
from taxonomy.db import constants, models
from taxonomy.db.models import Article, Collection, Name, TypeTag
from taxonomy.db.models.name import lint

from .lib import DATA_DIR, get_type_specimens

INTERACTIVE = True


@functools.cache
def bmnh() -> Collection:
    coll = Collection.getter("label")("BMNH")
    assert coll is not None
    return coll


def get_hesp_data() -> dict[str, list[Name]]:
    return get_type_specimens(bmnh())


def get_bmnh_db() -> Iterable[dict[str, str]]:
    path = "bmnh-mammalia-all.csv"
    with (DATA_DIR / path).open() as f:
        rows = csv.DictReader(f)
        yield from rows


def find_name(original_name: str, authority: str, year: str) -> Name | None:
    query = Name.filter(Name.corrected_original_name == original_name)
    num_year = int(year)
    nams = [
        nam
        for nam in query
        if nam.taxonomic_authority() == authority
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
    text, *_ = text.split("|")
    text = text.strip()
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
    art = Article.getter("name")("BMNH-catalog")
    assert art is not None
    return art


AGE_MAP = {
    "adult": constants.SpecimenAge.adult,
    "immature": constants.SpecimenAge.subadult,
    "infant": constants.SpecimenAge.juvenile,
    "juvenile": constants.SpecimenAge.juvenile,
    "subadult": constants.SpecimenAge.subadult,
}
SEX_MAP = {
    "male": constants.SpecimenGender.male,
    "female": constants.SpecimenGender.female,
}
DETAIL_COLUMNS = [
    "catalogNumber",
    "determinationNames",
    "donorName",
    "lifeStage",
    "recordedBy",
    "sex",
    "typeStatus",
]


def get_tags(row: dict[str, str]) -> Iterable[TypeTag]:
    locality = row["locality"].strip("/").strip()
    if "[" in locality:
        locality += " [brackets original]"
    if locality:
        yield TypeTag.LocationDetail(locality, get_source())
    type_status = row["typeStatus"].lower()
    if type_status not in ("syntype", "cotype"):
        if row["lifeStage"] in AGE_MAP:
            yield TypeTag.Age(AGE_MAP[row["lifeStage"]])
        if row["sex"] in SEX_MAP:
            yield TypeTag.Gender(SEX_MAP[row["sex"]])
    url = f"https://data.nhm.ac.uk/object/{row['occurrenceID']}"
    text = " ... ".join(
        f"[{column}] {row[column]}" for column in DETAIL_COLUMNS if row[column]
    )
    text += f" [at {url}]"
    yield TypeTag.SpecimenDetail(text, get_source())
    yield TypeTag.TypeSpecimenLink(url)


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
    if getinput.yes_no("add tags? "):
        for tag in tags:
            nam.add_type_tag(tag)
    if getinput.yes_no("add to exclude? "):
        exclude_list.write(f"{cat_num}\n")


def handle_cannot_find(
    row: dict[str, str], cat_num: str, url: str, exclude_list: IO[str], *, dry_run: bool
) -> str:
    parsed = split_name(row["determinationNames"])
    if parsed is None:
        if INTERACTIVE:
            for key, value in row.items():
                if value:
                    print(f"{key}: {value}")
        print("cannot parse:", row["determinationNames"])
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
        print(f"cannot find {name} {authority}, {year} ({cat_num})")
        if INTERACTIVE:
            nam = Name.getter(None).get_one("name> ")
            if nam is None:
                if getinput.yes_no("add to exclude? "):
                    exclude_list.write(f"{cat_num}\n")
                return "cannot find name"
            nam.display()
            if not getinput.yes_no("confirm? "):
                return "cannot find name"
        else:
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
        return f"has status {nam.nomenclature_status.name}"
    if nam.collection is not None or nam.type_specimen is not None:
        handle_interactively(nam, row, cat_num, exclude_list)
        return "matched with existing type"
    if models.collection._validate_bmnh("Mamm", cat_num) is not None:
        handle_interactively(nam, row, cat_num, exclude_list)
        return "matched but number is invalid"
    if nam.numeric_year() < 1840:
        if not INTERACTIVE:
            return "matched but too old"
        getinput.print_header(nam)
        nam.display()
        print(
            f"{cat_num} {row['typeStatus']}: mapped to {nam}, {nam.type_specimen},"
            f" {nam.collection}, {tags}"
        )
        if not getinput.yes_no("accept? "):
            return "matched but too old"
    print("matched:", cat_num, nam)
    print(
        f"{cat_num} {row['typeStatus']}: mapped to {nam}, {nam.type_specimen},"
        f" {nam.collection}, {tags}"
    )
    if not dry_run:
        for tag in tags:
            nam.add_type_tag(tag)
        if nam.type_specimen is None:
            nam.type_specimen = cat_num
        if nam.collection is None:
            nam.collection = bmnh()
        if nam.species_type_kind is None:
            type_kind = get_type_kind(row)
            if type_kind is not None:
                nam.species_type_kind = type_kind
    return "matched"


def get_type_kind(row: dict[str, str]) -> constants.SpeciesGroupType | None:
    type_status = row["typeStatus"].lower()
    try:
        return constants.SpeciesGroupType[type_status]
    except KeyError:
        pass
    if "syntype" in type_status or "cotype" in type_status:
        return constants.SpeciesGroupType.syntypes
    if type_status == "type":
        return constants.SpeciesGroupType.holotype
    return None


def make_cat_num(no: str) -> str:
    no = (
        no.removeprefix("NHMUK ")
        .removeprefix("ZD ")
        .removeprefix("ZD. ")
        .removeprefix("GMCM ")
        .removeprefix("GERM ")
        .strip()
    )
    no = re.sub(r"^(\d+)\.([a-z])", r"\1\2", no)
    cat_num = f"BMNH {no}"
    cat_num = (
        cat_num.removesuffix(".[skin]")
        .removesuffix(".[skull]")
        .removesuffix(".skin")
        .removesuffix(".skull")
        .removesuffix("[a]")
        .removesuffix("[b]")
        .removesuffix(".")
    )
    cat_num = lint.clean_up_bmnh_type(cat_num)
    return cat_num.strip()


def should_ignore_type(type_status: str) -> bool:
    type_status = type_status.lower().strip()
    if not type_status:
        return True
    if "para" in type_status:
        return True
    if "cast of" in type_status:
        return True
    if type_status in ("non-type", "figured", "referred", "cited", "topotype"):
        return True
    return False


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
    for k, v in row.items():
        if k in (
            "basisOfRecord",
            "class",
            "collectionCode",
            "continent",
            "created",
            "determinationFiledAs",
            "family",
            "genus",
            "gbifID",
            "gbifIssue",
            "higherClassification",
            "higherGeography",
            "institutionCode",
            "kingdom",
            "modified",
            "occurrenceID",
            "occurrenceStatus",
            "order",
            "otherCatalogNumbers",
            "phylum",
            "registrationCode",
            "specificEpithet",
            "subDepartment",
        ):
            continue
        if v:
            print(f"{k}: {v}")


def main(*, dry_run: bool = True) -> None:
    hesp_data = get_hesp_data()
    total = cannot_find = added_tag = tag_present = excluded_count = not_a_type = (
        name_doesnt_match
    ) = 0
    statuses: Counter[str] = Counter()
    exclude_file = DATA_DIR / "bmnh-exclude.txt"
    excluded = {line.strip() for line in exclude_file.read_text().splitlines()}
    with exclude_file.open("a") as exclude_f:
        for row in getinput.print_every_n(get_bmnh_db(), label="specimens", n=100):
            type_status = row["typeStatus"]
            non_type = should_ignore_type(type_status)
            no = row["catalogNumber"]
            if no.startswith("PV"):
                # ignore fossils for now
                continue
            total += 1
            cat_num = make_cat_num(no)
            if cat_num in excluded:
                excluded_count += 1
                continue
            url = f"https://data.nhm.ac.uk/object/{row['occurrenceID']}"
            if cat_num not in hesp_data:
                if non_type:
                    not_a_type += 1
                    continue
                # print(f"Cannot find: {cat_num}: {row['determinationNames']}, {row['typeStatus']}, {url}")
                statuses[
                    handle_cannot_find(row, cat_num, url, exclude_f, dry_run=dry_run)
                ] += 1
                cannot_find += 1
                continue
            for nam in hesp_data[cat_num]:
                tag = TypeTag.TypeSpecimenLink(url)
                if tag in nam.type_tags:
                    tag_present += 1
                    continue
                if non_type:
                    print(
                        f"current type {nam.type_specimen}, row"
                        f" {cat_num} ({row['determinationNames']} vs. {nam})"
                    )
                    close = is_close_enough(nam, row)
                    if not close:
                        if close is None and INTERACTIVE:
                            getinput.print_header(nam)
                            nam.display()
                            print_row(row)
                            if not getinput.yes_no("accept match? "):
                                subprocess.check_call(["open", url])
                                nam.edit()
                                if getinput.yes_no("add to exclude list? "):
                                    exclude_f.write(f"{cat_num}\n")
                                continue
                        else:
                            name_doesnt_match += 1
                            continue
                print(f"{nam}: add tag {tag}")
                added_tag += 1
                if not dry_run:
                    nam.add_type_tag(tag)
                    for tag in get_tags(row):
                        print(f"{nam}: add tag {tag}")
                        nam.add_type_tag(tag)
    print(
        f"total = {total}, cannot_find = {cannot_find}, tag_present = {tag_present},"
        f" name_doesnt_match = {name_doesnt_match}, not_a_type = {not_a_type}, excluded"
        f" = {excluded_count}, added_tag = {added_tag}, statuses = {statuses}"
    )


if __name__ == "__main__":
    try:
        main(dry_run="--dry-run" in sys.argv)
    except getinput.StopException:
        print("Stopped!")
