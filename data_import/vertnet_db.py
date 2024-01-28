import argparse
import csv
import functools
import re
import string
from collections import Counter, defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import IO, TypedDict

from taxonomy import getinput
from taxonomy.db import constants
from taxonomy.db.models import Article, Collection, Name, TypeTag, name_lint

from .lib import DATA_DIR

Row = TypedDict(
    "Row",
    {
        "type": str,
        "modified": str,
        "license": str,
        "rightsholder": str,
        "accessrights": str,
        "bibliographiccitation": str,
        "references": str,
        "institutionid": str,
        "collectionid": str,
        "datasetid": str,
        "institutioncode": str,
        "collectioncode": str,
        "datasetname": str,
        "basisofrecord": str,
        "informationwithheld": str,
        "datageneralizations": str,
        "dynamicproperties": str,
        "occurrenceid": str,
        "catalognumber": str,
        "recordnumber": str,
        "recordedby": str,
        "individualcount": str,
        "sex": str,
        "lifestage": str,
        "reproductivecondition": str,
        "behavior": str,
        "establishmentmeans": str,
        "occurrencestatus": str,
        "preparations": str,
        "disposition": str,
        "associatedmedia": str,
        "associatedreferences": str,
        "associatedsequences": str,
        "associatedtaxa": str,
        "othercatalognumbers": str,
        "occurrenceremarks": str,
        "organismid": str,
        "organismname": str,
        "organismscope": str,
        "associatedoccurrences": str,
        "associatedorganisms": str,
        "previousidentifications": str,
        "organismremarks": str,
        "materialsampleid": str,
        "eventid": str,
        "fieldnumber": str,
        "eventdate": str,
        "eventtime": str,
        "startdayofyear": str,
        "enddayofyeayear": str,
        "month": str,
        "day": str,
        "verbatimeventdate": str,
        "habitat": str,
        "samplingprotocol": str,
        "samplingeffort": str,
        "fieldnotes": str,
        "eventremarks": str,
        "locationid": str,
        "highergeographyid": str,
        "highergeography": str,
        "continent": str,
        "waterbody": str,
        "islandgroup": str,
        "island": str,
        "country": str,
        "countrycode": str,
        "stateprovince": str,
        "county": str,
        "municipality": str,
        "locality": str,
        "verbatimlocality": str,
        "minimumelevationinmeters": str,
        "maximumelevationinmeters": str,
        "verbatimelevation": str,
        "minimumdepthinmeters": str,
        "maximumdepthinmeters": str,
        "verbatimdepth": str,
        "minimumdistanceabovesurfaceinmeters": str,
        "maximumdistanceabovesurfaceinmeters": str,
        "locationaccordingto": str,
        "locationremarks": str,
        "decimallatitude": str,
        "decimallongitude": str,
        "geodeticdatum": str,
        "coordinateuncertaintyinmeters": str,
        "coordinateprecisionverbatimcoordinates": str,
        "verbatimlatitude": str,
        "verbatimlongitude": str,
        "verbatimcoordinatesystem": str,
        "verbatimsrs": str,
        "footprintwkt": str,
        "footprintsrs": str,
        "georeferencedby": str,
        "georeferenceddate": str,
        "georeferenceprotocol": str,
        "georeferencesources": str,
        "georeferenceverificationstatus": str,
        "georeferenceremarks": str,
        "geologicalcontextid": str,
        "earliesteonorlowesteonothem": str,
        "latesteonorhighesteonothem": str,
        "earliesteraorlowesterathem": str,
        "latesteraorhighesterathem": str,
        "earliestperiodorlowestsystem": str,
        "latestperiodorhighestsystem": str,
        "earliestepochorlowestseries": str,
        "latestepochorhighestseries": str,
        "earliestageorloweststage": str,
        "latestageorhigheststage": str,
        "lowestbiostratigraphiczone": str,
        "highestbiostratigraphiczone": str,
        "lithostratigraphicterms": str,
        "group": str,
        "formation": str,
        "member": str,
        "bed": str,
        "identificationid": str,
        "identificationqualifier": str,
        "typestatus": str,
        "identifiedby": str,
        "dateidentified": str,
        "identificationreferences": str,
        "identificationverificationstatus": str,
        "identificationremarks": str,
        "scientificnameid": str,
        "namepublishedinid": str,
        "scientificname": str,
        "acceptednameusage": str,
        "originalnameusage": str,
        "namepublishedin": str,
        "namepublishedinyearhigherclassification": str,
        "kingdom": str,
        "phylum": str,
        "class": str,
        "order": str,
        "family": str,
        "genus": str,
        "subgenus": str,
        "specificepithet": str,
        "infraspecificepithet": str,
        "taxonrank": str,
        "verbatimtaxonrank": str,
        "scientificnameauthorship": str,
        "vernacularname": str,
        "nomenclaturalcode": str,
        "taxonomicstatus": str,
        "taxonremarks": str,
        "lengthinmm": str,
        "lengthtype": str,
        "lengthunitsinferred": str,
        "massing": str,
        "massunitsinferred": str,
        "underivedlifestage": str,
        "underivedsex": str,
        "dataset_url": str,
        "dataset_citation": str,
        "gbifdatasetid": str,
        "gbifpublisherid": str,
        "dataset_contact_email": str,
        "dataset_contact": str,
        "dataset_pubdate": str,
        "lastindexed": str,
        "migrator_version": str,
        "hasmedia": str,
        "hastissue": str,
        "wascaptive": str,
        "isfossil": str,
        "isarch": str,
        "vntype": str,
        "haslength": str,
    },
)


@functools.cache
def collection_by_label(label: str) -> Collection:
    if label == "O":
        label = "NHMO"
    if label == "RBINS":
        label = "IRSNB"
    if label == "ZIN":
        label = "ZISP"
    if label == "UAM":
        label = "UAM (Alaska)"
    if label == "UCM":
        label = "UCM (Colorado)"
    coll = Collection.getter("label")(label)
    assert coll is not None
    return coll


def _get_hesp_data() -> dict[str, list[Name]]:
    output = defaultdict(list)
    for coll in _all_collections():
        for nam in coll.type_specimens:
            if nam.type_specimen is None:
                continue
            for spec in name_lint.parse_type_specimen(nam.type_specimen):
                if isinstance(spec, name_lint.Specimen):
                    output[spec.text].append(nam)
        for nam in coll.get_derived_field("former_specimens") or ():
            if nam.type_specimen is None:
                continue
            for spec in name_lint.parse_type_specimen(nam.type_specimen):
                if isinstance(spec, name_lint.SpecimenRange):
                    continue
                for text in spec.former_texts:
                    if text.startswith(tuple(INCLUDED_CODES)):
                        output[text].append(nam)
        for nam in coll.get_derived_field("future_specimens") or ():
            if nam.type_specimen is None:
                continue
            for spec in name_lint.parse_type_specimen(nam.type_specimen):
                if isinstance(
                    spec, (name_lint.SpecimenRange, name_lint.SpecialSpecimen)
                ):
                    continue
                for text in spec.future_texts:
                    if text.startswith(tuple(INCLUDED_CODES)):
                        output[text].append(nam)
        for nam in coll.get_derived_field("extra_specimens") or ():
            if nam.type_specimen is None:
                continue
            for spec in name_lint.parse_type_specimen(nam.type_specimen):
                if isinstance(
                    spec, (name_lint.SpecimenRange, name_lint.SpecialSpecimen)
                ):
                    continue
                for text in spec.extra_texts:
                    if text.startswith(tuple(INCLUDED_CODES)):
                        output[text].append(nam)
    multiple = collection_by_label("multiple")
    for nam in multiple.type_specimens:
        if nam.type_specimen is None:
            continue
        try:
            for spec in name_lint.parse_type_specimen(nam.type_specimen):
                if isinstance(spec, name_lint.Specimen) and spec.text.startswith(
                    tuple(INCLUDED_CODES)
                ):
                    output[spec.text].append(nam)
        except ValueError as e:
            print(f"failed to parse {nam} due to {e!r}")
    return output


def get_vertnet_db(filename: Path) -> Iterable[Row]:
    with filename.open() as f:
        rows: Iterable[Row] = csv.DictReader(f, dialect="excel-tab")  # type: ignore
        yield from rows


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
    (?P<authority>[A-Z]\D+)
    (?P<year>\d{4})
    \.?
    """,
    re.VERBOSE,
)


def _split_name(row: Row) -> tuple[str, str, str] | None:
    text = row["scientificnameauthorship"].strip().replace("(", "").replace(")", "")
    match = _NAME_REGEX.fullmatch(text)
    if match is None:
        return None
    authority = match.group("authority").strip().strip(",")
    authority = authority.replace(" and ", " & ")
    authority = re.sub(r"\s+", " ", authority)
    return row["scientificname"], authority, match.group("year")


@functools.cache
def get_source() -> Article:
    art = Article.getter("name")("VertNet-catalog")
    assert art is not None
    return art


DETAIL_COLUMNS = [
    "institutioncode",
    "collectioncode",
    "catalognumber",
    "recordedby",
    "sex",
    "lifestage",
    "preparations",
    "occurrenceremarks",
    "typestatus",
    "eventdate",
]
LOCALITY_COLUMNS = [
    "highergeography",
    "locality",
    "verbatimelevation",
    "earliestepochorlowestseries",
    "earliestageorloweststage",
    "formation",
]


def _get_ts_link(row: Row) -> TypeTag:
    if row["institutioncode"] == "USNM":
        link = row["occurrenceid"]
    else:
        link = name_lint.fix_type_specimen_link(row["references"])
    return TypeTag.TypeSpecimenLinkFor(link, _get_cat_num(row))


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
    text += f" [at {row['references']}]"
    yield TypeTag.SpecimenDetail(text, get_source())
    yield _get_ts_link(row)
    type_status = get_type_kind(row["typestatus"])
    if type_status is not constants.SpeciesGroupType.syntypes:
        if life_stage := parse_life_stage(row["lifestage"]):
            yield TypeTag.Age(life_stage)
        if sex := parse_sex(row["sex"]):
            yield TypeTag.Gender(sex)


def parse_life_stage(text: str) -> constants.SpecimenAge | None:
    match text:
        case "adult" | "oud":
            return constants.SpecimenAge.adult
        case "juvenile" | "neonate" | "embryo":
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


def _handle_interactively(
    nam: Name, row: Row, cat_num: str, exclude_list: IO[str], interactive: bool
) -> None:
    if not interactive:
        return
    getinput.print_header(nam)
    tags = list(get_tags(row))
    nam.display()
    print(f"{cat_num}: mapped to {nam}, {nam.type_specimen}, {nam.collection}, {tags}")
    nam.edit()
    if nam.type_specimen and cat_num not in nam.type_specimen:
        if getinput.yes_no("exclude? "):
            exclude_list.write(f"{cat_num}\n")


def _can_replace_collection(coll: Collection | None, row: Row) -> bool:
    return coll is None or coll == _get_collection(row)


def _can_replace_type_secimen(text: str | None, row: Row) -> bool:
    if text is None:
        return True
    return False


SUFFIXES = set(string.ascii_lowercase) | set(string.digits)


def _can_add_to_type_specimen(nam: Name, row: Row) -> bool:
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
            cat_num = _get_cat_num(row)
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


def _maybe_get(exclude_list: IO[str], cat_num: str, interactive: bool) -> Name | None:
    if interactive:
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


def _handle_cannot_find(
    row: Row, cat_num: str, exclude_list: IO[str], dry_run: bool, interactive: bool
) -> str:
    parsed = _split_name(row)
    if parsed is None:
        if interactive:
            print_row(row)
        print("cannot parse:", row["scientificname"])
        if interactive:
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
                f" {row['typestatus']})"
            )
            nam = _maybe_get(exclude_list, cat_num, interactive)
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
            f"{cat_num} {row['typestatus']}: mapped to {nam}, {nam.type_specimen},"
            f" {nam.collection}, {tags}"
        )
        new_nam = _maybe_get(exclude_list, cat_num, interactive)
        if new_nam is None:
            return f"has status {nam.nomenclature_status.name}"
        nam = new_nam
    if not _can_replace_collection(nam.collection, row) or (
        not _can_replace_type_secimen(nam.type_specimen, row)
        and not _can_add_to_type_specimen(nam, row)
    ):
        _handle_interactively(nam, row, cat_num, exclude_list, interactive)
        return "matched with existing type"
    print("matched:", cat_num, nam)
    print(
        f"{cat_num} {row['typestatus']}: mapped to {nam}, {nam.type_specimen},"
        f" {nam.collection}, {tags}"
    )
    if nam.type_specimen is not None:
        print(f"! replace type {nam.type_specimen} -> {cat_num}")
    for tag in tags:
        print(f"{nam}: add tag {tag}")
        if not dry_run:
            nam.add_type_tag(tag)
    if _can_replace_type_secimen(nam.type_specimen, row):
        print(f"{nam}: set type {nam.type_specimen!r} -> {cat_num!r}")
        if not dry_run:
            nam.type_specimen = cat_num
    elif _can_add_to_type_specimen(nam, row):
        new_type = f"{nam.type_specimen}, {cat_num}"
        print(f"{nam}: set type {nam.type_specimen!r} -> {new_type!r}")
        if not dry_run:
            nam.type_specimen = new_type
    if _can_replace_collection(nam.collection, row):
        coll = _get_collection(row)
        if coll != nam.collection:
            print(f"{nam}: set collection {nam.collection!r} -> {coll!r}")
            if not dry_run:
                nam.collection = coll
    if nam.species_type_kind is None:
        type_kind = get_type_kind(row["typestatus"])
        if type_kind is not None:
            print(f"{nam}: set type kind to {type_kind}")
            if not dry_run:
                nam.species_type_kind = type_kind
    if not dry_run:
        nam.format()
    return "matched"


_TABLE = str.maketrans({ord(c): None for c in ("+", "?", ".", ",", ";", ":", "]")})


def get_type_kind(type_status: str) -> constants.SpeciesGroupType | None:
    if not type_status:
        return None
    type_status = type_status.lower().split()[0].translate(_TABLE)
    try:
        return constants.SpeciesGroupType[type_status]
    except KeyError:
        pass
    match type_status:
        case "syntype" | "cotype" | "types" | "cotypes":
            return constants.SpeciesGroupType.syntypes
        case "type" | "holo" | "holótipo":
            return constants.SpeciesGroupType.holotype
        case (
            "fig"
            | "figured"
            | "--"
            | "y"
            | "allotype"
            | "hypodigm"
            | "typodigm"
            | "part"
            | "referred"
            | "paralectotype"
            | "publ"
            | "voucher"
            | "host"
            | "basis"
            | "parasymbiotype"
            | "symbiotype"
            | "topotype"
            | "erroneous"
            | "paratype"
            | "paratypes"
            | "referral"
            | "genetic"
            | "para"
            | "cited"
            | "paralectotype"
            | "to"
            | "paratype(s)"
            | "plastosyntype"
            | "plesiotype"
            | "plastoholotype"
            | "cast"
            | "plastoparatype"
            | "parátipo"
            | "typemateriale"
            | "putative"
            | "possible"
            | "paralectotypes"
            | "nomen"
            | "stated"
            | "topotypes"
            | "metatype"
            | "lectoparatype"
            | "paralecto"
            | "hypotype"
            | "none"
        ):
            return None
    print(type_status)
    return None


def print_row(row: Row, concise: bool = False, full: bool = False) -> None:
    if concise:
        text = (
            f"{_get_cat_num(row)},"
            f" {row['typestatus']}, identified as {row['scientificname']}, reference"
            f" {row['references']}"
        )
        if row["occurrenceremarks"]:
            text += f", comment {row['occurrenceremarks']}"
        print(text)
        return
    print("-----")
    for k, v in row.items():
        if not full and k in (
            "modified",
            "rightsholder",
            "accessrights",
            "dynamicproperties",
            "nomenclaturalcode",
            "license",
            "type",
            "bibliographiccitation",
            "basisofrecord",
            "startdayofyear",
            "enddayofyear",
            "year",
            "month",
            "day",
            "continent",
            "country",
            "stateprovince",
            "geodeticdatum",
            "coordinateuncertaintyinmeters",
            "georeferencedby",
            "georeferenceddate",
            "georeferenceprotocol",
            "georeferencesources",
            "georeferenceverificationstatus",
            "georeferenceremarks",
            "higherclassification",
            "kingdom",
            "phylum",
            "class",
            "order",
            "dataset_citation",
            "gbifdatasetid",
            "gbifpublisherid",
            "dataset_contact_email",
            "dataset_contact",
            "dataset_pubdate",
            "lastindexed",
            "migrator_version",
            "hasmedia",
            "hastissue",
            "wascaptive",
            "isfossil",
            "isarch",
            "vntype",
            "genus",
            "specificepithet",
            "haslength",
        ):
            continue
        if v:
            print(f"{k}: {v}")


# Did these separately based on their own databases.
# NRM has only one type, apparently from some whale-specific database
# Similar for NSMT, RCS, RBINS, ZIN, ZMMU
# For O I don't like that they use "O" as their code, and I already checked their published type catalog
# Empty string is also some whale database
SKIPPED_CODES = {
    "CAS",
    "MCZ",
    "MNHN",
    "NHMUK",
    "RMNH",
    "NRM",
    "NSMT",
    "O",
    "RCS",
    "RBINS",
    "ZIN",
    "ZMMU",
    "",
    "UMMZ",  # not enough types
    "UNICAMP",  # not sure what this is, doesn't show up in online VertNet search form
}
# Finished, stop re-checking them for performance
RETIRED_CODES = {
    "YPM",
    "AMNH",
    "BPBM",
    "CHAS",
    "KU",
    "FMNH",
    "DMNS",
    "LACM",
    "UCLA",
    "MBML",
    "MHNC",
    "MVZ",
    "MSB",
    "LSUMZ",
    "NCSM",
    "NMR",
    "OMNH",
    "ROM",
    "SDNHM",
    "UAM",
    "UCM",
    "UCMP",
    "UF",
    "UMNH",
}
INCLUDED_CODES = {"UMZC", "UTEP", "USNM"}


def _get_cat_num(row: Row) -> str:
    match row["institutioncode"]:
        case (
            "YPM"
            | "CHAS"
            | "DMNS"
            | "MSB"
            | "MVZ"
            | "NMR"
            | "NRM"
            | "NSMT"
            | "O"
            | "RCS"
            | "RBINS"
            | "ZIN"
            | "ZMMU"
            | "UAM"
            | "UCM"
            | "UTEP"
        ):
            return row["catalognumber"]
        case "AMNH" | "UMZC":
            return f"{row['institutioncode']} {row['catalognumber']}"
        case "KU" | "MBML" | "NCSM":
            return f"{row['collectioncode']} {row['catalognumber']}"
        case "BPBM" | "FMNH" | "LACM" | "UCLA" | "LSUMZ" | "MHNC":
            return f"{row['institutioncode']} {row['collectioncode']} {row['catalognumber'].lstrip('0')}"
        case "OMNH" | "UCMP" | "UMMZ" | "UNICAMP":
            return f"{row['institutioncode']}:{row['collectioncode']}:{row['catalognumber'].lstrip('0')}"
        case "ROM" | "SDNHM" | "UMNH":
            return f"{row['institutioncode']}:{row['collectioncode'].replace('Mammals', 'MAM').replace('Mammal specimens', 'Mamm')}:{row['catalognumber'].lstrip('0')}"
        case "USNM":
            match row["collectioncode"]:
                case "Mammals":
                    return f"USNM:MAMM:{row['catalognumber']}"
                case "Paleobiology":
                    match = re.match(r"([A-Z]+)(\d+)", row["catalognumber"])
                    assert match is not None, row["catalognumber"]
                    return f"USNM:{match.group(1)}:{match.group(2)}"
                case "Amphibians & Reptiles":
                    return f"USNM:HERP:{row['catalognumber']}"
                case "Birds":
                    return f"USNM:BIRDS:{row['catalognumber']}"
                case "Fish" | "Fishes":
                    return f"USNM:FISH:{row['catalognumber']}"
                case unhandled:
                    print_row(row)
                    raise ValueError(unhandled)
        case "UF":
            match row["collectioncode"]:
                case "UF/FGS":
                    return f"UF:FGS:{row['catalognumber']}"
                case "UF/IGM":
                    return f"UF:IGM:{row['catalognumber']}"
                case "UF/PB":
                    return f"UF:PB:{row['catalognumber']}"
                case "UF/TRO":
                    return f"UF:TRO:{row['catalognumber']}"
                case "UF":
                    return f"UF:VP:{row['catalognumber']}"
                case "Mammals":
                    return f"UF:MAM:{row['catalognumber']}"
                case "Herp":
                    return f"UF:HERP:{row['catalognumber']}"
                case "Fish":
                    return f"UF:FISH:{row['catalognumber']}"
                case unhandled:
                    print_row(row)
                    raise ValueError(unhandled)
        case unhandled:
            print_row(row)
            raise ValueError(unhandled)


def _get_collection(row: Row) -> Collection:
    match row["institutioncode"]:
        case unhandled:
            return collection_by_label(unhandled)


def _all_collections() -> list[Collection]:
    return [collection_by_label(code) for code in INCLUDED_CODES - RETIRED_CODES]


def main(
    filename: Path,
    dry_run: bool = True,
    interactive: bool = False,
    include_non_types: bool = False,
) -> None:
    hesp_data = _get_hesp_data()
    total = cannot_find = added_tag = tag_present = excluded_count = 0
    excluded_status = excluded_cast = retired_codes = excluded_codes = 0
    statuses: Counter[str] = Counter()
    exclude_file = DATA_DIR / "vertnet-exclude.txt"
    excluded = {line.strip() for line in exclude_file.read_text().splitlines()}
    pending_codes = set()
    first = True
    with exclude_file.open("a") as exclude_f:
        for row in getinput.print_every_n(
            get_vertnet_db(filename), label="specimens", n=1000
        ):
            type_status = get_type_kind(row["typestatus"])
            not_a_type = type_status is None
            if (not include_non_types) and not_a_type:
                excluded_status += 1
                continue
            if "cast" in row["preparations"].lower() or (
                row["institutioncode"] == "UCMP" and row["othercatalognumbers"]
            ):
                if include_non_types:
                    not_a_type = True
                else:
                    excluded_cast += 1
                    continue
            if row["institutioncode"] in RETIRED_CODES | SKIPPED_CODES:
                retired_codes += 1
                continue
            if row["institutioncode"] not in INCLUDED_CODES:
                excluded_codes += 1
                pending_codes.add(row["institutioncode"])
                continue
            if row["institutioncode"] == "UCMP":
                # UCMP lists tons of casts as type specimens, I gave up on going through them all
                not_a_type = True
            cat_num = _get_cat_num(row)
            total += 1
            if cat_num in excluded:
                excluded_count += 1
                continue
            if cat_num not in hesp_data:
                if not_a_type:
                    continue
                if first:
                    print_row(row, full=True)
                    first = False
                if not interactive:
                    print_row(row, concise=True)
                result = _handle_cannot_find(
                    row, cat_num, exclude_f, dry_run, interactive
                )
                statuses[result] += 1
                cannot_find += 1
                continue
            for nam in hesp_data[cat_num]:
                tag = _get_ts_link(row)
                if tag in nam.type_tags:
                    tag_present += 1
                    continue
                print(f"{nam}: add URL {tag} (identified as {row['scientificname']})")
                added_tag += 1
                if not dry_run:
                    for tag in get_tags(row):
                        print(f"{nam}: add tag {tag}")
                        nam.add_type_tag(tag)
                    expected = _get_collection(row)
                    if (
                        nam.collection != expected
                        and nam.collection != collection_by_label("multiple")
                    ):
                        if nam.collection is None:
                            print(f"{nam}: set collection")
                            nam.collection = expected
                        else:
                            print(
                                f"{nam}: collection is {nam.collection} but expected"
                                f" {expected}"
                            )
                    nam.format()
    print(
        f"total = {total}, cannot_find = {cannot_find}, tag_present = {tag_present},"
        f" excluded_status = {excluded_status}, excluded_cast = {excluded_cast},"
        f" excluded = {excluded_count}, retired codes = {retired_codes}, excluded codes"
        f" = {excluded_codes}, added_tag = {added_tag}, statuses = {statuses}"
    )
    print("missing codes:", ", ".join(sorted(pending_codes)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--filename", default=DATA_DIR / "vertnet-mammalia-types.tsv", type=Path
    )
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--interactive", action="store_true", default=False)
    parser.add_argument("--include-non-types", action="store_true", default=False)
    args = parser.parse_args()

    try:
        main(
            dry_run=args.dry_run,
            interactive=args.interactive,
            filename=args.filename,
            include_non_types=args.include_non_types,
        )
    except getinput.StopException:
        print("Stopped!")
