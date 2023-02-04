"""Data import from

Lawrence, M.A. 1993. Catalog of Recent mammal types in the American Museum of Natural History.
Bulletin of the American Museum of Natural History 217:1-200.

Assumes there is a file "lawrence1993.txt" in the data/ directory with the OCR text for
the paper.

"""
import enum
import json
import re
from collections import Counter
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import Levenshtein
import unidecode

from taxonomy import getinput
from taxonomy.db import constants, helpers, models
from taxonomy.db.constants import SpecimenOrgan
from taxonomy.db.models import TypeTag

from . import lib

# Generally useful functions that should perhaps be generalized to other data imports later.
DATA_DIR = Path(__file__).parent / "data"
MONTHS = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
DMY = re.compile(f'\\d+\\s+({"|".join(MONTHS)}),?\\s+\\d{{4}}')  # 10 January 2018
MDY = re.compile(f'({"|".join(MONTHS)})\\s+\\d+,\\s+\\d{{4}}')  # January 10, 2018
MY = re.compile(f'({"|".join(MONTHS)})\\s+\\d{{4}}')  # January 2018
REMOVE_PARENS = re.compile(r" \([A-Z][a-z]+\)")

DataT = Iterable[dict[str, Any]]


def find_name(original_name: str, authority: str) -> models.Name | None:
    try:
        return models.Name.get(
            models.Name.original_name == original_name,
            models.Name.authority == authority,
        )
    except models.Name.DoesNotExist:
        root_name = original_name.split()[-1]
        possible_genus_names = [original_name.split()[0]]
        # try subgenus
        match = re.search(r"\(([A-Z][a-z]+)\)", original_name)
        if match:
            possible_genus_names.append(match.group(1))
        for genus in possible_genus_names:
            names = models.Name.filter(
                models.Name.root_name == root_name, models.Name.authority == authority
            )
            names = [name for name in names if lib.genus_name_of_name(name) == genus]
            if len(names) == 1:
                return names[0]
    # fuzzy match
    matches = [
        name
        for name in models.Name.filter(
            models.Name.original_name != None, models.Name.authority == authority
        )
        if Levenshtein.distance(original_name, name.original_name) < 3
        or REMOVE_PARENS.sub("", original_name)
        == REMOVE_PARENS.sub("", name.original_name)
    ]
    if len(matches) == 1:
        return matches[0]
    return None


def name_variants(original_name: str, authority: str) -> Iterable[tuple[str, str]]:
    yield original_name, authority
    original_authority = authority
    authority = re.sub(r"([A-Z]\.)+ ", "", authority).strip()
    if authority != original_authority:
        yield original_name, authority
    if " in " in authority:
        authority = re.sub(r" in .*$", "", authority)
        yield original_name, authority
        yield original_name, re.sub(r"^.* in ", "", original_authority)
    # This should be generalized (initials are in the DB but not the source)
    if authority.startswith("Allen"):
        yield original_name, "J.A. " + authority
        yield original_name, "J.A. " + authority + " & Chapman"
    if authority.startswith("Anthony"):
        yield original_name, "H.E. " + authority
    if authority.startswith("Bryant"):
        yield original_name, "W. " + authority
        yield original_name, "W.E. " + authority
    if authority.startswith("Davis"):
        yield original_name, "W.B. " + authority
    if authority == "Andersen":
        yield original_name, "K. Andersen"
    if authority.startswith("Howell"):
        yield original_name, "A.H. Howell"
    if authority == "Hill":
        yield original_name, "J. Eric Hill"
    if "ue" in original_name:
        yield original_name.replace("ue", "ü"), authority
    if authority in ("Wied", "Wied-Neuwied"):
        yield original_name, "Wied-Neuwied"
        yield original_name, "Schinz"
    if authority == "Geoffroy":
        yield original_name, "É. Geoffroy Saint-Hilaire"
    if authority == "Schwartz":
        yield original_name, "Schwarz"
    if authority == "Tate":
        yield original_name, "Tate & Archbold"
    if authority == "Fischer":
        yield original_name, "J.B. Fischer"
    if "Dolman" in authority:
        yield original_name, authority.replace("Dolman", "Dollman")
    if original_name == "Hyosciurus heinrichi":
        yield original_name, "Archbold & Tate"
    if original_name == "Anoura geoffroyi antricola":
        yield original_name, "H.E. Anthony"
    if original_name == "Nyctalus vetulinus":
        yield "Nyctalus velutinus", authority
    for used, correct in [("Oryzomys", "Oecomys"), ("Peromyscus", "Baiomys")]:
        if original_name.startswith(used):
            yield original_name.replace(used, correct), authority


def extract_altitude(text: str) -> tuple[str, constants.AltitudeUnit] | None:
    # Try feet first because feet are often given with a conversion into meters, and we
    # want the original measurement.
    match = re.search(r"(\d+(-\d+)?) ft", text)
    if match:
        return match.group(1), constants.AltitudeUnit.ft
    # Only match at the end of a sentence or phrase to avoid matching things like
    # "100 m N of somewhere".
    match = re.search(r"(\d+) m[\.,]", text)
    if match:
        return match.group(1), constants.AltitudeUnit.m
    return None


def extract_date(text: str) -> str | None:
    for rgx in (DMY, MDY, MY):
        match = rgx.search(text)
        if match:
            return match.group()
    return None


def extract_geographical_components(text: str) -> list[tuple[str, ...]]:
    # replace B(ritish) C(olumbia) -> British Columbia
    text = re.sub(r"([A-Z])\(([a-z]+)\)", r"\1\2", text)
    parts = re.split(r"[():,;\.]+", text)
    parts = list(filter(None, (part.strip() for part in parts)))
    out: list[Any] = []
    for part in parts:
        if part.startswith("=") and out:
            out[-1] = (out[-1], part[1:].strip())
        else:
            out.append(part)
    return [(p,) if isinstance(p, str) else p for p in out]


test_case = "Congo (= Zaire): (Haut Zaire); Niapu. November 24, 1913."
assert extract_geographical_components(test_case) == [
    ("Congo", "Zaire"),
    ("Haut Zaire",),
    ("Niapu",),
    ("November 24",),
    ("1913",),
], extract_geographical_components(test_case)


def get_possible_names(names: Iterable[str]) -> Iterable[str]:
    for name in names:
        yield name
        yield lib.NAME_SYNONYMS.get(name, name)
        if name.endswith(" Island"):
            fixed = name[: -len(" Island")]
            yield fixed
            yield lib.NAME_SYNONYMS.get(fixed, fixed)
        without_direction = re.sub(
            r"^(North|South|West|East)(west|east)?(ern)? ", "", name
        )
        if without_direction != name:
            yield without_direction
            yield lib.NAME_SYNONYMS.get(without_direction, without_direction)
        without_diacritics = unidecode.unidecode(name)
        if name != without_diacritics:
            yield without_diacritics


def get_region_from_name(raw_names: Iterable[str]) -> models.Region | None:
    for name in get_possible_names(raw_names):
        name = lib.NAME_SYNONYMS.get(name, name)
        try:
            return models.Region.get(models.Region.name == name)
        except models.Region.DoesNotExist:
            pass
    return None


def extract_region(text: str) -> models.Location | None:
    components = extract_geographical_components(text)
    possible_region = get_region_from_name(components[0])
    if possible_region is None:
        # print(f'could not extract region from {components}')
        return None
    region = possible_region
    if region.children.count() > 0:
        for name in get_possible_names(components[1]):
            name = lib.NAME_SYNONYMS.get(name, name)
            try:
                region = region.children.filter(models.Region.name == name).get()
                break
            except models.Region.DoesNotExist:
                pass
        else:
            # Child regions for these are just for some outlying islands, so we don't care
            # if we can't extract a child.
            if region.name not in ("Colombia", "Ecuador", "Honduras"):
                pass  # print(f'could not extract subregion from {components}')
    return region.get_location()


def extract_collector(text: str) -> str:
    text = re.sub(r"\. Original( skin)? number .*$", "", text)
    return re.sub(r" \([A-Z\d]+\)$", "", text)


SKIN = TypeTag.Organ(SpecimenOrgan.skin, "", "")
SKULL = TypeTag.Organ(SpecimenOrgan.skull, "", "")
IN_ALCOHOL = TypeTag.Organ(SpecimenOrgan.in_alcohol, "", "")
SKELETON = TypeTag.Organ(SpecimenOrgan.postcranial_skeleton, "", "")


def enum_has_member(enum_cls: type[enum.Enum], member: str) -> bool:
    try:
        enum_cls[member]
    except KeyError:
        return False
    else:
        return True


def extract_type_specimen(text: str) -> dict[str, Any]:
    sentences = text.split(".")
    out: dict[str, Any] = {}
    out["type_specimen"] = f"AMNH {sentences[0]}"
    organs = sentences[1].strip().lower()
    if organs in ("skin and skull", "skin and cranium"):
        tags = [SKIN, SKULL]
    elif "in alcohol" in organs:
        tags = [IN_ALCOHOL]
        if "skull" in organs:
            tags.append(SKULL)
    elif organs.startswith("skin, skull,"):
        tags = [SKIN, SKULL]
        if "skeleton" in organs:
            tags.append(SKELETON)
        elif "in alcohol" in organs:
            tags.append(IN_ALCOHOL)
    elif organs.startswith(("skull only", "cranium only")):
        tags = [SKULL]
    elif organs.startswith("skin only"):
        tags = [SKIN]
    else:
        tags = []
    if tags:
        out["organs"] = tags
    if len(sentences) > 2 and sentences[2].strip():
        specimen = sentences[2].strip().lower()
        if enum_has_member(constants.SpecimenAge, specimen):
            out["age"] = constants.SpecimenAge[specimen]
        elif enum_has_member(constants.SpecimenGender, specimen):
            out["gender"] = constants.SpecimenGender[specimen]
        elif specimen == "unsexed adult":
            out["age"] = constants.SpecimenAge.adult
            out["gender"] = constants.SpecimenGender.unknown
        elif " " in specimen:
            age, gender = specimen.rsplit(maxsplit=1)
            if enum_has_member(constants.SpecimenAge, age):
                out["age"] = constants.SpecimenAge[age]
            elif age == "immature":
                out["age"] = constants.SpecimenAge.juvenile
            elif age == "young adult":
                out["age"] = constants.SpecimenAge.subadult
            if enum_has_member(constants.SpecimenGender, gender):
                out["gender"] = constants.SpecimenGender[gender]
    return out


AUTHOR_NAME_RGX = re.compile(
    r"""
    (?P<name>[A-Z][a-z]+(\s\([A-Z][a-z]+\??\))?(\s[a-z\'-]{3,})?\s[a-z\'-]{3,})
    \s
    \(?(?P<authority>([A-Z]\.\s)*[a-zA-Z,\-\. ]+)(,\s\d+)?\)?$
""",
    re.VERBOSE,
)


def extract_name_and_author(text: str) -> dict[str, str]:
    match = AUTHOR_NAME_RGX.match(text.replace(" [sic]", ""))
    assert match, f"failed to match {text}"
    authority = (
        match.group("authority")
        .replace(", and ", " & ")
        .replace(" and ", " & ")
        .replace(", in", "")
    )
    authority = re.sub(r"(?<=\.) (?=[A-Z]\.)", "", authority)
    return {"original_name": match.group("name"), "authority": authority}


# Code specific to this extraction task.

FILE_PATH = DATA_DIR / "lawrence1993-layout.txt"
SOURCE = "AMNH-types.pdf"


def extract_pages() -> Iterable[tuple[int, list[str]]]:
    """Split the text into pages."""
    current_page = None
    current_lines = []
    with FILE_PATH.open() as f:
        for line in f:
            if line.startswith("\x0c"):
                if current_page is not None:
                    yield current_page, current_lines
                    current_lines = []
                line = line[1:].strip()
                if "BULLETIN AMERICAN MUSEUM OF NATURAL HISTORY" in line:
                    current_page = int(line.split()[0])
                else:
                    assert "LAWRENCE: MAMMAL TYPES CATALOG" in line, line
                    if line.endswith("ill"):
                        current_page = 111
                    elif line.endswith("16-7"):
                        current_page = 167
                    else:
                        current_page = int(line.split()[-1])
            else:
                current_lines.append(line)
        # last page
        assert current_page is not None
        yield current_page, current_lines


def align_columns() -> Iterable[tuple[int, list[str]]]:
    """Rearrange the text to separate the two columns on each page."""
    for page, lines in extract_pages():
        # find a position that is blank in every line
        max_len = max(len(line) for line in lines)
        best_blank = -1
        for i in range(max_len):
            if not all(len(line) <= i or line[i] == " " for line in lines):
                continue
            num_lines = len([line for line in lines if len(line) > i])
            if num_lines < 10:
                continue
            best_blank = i
        assert best_blank != -1, f"failed to find split for {page}"
        first_column = [line[:best_blank].rstrip() for line in lines]
        second_column = [line[best_blank + 1 :].rstrip() for line in lines]
        yield page, first_column + second_column


def extract_names() -> Iterable[dict[str, Any]]:
    """Extracts names from the text, as dictionaries."""
    current_name: dict[str, Any] | None = None
    current_label: str | None = None
    current_lines: list[str] = []

    def start_label(label: str, line: str) -> None:
        nonlocal current_label, current_lines
        assert current_name is not None
        assert current_label is not None
        assert (
            current_label not in current_name
        ), f"duplicate label {current_label} in {current_name}"
        current_name[current_label] = current_lines
        current_label = label
        current_lines = [line]

    for page, lines in align_columns():
        if current_name is not None:
            current_name["pages"].append(page)
        for line in lines:
            # ignore family/genus headers
            if re.match(
                r"^\s*(Genus|Family|Subfamily|Order) [A-Z]+ [A-Z][a-zA-Z ]+$", line
            ):
                continue
            # ignore blank lines
            if not line:
                continue
            if line.startswith(" "):
                current_lines.append(line)
            elif re.match(r"^[A-Z]+: ", line):
                start_label(line.split()[0][:-1], line)
            elif (
                current_label in ("CONDITION", "COMMENT", "COLLECTOR")
                or current_label is None
            ):
                # new name
                if current_name is not None:
                    yield current_name
                current_name = {"pages": [page]}
                current_label = "name"
                current_lines = [line]
            elif current_label == "name":
                if re.search(
                    r"\d|\b[A-Z][a-z]+\.|\baus\b|\bDas\b|\bPreliminary\b", line
                ):
                    start_label("verbatim_citation", line)
                else:
                    # probably continuation of the author
                    current_lines.append(line)
            elif current_label == "verbatim_citation":
                start_label("synonymy", line)
    assert current_name is not None
    assert current_label is not None
    current_name[current_label] = current_lines
    yield current_name


def clean_text() -> Iterable[dict[str, Any]]:
    """Puts each field into a single line and undoes line breaks within words."""
    for name in extract_names():
        new_name = {}
        for key, value in name.items():
            if key == "pages":
                new_name[key] = value
            else:
                text = "\n".join(value)
                text = re.sub(r"-\n +", "", text)
                text = re.sub(r"\s+", " ", text)
                if text.startswith(key + ": "):
                    text = text[len(key) + 2 :]
                new_name[key] = text
        yield new_name


# next: change data into a format resembling what we want in the DB.


def extract_data() -> Iterable[dict[str, Any]]:
    """Extract raw data into a format closer to what the DB wants."""
    for name in clean_text():
        name["raw_text"] = dict(name)
        if "LOCALITY" in name:
            name["type_locality"] = extract_region(name["LOCALITY"])
            name["date"] = extract_date(name["LOCALITY"])
            name["altitude"] = extract_altitude(name["LOCALITY"])
        if "COLLECTOR" in name:
            name["collector"] = extract_collector(name["COLLECTOR"])
        type_text = None
        for type_key in ("HOLOTYPE", "LECTOTYPE", "NEOTYPE"):
            if type_key in name:
                type_text = name[type_key]
                name["species_type_kind"] = getattr(
                    constants.SpeciesGroupType, type_key.lower()
                )
                name.update(extract_type_specimen(type_text))
        name.update(extract_name_and_author(name["name"]))
        all_tags = []
        if name.get("date"):
            all_tags.append(TypeTag.Date(name["date"]))
        if name.get("altitude"):
            all_tags.append(TypeTag.Altitude(*name["altitude"]))
        if name.get("collector"):
            all_tags.append(TypeTag.Collector(name["collector"]))
        if name.get("age"):
            all_tags.append(TypeTag.Age(name["age"]))
        if name.get("gender"):
            all_tags.append(TypeTag.Gender(name["gender"]))
        if name.get("organs"):
            all_tags += name["organs"]
        if name.get("LOCALITY"):
            all_tags.append(TypeTag.LocationDetail(name["LOCALITY"], SOURCE))
        if type_text is not None:
            if "CONDITION" in name:
                type_text = f'"{type_text}" Condition: "{name["CONDITION"]}"'
            all_tags.append(TypeTag.SpecimenDetail(type_text, SOURCE))
        name["type_tags"] = all_tags
        name["collection"] = models.Collection.by_label("AMNH")
        yield name


def associate_names() -> Iterable[dict[str, Any]]:
    total = 0
    found = 0
    for name in extract_data():
        name_obj = None
        total += 1
        for original_name, authority in name_variants(
            name["original_name"], name["authority"]
        ):
            name_obj = find_name(original_name, authority)
            if name_obj is not None:
                break
        if name_obj:
            found += 1
        else:
            print(f'could not find name {name["original_name"]} {name["authority"]}')
        name["name_obj"] = name_obj
        yield name
    print(f"found: {found}/{total}")


def write_to_db(dry_run: bool = True) -> None:
    name_discrepancies = []
    num_changed: Counter[str] = Counter()
    for name in associate_names():
        nam = name["name_obj"]
        print(f"--- processing {nam} ---")
        pages = "-".join(map(str, name["pages"]))
        for attr in (
            "type_locality",
        ):  # ('type_tags', 'collection', 'type_specimen', 'species_type_kind', 'holotype', 'verbatim_citation', 'original_name', 'type_specimen_source'):
            if attr not in name or name[attr] is None:
                continue
            current_value = getattr(nam, attr)
            new_value = name[attr]
            if current_value == new_value:
                continue
            elif current_value is not None:
                if attr == "verbatim_citation":
                    new_value = f"{current_value} [From {{{SOURCE}}}: {new_value}]"
                else:
                    print(
                        f"value for {attr} differs: (new) {new_value} vs. (current)"
                        f" {current_value}"
                    )
                if attr == "type_tags":
                    new_tags = set(new_value) - set(current_value)
                    existing_types = tuple({type(tag) for tag in current_value})
                    tags_of_new_types = {
                        tag for tag in new_tags if not isinstance(tag, existing_types)
                    }
                    print(f"adding tags: {tags_of_new_types}")
                    if not dry_run:
                        nam.type_tags = sorted(nam.type_tags + tuple(tags_of_new_types))
                    new_tags -= tags_of_new_types
                    if new_tags:
                        print(f"new tags: {new_tags}")
                        if not dry_run:
                            nam.fill_field("type_tags")
                    continue
                elif attr == "original_name":
                    new_root_name = helpers.root_name_of_name(
                        new_value, constants.Rank.species
                    )
                    if (
                        helpers.root_name_of_name(
                            nam.original_name, constants.Rank.species
                        ).lower()
                        != new_root_name.lower()
                    ):
                        try:
                            existing = models.Name.filter(
                                models.Name.original_name == new_value
                            ).get()
                        except models.Name.DoesNotExist:
                            print(f"creating ISS with orig name={new_value}")
                            if not dry_run:
                                nam.open_description()
                                if getinput.yes_no(
                                    "Is the original spelling"
                                    f" {nam.original_name} correct? "
                                ):
                                    nam.add_variant(
                                        new_root_name,
                                        constants.NomenclatureStatus.incorrect_subsequent_spelling,
                                        paper=SOURCE,
                                        page_described=name["pages"][0],
                                        original_name=new_value,
                                    )
                                    continue
                        else:
                            if existing.original_citation == SOURCE:
                                continue
                    name_discrepancies.append((nam, current_value, new_value))
                    continue
            num_changed[attr] += 1
            if not dry_run:
                setattr(nam, attr, new_value)

        if not dry_run:
            nam.add_comment(
                constants.CommentKind.structured_quote,
                json.dumps(name["raw_text"]),
                SOURCE,
                pages,
            )
            nam.save()

    for nam, current, new in name_discrepancies:
        print("----------")
        print(f"discrepancy for {nam}")
        print(f"current: {current}")
        print(f"new: {new}")
        if not dry_run:
            nam.open_description()
            getinput.get_line("press enter to continue> ")

    for attr, value in num_changed.most_common():
        print(f"{attr}: {value}")


if __name__ == "__main__":
    write_to_db(dry_run=False)
