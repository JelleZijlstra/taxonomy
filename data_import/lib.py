import enum
import functools
import itertools
import json
import re
import unicodedata
from collections import Counter, defaultdict
from collections.abc import Callable, Collection, Container, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, NamedTuple

import Levenshtein
import unidecode

from taxonomy import getinput, shell
from taxonomy.db import constants, helpers, models
from taxonomy.db.models import TypeTag

DATA_DIR = Path(__file__).parent / "data"
NAME_SYNONYMS = {
    "Costa 'Rica": "Costa Rica",
    "Bahama Islands": "Bahamas",
    "British Guiana": "Guyana",
    "Burma": "Myanmar",
    "Cape York": "Queensland",
    "Celebes": "Sulawesi",
    "French Congo": "Rep Congo",
    "Fukien": "Fujian",
    "Hainan Island": "Hainan",
    "Irian Jaya": "Western New Guinea",
    "Kazakstan": "Kazakhstan",
    "Matto Grosso do Sul": "Mato Grosso do Sul",
    "Matto Grosso": "Mato Grosso",
    "Netherlands New Guinea": "Western New Guinea",
    "Newfoundland": "Newfoundland and Labrador",
    "Nicaraugua": "Nicaragua",
    "Northwest Territory": "Northwest Territories",
    "Philippine Islands": "Philippines",
    "Russian Federation": "Russia",
    "Shensi": "Shaanxi",
    "Siam": "Thailand",
    "Sulawesi Selatan": "Sulawesi",
    "Timor Island": "West Timor",
    "Vera Cruz": "Veracruz",
    "Zaire": "DR Congo",
    "Baja California [Sur]": "Baja California Sur",
    "Estado de México": "Mexico State",
    "Panama Canal Zone": "Panama",
    "Labrador": "Newfoundland and Labrador",
    "Greater Antilles": "West Indies",
    "Lesser Antilles": "West Indies",
    "Federated States of Micronesia": "Micronesia",
    "Marocco": "Morocco",
    "Tchad": "Chad",
    "New Britain": "Papua New Guinea",
    "Bismarck Archipelago": "Papua New Guinea",
    "Kei Islands": "Kai Islands",
    "North-East New Guinea": "Papua New Guinea",
    "Papua": "New Guinea",
    "Batchian": "Batjan",
    "Ceram": "Seram",
    "Amboina": "Ambon",
    "Banda Islands": "Moluccas",
    "Sicily": "Italy",
    "Malay States": "Peninsular Malaysia",
    "New Ireland": "Papua New Guinea",
    "Admiralty Islands": "Papua New Guinea",
    "Trobriand Islands": "Papua New Guinea",
    "D'Entrecasteaux Archipelago": "Papua New Guinea",
    "Waigeu": "Waigeo",
    "Maldive Islands": "Maldives",
    "U.S.A.": "United States",
    "Tanganyika Territory": "Tanzania",
    "Tanganyika Territoiy": "Tanzania",
    "Anglo-Egyptian Sudan": "Africa",
    "Malagasy Republic": "Madagascar",
    "British East Africa": "Kenya",
    "Belgian Congo": "DR Congo",
    "Nyasaland": "Malawi",
    "Cameroun": "Cameroon",
    "Cameroons": "Cameroon",
    "Asia Minor": "Turkey",
    "British Honduras": "Belize",
    "Anglo- Egyptian Sudan": "Africa",
    "Surinam": "Suriname",
    "Chili": "Chile",
    "Brésil": "Brazil",
    "USA": "United States",
    "UK": "United Kingdom",
    "Calif": "California",
    "Wyo": "Wyoming",
    "Fla": "Florida",
    "Kenya Colony": "Kenya",
    "British Nigeria": "Africa",
    "Cameroon Mandate": "Cameroon",
    "Dutch New Guinea": "Western New Guinea",
    "French Indo-China": "Southeast Asia",
    "Tonkin": "Vietnam",
    "British West Indies": "West Indies",
    "NSW": "New South Wales",
    "Kenia": "Kenya",
    "former Yugoslavia": "Europe",
    "Republic of the Philippines": "Philippines",
    "Republic of the Congo": "Rep Congo",
    "Malacca": "Peninsular Malaysia",
    "Malay Peninsula": "Peninsular Malaysia",
    "Selangor": "Peninsular Malaysia",
    "Perak": "Peninsular Malaysia",
    "Pahang": "Peninsular Malaysia",
    "Tioman": "Peninsular Malaysia",
    "Penang": "Peninsular Malaysia",
    "East Perhentian": "Peninsular Malaysia",
    "Great Redang": "Peninsular Malaysia",
    "Aor": "Peninsular Malaysia",
    "Tenasserim": "Myanmar",
    "Pemanggil": "Peninsular Malaysia",
    "Terutau": "Thailand",  # Ko Tarutao
    "British North Borneo": "Sabah",
    "Lingga": "Riau Islands",
    "Banka": "Bangka-Belitung",
    "Billiton": "Bangka-Belitung",
    "Bintang": "Riau Islands",
    "Bunguran": "Riau Islands",
    "Subi": "Riau Islands",
    "Sinkep": "Riau Islands",
    "Batam": "Riau Islands",
    "Mapor": "Riau Islands",
    "Karimon": "Riau Islands",
    "Riabu": "Riau Islands",
    "Jimaja": "Riau Islands",
    "Singkep": "Riau Islands",
    "Kundur": "Riau Islands",
    "Sugi Bawa": "Riau Islands",
    "Sugi": "Riau Islands",
    "Bulan": "Riau Islands",
    "North Pagi": "North Pagai",
    "Siantan": "Riau Islands",
    "Sirhassen": "Riau Islands",
    "Laut": "Riau Islands",
    "Tana Masa": "Batu Islands",
    "Tana Bala": "Batu Islands",
    "Pinie": "Batu Islands",
    "Panebangan": "Kalimantan",
    "Karimata": "Kalimantan",
    "Lamukotan": "Kalimantan",
    "Tuangku": "Banyak Islands",
    "Bangkaru": "Banyak Islands",
    "Junk Seylon": "Thailand",  # Phuket
    "Koh Pipidon": "Thailand",
    "Banguey": "Sabah",  # Banggi
    "Simalur": "Simeulue",
    "Mansalar": "Sumatra",
    "Engano": "Enggano",
    "Peninsular Siam": "Thailand",
    "Koh Samui": "Thailand",
    "Koh (Island) Samui": "Thailand",
    "Koh Pennan": "Thailand",
    "Koh (Island) Pennan": "Thailand",
    "Telibon": "Thailand",
    "Rawi": "Thailand",
    "Adang": "Thailand",
    "Langkawi": "Peninsular Malaysia",
    "Dayang Bunting": "Peninsular Malaysia",
    "Pegu": "Myanmar",
    "Nicobar Islands": "Andamans and Nicobars",
    "Mergui Archipelago": "Myanmar",
    "Kangean": "Kangean Islands",
    "South Pagi": "South Pagai",
    "Bengal": "South Asia",
    "Babi": "Simeulue",
    "Mallewalle": "Sabah",
    "Balambangan": "Sabah",
    "Sebuko": "Kalimantan",
    "Maratua": "Kalimantan",
    "Chombol": "Riau Islands",
    "Jarak": "Peninsular Malaysia",
    "Johore": "Peninsular Malaysia",
    "Rumbia": "Peninsular Malaysia",
    "Tinggi": "Peninsular Malaysia",
    "Lasia": "Simeulue",
    "Datu": "Kalimantan",
    "Pipidon": "Thailand",
    "Mt. Kinabalu": "Sabah",
    "Serutu": "Kalimantan",  # Karimata islands
    "Mata Siri": "Kalimantan",  # Matasiri, Laut Kecil Islands, South Kalimantan
    "Southern Rhodesia": "Zimbabwe",
    "Transvaal": "South Africa",
    "Abyssinia": "Ethiopia",
    "Orange Free State": "South Africa",
    "Cape Colony": "South Africa",
    "Natal": "South Africa",
    "Cape of Good Hope": "South Africa",
    "Southwest Africa": "Namibia",
    "Dahomey": "Benin",
    "Congo Belge": "DR Congo",
    "Bechuanaland": "Botswana",
    "Gaboon": "Gabon",
    "Gold Coast": "Ghana",
    "Somaliland": "Somalia",
    "Lado Enclave": "Africa",  # TODO: it's in Uganda?
    "British Somaliland": "Somalia",
    "Fernando Po": "Bioko",
    "Northern Rhodesia": "Zambia",
    "Kordofan": "Sudan",
    "Island of Fernando Po": "Bioko",
    "AngloEgyptian Sudan": "Sudan",
    "Italian Somaliland": "Somalia",
    "Tripoli": "Libya",
    "Zanzibar": "Tanzania",
    "Sennaar": "Sudan",
    "Tunis": "Tunisia",
    "Spanish Guinea": "Rio Muni",
    "Portuguese Guinea": "Guinea-Bissau",
    "Ivory Coast": "Cote d'Ivoire",
    "Darfur": "Sudan",
    "Cape Province": "South Africa",
    "Shoa": "Ethiopia",
    "French Gambia": "Senegal",
    "Portuguese East Africa": "Mozambique",
    "RDCongo": "DR Congo",
    "Yukon Territory": "Yukon",
    "NWT": "Northwest Territories",
    "Tadzhikistan": "Tajikistan",
    "Makedonia": "Macedonia",
    "Kirgizia": "Kyrgyzstan",
    "Cameron": "Cameroon",
}
REMOVE_PARENS = re.compile(r" \([A-Z][a-z]+\)")

DataT = Iterable[dict[str, Any]]
PagesT = Iterable[tuple[int, list[str]]]


class Source(NamedTuple):
    inputfile: str
    source: str

    def get_source(self) -> models.Article:
        return models.Article.get(name=self.source)


class NameConfig(NamedTuple):
    original_name_fixes: Mapping[str, str] = {}
    authority_fixes: Mapping[str, str] = {}
    ignored_names: Collection[tuple[str, str]] = ()


def initial_count(s: str, char: str) -> int:
    """Return the number of occurrences of char at the beginning of s."""
    count = 0
    for c in s:
        if c == char:
            count += 1
        else:
            break
    return count


def dedent_lines(lines: list[str]) -> list[str]:
    dedent_by = min(
        (initial_count(line, " ") for line in lines if line.rstrip()), default=0
    )
    return [line[dedent_by:] for line in lines]


def get_text(source: Source, encoding: str = "utf-8") -> Iterable[str]:
    with (DATA_DIR / source.inputfile).open(encoding=encoding) as f:
        yield from f


def extract_pages(lines: Iterable[str], *, permissive: bool = False) -> PagesT:
    """Split the text into pages."""
    current_page = None
    current_lines = []
    for line in lines:
        line = line.replace(" ", " ")
        if line.startswith("\x0c"):
            if current_page is not None:
                yield current_page, current_lines
                current_lines = []
            line = line[1:].strip()
            try:
                if re.search(r"^\d+ ", line):
                    # page number on the left
                    current_page = int(line.split()[0])
                else:
                    # or the right
                    current_page = int(line.split()[-1])
            except ValueError as e:
                if permissive:
                    if current_page is not None:
                        current_page += 1
                    else:
                        continue
                else:
                    raise ValueError(
                        f"failure extracting from {line!r} while on {current_page}"
                    ) from e
        elif current_page is not None:
            current_lines.append(line)
    # last page
    assert current_page is not None
    yield current_page, current_lines


def validate_pages(
    pages: PagesT, *, verbose: bool = True, check: bool = True
) -> PagesT:
    current_page: int | None = None
    for page, lines in pages:
        if verbose:
            print(f"got page {page}")
        if current_page is not None:
            if page != current_page + 1:
                message = f"missing {current_page + 1}"
                if check:
                    assert False, message
                else:
                    print("---", message, "---")
        current_page = page
        yield page, lines


class NoSplitFound(Exception):
    pass


def split_lines(
    lines: list[str],
    page: int,
    *,
    single_column_pages: Container[int] = frozenset(),
    use_first: bool = False,
    min_column: int = 0,
    dedent_right: bool = True,
) -> list[str]:
    if not any(line.rstrip() for line in lines):
        return []
    # find a position that is blank in every line
    max_len = max(len(line) for line in lines)
    possible_splits = []
    for i in range(min_column, max_len):
        if not all(len(line) <= i or line[i] == " " for line in lines):
            continue
        num_lines = len([line for line in lines if len(line) > i])
        if num_lines < 5:
            continue
        possible_splits.append(i)
    if not possible_splits:
        if page in single_column_pages:
            return [line.rstrip() for line in lines]
        else:
            raise NoSplitFound(f"failed to find split for {page}")
    else:
        if use_first:
            best_blank = min(possible_splits)
        else:
            best_blank = max(possible_splits)
        first_column = [line[:best_blank].rstrip() for line in lines]
        second_column = [line[best_blank + 1 :].rstrip() for line in lines]
        if dedent_right:
            num_lines = len(second_column)
            while (
                len([line for line in second_column if line.startswith(" ")])
                > num_lines / 2
            ):
                second_column = [
                    line[1:] if line.startswith(" ") else line for line in second_column
                ]
        return first_column + second_column


def align_columns(
    pages: PagesT,
    *,
    single_column_pages: Container[int] = frozenset(),
    use_first: bool = False,
    min_column: int = 0,
    dedent_right: bool = True,
) -> PagesT:
    """Rearrange the text to separate the two columns on each page."""
    for page, lines in pages:
        lines = split_lines(
            lines,
            page,
            single_column_pages=single_column_pages,
            use_first=use_first,
            min_column=min_column,
            dedent_right=dedent_right,
        )
        if not lines:
            continue
        yield page, lines


def clean_text(names: DataT, *, clean_labels: bool = True) -> DataT:
    """Puts each field into a single line and undoes line breaks within words."""
    for name in names:
        yield clean_text_dict(name, clean_labels=clean_labels)


def clean_text_dict(
    name: dict[str, Any], *, clean_labels: bool = True
) -> dict[str, Any]:
    new_name = {}
    for key, value in name.items():
        if key == "pages" or not isinstance(value, list):
            if isinstance(value, str):
                value = clean_string(value)
            elif isinstance(value, dict):
                value = clean_text_dict(value, clean_labels=clean_labels)
            new_name[key] = value
        elif key == "names":
            new_name[key] = [clean_line_list(name) for name in value]
        else:
            text = clean_line_list(value)
            if clean_labels and isinstance(key, str):
                text = re.sub(r"^\s*" + re.escape(key) + r"[\-—:\. ]+", "", text)
            new_name[key] = text.strip()
    return new_name


def clean_line_list(lines: Iterable[str]) -> str:
    text = "\n".join(lines)
    return clean_string(text)


def clean_string(text: str) -> str:
    text = unicodedata.normalize("NFC", text)
    text = text.replace(" \xad ", "")
    text = text.replace("\xad", "")
    text = text.replace("’", "'")
    text = re.sub(r"[“”]", '"', text)
    text = re.sub(r"- *\n+ *", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_text_simple(names: DataT) -> DataT:
    for name in names:
        new_name = {"pages": name["pages"]}
        for key, value in name.items():
            if key == "pages":
                continue
            if key == "lines":
                new_name["text"] = " ".join(line.strip() for line in value)
            else:
                new_name[key] = " ".join(line.strip() for line in value)
        yield new_name


def split_name_authority(
    name_authority: str, *, try_harder: bool = False, quiet: bool = False
) -> dict[str, str]:
    name_authority = re.sub(
        r"([A-Za-z][a-z]*)\[([a-z?]+( \([A-Z][a-z]+\))?)\]\.", r"\1\2", name_authority
    )
    name_authority = re.sub(r"([A-Z][a-z]*)\[([a-z]+)\]", r"\1\2", name_authority)
    name_authority = re.sub(r"^\[([A-Z][a-z]+)\]", r"\1", name_authority)
    name_authority = re.sub(r"\[\([A-Z][a-z]+\)\] ", r"", name_authority)
    name_authority = re.sub(
        r"^\[[A-Z][a-z]+ \(\]([A-Z][a-z]+)\[\)\]", r"\1", name_authority
    )
    name_authority = re.sub(r"\b([a-z])\[([a-z]+)\](?= )", r"\1\2", name_authority)
    name_authority = re.sub(r"^\[([A-Za-z ]+)\]", r"\1", name_authority)
    name_authority = name_authority.replace("(sic)", "").replace("[sic]", "")
    name_authority = re.sub(r"\s+", " ", name_authority)
    regexes = [
        # Lets us manually put | in to separate original name and authority in hard cases
        r"^(?P<original_name>[^\|]+) \| (?P<authority>.*)$",
        (
            r"^(?P<original_name>[A-ZÑ][a-zëöiïü]+)"
            r" (?P<authority>(d\')?[A-ZÁ][a-zA-Z\-âöáéüšñ\.èç]+)$"
        ),
        (
            r"^(?P<original_name>(\? )?[A-ZÑ][a-zëöiïü]+\??("
            r" \([A-Z][a-z]+\.?\??\))?((,? var\.| \(\?\)| \?)?"
            r" [a-z]{3,}(-[a-z]{3,})?){1,2}) "
            r"(?P<authority>de Beaux|de Blainville|de Winton|de Beerst|von Bloeker|de"
            r" Selys Longchamps|de Filippi|(d\'|de la )?[A-ZÁ][a-zA-Z\-âöüášéèíñç\.,\'&"
            r" ]+)( \(ex [^\)]+\))?$"
        ),
        r"^(?P<original_name>(\? )?.*?) (?P<authority>[A-ZÉ]\.[\- ].*)$",
        (
            r"^(?P<original_name>(\? )?[A-ZÑ][a-zëöíïü]+) (?P<authority>(d\'|de la"
            r" )?[A-ZÁ][a-zA-Z\-öáéšíñ\., ]+ (and|&) [A-ZÁ][a-zA-Z\-âöüáéèíñç]+)$"
        ),
        r"^(?P<original_name>[A-Z][a-z]+) (?P<authority>Hamilton Smith|Von Dueben)$",
    ]
    if try_harder:
        regexes += [
            r"^(?P<original_name>[a-z]+) (?P<authority>[A-Z].*? and [A-Z].*)$",
            r"^(?P<original_name>.* [a-zë\-]+) (?P<authority>[A-ZÁÉ].*)$",
            r"^(?P<original_name>.*) (?P<authority>[^ ]+)$",
        ]
    for rgx in regexes:
        match = re.match(rgx, name_authority)
        if match:
            return match.groupdict()
    if not quiet:
        print(name_authority)
    return {}


def translate_to_db(
    names: DataT,
    collection_name: str | None = None,
    source: Source | None = None,
    *,
    verbose: bool = False,
) -> DataT:
    coll: models.Collection | None
    if collection_name is not None:
        coll = models.Collection.by_label(collection_name)
    else:
        coll = None
    for name in names:
        if "taxon_name" in name and "taxon" not in name:
            try:
                name["taxon"] = models.Taxon.get(
                    models.Taxon.valid_name == name["taxon_name"]
                )
            except models.Taxon.DoesNotExist:
                if verbose:
                    print(f'failed to find taxon {name["taxon_name"]}')
        if (
            "orig_name_author" in name
            and "original_name" not in name
            and "authority" not in name
        ):
            name.update(
                split_name_authority(name["orig_name_author"], quiet=not verbose)
            )
        if "authority" in name:
            name["authority"] = unspace_initials(name["authority"])
        if "authority" in name and " and " in name["authority"]:
            name["authority"] = name["authority"].replace(" and ", " & ")
        if "species_type_kind" in name:
            if coll is not None and "collection" not in name:
                name["collection"] = coll
        type_tags: list[models.TypeTag] = name.get("type_tags", [])
        for field in ("age_gender", "gender_age", "gender"):
            if field in name:
                gender_age = extract_gender_age(name[field])
                type_tags += gender_age
                if verbose and not gender_age:
                    print(f"failed to parse gender age {name[field]!r}")
        if "gender_value" in name:
            type_tags.append(TypeTag.Gender(name["gender_value"]))
        if "body_parts" in name:
            body_parts = extract_body_parts(name["body_parts"])
            if body_parts:
                type_tags += body_parts
            else:
                if verbose:
                    print(f'failed to parse body parts {name["body_parts"]!r}')
                assert source is not None, f"missing source (at {name})"
                type_tags.append(
                    models.TypeTag.SpecimenDetail(
                        name["body_parts"], source.get_source()
                    )
                )
        if "loc" in name:
            text = name["loc"]
            assert source is not None, f"missing source (at {name})"
            type_tags.append(models.TypeTag.LocationDetail(text, source.get_source()))
            coords = helpers.extract_coordinates(text)
            if coords:
                type_tags.append(models.TypeTag.Coordinates(coords[0], coords[1]))
        if "collector" in name:
            type_tags.append(models.TypeTag.Collector(name["collector"]))
        if "altitude" in name:
            match = re.match(r"^(\d[\d\-,]*) +(feet|m|meters)$", name["altitude"])
            if match:
                altitude = match.group(1).replace(",", "")
                unit = (
                    constants.AltitudeUnit.ft
                    if match.group(2) == "feet"
                    else constants.AltitudeUnit.m
                )
                type_tags.append(models.TypeTag.Altitude(altitude, unit))
            elif verbose:
                print(f'failed to match altitude {name["altitude"]!r}')
        if "date" in name:
            date = name["date"]
            try:
                date = helpers.standardize_date(date)
            except ValueError:
                if verbose:
                    print(f"failed to parse date {date!r}")
                if "specimen_detail" in name and date in name["specimen_detail"]:
                    pass  # it will be included elsewhere
                else:
                    assert source is not None, f"missing source (at {name})"
                    type_tags.append(
                        models.TypeTag.SpecimenDetail(
                            f'Collected: "{date}"', source.get_source()
                        )
                    )
            else:
                if date is not None:
                    type_tags.append(models.TypeTag.Date(date))
        if "specimen_detail" in name:
            assert source is not None, f"missing source (at {name})"
            type_tags.append(
                models.TypeTag.SpecimenDetail(
                    name["specimen_detail"], source.get_source()
                )
            )
        if "original_name" in name:
            name["original_name"] = re.sub(
                r"\[([a-z]+)\]\.", r"\1", name["original_name"]
            )
        if "verbatim_type" in name:
            assert source is not None, f"missing source (at {name})"
            type_tags.append(
                models.TypeTag.TypeSpeciesDetail(
                    name["verbatim_type"], source.get_source()
                )
            )

        if type_tags:
            name["type_tags"] = type_tags
        yield name


def translate_type_locality(
    names: DataT, *, start_at_end: bool = False, quiet: bool = False
) -> DataT:
    for name in names:
        if "loc" in name:
            loc = name["loc"]
            loc = loc.replace('"', "").rstrip(".")
            loc = re.sub(r", \d[\.\d –\-NWSE]+$", "", loc)
            loc = re.sub(r", \d[,\d]+ ft$", "", loc)
            loc = re.sub(r"\. Altitude, .*$", "", loc)
            loc = re.sub(r"[ \[]lat\. .*$", "", loc)
            loc = re.sub(r"[\.,;:\[ ]+$", "", loc)
            parts = [[re.sub(r" \([^\(]+\)$", "", part)] for part in loc.split(", ")]
            if start_at_end:
                parts.reverse()
            type_loc = extract_region(parts)
            if type_loc is not None:
                name["type_locality"] = type_loc
            elif not quiet:
                print("could not extract type locality from", loc)
        yield name


AUTHOR_NAME_RGX = re.compile(
    r"""
    (?P<name>[A-Z][a-z]+(\s\([A-Z][a-z]+\??\))?(\s\([a-z]+\??\))?(\s[a-z\'-]{3,})?(\svar\.)?\s[a-z\'-]{3,})
    \s
    \(?(?P<authority>([A-Z]\.\s)*[a-zA-Z,\-\. ]+)(,\s\d+)?\)?$
""",
    re.VERBOSE,
)


def extract_name_and_author(text: str) -> dict[str, str]:
    if text == "Sus oi Miller":
        return {"original_name": "Sus oi", "authority": "Miller"}
    text = re.sub(r" \[sic\.?\]", "", text)
    text = re.sub(r"\[([A-Za-z]+)\]\.?", r"\1", text)
    text = text.replace("\xad", "").replace("œ", "oe").replace("æ", "ae")
    match = AUTHOR_NAME_RGX.match(text)
    assert match, f"failed to match {text!r}"
    authority = (
        match.group("authority")
        .replace(", and ", " & ")
        .replace(" and ", " & ")
        .replace(", in", "")
    )
    authority = re.sub(r"(?<=\.) (?=[A-Z]\.)", "", authority)
    return {"original_name": match.group("name"), "authority": authority}


def enum_has_member(enum_cls: type[enum.Enum], member: str) -> bool:
    try:
        enum_cls[member]
    except KeyError:
        return False
    else:
        return True


def extract_species_type_kind(text: str) -> constants.SpeciesGroupType | None:
    text = text.lower()
    if enum_has_member(constants.SpeciesGroupType, text):
        return constants.SpeciesGroupType[text]
    elif text == "syntype":
        return constants.SpeciesGroupType.syntypes
    elif text in ("paralectotype", "paralectotypes"):
        # If there is a paralectotype, there must also be a lectotype
        return constants.SpeciesGroupType.lectotype
    else:
        return None


def extract_gender_age(text: str) -> list[TypeTag]:
    text = re.sub(r"\[.*?: ([^\]]+)\]", r"\1", text)
    text = text.strip().lower().replace("macho", "male").replace("hembra", "female")
    out = []
    if enum_has_member(constants.SpecimenAge, text):
        out.append(TypeTag.Age(constants.SpecimenAge[text]))
    elif enum_has_member(constants.SpecimenGender, text):
        out.append(TypeTag.Gender(constants.SpecimenGender[text]))
    elif text in (
        "unsexed adult",
        "adult, sex not given",
        "adult, sex unknown",
        "adult unsexed",
        "adult (not sexed)",
    ):
        out.append(TypeTag.Age(constants.SpecimenAge.adult))
        out.append(TypeTag.Gender(constants.SpecimenGender.unknown))
    elif text.endswith(", age not given"):
        gender = text.split(",")[0]
        if enum_has_member(constants.SpecimenGender, gender):
            out.append(TypeTag.Gender(constants.SpecimenGender[gender]))
    elif " " in text:
        age, gender = text.rsplit(maxsplit=1)
        if enum_has_member(constants.SpecimenAge, age):
            out.append(TypeTag.Age(constants.SpecimenAge[age]))
        elif age in ("immature", "young"):
            out.append(TypeTag.Age(constants.SpecimenAge.juvenile))
        elif age == "young adult":
            out.append(TypeTag.Age(constants.SpecimenAge.subadult))
        elif age in ("old", "old adult", "aged"):
            out.append(TypeTag.Age(constants.SpecimenAge.adult))
        if enum_has_member(constants.SpecimenGender, gender):
            out.append(TypeTag.Gender(constants.SpecimenGender[gender]))
    elif text == "f":
        out.append(TypeTag.Gender(constants.SpecimenGender.female))
    elif text == "m":
        out.append(TypeTag.Gender(constants.SpecimenGender.male))
    return out


SKIN = TypeTag.Organ(constants.SpecimenOrgan.skin, "", "")
SKULL = TypeTag.Organ(constants.SpecimenOrgan.skull, "", "")
IN_ALCOHOL = TypeTag.Organ(constants.SpecimenOrgan.in_alcohol, "", "")
SKELETON = TypeTag.Organ(constants.SpecimenOrgan.postcranial_skeleton, "", "")


def extract_body_parts(organs: str) -> list[TypeTag]:
    organs = organs.lower().replace("[", "").replace("]", "")
    organs = re.sub(r"sk..?ll", "skull", organs).replace("skufl", "skull").strip()
    if (
        organs
        in (
            "skin and skull",
            "skin and cranium",
            "study skin and skull",
            "skull and skin",
            "mounted skin and skull",
            "skull and head skin",
            "mounted skin, skull separate",
            "tanned (flat) skin and skull",
        )
        or "(skin and skull)" in organs
    ):
        tags = [SKIN, SKULL]
    elif organs == "skin and skeleton":
        tags = [SKIN, SKULL, SKELETON]
    elif organs.startswith("skin, skull,"):
        tags = [SKIN, SKULL]
        if "skeleton" in organs:
            tags.append(SKELETON)
        elif "in alcohol" in organs:
            tags.append(IN_ALCOHOL)
    elif "in alcohol" in organs or "alcoholic" in organs or "in spirits" in organs:
        tags = [IN_ALCOHOL]
        if "skull" in organs:
            tags.append(SKULL)
    elif (
        organs.startswith(("skull only", "cranium only"))
        or organs == "skull without skin"
    ):
        tags = [SKULL]
    elif organs.startswith("skin only") or organs == "skin without skull":
        tags = [SKIN]
    elif organs in ("skin", "mounted skin, skull inside"):
        tags = [SKIN]
    elif organs in ("skull", "cranium"):
        tags = [SKULL]
    elif organs == "skull and postcranial skeleton":
        tags = [SKULL, SKELETON]
    elif "mandible" in organs or "ramus" in organs:
        tags = [TypeTag.Organ(constants.SpecimenOrgan.mandible, organs, "")]
    else:
        tags = []
    if organs.startswith("Шкура"):
        tags.append(SKIN)
    if "череп " in organs or "череп:" in organs or organs.startswith("Череп"):
        tags.append(SKULL)
    return tags


def get_possible_names(names: Iterable[str]) -> Iterable[str]:
    for name in names:
        yield name
        yield NAME_SYNONYMS.get(name, name)
        if name.endswith(" Island"):
            fixed = name[: -len(" Island")]
            yield fixed
            yield NAME_SYNONYMS.get(fixed, fixed)
        without_direction = re.sub(
            (
                r"^(North|South|West|East|NE|SE|NW|SW|Republic"
                r" of|Central|Middle)-?(west|east)?(ern)? (central )?"
            ),
            "",
            name,
            flags=re.IGNORECASE,
        )
        if without_direction != name:
            yield without_direction
            yield NAME_SYNONYMS.get(without_direction, without_direction)
        without_diacritics = unidecode.unidecode(name)
        if name != without_diacritics:
            yield without_diacritics


def get_region_from_name(raw_names: Sequence[str]) -> models.Region | None:
    for name in get_possible_names(raw_names):
        name = NAME_SYNONYMS.get(name, name)
        try:
            return models.Region.get(models.Region.name == name)
        except models.Region.DoesNotExist:
            pass
    return None


def extract_region(components: Sequence[Sequence[str]]) -> models.Location | None:
    possible_region = get_region_from_name(components[0])
    if possible_region is None:
        # print(f'could not extract region from {components}')
        return None
    region = possible_region
    if len(components) > 1 and region.children.count() > 0:
        for name in get_possible_names(components[1]):
            name = NAME_SYNONYMS.get(name, name)
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


def genus_name_of_name(name: models.Name) -> str | None:
    try:
        return name.taxon.parent_of_rank(constants.Rank.genus).valid_name
    except ValueError:
        return None


def genus_of_name(name: models.Name) -> models.Taxon | None:
    try:
        return name.taxon.parent_of_rank(constants.Rank.genus)
    except ValueError:
        return None


def find_name(
    original_name: str, authority: str, max_distance: int = 3, year: str | None = None
) -> models.Name | None:
    # Exact match
    query = models.Name.filter(
        models.Name.original_name == original_name, models.Name.authority == authority
    )
    if year:
        query = query.filter(models.Name.year == year)
    try:
        return query.get()
    except models.Name.DoesNotExist:
        pass

    if original_name.islower() and " " not in original_name:
        candidates = models.Name.filter(
            models.Name.root_name == original_name, models.Name.authority == authority
        )
        if year:
            candidates = candidates.filter(models.Name.year == year)
        count = candidates.count()
        if count == 1:
            return candidates.get()
        elif count > 1:
            available_names = candidates.filter(
                models.Name.nomenclature_status
                == constants.NomenclatureStatus.available
            )
            if available_names.count() == 1:
                return available_names.get()

    # Names without original names, but in the same genus or subgenus
    root_name = original_name.split()[-1]
    genus_name = helpers.genus_name_of_name(original_name)
    possible_genus_names = [genus_name]
    # try subgenus
    match = re.search(r"\(([A-Z][a-z]+)\)", original_name)
    if match:
        possible_genus_names.append(match.group(1))
    all_names = models.Name.filter(
        models.Name.root_name == root_name, models.Name.authority == authority
    )
    if year:
        all_names = all_names.filter(models.Name.year == year)
    for genus in possible_genus_names:
        names = [name for name in all_names if genus_name_of_name(name) == genus]
        if len(names) == 1:
            return names[0]

    # If the genus name is a synonym, try its valid equivalent.
    genus_nams = list(
        models.Name.filter(
            models.Name.group == constants.Group.genus,
            models.Name.root_name == genus_name,
        )
    )
    if len(genus_nams) == 1:
        txn = genus_nams[0].taxon.parent_of_rank(constants.Rank.genus)
        names = [name for name in all_names if genus_of_name(name) == txn]
        if len(names) == 1:
            return names[0]
    # Fuzzy match on original name
    candidates = models.Name.filter(
        models.Name.original_name != None, models.Name.authority == authority
    )
    if year:
        candidates = candidates.filter(models.Name.year == year)
    matches = [
        name
        for name in candidates
        if Levenshtein.distance(original_name, name.original_name) < max_distance
        or REMOVE_PARENS.sub("", original_name)
        == REMOVE_PARENS.sub("", name.original_name)
    ]
    if len(matches) == 1:
        return matches[0]

    # Find names without an original name in similar genera.
    name_genus_pairs, genus_to_orig_genera = build_original_name_map(
        root_name, authority, year=year
    )
    matches = []
    for nam, genus in name_genus_pairs:
        if genus_name in genus_to_orig_genera[genus]:
            matches.append(nam)
    if len(matches) == 1:
        return matches[0]
    return None


@functools.lru_cache(maxsize=1024)
def build_original_name_map(
    root_name: str, authority: str, year: str | None = None
) -> tuple[list[tuple[models.Name, models.Taxon]], dict[models.Taxon, set[str]]]:
    nams: list[tuple[models.Name, models.Taxon]] = []
    genus_to_orig_genera: dict[models.Taxon, set[str]] = {}
    query = models.Name.filter(
        models.Name.group == constants.Group.species,
        models.Name.original_name >> None,
        models.Name.root_name == root_name,
        models.Name.authority == authority,
    )
    if year:
        query = query.filter(models.Name.year == year)
    for nam in query:
        try:
            genus = nam.taxon.parent_of_rank(constants.Rank.genus)
        except ValueError:
            continue
        nams.append((nam, genus))
        if genus not in genus_to_orig_genera:
            genus_to_orig_genera[genus] = get_original_genera_of_genus(genus)
    return nams, genus_to_orig_genera


@functools.lru_cache(maxsize=1024)
def get_original_genera_of_genus(genus: models.Taxon) -> set[str]:
    return {
        helpers.genus_name_of_name(nam.original_name)
        for nam in genus.all_names()
        if nam.group == constants.Group.species and nam.original_name is not None
    }


def unspace_initials(authority: str) -> str:
    return re.sub(r"([A-Z]\.) (?=[A-Z]\.)", r"\1", authority).strip()


def name_variants(original_name: str, authority: str) -> Iterable[tuple[str, str]]:
    authority = authority.replace(" and ", " & ")
    yield original_name, authority
    original_authority = authority
    if "œ" in original_name:
        original_name = original_name.replace("œ", "oe")
        yield original_name, authority
    if "æ" in original_name:
        original_name = original_name.replace("æ", "ae")
        yield original_name, authority
    unspaced = unspace_initials(authority)
    if original_authority != unspaced:
        yield original_name, unspaced
    if authority.endswith("f"):
        yield original_name, authority[:-1]
    authority = re.sub(r"([A-ZÉ]\.)+ ", "", authority).strip()
    if authority != original_authority:
        yield original_name, authority
    if " in " in authority:
        authority = re.sub(r",? in .*$", "", authority)
        yield original_name, authority
        yield original_name, re.sub(r"^.* in ", "", original_authority)
    if authority == "Hill":
        yield original_name, "J. Eric Hill"
    if "ue" in original_name:
        yield original_name.replace("ue", "ü"), authority
    if authority == "Schwartz":
        yield original_name, "Schwarz"
    if authority == "Linné":
        yield original_name, "Linnaeus"
    if authority == "Mjoberg":
        yield original_name, "Mjöberg"
    if authority == "Forster":
        yield original_name, "Förster"
    if authority == "Forster & Rothschild":
        yield original_name, "Förster & Rothschild"
    if authority == "Rummler":
        yield original_name, "Rümmler"
    if authority in ("Müller & Schlegel", "Schlegel & Müller"):
        # many names that were previously attributed to M & S were earlier described by M alone
        yield original_name, "Müller"
    parts = re.split(r", | & ", authority)
    initials_map = get_initials_map()
    options = [initials_map.get(name, set()) | {name} for name in parts]
    for authors_list in itertools.product(*options):
        yield original_name, helpers.unsplit_authors(authors_list)


@functools.lru_cache
def get_initials_map() -> dict[str, set[str]]:
    result: dict[str, set[str]] = defaultdict(set)
    for nam in models.Name.filter(models.Name.authority.contains(". ")):
        for author in nam.get_authors():
            if ". " in author:
                uninitialed = re.sub(r"([A-ZÉ]\.)+ ", "", author).strip()
                result[uninitialed].add(author)
    return dict(result)


def associate_types(
    names: DataT, *, name_config: NameConfig = NameConfig(), quiet: bool = False
) -> DataT:
    success = tried = 0
    for name in names:
        if "type_name" in name and "type_authority" in name:
            tried += 1
            typ = identify_name(
                name["type_name"], name["type_authority"], name_config, quiet=quiet
            )
            if typ:
                success += 1
                name["type"] = typ
        yield name
    print(f"types success: {success}/{tried}")


def associate_variants(
    names: DataT, *, name_config: NameConfig = NameConfig(), quiet: bool = False
) -> DataT:
    success = tried = 0
    for name in names:
        if "variant_name" in name and "variant_authority" in name:
            tried += 1
            typ = identify_name(
                name["variant_name"],
                name["variant_authority"],
                name_config,
                quiet=quiet,
            )
            if typ:
                success += 1
                name["variant_target"] = typ
        yield name
    print(f"variants success: {success}/{tried}")


def fix_author(author: str, name_config: NameConfig) -> str:
    author = name_config.authority_fixes.get(author, author)
    author = author.replace(" and ", " & ").replace(", & ", " & ")
    authors = re.split(r", | & ", author)
    authors = [name_config.authority_fixes.get(author, author) for author in authors]
    if len(authors) > 1:
        return " & ".join([", ".join(authors[:-1]), authors[-1]])
    else:
        return authors[0]


def identify_name(
    orig_name: str,
    author: str,
    name_config: NameConfig = NameConfig(),
    *,
    quiet: bool = False,
    max_distance: int = 3,
    use_taxon_match: bool = False,
    year: str | None = None,
) -> models.Name | None:
    author = author.replace(" and ", " & ").replace(", & ", " & ")
    if (orig_name, author) in name_config.ignored_names:
        return None
    if use_taxon_match:
        taxon_matches = models.Taxon.filter(models.Taxon.valid_name == orig_name)
        if taxon_matches.count() == 1:
            return taxon_matches.get().base_name

    name_obj = None
    author = fix_author(author, name_config)
    orig_name = name_config.original_name_fixes.get(orig_name, orig_name)

    for original_name, authority in name_variants(orig_name, author.strip()):
        name_obj = find_name(
            original_name, authority, max_distance=max_distance, year=year
        )
        if name_obj is not None:
            break
    if name_obj:
        return name_obj
    else:
        if not quiet and (orig_name, author) not in name_config.ignored_names:
            print(f"--- finding name {orig_name} -- {author}")
            print(
                f"could not find name {orig_name} -- {author} (tried variants"
                f" {list(name_variants(orig_name, author))})"
            )
        return None


def manually_associate_name(name: dict[str, Any]) -> models.Name | None:
    print(name["raw_text"])

    taxon_getter = models.Taxon.getter("valid_name")
    while True:
        valid_name = taxon_getter.get_one_key("valid_name> ")
        if not valid_name:
            break
        try:
            txn = models.Taxon.get(models.Taxon.valid_name == valid_name)
        except models.Taxon.DoesNotExist:
            print(f"invalid name {valid_name!r}")
            continue
        else:
            txn.display()
            if getinput.yes_no("Is this correct?"):
                return txn.base_name

    name_getter = models.Name.getter("original_name")
    while True:
        original_name = name_getter.get_one_key("original_name> ")
        if not original_name:
            break
        try:
            name_obj = models.Name.get(models.Name.original_name == original_name)
        except models.Name.DoesNotExist:
            print(f"invalid name {original_name!r}")
            continue
        else:
            name_obj.display()
            if getinput.yes_no("Is this correct?"):
                return name_obj
    return None


def associate_names(
    names: DataT,
    name_config: NameConfig = NameConfig(),
    *,
    start_at: str | None = None,
    name_field: str = "original_name",
    quiet: bool = False,
    try_manual: bool = False,
    max_distance: int = 3,
    use_taxon_match: bool = False,
    match_year: bool = False,
) -> DataT:
    total = 0
    found = 0
    ignored = 0
    found_first = start_at is None
    for name in names:
        if not found_first:
            if name_field in name and name[name_field] == start_at:
                found_first = True
            else:
                continue
        total += 1
        if name_field in name and "authority" in name:
            name["authority"] = fix_author(name["authority"], name_config)
            name_quiet = "variant_target" in name or "name_quiet" in name
            name_obj = identify_name(
                name[name_field],
                name["authority"],
                name_config,
                quiet=quiet or name_quiet,
                max_distance=max_distance,
                use_taxon_match=use_taxon_match,
                year=name["year"] if match_year else None,
            )
            if name_obj:
                found += 1
                name["name_obj"] = name_obj
            elif name_quiet:
                ignored += 1
            elif (name[name_field], name["authority"]) not in name_config.ignored_names:
                if try_manual:
                    name_obj = manually_associate_name(name)
                    if name_obj is not None:
                        name["name_obj"] = name_obj
                    else:
                        del shell.ns["nam"]
                        print(
                            '== Starting a shell; set the name in the variable "nam" =='
                        )
                        shell.run_shell()
                        if "nam" in shell.ns:
                            print(f'Using name {shell.ns["nam"]}')
                            name["name_obj"] = shell.ns["nam"]
                if not quiet and "name_obj" not in name:
                    if "cyrillic_authority" in name:
                        print(name["cyrillic_authority"])
                    else:
                        print(name["raw_text"])
                # for key, value in name.items():
                #     print(f'{key}: {value!r}')
        yield name
    print(f"found: {found + ignored}/{total} (ignored: {ignored})")


def maybe_add_iss(name: dict[str, Any]) -> models.Name | None:
    if "variant_target" not in name:
        return None
    root_name = helpers.root_name_of_name(name["original_name"], constants.Rank.species)
    year = re.sub(r"[a-z]", "", name["year"])
    nam = name["variant_target"].add_variant(
        root_name, name["variant_kind"], interactive=False
    )
    nam.authority = name["authority"]
    nam.year = year
    print("added incorrect subsequent spelling")
    nam.display()
    return nam


def write_to_db(
    names: DataT,
    source: Source,
    *,
    dry_run: bool = True,
    edit_if_no_holotype: bool = True,
    edit_if: Callable[[dict[str, Any]], bool] = lambda _: False,
    always_edit: bool = False,
    skip_fields: Container[str] = frozenset(),
) -> DataT:
    num_changed: Counter[str] = Counter()
    for i, name in enumerate(names):
        if "name_obj" not in name:
            if dry_run:
                continue
            else:
                new_name = maybe_add_iss(name)
                if new_name:
                    name["name_obj"] = new_name
                else:
                    continue
        nam = name["name_obj"]
        if "pages" not in name:
            pages = ""
        elif len(name["pages"]) == 1:
            pages = str(name["pages"][0])
        else:
            pages = f'{name["pages"][0]}-{name["pages"][-1]}'

        print(f"--- processing {nam} (i={i}; p. {pages}) ---")
        yield name

        if "variant_target" in name:
            if nam.nomenclature_status != name["variant_kind"] and not dry_run:
                comment = f"See {{{source.source}}} p. {pages}"
                if nam.nomenclature_status == constants.NomenclatureStatus.available:
                    nam.make_variant(
                        name["variant_kind"], name["variant_target"], comment
                    )
                else:
                    nam.add_tag(
                        models.name.name.CONSTRUCTABLE_STATUS_TO_TAG[
                            name["variant_kind"]
                        ](name=name["variant_target"], comment=comment)
                    )

        for attr in (
            "type_tags",
            "type_locality",
            "collection",
            "type_specimen",
            "species_type_kind",
            "verbatim_citation",
            "original_name",
            "type",
            "nomenclature_status",
            "genus_type_kind",
            "page_described",
            "authority",
            "year",
            "taxon",
        ):
            if attr not in name or name[attr] is None:
                continue
            current_value = getattr(nam, attr)
            new_value = name[attr]
            if current_value == new_value:
                continue
            elif current_value is not None:
                if attr == "type_locality":
                    # if the new TL is a parent of the current, ignore it
                    if new_value.region in current_value.region.all_parents():
                        continue
                if attr == "type_tags":
                    new_tags = set(new_value) - set(current_value)
                    existing_types = tuple({type(tag) for tag in current_value})
                    tags_of_new_types = {
                        tag
                        for tag in new_tags
                        # Always add LocationDetail tags, because it has a source field and it's OK to have multiple tags
                        if (not isinstance(tag, existing_types))
                        or isinstance(
                            tag, (TypeTag.LocationDetail, TypeTag.SpecimenDetail)
                        )
                    }
                    if not tags_of_new_types:
                        continue
                    print(f"adding tags: {tags_of_new_types}")
                    if not dry_run:
                        nam.type_tags = sorted(nam.type_tags + tuple(tags_of_new_types))
                    new_tags -= tags_of_new_types
                    if new_tags:
                        print(f"new tags: {new_tags}")
                        if not dry_run:
                            nam.fill_field("type_tags")
                    continue
                elif attr == "authority":
                    # If the names just differ in the addition of initials, ignore the difference. But don't do this if both
                    # have different initials; then we should review manually.
                    new_no_initials = re.sub(r"^([A-Z]\.)+ ", "", new_value)
                    if new_no_initials == current_value:
                        continue
                    current_no_initials = re.sub(r"^([A-Z]\.)+ ", "", current_value)
                    if current_no_initials == new_value:
                        continue

                    # don't ask (this is for northamerica.py)
                    if new_value == current_value + "f":
                        continue

                if attr == "taxon":
                    if new_value.is_child_of(
                        current_value
                    ) or current_value.is_child_of(new_value):
                        continue

                if attr == "verbatim_citation":
                    if new_value in current_value:
                        continue
                    new_value = (
                        f"{current_value} [From {{{source.source}}}: {new_value}]"
                    )
                else:
                    print("---")
                    print(
                        f"value for {attr} differs: (new) {new_value} vs. (current)"
                        f" {current_value}"
                    )

                if attr == "original_name":
                    if new_value == nam.corrected_original_name:
                        continue
                    new_root_name = helpers.root_name_of_name(
                        new_value, constants.Rank.species
                    )
                    if (
                        helpers.root_name_of_name(
                            nam.original_name, constants.Rank.species
                        ).lower()
                        != new_root_name.lower()
                    ):
                        if not dry_run and not getinput.yes_no(
                            f"Is the source's spelling {new_value} correct?"
                        ):
                            continue
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
                                    if "pages" in name:
                                        page_described = name["pages"][0]
                                    else:
                                        page_described = None
                                    nam.add_variant(
                                        new_root_name,
                                        constants.NomenclatureStatus.incorrect_subsequent_spelling,
                                        paper=source.get_source(),
                                        page_described=page_described,
                                        original_name=new_value,
                                    )
                                    continue
                        else:
                            if existing.original_citation == source.get_source():
                                continue

                if attr != "verbatim_citation" and not (
                    attr == "nomenclature_status"
                    and current_value == constants.NomenclatureStatus.available
                ):
                    if not dry_run:
                        nam.display()
                        nam.open_description()
                        nam.fill_field(attr)
                    continue
            elif attr == "verbatim_citation":
                new_value = f"{new_value} [from {{{source.source}}}]"
            num_changed[attr] += 1
            if not dry_run:
                setattr(nam, attr, new_value)

        if not dry_run:
            should_edit = False
            if always_edit:
                should_edit = True
            if edit_if(name):
                should_edit = True

            if (
                not should_edit
                and edit_if_no_holotype
                and (
                    "species_type_kind" not in name
                    or "type_specimen" not in name
                    or name["species_type_kind"] != constants.SpeciesGroupType.holotype
                )
            ):
                print(f"{nam} does not have a holotype")
                for key, value in sorted(name.items()):
                    print(f"-- {key} -- ")
                    print(value)
                should_edit = True

            if should_edit:
                nam.display()
                empty_fields = list(nam.get_empty_required_fields())
                if empty_fields:
                    nam.fill_required_fields(skip_fields=skip_fields)
                else:
                    nam.fill_field("type_tags")

            if (
                nam.comments.filter(
                    models.NameComment.source == source.get_source()
                ).count()
                == 0
            ):
                nam.add_comment(
                    constants.CommentKind.structured_quote,
                    json.dumps(name["raw_text"]),
                    source.get_source(),
                    pages,
                )

    for attr, value in num_changed.most_common():
        print(f"{attr}: {value}")


def print_counts(names: DataT, field: str) -> None:
    counts: Counter[Any] = Counter(name[field] for name in names if field in name)
    for value, count in counts.most_common():
        print(count, value)


def print_counts_if_no_tag(names: DataT, field: str, tag_cls: TypeTag) -> None:
    counts: Counter[Any] = Counter()
    for name in names:
        if field in name and (
            "type_tags" not in name
            or not any(isinstance(tag, tag_cls) for tag in name["type_tags"])
        ):
            counts[name[field]] += 1
    for value, count in counts.most_common():
        print(count, value)


def print_field_counts(names: DataT) -> None:
    counts: Counter[str] = Counter()
    for name in names:
        for field, value in name.items():
            counts[field] += 1
            if field == "type_tags":
                tags = sorted({type(tag).__name__ for tag in value})
                for tag in tags:
                    counts[tag] += 1

    for value, count in counts.most_common():
        print(count, value)


def print_if_missing_field(names: DataT, field: str) -> DataT:
    for name in names:
        if field not in name:
            print(name)
        yield name


def get_type_specimens(*colls: models.Collection) -> dict[str, list[models.Name]]:
    multiple = models.Collection.getter("label")("multiple")
    assert multiple is not None
    output = defaultdict(list)
    for coll in colls:
        for nam in coll.type_specimens:
            if nam.type_specimen is None:
                continue
            for spec in models.name.type_specimen.parse_type_specimen(
                nam.type_specimen
            ):
                if isinstance(spec, models.name.type_specimen.Specimen):
                    output[spec.base.stringify()].append(nam)
        for nam in coll.get_derived_field("former_specimens") or ():
            if nam.type_specimen is None:
                continue
            for spec in models.name.type_specimen.parse_type_specimen(
                nam.type_specimen
            ):
                if isinstance(spec, models.name.type_specimen.SpecimenRange):
                    continue
                for former_spec in spec.former_texts:
                    if (
                        not isinstance(
                            former_spec,
                            models.name.type_specimen.InformalWithoutInstitution,
                        )
                        and former_spec.institution_code == coll.label
                    ):
                        output[former_spec.stringify()].append(nam)
        for nam in coll.get_derived_field("future_specimens") or ():
            if nam.type_specimen is None:
                continue
            for spec in models.name.type_specimen.parse_type_specimen(
                nam.type_specimen
            ):
                if isinstance(spec, models.name.type_specimen.SpecimenRange):
                    continue
                for future_spec in spec.future_texts:
                    if future_spec.institution_code == coll.label:
                        output[future_spec.stringify()].append(nam)
        for nam in coll.get_derived_field("extra_specimens") or ():
            if nam.type_specimen is None:
                continue
            for spec in models.name.type_specimen.parse_type_specimen(
                nam.type_specimen
            ):
                if isinstance(spec, models.name.type_specimen.SpecimenRange):
                    continue
                for extra_spec in spec.extra_texts:
                    if extra_spec.institution_code == coll.label:
                        output[extra_spec.stringify()].append(nam)
    codes = {coll.label for coll in colls}
    for nam in multiple.type_specimens:
        if nam.type_specimen is None:
            continue
        for spec in models.name.type_specimen.parse_type_specimen(nam.type_specimen):
            if (
                isinstance(spec, models.name.type_specimen.Specimen)
                and spec.base.institution_code in codes
            ):
                output[spec.base.stringify()].append(nam)
    return output
