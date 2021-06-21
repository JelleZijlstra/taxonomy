"""Helper functions"""

from contextlib import contextmanager
import datetime
import json
import re
import time
from typing import Dict, Iterable, Iterator, Mapping, Optional, Sequence, Tuple, TypeVar
import unicodedata
import unidecode

from . import constants
from .constants import Group, Rank

SPECIES_RANKS = [Rank.subspecies, Rank.species, Rank.species_group]
GENUS_RANKS = [Rank.subgenus, Rank.genus]
FAMILY_RANKS = [
    Rank.infratribe,
    Rank.subtribe,
    Rank.tribe,
    Rank.subfamily,
    Rank.family,
    Rank.superfamily,
    Rank.hyperfamily,
]
HIGH_RANKS = [
    Rank.root,
    43,
    Rank.division,
    Rank.parvorder,
    Rank.infraorder,
    Rank.suborder,
    Rank.order,
    Rank.superorder,
    Rank.subcohort,
    Rank.cohort,
    Rank.supercohort,
    Rank.infraclass,
    Rank.subclass,
    Rank.class_,
    Rank.superclass,
    Rank.infraphylum,
    Rank.subphylum,
    Rank.phylum,
    Rank.superphylum,
    Rank.infrakingdom,
    Rank.subkingdom,
    Rank.kingdom,
    Rank.superkingdom,
    Rank.domain,
    Rank.unranked,
]
SUFFIXES = {
    Rank.infratribe: "ita",
    Rank.subtribe: "ina",
    Rank.tribe: "ini",
    Rank.subfamily: "inae",
    Rank.family: "idae",
    Rank.superfamily: "oidea",
    Rank.hyperfamily: "oides",
}
VALID_SUFFIXES = set(SUFFIXES.values())

_RANKS = {
    "root": Rank.root,
    "Unnamed rank": Rank.root,
    "Classis": Rank.class_,
    "Class": Rank.class_,
    "Subclassis": Rank.subclass,
    "Subclass": Rank.subclass,
    "Infraclassis": Rank.infraclass,
    "Infraclass": Rank.infraclass,
    "Superlegion": 89,
    "Legion": 88,
    "Sublegion": 87,
    "Supracohors": Rank.supercohort,
    "Supercohors": Rank.supercohort,
    "Supercohort": Rank.supercohort,
    "Cohors": Rank.cohort,
    "Cohort": Rank.cohort,
    "Subcohors": Rank.subcohort,
    "Magnorder": 72,
    "Grandorder": 71,
    "Superordo": Rank.superorder,
    "Supraordo": Rank.superorder,
    "Superorder": Rank.superorder,
    "Mirorder": 69,
    "Ordo": Rank.order,
    "Order": Rank.order,
    "Subordo": Rank.suborder,
    "Suborder": Rank.suborder,
    "Infraordo": Rank.infraorder,
    "Infraorder": Rank.infraorder,
    "Parvordo": Rank.parvorder,
    "Parvorder": Rank.parvorder,
    "Superfamilia": Rank.superfamily,
    "Suprafamilia": Rank.superfamily,
    "Superfamily": Rank.superfamily,
    "Clade": 43,  # Hack to allow for Eumuroida and Spalacodonta
    "Familia": Rank.family,
    "Family": Rank.family,
    "Subfamilia": Rank.subfamily,
    "Subfamily": Rank.subfamily,
    "Infrafamily": 34,
    "Tribus": Rank.tribe,
    "Tribe": Rank.tribe,
    "Subtribus": Rank.subtribe,
    "Subtribe": Rank.subtribe,
    "Infratribe": Rank.infratribe,
    "Division": Rank.division,
    "Genus": Rank.genus,
    "Subgenus": Rank.subgenus,
}
LATLONG = re.compile(
    r"""
    (?P<latitude>\d+(\.\d+)?\s*[°*]\s*(\d+(\.\d+)?\s*')?\s*[NS])[,\s\[\]]+
    (long\.\s)?(?P<longitude>\d+(\.\d+)?\s*[°*]\s*(\d+(\.\d+)?\s*')?\s*[EW])
""",
    re.VERBOSE,
)


def group_of_rank(rank: Rank) -> Group:
    if rank in SPECIES_RANKS:
        return Group.species
    elif rank in GENUS_RANKS:
        return Group.genus
    elif rank in FAMILY_RANKS or rank == 34 or rank == 24:
        return Group.family
    elif rank in HIGH_RANKS or rank > Rank.hyperfamily:
        return Group.high
    else:
        raise ValueError("Unrecognized rank: " + str(rank))


def name_with_suffixes_removed(name: str) -> Iterable[str]:
    suffixes = list(SUFFIXES.values()) + ["ida", "oidae", "ides", "i", "a", "ae", "ia"]
    for suffix in suffixes:
        if name.endswith(suffix):
            yield re.sub(r"%s$" % suffix, "", name)


def suffix_of_rank(rank: Rank) -> str:
    return SUFFIXES[rank]


def rank_of_string(s: str) -> Rank:
    try:
        return _RANKS[s]  # type: ignore
    except KeyError:
        raise ValueError("Unknown rank: " + s)


def root_name_of_name(s: str, rank: Rank) -> str:
    if rank == Rank.species or rank == Rank.subspecies:
        return s.split()[-1]
    elif group_of_rank(rank) == Group.family:
        return strip_rank(s, rank)
    else:
        return s


def strip_rank(name: str, rank: Rank, quiet: bool = False) -> str:
    def strip_of_suffix(name: str, suffix: str) -> Optional[str]:
        if re.search(suffix + "$", name):
            return re.sub(suffix + "$", "", name)
        else:
            return None

    expected_suffix = suffix_of_rank(rank)
    try:
        res = strip_of_suffix(name, expected_suffix)
    except KeyError:
        res = None
    if res is None:
        if not quiet:
            print(f"Warning: Cannot find suffix -{expected_suffix} on name {name}")
        for suffix in SUFFIXES.values():
            res = strip_of_suffix(name, suffix)
            if res is not None:
                return res
        return name
    else:
        return res


def spg_of_species(species: str) -> str:
    """Returns a species group name from a species name"""
    return re.sub(r" ([a-z]+)$", r" (\1)", species)


def species_of_subspecies(ssp: str) -> str:
    return re.sub(r" ([a-z]+)$", r"", ssp)


def is_nominate_subspecies(ssp: str) -> bool:
    parts = re.sub(r' \(([A-Za-z"\-\. ]+)\)', "", ssp).split(" ")
    if len(parts) != 3:
        print(parts)
        raise Exception("Invalid subspecies name: " + ssp)
    return parts[1] == parts[2]


def genus_name_of_name(name: str) -> str:
    if name.lower().startswith("cf. "):
        return name.split()[1]
    return name.split()[0].replace("?", "")


_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")


def remove_null(d: Mapping[_T1, Optional[_T2]]) -> Dict[_T1, _T2]:
    out = {}
    for k, v in d.items():
        if v is not None:
            out[k] = v
    return out


def fix_data(data: str) -> Optional[str]:
    if data:
        data = json.dumps(remove_null(json.loads(data)))
        if data == "{}":
            return None
        else:
            return data
    else:
        return None


def convert_gender(name: str, gender: constants.GrammaticalGender) -> str:
    name = _canonicalize_gender(name)
    if gender == constants.GrammaticalGender.masculine:
        return name
    elif gender == constants.GrammaticalGender.feminine:
        # TODO this will fail occasionally
        if name.endswith("us"):
            return re.sub(r"us$", "a", name)
        elif name.endswith("er"):
            return name + "a"
        else:
            return name
    elif gender == constants.GrammaticalGender.neuter:
        # should really only be ensis but let's be broader
        if name.endswith("is"):
            return re.sub(r"is$", "e", name)
        elif name.endswith("us"):
            return re.sub(r"us$", "um", name)
        else:
            return name
    else:
        raise ValueError(f"unknown gender {gender}")


def _canonicalize_gender(name: str) -> str:
    if name.endswith("e"):
        return re.sub(r"e$", "is", name)
    elif name.endswith("era"):
        return name[:-1]
    elif name.endswith("a"):
        # TODO this will have a boatload of false positives
        return re.sub(r"a$", "us", name)
    elif name.endswith("um"):
        # TODO this will have a boatload of false positives
        return re.sub(r"um$", "us", name)
    else:
        return name


def standardize_date(date: str) -> Optional[str]:
    """Fixes the format of date fields."""
    if date in ("unknown date", "on unknown date", "on an unknown date"):
        return None
    date = re.sub(r"\]", "", date)
    date = re.sub(r"\[[A-Z a-n]+: ", "", date)
    date = re.sub(
        r", not [\dA-Za-z]+( [A-Z][a-z][a-z])? as( given)? in original description(, ?|$)",
        "",
        date,
    )
    if re.match(r"^\d{4}$", date):
        # year
        return date
    match = re.match(r"^in (\d{4})$", date)
    if match:
        return match.group(1)
    date_month_formats = [
        "%b %Y",  # Feb 1992
        "%b. %Y",  # Feb. 1992
        "%b, %Y",  # Feb, 1992
        "%B %Y",  # February 1992
        "%B, %Y",  # February, 1992
        "%bt %Y",  # Sept 1992
        "%m.%Y",  # 02.1992
    ]
    for fmt in date_month_formats:
        try:
            dt = datetime.datetime.strptime(date, fmt)
        except ValueError:
            pass
        else:
            return dt.strftime("%B %Y")
    dmy_formats = [
        "%d %B %Y",  # 24 February 1992
        "%d %b %Y",  # 24 Feb 1992
        "%d %bt. %Y",  # 24 Sept. 1992
        "%d %bt %Y",  # 24 Sept 1992
        "%d %b%Y",  # 24 Feb1992
        "%d %b. %Y",  # 24 Feb. 1992
        "%B %d, %Y",  # February 24, 1992
        "%b %d, %Y",  # Feb 24, 1992
        "%b. %d, %Y",  # Feb. 24, 1992
        "%d.%m.%Y",  # 24.02.1992
    ]
    for fmt in dmy_formats:
        try:
            dt = datetime.datetime.strptime(date, fmt)
        except ValueError:
            pass
        else:
            return dt.strftime("%-d %B %Y")
    raise ValueError(date)


COORDINATE_RGX = re.compile(
    r"""
    ^(?P<degrees>\d+(\.\d+)?)°
    ((?P<minutes>\d+(\.\d+)?)'
    ((?P<seconds>\d+(\.\d+)?)")?)?
    (?P<direction>[NSWE])$
""",
    re.VERBOSE,
)


class InvalidCoordinates(Exception):
    pass


def standardize_coordinates(text: str, *, is_latitude: bool) -> str:
    text = re.sub(r"\s", "", text)
    text = text.replace("·", ".")
    text = re.sub(r"[\*◦]", "°", text)
    text = re.sub(r"[`ʹ’‘′ ́]", "'", text)
    text = re.sub(r"(''|”)", '"', text)

    match = COORDINATE_RGX.match(text)
    if not match:
        raise InvalidCoordinates(f"could not match {text!r}")

    degrees = match.group("degrees")
    minutes = match.group("minutes")
    seconds = match.group("seconds")
    direction = match.group("direction")

    if "." in degrees and minutes:
        raise InvalidCoordinates("fractional degrees when minutes are given")
    if float(degrees) > (90 if is_latitude else 180):
        raise InvalidCoordinates(f"invalid degree {degrees}")

    if minutes:
        if "." in minutes and seconds:
            raise InvalidCoordinates("fractional degrees when minutes are given")
        if float(minutes) > 60:
            raise InvalidCoordinates(f"invalid minutes {minutes}")

    if seconds:
        if float(seconds) > 60:
            raise InvalidCoordinates(f"invalid seconds {seconds}")

    if is_latitude:
        if direction not in ("N", "S"):
            raise InvalidCoordinates(f"invalid latitude {direction}")
    else:
        if direction not in ("W", "E"):
            raise InvalidCoordinates(f"invalid longitude {direction}")
    return text


def extract_coordinates(text: str) -> Optional[Tuple[str, str]]:
    """Attempts to extract latitude and longitude from a location description."""
    match = LATLONG.search(text)
    if match:
        try:
            latitude = standardize_coordinates(
                match.group("latitude"), is_latitude=True
            )
        except InvalidCoordinates:
            return None
        try:
            longitude = standardize_coordinates(
                match.group("longitude"), is_latitude=False
            )
        except InvalidCoordinates:
            return None
        return latitude, longitude
    else:
        return None


def clean_text(text: str) -> str:
    text = text.replace("a ́", "á")
    text = text.replace("e ́", "é")
    text = text.replace("i ́", "í")
    text = text.replace("o ́", "ó")
    text = text.replace("u ́", "ú")
    text = text.replace(" ́ı", "í")
    text = text.replace("a ̃", "ã")
    text = text.replace("‘‘", '"')
    text = text.replace("’’", '"')
    text = re.sub(r"(?<=[a-z])- (?=[a-z])", "", text)
    text = re.sub(r" @$", " [brackets original]", text)
    return text


def unsplit_authors(authors: Sequence[str]) -> str:
    if len(authors) > 1:
        return " & ".join([", ".join(authors[:-1]), authors[-1]])
    else:
        return authors[0]


class TimeHolder:
    def __init__(self, label: str) -> None:
        self.label = label
        self.time: float = 0.0


@contextmanager
def timer(label: str) -> Iterator[TimeHolder]:
    th = TimeHolder(label)
    start_time = time.time()
    try:
        yield th
    finally:
        end_time = time.time()
        taken = end_time - start_time
        print(f"{label} took {taken:.03f} s")
        th.time = taken


TABLE = {
    "ъ": '"',
    "ь": "'",
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "y",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "kh",
    "ц": "ts",
    "ч": "ch",
    "ш": "sh",
    "щ": "shch",
    "ы": "y",
    "э": "e",
    "ю": "yu",
    "я": "ya",
}
NEED_Y = {"а", "и", "й", "о", "ы", "э", "ю", "я", "ъ", "ь", "е", "ё"}


def romanize_russian(cyrillic: str) -> str:
    """Romanize a Russian name.

    Uses the BGN/PCGN romanization: https://en.wikipedia.org/wiki/BGN/PCGN_romanization_of_Russian

    We omit all optional mid-dots.

    """
    out = []
    for i, c in enumerate(cyrillic):
        is_upper = c.isupper()
        c = c.lower()
        if c in TABLE:
            new_c = TABLE[c]
        elif c in ("е", "ё"):
            vowel = "e" if c == "е" else "ë"
            if i == 0 or cyrillic[i - 1] in NEED_Y:
                new_c = f"y{vowel}"
            else:
                new_c = vowel
        else:
            new_c = c
        if is_upper:
            new_c = f"{new_c[0].upper()}{new_c[1:]}"
        out.append(new_c)
    return "".join(out)


def extract_sources(text: str) -> Iterable[str]:
    for source in re.findall(r"{[^}]+}", text):
        yield source[1:-1]


def _clean_up_word(word: str) -> str:
    if word in ("the", "de", "des", "der", "of", "la", "le"):
        return ""
    return word.rstrip("s")


def simplify_string(text: str, clean_words: bool = True) -> str:
    """Simplify a string.

    This is intended to remove punctuation, casing, and similar
    to help compare strings.

    """
    text = re.sub(r"[\.,]", "", text)
    text = unidecode.unidecode(text)
    text = clean_string(text).lower()
    if clean_words:
        text = "".join(_clean_up_word(word) for word in text.split())
    else:
        text = text.replace(" ", "")
    return text


def is_clean_string(text: str) -> bool:
    return clean_string(text) == text


def clean_string(text: str) -> str:
    """Clean a string.

    This is intended as a safe operation that can be applied to any
    text (e.g., for cleaning up user input).

    """
    text = unicodedata.normalize("NFC", text)
    text = text.replace(" \xad ", "")
    text = text.replace("\xad", "")
    text = text.replace("’", "'")
    text = text.replace("‐", "-")  # use ASCII hyphen
    text = re.sub(r"[“”]", '"', text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_strings_recursively(obj: object) -> object:
    if isinstance(obj, str):
        return clean_string(obj)
    elif isinstance(obj, dict):
        return {
            clean_strings_recursively(key): clean_strings_recursively(value)
            for key, value in obj.items()
        }
    elif isinstance(obj, (list, set, tuple)):
        return type(obj)(clean_strings_recursively(elt) for elt in obj)
    else:
        return obj


def to_int(string: Optional[str]) -> int:
    """Convert a usually int-like string to a number, to be used as a sort key."""
    if string is None:
        return 0
    match = re.match(r"^(\d+)", string)
    if match:
        return int(match.group(1))
    else:
        return 0
