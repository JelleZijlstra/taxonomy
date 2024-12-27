"""Helper functions"""

import calendar
import datetime
import json
import re
import time
import unicodedata
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import TypeVar, cast

import unidecode

from taxonomy import getinput

from . import constants
from .constants import Group, Rank

SPECIES_RANKS = [
    Rank.subspecies,
    Rank.species,
    Rank.species_group,
    Rank.variety,
    Rank.form,
    Rank.infrasubspecific,
    Rank.synonym_species,
    Rank.aberratio,
    Rank.morph,
    Rank.natio,
    Rank.subvariety,
    Rank.other_species,
    Rank.informal_species,
    Rank.mutation,
    Rank.race,
]
GENUS_RANKS = [Rank.subgenus, Rank.genus, Rank.synonym_genus, Rank.other_subgeneric]
FAMILY_RANKS = [Rank.synonym_family, Rank.other_family, Rank.unranked_family]
HIGH_RANKS = {
    Rank.division,
    Rank.unranked,
    Rank.informal,
    Rank.other,
    Rank.synonym_high,
}
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

GROUP_TO_SYNONYM_RANK = {
    Group.species: Rank.synonym_species,
    Group.genus: Rank.synonym_genus,
    Group.family: Rank.synonym_family,
    Group.high: Rank.synonym_high,
}

_RANKS = {
    "root": Rank.root,
    "Unnamed rank": Rank.root,
    "Classis": Rank.class_,
    "Class": Rank.class_,
    "Subclassis": Rank.subclass,
    "Subclass": Rank.subclass,
    "Infraclassis": Rank.infraclass,
    "Infraclass": Rank.infraclass,
    "Supracohors": Rank.supercohort,
    "Supercohors": Rank.supercohort,
    "Supercohort": Rank.supercohort,
    "Cohors": Rank.cohort,
    "Cohort": Rank.cohort,
    "Subcohors": Rank.subcohort,
    "Superordo": Rank.superorder,
    "Supraordo": Rank.superorder,
    "Superorder": Rank.superorder,
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
    "Familia": Rank.family,
    "Family": Rank.family,
    "Subfamilia": Rank.subfamily,
    "Subfamily": Rank.subfamily,
    "Tribus": Rank.tribe,
    "Tribe": Rank.tribe,
    "Subtribus": Rank.subtribe,
    "Subtribe": Rank.subtribe,
    "Infratribe": Rank.infratribe,
    "Division": Rank.division,
    "Genus": Rank.genus,
    "Subgenus": Rank.subgenus,
}


def group_of_rank(rank: Rank) -> Group:
    if rank in SPECIES_RANKS:
        return Group.species
    elif rank in GENUS_RANKS:
        return Group.genus
    elif Rank.infratribe <= rank <= Rank.hyperfamily or rank in FAMILY_RANKS:
        return Group.family
    elif Rank.parvorder <= rank <= Rank.root or rank in HIGH_RANKS:
        return Group.high
    else:
        raise ValueError(f"Unrecognized rank: {rank!r}")


def strip_standard_suffixes(name: str) -> str:
    for suffix in VALID_SUFFIXES:
        if name.endswith(suffix):
            return name.removesuffix(suffix)
    return name


def name_with_suffixes_removed(name: str) -> Iterable[str]:
    suffixes = [*SUFFIXES.values(), "ida", "oidae", "ides", "i", "a", "ae", "ia"]
    for suffix in suffixes:
        if name.endswith(suffix):
            yield name.removesuffix(suffix)


def suffix_of_rank(rank: Rank) -> str:
    return SUFFIXES[rank]


def rank_of_string(s: str) -> Rank:
    try:
        return _RANKS[s]
    except KeyError:
        raise ValueError(f"Unknown rank: {s}") from None


def root_name_of_name(s: str, rank: Rank) -> str:
    if rank in (Rank.species, Rank.subspecies):
        return s.split()[-1]
    elif group_of_rank(rank) == Group.family:
        return strip_rank(s, rank)
    else:
        return s


def strip_rank(name: str, rank: Rank, *, quiet: bool = False) -> str:
    def strip_of_suffix(name: str, suffix: str) -> str | None:
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
        raise ValueError("Invalid subspecies name: " + ssp)
    return parts[1] == parts[2]


def genus_name_of_name(name: str) -> str:
    if name.lower().startswith("cf. "):
        return name.split()[1]
    return name.split()[0].replace("?", "")


_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")


def remove_null(d: Mapping[_T1, _T2 | None]) -> dict[_T1, _T2]:
    return {k: v for k, v in d.items() if v is not None}


def fix_data(data: str) -> str | None:
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
        # TODO: this will fail occasionally
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
        # TODO: this will have a boatload of false positives
        return re.sub(r"a$", "us", name)
    elif name.endswith("um"):
        # TODO: this will have a boatload of false positives
        return re.sub(r"um$", "us", name)
    else:
        return name


def standardize_date(date: str) -> str | None:
    """Fixes the format of date fields."""
    if date in ("unknown date", "on unknown date", "on an unknown date"):
        return None
    # A leading < is allowed to indicate "before"
    if date.startswith("<"):
        return "<" + _standardize_inner(date[1:])
    return _standardize_inner(date)


def _standardize_inner(date: str) -> str:
    date = re.sub(r"\]", "", date)
    date = re.sub(r"\[[A-Z a-n]+: ", "", date)
    date = re.sub(
        (
            r", not [\dA-Za-z]+( [A-Z][a-z][a-z])? as( given)? in original"
            r" description(, ?|$)"
        ),
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
            dt = datetime.datetime.strptime(date, fmt).astimezone(datetime.UTC)
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
            dt = datetime.datetime.strptime(date, fmt).astimezone(datetime.UTC)
        except ValueError:
            pass
        else:
            return dt.strftime("%-d %B %Y")
    raise ValueError(date)


COORDINATE_RGX = re.compile(
    r"""
    ^(?P<degrees>\d+(\.\d+)?)°?
    ((?P<minutes>\d+(\.\d+)?)'
    ((?P<seconds>\d+(\.\d+)?)")?)?
    \s*
    (?P<direction>[NSWE])$
    """,
    re.VERBOSE,
)


class InvalidCoordinates(Exception):
    pass


def standardize_coordinates(text: str, *, is_latitude: bool) -> tuple[str, float]:
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

    numeric_degrees = float(degrees)
    if numeric_degrees > (90 if is_latitude else 180):
        raise InvalidCoordinates(f"invalid degree {degrees}")
    numeric_value = numeric_degrees

    if minutes:
        if "." in minutes and seconds:
            raise InvalidCoordinates("fractional degrees when minutes are given")
        if float(minutes) > 60:
            raise InvalidCoordinates(f"invalid minutes {minutes}")
        numeric_value += float(minutes) / 60

    if seconds:
        if float(seconds) > 60:
            raise InvalidCoordinates(f"invalid seconds {seconds}")
        numeric_value += float(seconds) / 3600

    if numeric_value > (90 if is_latitude else 180):
        raise InvalidCoordinates(f"invalid coordinates {text}")

    if is_latitude:
        if direction not in ("N", "S"):
            raise InvalidCoordinates(f"invalid latitude {direction}")
        if direction == "S":
            numeric_value = -numeric_value
    else:
        if direction not in ("W", "E"):
            raise InvalidCoordinates(f"invalid longitude {direction}")
        if direction == "W":
            numeric_value = -numeric_value
    text = f"{_display(numeric_degrees)}°"
    if minutes:
        text += f"{_display(float(minutes))}'"
        if seconds:
            text += f'{_display(float(seconds))}"'
    text += direction
    return text, numeric_value


def _display(value: float) -> str:
    if value == int(value):
        return str(int(value))
    else:
        return str(value)


LATLONG = re.compile(
    r"""
    (?P<latitude>\d+(\.\d+)?\s*[°*]\s*(\d+(\.\d+)?\s*')?(\d+(\.\d+)?\s*")?\s*[NS])[,\s\[\]]+
    (long\.\s)?(?P<longitude>\d+(\.\d+)?\s*[°*]\s*(\d+(\.\d+)?\s*')?(\d+(\.\d+)?\s*")?\s*[EW])
    """,
    re.VERBOSE,
)
LATLONG_NO_SIGN = re.compile(
    r"""
    (?P<latitude>\d+\.\d+\s*[NS])[,\s]+
    (?P<longitude>\d+\.\d+\s*[EW])
    """,
    re.VERBOSE,
)


def extract_coordinates(text: str) -> tuple[str, str] | None:
    """Attempts to extract latitude and longitude from a location description."""
    for rgx in (LATLONG, LATLONG_NO_SIGN):
        match = rgx.search(text)
        if not match:
            continue
        try:
            latitude, _ = standardize_coordinates(
                match.group("latitude"), is_latitude=True
            )
        except InvalidCoordinates:
            continue
        try:
            longitude, _ = standardize_coordinates(
                match.group("longitude"), is_latitude=False
            )
        except InvalidCoordinates:
            continue
        return latitude, longitude
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
    text = text.replace("ʹ", "'")
    text = re.sub(r"(?<=[a-z])- (?=[a-z])", "", text)
    return re.sub(r" @$", " [brackets original]", text)


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
    "ѣ": "e",
}
NEED_Y = {"а", "и", "й", "о", "ы", "э", "ю", "я", "ъ", "ь", "е", "ё", "ѣ"}


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


def simplify_string(text: str, *, clean_words: bool = True) -> str:
    """Simplify a string.

    This is intended to remove punctuation, casing, and similar
    to help compare strings.

    """
    # At least one DOI has "&amp;amp;"
    text = (
        text.replace("&amp;", "&")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("„", "")
        .replace("&nbsp;", " ")
    )
    text = re.sub(r"^\d+", "", text)
    text = re.sub(r"</?(b|i|strong|em|sub|sup|p|br ?|scp)/?>", "", text)
    text = re.sub(r"[\.,_\\]", "", text)
    text = unidecode.unidecode(text)
    text = re.sub(r"[\-—–]+", "-", text).replace(":", "")
    text = clean_string(text).casefold()
    if clean_words:
        text = "".join(_clean_up_word(word) for word in text.split())
    else:
        text = text.replace(" ", "")
    return text


def is_clean_string(text: str) -> bool:
    return clean_string(text) == text


HYPHEN_IGNORE_PATTERNS = {"and ", "bis ", "en ", "oder ", "u. ", "und "}
HYPHEN_REPLACE_PATTERNS = {
    "adae",
    "akh",
    "al)",
    "anthrop",
    "arb",
    "ation ",
    "auro",
    "bercula",
    "bergiana",
    "bin",
    "boneb",
    "bosphe",
    "buch",
    "cae ",
    "cal ",
    "can",
    "cata",
    "cene",
    "ceous",
    "cep",
    "che",
    "chus",
    "cidians ",
    "cies",
    "cies ",
    "cion ",
    "ciones",
    "citor ",
    "cording ",
    "cre",
    "cène",
    "dacty",
    "dae",
    "dae ",
    "danod",
    "de. ",
    "dea",
    "dectiformes",
    "del",
    "dian",
    "dies.",
    "dilia",
    "dinn,",
    "dontoidea",
    "eide",
    "ence",
    "ence ",
    "ens.",
    "eralogie",
    "ern ",
    "eskog",
    "ety",
    "gen",
    "gethi",
    "gica",
    "gie,",
    "gler",
    "glich",
    "gol",
    "grap",
    "graphischer",
    "ian ",
    "ian,",
    "ica ",
    "ico",
    "ing ",
    "ior",
    "ischen ",
    "isième",
    "its ",
    "ity ",
    "ized",
    "kalk,",
    "katche",
    "kerau",
    "kohle ",
    "kunde",
    "late",
    "lates",
    "lected",
    "lemes ",
    "leont",
    "leti",
    "lia.",
    "lic",
    "linidae",
    "lle",
    "lodont",
    "log",
    "logi",
    "louse",
    "lun",
    "lutio",
    "ly ",
    "maci",
    "mack",
    "mali",
    "malium",
    "mals",
    "man ",
    "marks",
    "mat",
    "ment",
    "min",
    "morpha",
    "mou",
    "mét",
    "nal",
    "nan.",
    "nean",
    "ngs",
    "nik ",
    "no.",
    "nod",
    "ogisc",
    "ogy",
    "olog",
    "oni",
    "onto",
    "opte",
    "pas",
    "pedia",
    "pedition",
    "per",
    "pho",
    "piali",
    "pidae",
    "plementary ",
    "pra",
    "pteryx",
    "querq",
    "rap",
    "reisen",
    "ren",
    "rhy",
    "ria",
    "ric",
    "ropod",
    "ros",
    "rosa",
    "sau",
    "sen",
    "seum",
    "sian",
    "sion,",
    "sta",
    "sum",
    "sup",
    "tace",
    "tagne",
    "tained",
    "tana",
    "tary",
    "tei",
    "ten",
    "ter ",
    "tho",
    "tieff ",
    "til",
    "tino",
    "tinued ",
    "tio",
    "tion",
    "tisch",
    "titub",
    "tlic",
    "to ",
    "tolog",
    "tor",
    "tra",
    "treter",
    "troduction ",
    "trópica",
    "tuberc",
    "tuto",
    "tyl",
    "ungen",
    "veaux ",
    "versit",
    "virons",
    "wan",
}


def interactive_clean_string(
    text: str,
    *,
    clean_whitespace: bool = True,
    verbose: bool = False,
    interactive: bool = True,
) -> str:
    text = clean_string(text, clean_whitespace=clean_whitespace)
    text = text.replace("\n- ", "\n-\\ -")
    if "- " not in text:
        return text
    if "| --" in text or ":\n- " in text:
        return text  # advanced Markdown formatting
    getinput.print_header(text)

    def repl(m: re.Match[str]) -> str:
        after = m.group(1)
        if any(after.startswith(pattern) for pattern in HYPHEN_IGNORE_PATTERNS):
            if verbose:
                print(f"autofix {after!r} in {text}")
            return m.group().replace("- ", "-\\ ")
        elif any(after.startswith(pattern) for pattern in HYPHEN_REPLACE_PATTERNS):
            if verbose:
                print(f"autofix {after!r} in {text}")
            return after
        elif re.search(r'^[a-zéèíóüöäа-я]{2,}([ ,)\.;:"]|\'s)', after):
            if verbose:
                print(f"autofix {after!r} in {text}")
            return after
        if not interactive:
            return m.group()
        print(f"{after!r} in {text!r}")
        if getinput.yes_no("remove '- '? "):
            return after
        else:
            if getinput.yes_no("remove space? "):
                return "-" + after
            return m.group()

    text = re.sub(r"(?<! )- ([^-]+)", repl, text)
    if interactive and "- " in text:
        text = getinput.edit_by_word(text)
    return text


def is_string_clean(text: str) -> str | None:
    if " \N{COMBINING ACUTE ACCENT}" in text:
        return "combining acute accent ( ́)"
    if " \N{COMBINING GRAVE ACCENT}" in text:
        return "combining grave accent ( ̀)"
    if " \N{COMBINING CIRCUMFLEX ACCENT}" in text:
        return "combining circumflex accent ( ̂)"
    if " \N{COMBINING TILDE}" in text:
        return "combining tilde ( ̃)"
    if " \N{COMBINING MACRON}" in text:
        return "combining macron ( ̄)"
    if " \N{COMBINING BREVE}" in text:
        return "combining breve ( ̆)"
    if " \N{COMBINING DOT ABOVE}" in text:
        return "combining dot above ( ̇)"
    if " \N{COMBINING DIAERESIS}" in text:
        return "combining diaeresis ( ̈)"
    if " \N{COMBINING RING ABOVE}" in text:
        return "combining ring above ( ̊)"
    if " \N{COMBINING DOUBLE ACUTE ACCENT}" in text:
        return "combining double acute accent ( ̋)"
    if " \N{COMBINING OGONEK}" in text:
        return "combining ogonek ( ̨)"
    if " \N{COMBINING CARON}" in text:
        return "combining caron ( ̌)"
    if " \N{COMBINING CEDILLA}" in text:
        return "combining cedilla ( ̧)"
    return None


def clean_string(text: str, *, clean_whitespace: bool = True) -> str:
    """Clean a string.

    This is intended as a safe operation that can be applied to any
    text (e.g., for cleaning up user input).

    """
    # As an optimization, skip various expensive transformations if we know we
    # don't need them.
    if not text.isascii():
        text = unicodedata.normalize("NFC", text)
        text = text.replace("’", "'")
        text = text.replace("′", "'")
        text = text.replace("ʹ", "'")
        text = text.replace("‐", "-")  # use ASCII hyphen
        text = text.replace("◦", "°")
        text = re.sub(r"[“”]", '"', text)
        text = re.sub(r"(\N{DEGREE SIGN}\s*\d+)\x01", r"\1'", text)
        text = text.replace("u€", "ü")
        text = text.replace("o€", "ö")
        text = text.replace("€a", "ä")
        text = re.sub(r"([aeiouAEIOU]) ̈", r"\1" + "\N{COMBINING DIAERESIS}", text)
        text = re.sub(r"([aeiouAEIOUnN]) ̃", r"\1" + "\N{COMBINING TILDE}", text)
        text = re.sub(r"([aeiouAEIOUnN]) ́", r"\1" + "\N{COMBINING ACUTE ACCENT}", text)
        text = re.sub(r"([aeiouAEIOU]) ̀", r"\1" + "\N{COMBINING GRAVE ACCENT}", text)
        text = re.sub(
            r"([aeiouAEIOU]) ̂", r"\1" + "\N{COMBINING CIRCUMFLEX ACCENT}", text
        )
        text = re.sub(r"([aeiouAEIOUcC]) ̌", r"\1" + "\N{COMBINING CARON}", text)
        text = re.sub(r"([aeiouAEIOU]) ̆", r"\1" + "\N{COMBINING BREVE}", text)
        text = re.sub(r"([aeiouAEIOU]) ̄", r"\1" + "\N{COMBINING MACRON}", text)
        text = re.sub(r"([aeiouAEIOU]) ̊", r"\1" + "\N{COMBINING RING ABOVE}", text)
        text = re.sub(
            r"([aeiouAEIOU]) ̋", r"\1" + "\N{COMBINING DOUBLE ACUTE ACCENT}", text
        )
        text = re.sub(r"([cCsStT]) ̧", r"\1" + "\N{COMBINING CEDILLA}", text)
        text = text.replace(" ́ı", "í")
        text = text.replace("ı́", "í")
        text = text.replace("ı̈", "ï")
        text = text.replace(" d ́", " d'")
        text = text.replace(" l ́", " l'")
        text = text.replace(" ́s ", "'s ")
        text = text.replace("ü̈", "ü")
        text = text.replace("ö̈", "ö")
        text = re.sub(r"(?<=\d) ́", "'", text)
        text = re.sub(r"(?<=\d) ̋", '"', text)
        text = re.sub(r"(?<=\d) ̊", "\N{DEGREE SIGN}", text)
        text = text.replace("' ́", "''")
        # fallbacks: sometimes it's before the letter instead
        text = re.sub(r" ̧([cCsStT])", r"\1" + "\N{COMBINING CEDILLA}", text)
        text = re.sub(r" ́([aeiouAEIOUnN])", r"\1" + "\N{COMBINING ACUTE ACCENT}", text)
        text = text.replace("\N{LEFT SINGLE QUOTATION MARK}", "'")
        text = text.replace("\U0010ff4e", "'")
        text = text.replace("*\U0010fc0d", "°")
        text = text.replace("'\U0010fc01", "'")
        text = text.replace('"\U0010fc08', '"')
        text = text.replace("\U0010fc03[M]", "\N{MALE SIGN}")
        text = text.replace("\U0010fe1f[M]", "\N{MALE SIGN}")
        text = text.replace("\U0010fc00[M]", "\N{MALE SIGN}")
        text = text.replace("\U0010fe20[F]", "\N{FEMALE SIGN}")
        text = text.replace("\U0010fc04", "\N{MULTIPLICATION SIGN}")
        text = text.replace("\U0010fc03+", "+")
        text = text.replace("\U0010fd79 ", "")
        text = text.replace("\U0010fc25 ", "")
        text = text.replace("\U0010fc44", "=")
        text = text.replace("\U0010fc00", "=")
        text = text.replace("\uf8e7", "\N{EM DASH}")
        text = text.replace("\U0010fc94", "≈")
        text = re.sub(r"(\d)\x96(\d)", r"\1-\2", text)
        text = text.replace("\x92", "'")
        text = text.replace("\x94", '"')
        text = text.replace("\x97", "–")
        text = text.replace(" \xad ", "")
        text = text.replace("\xad", "")
        text = text.replace("\x91%", "\N{DEGREE SIGN}")
    if not text.isprintable():
        text = re.sub(r"(\d)\x01(\d)", r"\1-\2", text)
        text = re.sub(r"(\d)\x02(?!\d)", r"\1'", text)
        text = re.sub(r"('\s*\d+)\x01\x01", r'\1"', text)
        text = text.replace("\x18a", "à")
        text = text.replace("\x18e", "è")
        text = text.replace("\x19a", "á")
        text = text.replace("\x19e", "é")
        text = text.replace("\x19ı", "í")
        text = text.replace("\x19o", "ó")
        text = text.replace("\x19u", "ú")
        text = text.replace("a\x18", "à")
        text = text.replace("a\x19", "á")
        text = text.replace("e\x19", "é")
        text = text.replace("ı\x19", "í")
        text = text.replace("o\x19", "ó")
        text = text.replace("u\x19", "ú")
        text = text.replace("e\x18", "è")
    text = text.replace("+/-", "±")
    text = text.replace("''", '"')
    if "- " in text:
        text = text.replace(" :- ", ": \N{EN DASH} ")
        text = re.sub(r" -+(?= )", " \N{EN DASH}", text)
        text = re.sub(r"([A-Z])- (\d)", r"\1-\2", text)
        text = re.sub(r"(\d)- ([A-Za-z\d])", r"\1-\2", text)
        text = re.sub(r"([A-Z])\.- ([A-Z])\.", r"\1.-\2.", text)
        text = re.sub(r"([a-zа-я])- ([A-ZА-Я])", r"\1-\2", text)
        text = re.sub(r"(\d\)|[a-z])\.- ([A-Z])", r"\1.—\2", text)
        text = re.sub(r"\.- —", r". —", text)
        text = re.sub(r"(\d)- (\d)", r"\1-\2", text)
    if clean_whitespace:
        text = re.sub(r"\s+", " ", text)
    text = unicodedata.normalize("NFC", text)
    return text.strip()


T = TypeVar("T")


def clean_strings_recursively(obj: T) -> T:
    if isinstance(obj, str):
        return cast(T, clean_string(obj))
    elif isinstance(obj, dict):
        return cast(
            T,
            {
                clean_strings_recursively(key): clean_strings_recursively(value)
                for key, value in obj.items()
            },
        )
    elif isinstance(obj, (list, set, tuple)):
        return cast(T, type(obj)(clean_strings_recursively(elt) for elt in obj))
    else:
        return obj


def to_int(string: str | None) -> int:
    """Convert a usually int-like string to a number, to be used as a sort key."""
    if string is None:
        return 0
    match = re.match(r"^(\d+)", string)
    if match:
        return int(match.group(1))
    else:
        return 0


def print_character_names(string: str) -> None:
    for i, c in enumerate(string):
        try:
            name = unicodedata.name(c)
        except ValueError as e:
            name = repr(e)
        print(f"{i} {c!r} – {name}")


def trimdoi(doi: str) -> str:
    """Cleans up a DOI."""
    doi = doi.strip()
    doi = re.sub(r"[\.;\(]$|^:|^doi:|^http:\/\/dx\.doi\.org\/", "", doi)
    return doi.strip()


def is_valid_year(year: str, *, allow_empty: bool = True) -> str | None:
    if not year:
        if allow_empty:
            return None
        else:
            return "year is empty"
    if not year.isnumeric() or len(year) != 4:
        return f"{year} does not look like a year"
    numeric_year = int(year)
    # a generous range of years that could appear in the database
    if 1500 <= numeric_year <= 2100:
        return None
    return f"{numeric_year} is out of range"


_DATE_REGEX = re.compile(
    r"^(?P<year>\d{4})(-(?P<end_year>\d{4})|-(?P<month>[01]\d)|-(?P<month2>[01]\d)-(?P<day>[0-3]\d))?$"
)
_DEFAULT_DATE = datetime.date(1, 1, 1)


def is_valid_date(date: str) -> bool:
    date_obj = get_date_object(date)
    return date_obj is not _DEFAULT_DATE


def get_date_object(date: str | None) -> datetime.date:
    if date is None:
        return _DEFAULT_DATE
    match = _DATE_REGEX.fullmatch(date)
    if match is None:
        return _DEFAULT_DATE
    # IZCN Art. 21.3, 21.6: If the date is not precisely known, use the last possible date
    if match.group("end_year"):
        year = int(match.group("end_year"))
    else:
        year = int(match.group("year"))
    if match.group("month"):
        month = int(match.group("month"))
    elif match.group("month2"):
        month = int(match.group("month2"))
    else:
        month = 12
    if match.group("day"):
        day = int(match.group("day"))
    else:
        _, day = calendar.monthrange(year, month)
    try:
        return datetime.date(year, month, day)
    except ValueError:
        return _DEFAULT_DATE


def is_date_range(date: str) -> bool:
    match = _DATE_REGEX.fullmatch(date)
    if match is None:
        return False
    return bool(match.group("end_year"))


def is_more_specific_date(left: str | None, right: str | None) -> bool:
    if left is None:
        return right is None
    if right is None:
        return left is not None
    return (
        not is_date_range(right)
        and is_valid_date(left)
        and is_valid_date(right)
        and left != right
        and left.startswith(right)
    )


def is_valid_regex(rgx: str) -> str | None:
    try:
        re.compile(rgx)
    except re.error as e:
        return f"regex {rgx!r} failed to compile: {e!r}"
    if re.match(r"(?!\\)\.", rgx):
        return f"regex {rgx!r} contains unescaped dot"
    return None


MONTHS = list(calendar.month_name)


def parse_month(month: str) -> int:
    if month.isnumeric():
        return int(month)
    if len(month) >= 3:
        for i, candidate in enumerate(MONTHS):
            if candidate.startswith(month):
                return i
    raise ValueError(f"Unrecognized month {month!r}")


def parse_date(year: str, month: str | None, day: str | None) -> str:
    result = year
    if month:
        month_num = parse_month(month)
        result += f"-{month_num:02d}"
    if day:
        day_num = int(day)
        result += f"-{day_num:02d}"
    if not is_valid_date(result):
        raise ValueError(f"produced invalid date {result!r} from {year} {month} {day}")
    return result


def is_valid_roman_numeral(s: str) -> bool:
    # TODO: stricter validation
    return bool(re.fullmatch(r"[ivxlc]+", s))


LETTER_TO_VALUE = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
_SORTED_VALUES = sorted(LETTER_TO_VALUE.items(), key=lambda p: -p[1])


def make_roman_numeral(i: int) -> str:
    """Not the best option for higher numbers, but works."""
    assert 0 <= i <= 4000
    for letter, value in _SORTED_VALUES:
        if i == value:
            return letter
        elif i == value - 1:
            return f"I{letter}"
        elif i > value:
            rest = i - value
            return letter + make_roman_numeral(rest)
    return ""


def parse_roman_numeral(s: str) -> int:
    s = s.upper()
    for letter, value in _SORTED_VALUES:
        if letter in s:
            before, after = s.split(letter, maxsplit=1)
            return value - parse_roman_numeral(before) + parse_roman_numeral(after)
    if s:
        raise ValueError(f"unrecognized Roman numeral: {s!r}")
    return 0


def split_iterable(
    iterable: Iterable[T], predicate: Callable[[T], bool]
) -> tuple[list[T], list[T]]:
    """Split an iterable into two lists based on a predicate."""
    true_list = []
    false_list = []
    for elt in iterable:
        if predicate(elt):
            true_list.append(elt)
        else:
            false_list.append(elt)
    return true_list, false_list


def sift(objs: Iterable[T], pred: Callable[[T], bool]) -> tuple[list[T], list[T]]:
    true, false = [], []
    for obj in objs:
        if pred(obj):
            true.append(obj)
        else:
            false.append(obj)
    return true, false
