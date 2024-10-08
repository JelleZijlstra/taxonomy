from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from functools import partial
from re import Pattern


def unicode_range(start: str, end: str) -> set[str]:
    return {chr(i) for i in range(ord(start), ord(end) + 1)}


class Element:
    def to_regex(self) -> str:
        raise NotImplementedError

    def compile(self) -> Pattern[str]:
        return re.compile(f"^({self.to_regex()})$")

    def __or__(self, other: Element) -> OneOf:
        return OneOf([self, other])

    def __add__(self, other: Element) -> And:
        return And([self, other])


@dataclass
class Char(Element):
    alternatives: list[str]

    def to_regex(self) -> str:
        return f"[{''.join(self.alternatives)}]"


@dataclass
class Literal(Element):
    text: str

    def to_regex(self) -> str:
        return re.escape(self.text)


@dataclass
class OneOf(Element):
    alternatives: list[Element]

    @classmethod
    def from_strs(cls, strs: Iterable[str]) -> OneOf:
        return cls([Literal(s) for s in strs])

    def to_regex(self) -> str:
        return "|".join(f"({e.to_regex()})" for e in self.alternatives)


@dataclass
class And(Element):
    pieces: list[Element]

    def to_regex(self) -> str:
        return "".join(f"({e.to_regex()})" for e in self.pieces)


@dataclass
class Repetition(Element):
    elt: Element
    min: int | None = None
    max: int | None = None

    def to_regex(self) -> str:
        return f"({self.elt.to_regex()}){{{self.min or ''},{self.max or ''}}}"


Optional = partial(Repetition, min=0, max=1)
ZeroOrMore = partial(Repetition, min=0, max=None)
OneOrMore = partial(Repetition, min=1, max=None)


C = Char
L = Literal

digit = C(sorted(unicode_range("0", "9")))
upper = C(
    sorted(
        unicode_range("A", "Z")
        | unicode_range("Α", "Ρ")
        | unicode_range("Σ", "Ω")
        | {
            "İ",
            "À",
            "Ć",
            "Á",
            "Å",
            "Ä",
            "Ç",
            "Č",
            "Ď",
            "È",
            "É",
            "Í",
            "Î",
            "Ó",
            "Ö",
            "Ø",
            "Ú",
            "Ü",
            "Ľ",
            "Ñ",
            "Ő",
            "Ō",
            "Đ",
            "Ł",
            "Ş",
            "Š",
            "Ș",
            "Ś",
            "Ț",
            "Ř",
            "Ş",
            "Ż",
            "Ž",
            "Œ",
            "Ḥ",
        }
    )
)
latin_upper = C(sorted(unicode_range("A", "Z")))
cyrillic_upper = C(sorted(unicode_range("А", "Я")))
lower = C(
    sorted(
        unicode_range("a", "z")
        | unicode_range("α", "ω")
        | {
            "'",
            "ß",
            "à",
            "á",
            "â",
            "ã",
            "ä",
            "å",
            "ā",
            "ặ",
            "ầ",
            "æ",
            "ç",
            "è",
            "é",
            "ê",
            "ë",
            "ì",
            "í",
            "î",
            "ī",
            "ï",
            "ñ",
            "ò",
            "ó",
            "ô",
            "õ",
            "ö",
            "ø",
            "ú",
            "ü",
            "ý",
            "ÿ",
            "ă",
            "ą",
            "ć",
            "č",
            "đ",
            "ė",
            "ę",
            "ě",
            "ế",
            "ğ",
            "ı",
            "ľ",
            "ł",
            "ń",
            "ň",
            "ṅ",
            "ō",
            "ő",
            "ọ",
            "ř",
            "ś",
            "ş",
            "š",
            "ţ",
            "ť",
            "ů",
            "ư",
            "ź",
            "ż",
            "ž",
            "ơ",
            "ǎ",
            "ǧ",
            "ș",
            "ț",
            "ð",
            "þ",
            "ū",
        }
    )
)
cyrillic_lower = C([*sorted(unicode_range("а", "я")), "ѣ"])
latin_lower = C(sorted(unicode_range("a", "z")))
special_initials = {
    "Ch",
    "Chr",
    "Dj",
    "Dzh",
    "Fr",
    "Gy",
    "Iu",
    "Kh",
    "Ll",
    "Ph",
    "Rob",
    "Sh",
    "St",
    "Th",
    "Theo",
    "Ts",
    "Ya",
    "Ye",
    "Yo",
    "Yu",
    "Zh",
}
initial = ((L("Mc") + latin_upper) | OneOf.from_strs(special_initials) | upper) + L(".")
name_infixes = {
    "auf ",
    "al ",
    "d'",
    "da ",
    "dal ",
    "das ",
    "des ",
    "de ",
    "de la ",
    "de las ",
    "de los ",
    "del ",
    "della ",
    "delle ",
    "di ",
    "do ",
    "dos ",
    "du ",
    "el ",
    "le ",
    "ten ",
    "ul ",
    "von ",
    "van ",
    "van der ",
    "von der ",
    "van den ",
    "zu ",
    "zur ",
}
initials = initial + ZeroOrMore(
    Optional(OneOf.from_strs(" " + s for s in name_infixes) | L("-")) + initial
)
name_prefixes = {
    *name_infixes,
    "D'",
    "De la ",
    "De los ",
    "De",
    "Del",
    "Di",
    "Do",
    "Du",
    "Fitz",
    "La",
    "Le",
    "M'",
    "Mac",
    "Mc",
    "O'",
    "L'",
    "N'",
    "de",
    "de-",
    "vander",
    "Vander",
    "Vande",
    "Van",
    "Von",
    "Van der ",
    "Van den ",
    "Van de ",
    "St. ",
    "Md. ",
    "von",
    "ul-",
    "bin ",
    "ter",
    "Wolde",
    "O",
}
name_connectors = {"-", " i ", " y ", " e ", "-i-", "'", "-del-"}
name = (
    Optional(OneOf.from_strs(name_prefixes)) + upper + OneOrMore(lower)
) | OneOf.from_strs(["ffolliott", "LuAnn"])
compound_name = name + ZeroOrMore(OneOf.from_strs(name_connectors) + name)
names = compound_name + ZeroOrMore(
    L(" ") + Optional(OneOf.from_strs(name_prefixes)) + compound_name
)
special_family_names = {"MacC.", "S.D.W."}
family_name = (Optional(OneOf.from_strs(name_prefixes)) + names) | OneOf.from_strs(
    special_family_names
)

portuguese_name_infixes = {" da ", "-da-", "-dos-", " e ", "-e-", "-"}
portuguese_name = upper + OneOrMore(lower)
portuguese_compound_name = portuguese_name + ZeroOrMore(
    OneOf.from_strs(portuguese_name_infixes) + portuguese_name
)
portuguese_names = portuguese_compound_name + ZeroOrMore(
    L(" ") + portuguese_compound_name
)
portuguese_family_name = portuguese_names + Optional(L("-Jr."))

spanish_name = Optional(OneOf.from_strs(name_prefixes)) + upper + OneOrMore(lower)
spanish_second_name = (latin_upper | L("Á")) + L(".")
spanish_compound_name = spanish_name + ZeroOrMore(
    OneOf.from_strs(name_connectors) + (spanish_name | spanish_second_name)
)
spanish_names = spanish_compound_name + ZeroOrMore(
    L(" ") + Optional(OneOf.from_strs(name_prefixes)) + spanish_compound_name
)
spanish_family_name = (
    Optional(OneOf.from_strs(name_prefixes))
    + spanish_names
    + Optional(L(" ") + spanish_second_name)
)

nickname = L('"') + name + L('"')
given_names = (
    Optional(initials + L(" "))
    + names
    + Optional(L(" ") + Optional(OneOf.from_strs(name_infixes)) + initials)
    + Optional(L(" ") + nickname)
)

pinyin_initial = OneOf.from_strs(
    {
        "b",
        "p",
        "m",
        "f",
        "d",
        "t",
        "n",
        "z",
        "c",
        "s",
        "l",
        "zh",
        "ch",
        "sh",
        "r",
        "j",
        "q",
        "x",
        "g",
        "k",
        "h",
        "y",
        "w",
    }
)
pinyin_pre_vowel = C(["i", "u"])
pinyin_vowel = OneOf.from_strs(
    {"i", "e", "a", "o", "ou", "ao", "u", "ü", "ue", "ua", "üa", "üe"}
)
pinyin_coda = OneOf.from_strs({"i", "n", "ng", "r"})
pinyin_syllable = (
    Optional(pinyin_initial + Optional(pinyin_pre_vowel))
    + pinyin_vowel
    + Optional(pinyin_coda)
)
pinyin_given_names = pinyin_syllable + Repetition(
    L("-") + pinyin_syllable, min=0, max=2
)
pinyin_family_name = pinyin_syllable | OneOf.from_strs(
    ["ouyang", "jinggong", "jiangzuo", "fucha"]
)

chinese_lower = C(sorted(unicode_range("a", "z") | {"ü"}))
chinese_name = latin_upper + OneOrMore(chinese_lower)
pinyin_given_names_cased = chinese_name + Repetition(
    L("-") + OneOrMore(chinese_lower), min=0, max=2
)
chinese_given_names = (
    chinese_name
    + Optional((L("-") | L(" ")) + Optional(latin_upper) + OneOrMore(chinese_lower))
    + Optional(L(" ") + latin_upper + L("."))
)

russian_upper = cyrillic_upper | L("Ё")
russian_lower = cyrillic_lower | L("ё")
russian_name = (russian_upper + OneOrMore(russian_lower)) | (
    latin_upper + OneOrMore(latin_lower | Literal("'"))
)
russian_family_name = russian_name + ZeroOrMore(L("-") + russian_name)
russian_initial = (
    russian_upper | latin_upper | OneOf.from_strs({"Yu", "Ya", "Sh", "Dzh", "Zh", "Ts"})
)
russian_given_names = russian_name + Optional(
    L(" ") + (russian_name | (russian_initial + L(".")))
)
russian_initials = russian_initial + L(".") + Optional(russian_initial + L("."))

ukrainian_upper = cyrillic_upper | L("Ґ") | L("Є") | L("І") | L("Ї")
ukrainian_lower = cyrillic_lower | L("ґ") | L("є") | L("і") | L("ї")
ukrainian_name = (ukrainian_upper + OneOrMore(ukrainian_lower)) | (
    latin_upper + OneOrMore(latin_lower | Literal("'"))
)
ukrainian_family_name = russian_name + Optional(L("-") + ukrainian_name)
ukrainian_initial = (
    ukrainian_upper
    | latin_upper
    | OneOf.from_strs({"Yu", "Ya", "Sh", "Dzh", "Zh", "Ts"})
)
ukrainian_given_names = ukrainian_name + Optional(
    L(" ") + (ukrainian_name | (ukrainian_initial + L(".")))
)
ukrainian_initials = ukrainian_initial + L(".") + Optional(ukrainian_initial + L("."))

burmese_name = latin_upper + ZeroOrMore(latin_lower)
burmese_names = burmese_name + ZeroOrMore(L(" ") + burmese_name)

initials_pattern = initials.compile()
family_name_pattern = family_name.compile()
given_names_pattern = given_names.compile()

spanish_family_name_pattern = spanish_family_name.compile()
portuguese_family_name_pattern = portuguese_family_name.compile()

russian_family_name_pattern = russian_family_name.compile()
russian_given_names_pattern = russian_given_names.compile()
russian_initials_pattern = russian_initials.compile()

ukrainian_family_name_pattern = ukrainian_family_name.compile()
ukrainian_given_names_pattern = ukrainian_given_names.compile()
ukrainian_initials_pattern = ukrainian_initials.compile()

burmese_names_pattern = burmese_names.compile()

chinese_family_name_pattern = chinese_name.compile()
chinese_given_names_pattern = chinese_given_names.compile()
pinyin_family_name_lowercased_pattern = pinyin_family_name.compile()
pinyin_given_names_pattern = pinyin_given_names_cased.compile()
pinyin_given_names_lowercased_pattern = pinyin_given_names.compile()

special_collection = OneOf.from_strs(["in situ", "lost", "untraced", "multiple"])
personal_collection = family_name + L(" collection")
institutional_collection = upper + OneOrMore(upper | lower)
collection = special_collection | personal_collection | institutional_collection
collection_pattern = collection.compile()

collection_code = upper + ZeroOrMore(upper | lower | L("-") | digit)
collection_code_pattern = collection_code.compile()

specimen_label_pattern = re.compile(r"^([^ /\-\.:]+)")


def matches_grammar(text: str, grammar: Pattern[str]) -> bool:
    return bool(grammar.match(text))


def extract_collection_from_type_specimen(specimen: str) -> str | None:
    match = specimen_label_pattern.search(specimen)
    if match:
        return match.group(1)
    return None
