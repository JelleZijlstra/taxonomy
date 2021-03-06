from dataclasses import dataclass
from functools import partial
from typing import Set, Iterable, Pattern, List, Optional as TOptional
import re


def unicode_range(start: str, end: str) -> Set[str]:
    return {chr(i) for i in range(ord(start), ord(end) + 1)}


class Element:
    def to_regex(self) -> str:
        raise NotImplementedError

    def compile(self) -> Pattern[str]:
        return re.compile(f"^{self.to_regex()}$")

    def __or__(self, other: "Element") -> "OneOf":
        return OneOf([self, other])

    def __add__(self, other: "Element") -> "And":
        return And([self, other])


@dataclass
class Char(Element):
    alternatives: List[str]

    def to_regex(self) -> str:
        return f"[{''.join(self.alternatives)}]"


@dataclass
class Literal(Element):
    text: str

    def to_regex(self) -> str:
        return re.escape(self.text)


@dataclass
class OneOf(Element):
    alternatives: List[Element]

    @classmethod
    def from_strs(cls, strs: Iterable[str]) -> "OneOf":
        return cls([Literal(s) for s in strs])

    def to_regex(self) -> str:
        return "|".join(f"({e.to_regex()})" for e in self.alternatives)


@dataclass
class And(Element):
    pieces: List[Element]

    def to_regex(self) -> str:
        return "".join(f"({e.to_regex()})" for e in self.pieces)


@dataclass
class Repetition(Element):
    elt: Element
    min: TOptional[int] = None
    max: TOptional[int] = None

    def to_regex(self) -> str:
        return f"({self.elt.to_regex()}){{{self.min or ''},{self.max or ''}}}"


Optional = partial(Repetition, min=0, max=1)
ZeroOrMore = partial(Repetition, min=0, max=None)
OneOrMore = partial(Repetition, min=1, max=None)


C = Char
L = Literal

upper = C(
    sorted(
        unicode_range("A", "Z")
        | unicode_range("Α", "Ρ")
        | unicode_range("Σ", "Ω")
        | {
            "İ",
            "À",
            "Á",
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
            "Ó",
            "Ö",
            "Ø",
            "Ú",
            "Ü",
            "Ľ",
            "Ő",
            "Ō",
            "Đ",
            "Ł",
            "Ş",
            "Š",
            "Ș",
            "Ś",
            "Ř",
            "Ş",
            "Ż",
            "Ž",
            "Œ",
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
            "ğ",
            "ı",
            "ľ",
            "ł",
            "ń",
            "ň",
            "ō",
            "ő",
            "ř",
            "ś",
            "ş",
            "š",
            "ţ",
            "ť",
            "ů",
            "ư",
            "ý",
            "ź",
            "ż",
            "ž",
            "ơ",
            "ǎ",
            "ǧ",
            "ș",
            "ț",
            "ð",
        }
    )
)
cyrillic_lower = C(sorted(unicode_range("а", "я")))
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
    "d'",
    "da ",
    "dal ",
    "das ",
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
    "della " "e ",
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
name_connectors = {"-", " i ", " y ", " e ", "-i-", "'"}
name = (
    Optional(OneOf.from_strs(name_prefixes)) + upper + OneOrMore(lower)
) | OneOf.from_strs(["ffolliott", "LuAnn"])
spanish_second_name = (latin_upper | L("Á")) + L(".")
compound_name = name + ZeroOrMore(
    OneOf.from_strs(name_connectors) + (name | spanish_second_name)
)
names = compound_name + ZeroOrMore(
    L(" ") + Optional(OneOf.from_strs(name_prefixes)) + compound_name
)
special_family_names = {"MacC.", "S.D.W."}
family_name = (
    Optional(OneOf.from_strs(name_prefixes))
    + names
    + Optional(L(" ") + spanish_second_name)
) | OneOf.from_strs(special_family_names)
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
pinyin_given_names = pinyin_syllable + Optional(L("-") + pinyin_syllable)
pinyin_family_name = pinyin_syllable | OneOf.from_strs(
    ["Ouyang", "Jinggong", "Jiangzuo"]
)

chinese_lower = C(sorted(unicode_range("a", "z") | {"ü"}))
chinese_name = latin_upper + OneOrMore(chinese_lower)
pinyin_given_names_cased = chinese_name + Optional(L("-") + OneOrMore(chinese_lower))
chinese_given_names = chinese_name + Optional(
    (L("-") | L(" ")) + Optional(latin_upper) + OneOrMore(chinese_lower)
)

russian_name = (cyrillic_upper + OneOrMore(cyrillic_lower)) | (
    latin_upper + OneOrMore(latin_lower | Literal("'"))
)
russian_family_name = russian_name + Optional(L("-") + russian_name)
russian_initial = (
    cyrillic_upper
    | latin_upper
    | OneOf.from_strs({"Yu", "Ya", "Sh", "Dzh", "Zh", "Ts"})
)
russian_given_names = russian_name + Optional(
    L(" ") + (russian_name | (russian_initial + L(".")))
)
russian_initials = russian_initial + L(".") + Optional(russian_initial + L("."))

burmese_name = latin_upper + ZeroOrMore(latin_lower)
burmese_names = burmese_name + ZeroOrMore(L(" ") + burmese_name)

initials_pattern = initials.compile()
family_name_pattern = family_name.compile()
given_names_pattern = given_names.compile()

russian_family_name_pattern = russian_family_name.compile()
russian_given_names_pattern = russian_given_names.compile()
russian_initials_pattern = russian_initials.compile()

burmese_names_pattern = burmese_names.compile()

chinese_family_name_pattern = chinese_name.compile()
chinese_given_names_pattern = chinese_given_names.compile()
pinyin_family_name_lowercased_pattern = pinyin_family_name.compile()
pinyin_given_names_pattern = pinyin_given_names_cased.compile()
pinyin_given_names_lowercased_pattern = pinyin_given_names.compile()


def matches_grammar(text: str, grammar: Pattern[str]) -> bool:
    return bool(grammar.match(text))
