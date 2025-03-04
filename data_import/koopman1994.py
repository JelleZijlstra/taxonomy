import json
import re
from collections import Counter
from collections.abc import Container, Iterable, Iterator, Mapping
from dataclasses import asdict, dataclass, field
from typing import Literal, Self, assert_never, cast

from data_import import lib
from taxonomy import getinput
from taxonomy.db.constants import Rank
from taxonomy.db.models import Article

# Notes: got so far:
# - first doc. up to p. 80
# - koopman1994-2-short: 81-140
# - koopman1994-3-short: 141-150
# - koopman1994-4-short: 151-170 didn't work. Try again later.

SOURCE = lib.Source("koopman1994.md", "Chiroptera (Koopman 1994).pdf")
EXTRA_SOURCES = [
    lib.Source("koopman1994-2-short.md", "Chiroptera (Koopman 1994).pdf"),
    lib.Source("koopman1994-3-short.md", "Chiroptera (Koopman 1994).pdf"),
    lib.Source("koopman1994-4-short.md", "Chiroptera (Koopman 1994).pdf"),
]

HIGHER_RANKS = ["Suborder", "Infraorder", "Superfamily", "Family"]
LOWER_RANKS = ["Subfamily", "Tribe", "Subtribe", "Genus", "Subgenus"]
RANKS = {*HIGHER_RANKS, *LOWER_RANKS}

type ParagraphType = Literal["text", "higher_taxon", "species", "subspecies"]


@dataclass
class Paragraph:
    page: int
    type: ParagraphType
    text: str
    rank: Rank | None


def extract_pages(text: Iterable[str]) -> Iterable[tuple[int, list[str]]]:
    first_interesting = "18"
    for line in text:
        if line.strip() == first_interesting:
            break

    current_page_no = 18
    current_lines: list[str] = []
    for line in text:
        stripped_line = line.strip().strip("*")
        if stripped_line.isnumeric():
            if current_lines:
                yield current_page_no, current_lines
            assert (
                int(stripped_line) == current_page_no + 1
            ), f"Expected {current_page_no + 1}, got {line}"
            current_page_no = int(stripped_line)
            current_lines = []
        else:
            current_lines.append(line)
    yield current_page_no, current_lines


def classify_paragraphs(pairs: Iterable[tuple[int, list[str]]]) -> Iterable[Paragraph]:
    for page, lines in pairs:
        for line in lines:
            line = line.strip()
            if not line:
                continue
            first_word = line.split()[0].strip("*")
            para_type: ParagraphType
            rank: Rank | None = None
            if first_word in RANKS:
                para_type = "higher_taxon"
                rank = Rank[first_word.lower()]
            elif re.match(r"^\**\d+\\?\*? ?\.\*? ", line):
                para_type = "species"
                rank = Rank.species
            elif line.startswith("*") and re.match(
                r"^[A-Z]\. ?[a-zI]\.", line.replace("*", "")
            ):
                para_type = "subspecies"
                rank = Rank.subspecies
            else:
                para_type = "text"
            yield Paragraph(page, para_type, line, rank)


@dataclass
class RepushableIterator[T]:
    iter: Iterator[T]
    repushed: list[T] = field(default_factory=list)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> T:
        if self.repushed:
            return self.repushed.pop()
        return next(self.iter)

    def push(self, item: T) -> None:
        self.repushed.append(item)


def consolidate_text(paras: Iterable[Paragraph]) -> Iterable[Paragraph]:
    it = RepushableIterator(iter(paras))
    for para in it:
        if para.type == "higher_taxon":
            yield para
            continue
        texts = [para.text]
        for inner_para in it:
            if inner_para.type != "text":
                it.push(inner_para)
                break
            texts.append(inner_para.text)
        yield Paragraph(para.page, para.type, " ".join(texts), para.rank)


HIGHER_NAME_RE = re.compile(
    r"""
    ^(?P<rank>[A-Z][a-z]+)
    \s\**(?P<name>[A-Z][a-z]+)\**
    \s(?P<author>(?:[A-Z]\.\s)*[A-Z &\-]+)
    \s(?P<year>\d{4})
    """,
    re.VERBOSE,
)
SPECIES_NAME_RE = re.compile(
    r"""
    ^\**(?P<index>\d+)\\?\*?\s?\.\*?\s # species number
    \**(?P<genus>[A-Z](?:\.|[a-z]*))\**\s?
    \*?(?P<species>[a-z]+)\*?\s
    \(?(?:auctorum,\sprobably\snot\sof\s)?(?P<author>(?:[A-Z]\.\s)*(?:D')?[A-ZÜ &\-,\.']+)\s
    (?P<year>\d{4})
    """,
    re.VERBOSE,
)
SUBSPECIES_RE = re.compile(
    r"""
    (?P<genus>[A-Z])\.\s?
    (?P<species>[a-z])\.\s?
    (?P<subspecies>[a-z]+)\s
    (?: \(=\s*(?P<synonym>[^\(\)]+)\))?\s?
    \((?P<distribution>[^\(\)]+)\)
    """,
    re.VERBOSE,
)
SECTION_SPLIT_REGEX = re.compile(r" \*?\\-\*? ")


def extract_names(paras: Iterable[Paragraph]) -> Iterable[lib.CEDict]:
    it = RepushableIterator(iter(paras))
    source = SOURCE.get_source()
    for para in it:
        match para.type:
            case "higher_taxon":
                text_para = next(it)
                if text_para.type != "text":
                    raise RuntimeError(
                        f"Expected text, got {text_para.type} following {para}"
                    )
                assert para.rank is not None
                match = HIGHER_NAME_RE.search(para.text)
                if match is None:
                    print("!! [failed to match higher_taxon]", para.text)
                    continue
                rank = match.group("rank")
                extra_data: dict[str, str] = {}
                if rank in LOWER_RANKS:
                    pieces = SECTION_SPLIT_REGEX.split(text_para.text)
                    if len(pieces) == 3:
                        description, distribution, diversity = pieces
                        extra_data["description"] = description
                        extra_data["distribution"] = distribution
                        extra_data["diversity"] = diversity
                    else:
                        print("!! [invalid higher_taxon]", len(pieces), "!!", para.text)
                yield lib.CEDict(
                    article=source,
                    name=match.group("name"),
                    authority=match.group("author"),
                    year=match.group("year"),
                    rank=para.rank,
                    page=str(para.page),
                    extra_fields={
                        "name_line": para.text,
                        "text": text_para.text,
                        **extra_data,
                    },
                )
            case "text":
                raise RuntimeError(f"Unexpected text {para}")
            case "species":
                text = re.sub(r"(\d+) \\- (\d+)", r"\1 - \2", para.text)
                text = text.replace(r" group\]. ", r" group]. \- ")
                text = re.sub(r"\s+", " ", text)
                text = text.replace(r" \- \- ", r" \- ")
                pieces = SECTION_SPLIT_REGEX.split(text)
                if len(pieces) != 4:
                    print("!! [invalid species]", len(pieces), "!!", para.text)
                    continue
                name_line, description, distribution, diversity = pieces
                match = SPECIES_NAME_RE.search(name_line)
                if match is None:
                    print("!! [failed to match species]", para.text)
                    continue
                try:
                    next_para = next(it)
                except StopIteration:
                    extra_fields = {}
                else:
                    if next_para.type == "subspecies":
                        extra_fields = {"subspecies": next_para.text}
                    else:
                        extra_fields = {}
                    it.push(next_para)
                name = f"{match.group('genus')} {match.group('species')}"
                yield lib.CEDict(
                    article=source,
                    name=name,
                    authority=match.group("author"),
                    year=match.group("year"),
                    rank=Rank.species,
                    page=str(para.page),
                    extra_fields={
                        "name_line": name_line,
                        "description": description,
                        "distribution": distribution,
                        "diversity": diversity,
                        "species_index": match.group("index"),
                        **extra_fields,
                    },
                )
                if re.search(r"[A-Z]\. [a-z]\.", diversity):
                    match name:
                        case "P. melanotus":
                            # !! [ssp detected in diversity] 22\. *P. melanotus* BLYTH 1863 *\[melanotus* group]. !! Six subspecies are currently recognized: Ranging through a series of small islands from the An damans *(P. m. satyrus, P. m. tytleri)* through the Nicobars *(P. m. melanotus),.* Nias *(P. m. niadicus),* and Enggano *(P. m. modiglianii*) to Christmas island *(P. m. natalis).*
                            yield from _yield_subspecies(
                                "P. m.",
                                [
                                    ("satyrus", "Andamans"),
                                    ("tytleri", "Andamans"),
                                    ("melanotus", "Nicobars"),
                                    ("niadicus", "Nias"),
                                    ("modiglianii", "Enggano"),
                                    ("natalis", "Christmas Island"),
                                ],
                                source,
                                para,
                                diversity,
                            )
                        case "E. spelaea":
                            # !! [ssp detected in diversity] 2\. *E. spelaea* DOBSON 1871 . !! Three poorly defined subspecies, one of which, *E. s. rosenbergi* (northern Celebes), is generally treated as a separate species.
                            yield from _yield_subspecies(
                                "E. s.",
                                [("rosenbergi", "northern Celebes")],
                                source,
                                para,
                                diversity,
                            )
                        case "T. bidens":
                            # !! [ssp detected in diversity] 1\. *T. bidens* (SPIX 1823). !! A single living subspecies *(T. b. bidens).*
                            yield from _yield_subspecies(
                                "T. b.",
                                [("bidens", "single living subspecies")],
                                source,
                                para,
                                diversity,
                            )
                        case "M. schaubi":
                            # !! [ssp detected in diversity] 9\. *M. schaubi* KORMOS 1934 *\[nattereri* group]. !! Originally described on the basis of fossil material from Europe, the sole living subspecies is *M. s. araxenus.*
                            yield from _yield_subspecies(
                                "M. s.",
                                [("araxenus", "sole living subspecies")],
                                source,
                                para,
                                diversity,
                            )
                        case "P. paterculus":
                            # !! [ssp detected in diversity] 13\. *P. paterculus* THOMAS 1915 *\[pipistrellus* group\], !! A subspecies *(P. p. yunnanensis),* has been described from southwestern China.
                            yield from _yield_subspecies(
                                "P. p.",
                                [("yunnanensis", "southwestern China")],
                                source,
                                para,
                                diversity,
                            )
                        case "M. tuberculata":
                            # !! [ssp detected in diversity] 1\. *M. tuberculata* GRAY 1843\. !! Three subspecies are currently recognized, all occurring on North island, but only *M. t. tuberculata* occurring on South island.
                            yield from _yield_subspecies(
                                "M. t.",
                                [
                                    (
                                        "tuberculata",
                                        "all occurring on North island, but only M. t. tuberculata occurring on South island",
                                    )
                                ],
                                source,
                                para,
                                diversity,
                            )
                        case _:
                            print(
                                "!! [ssp detected in diversity]",
                                name_line,
                                "!!",
                                diversity,
                            )
            case "subspecies":
                text = para.text.replace("*", "")
                text = text.replace(". I. ", ". l. ")
                text = re.sub(r"\s+", " ", text)
                text = text.replace(r"( \=", "(=")
                text = text.replace(
                    "and two other subspecies ", ""
                )  # Pteropus dasymallus
                text = text.replace(
                    "E. w. haldemani and E. w. wahlbergi.",
                    "E. w. haldemani, E. w. wahlbergi",
                )
                text = text.replace(
                    "Comoros, (Mafia)", "(Comoros, Mafia)"
                )  # Pteropus seychellensis
                expected_count = 0
                last_match_end = 0
                for match in SUBSPECIES_RE.finditer(text):
                    in_between = text[last_match_end : match.start()]
                    yield from _process_in_between(in_between, para, source)
                    expected_count += 1
                    extra_fields = {
                        "distribution": match["distribution"],
                        "full_subspecies_text": para.text,
                    }
                    if match.group("synonym"):
                        expected_count += 1
                        extra_fields["synonym"] = match.group("synonym")
                    yield lib.CEDict(
                        article=source,
                        name=f"{match['genus']}. {match['species']}. {match['subspecies']}",
                        rank=Rank.subspecies,
                        extra_fields=extra_fields,
                        page=str(para.page),
                    )
                    last_match_end = match.end()
                at_end = text[last_match_end:]
                yield from _process_in_between(at_end, para, source, is_end=True)
            case _:
                assert_never(para.type)


def _yield_subspecies(
    prefix: str,
    subspecies: list[tuple[str, str]],
    source: Article,
    para: Paragraph,
    full_text: str,
) -> Iterable[lib.CEDict]:
    for name, distribution in subspecies:
        yield lib.CEDict(
            article=source,
            name=f"{prefix} {name}",
            rank=Rank.subspecies,
            extra_fields={
                "distribution": distribution,
                "full_subspecies_text": full_text,
            },
            page=str(para.page),
        )


def _process_in_between(
    in_between: str, para: Paragraph, source: Article, *, is_end: bool = False
) -> Iterable[lib.CEDict]:
    in_between = in_between.strip().removesuffix(" and").strip(",").strip()
    if not in_between or in_between in (".", ";"):
        return
    if is_end:
        if in_between.endswith(".") and re.search(
            r"^\. ([A-Z][a-z]{2}|A New|A third|In |\\- [A-Z][a-z]{2})", in_between
        ):
            return
        if "FELTEN & KOCK 1972" in in_between:
            return  # Pteropus anetianus I think
        if "and the remainder on various parts" in in_between:
            return  # Rhinolophus cornutus
        if "but there is some doubt concerning the validity":
            return  # Acerodon mackloti
    for sub in in_between.split(", "):
        if re.fullmatch(r"([A-Z])\. ([a-z])\. ([a-z]{3,})\.?", sub):
            yield lib.CEDict(
                article=source,
                name=sub,
                rank=Rank.subspecies,
                page=str(para.page),
                extra_fields={"full_subspecies_text": para.text},
            )
        elif sub.startswith("three subspecies ("):
            continue  # Megaderma spasma
        else:
            print("!! [leftover in between]", repr(in_between))


def clean_paras(paras: Iterable[Paragraph]) -> Iterable[Paragraph]:
    for para in paras:
        text = para.text
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"(?<=[a-z]{2})- (?=[a-z]{2})", "", text)
        yield Paragraph(para.page, para.type, text, para.rank)


def _clean_dict(d: Mapping[str, object]) -> dict[str, object]:
    new_d: dict[str, object] = {}
    for k, v in d.items():
        if isinstance(v, str):
            new_d[k] = v.strip().replace("\\", "").replace("*", "")
        elif isinstance(v, dict):
            new_d[k] = _clean_dict(v)
        else:
            new_d[k] = v
    return new_d


def clean_text(names: Iterable[lib.CEDict]) -> Iterable[lib.CEDict]:
    for ce in names:
        yield cast(lib.CEDict, _clean_dict(dict(ce)))


def get_text() -> Iterable[str]:
    getinput.print_header(SOURCE)
    yield from lib.get_text(SOURCE)
    for source in EXTRA_SOURCES:
        getinput.print_header(source)
        yield from lib.get_text(source)


def split_abbreviated_species_name(name: str) -> tuple[str, str] | None:
    if match := re.fullmatch(r"([A-Z])\. ([a-z]+)", name):
        return match.group(1), match.group(2)
    return None


def expand_abbreviations(
    names: Iterable[lib.CEDict], overrides: Mapping[str, str]
) -> Iterable[lib.CEDict]:
    current_genus: str | None = None
    current_epithet: str | None = None

    for name in names:
        if name["rank"] is Rank.genus:
            current_genus = name["name"]
            current_epithet = None
        elif name["rank"] > Rank.genus:
            current_epithet = None
            current_genus = None
        elif (
            name["rank"] is Rank.species
            and "corrected_name" not in name
            and current_genus is not None
        ):
            current_epithet = name["name"].split()[-1]
            if pair := split_abbreviated_species_name(name["name"]):
                initial, epithet = pair
                if initial == current_genus[0]:
                    corrected_name = f"{current_genus} {epithet}"
                    yield {**name, "corrected_name": corrected_name}
                    continue
        elif (
            name["rank"] is Rank.subspecies
            and "corrected_name" not in name
            and current_genus is not None
            and current_epithet is not None
        ):
            extra_fields: dict[str, str] = {}
            if "parent" in name and name["parent"] is not None:
                maybe_pair = split_abbreviated_species_name(name["parent"])
                if maybe_pair is not None:
                    parent_initial, parent_epithet = maybe_pair
                    if parent_initial == current_genus[0]:
                        extra_fields["parent"] = f"{current_genus} {parent_epithet}"

            if name["name"] in overrides:
                corrected_name = overrides[name["name"]]
                yield {**name, "corrected_name": corrected_name, **extra_fields}  # type: ignore[typeddict-item]
                continue
            match = re.fullmatch(r"([A-Z])\. ([a-z])\. ([a-z]+)", name["name"])
            if match is not None:
                initial, species_initial, subspecies = match.groups()
                if (
                    initial == current_genus[0]
                    and species_initial == current_epithet[0]
                ):
                    corrected_name = f"{current_genus} {current_epithet} {subspecies}"
                    yield {**name, "corrected_name": corrected_name, **extra_fields}  # type: ignore[typeddict-item]
                    continue
        yield name


def validate_species_indexes(
    names: Iterable[lib.CEDict], ignore: Container[str] = frozenset()
) -> Iterable[lib.CEDict]:
    for name in names:
        if name["rank"] is Rank.genus:
            expected_index = 1
        elif name["rank"] is Rank.species:
            index = int(name.get("extra_fields", {})["species_index"])
            if index != expected_index and name["name"] not in ignore:
                print("!! [invalid species index]", name["name"], index, expected_index)
            expected_index = index + 1
        yield name


def extract_species_count(name: lib.CEDict) -> int | None:
    if "extra_fields" not in name:
        return None
    for key in ("diversity", "text"):
        if key not in name["extra_fields"]:
            continue
        match = re.search(r" (\d+) species", name["extra_fields"][key])
        if match is not None:
            return int(match.group(1))
    return None


def validate_species_counts(names: Iterable[lib.CEDict]) -> Iterable[lib.CEDict]:
    stack: list[tuple[Rank, str, int, lib.CEDict]] = []
    counts: Counter[tuple[Rank, str]] = Counter()

    def flush_up_to(rank: Rank) -> None:
        while stack and stack[-1][0] <= rank:
            parent_rank, parent_name, species_count, ce_dict = stack.pop()
            actual_count = counts[(parent_rank, parent_name)]
            ce_dict.setdefault("extra_fields", {})["actual_species_count"] = str(
                actual_count
            )
            if species_count != actual_count:
                print(
                    f"!! [invalid species count] {parent_rank.name} {parent_name} {species_count} (claimed) != {actual_count} (actual)"
                )

    for name in names:
        if name["rank"] is Rank.species:
            for parent_rank, parent_name, _, _ in stack:
                counts[(parent_rank, parent_name)] += 1
        if name["rank"] > Rank.species:
            flush_up_to(name["rank"])
            if species_count := extract_species_count(name):
                name.setdefault("extra_fields", {})["claimed_species_count"] = str(
                    species_count
                )
                stack.append((name["rank"], name["name"], species_count, name))
        yield name

    flush_up_to(Rank.order)


IntOrRange = int | tuple[int, int]  # e.g. 1 or (1, 2) for 1-2


def sum_int_or_ranges(numbers: Iterable[IntOrRange]) -> tuple[int, int]:
    lower = 0
    upper = 0
    for number in numbers:
        if isinstance(number, int):
            lower += number
            upper += number
        else:
            lower += number[0]
            upper += number[1]
    return (lower * 2, upper * 2)


@dataclass
class DentalFormula:
    prefix: str | None
    upper_i: IntOrRange
    lower_i: IntOrRange
    upper_c: IntOrRange
    lower_c: IntOrRange
    upper_p: IntOrRange
    lower_p: IntOrRange
    upper_m: IntOrRange
    lower_m: IntOrRange
    total: IntOrRange

    def validate(self) -> str | None:
        lower, upper = sum_int_or_ranges(
            [
                self.upper_i,
                self.lower_i,
                self.upper_c,
                self.lower_c,
                self.upper_p,
                self.lower_p,
                self.upper_m,
                self.lower_m,
            ]
        )
        if lower == upper:
            if lower != self.total:
                return f"!! [invalid dental formula] {lower} != {self.total}"
        elif (lower, upper) != self.total:
            return f"!! [invalid dental formula] {lower}-{upper} != {self.total}"
        return None

    def __str__(self) -> str:
        def s(value: IntOrRange) -> str:
            if isinstance(value, int):
                return str(value)
            return f"{value[0]}-{value[1]}"

        prefix = f"{self.prefix} " if self.prefix is not None else ""
        return (
            f"{prefix}"
            f"i {s(self.lower_i)}/{s(self.upper_i)}, "
            f"c {s(self.lower_c)}/{s(self.upper_c)}, "
            f"p {s(self.lower_p)}/{s(self.upper_p)}, "
            f"m {s(self.lower_m)}/{s(self.upper_m)} "
            f"x 2 = {s(self.total)}"
        )

    def to_json_string(self) -> str:
        data = asdict(self)
        if error := self.validate():
            data["error"] = error
        return json.dumps(data)

    @classmethod
    def parse(cls, text: str) -> Self:
        text = (
            text.replace("¡", "i")
            .replace("—", "-")
            .replace("–", "-")
            .lstrip(",")
            .strip()
        )
        g = r"[\diIl\-]+"
        rgx = re.compile(
            rf"""
            (?P<prefix>[a-z ]*)
            i\s*(?P<i>{g})\s*/\s*(?P<I>{g}),\s*
            c\s*(?P<c>{g})\s*/\s*(?P<C>{g}),\s*
            p\s*(?P<p>{g})\s*/\s*(?P<P>{g}),\s*
            m\s*(?P<m>{g})\s*/\s*(?P<M>{g})\s*
            x\s*2\s*=\s*(?P<total>{g})
            """,
            re.VERBOSE,
        )
        match = rgx.fullmatch(text)
        assert match is not None, f"Failed to parse dental formula {text}"

        def p(text: str) -> IntOrRange:
            text = text.replace("l", "1").replace("i", "1").replace("I", "1")
            if "-" in text:
                lower, upper = text.split("-")
                return (int(lower), int(upper))
            return int(text)

        prefix = match.group("prefix").strip()
        return cls(
            prefix=prefix if prefix else None,
            upper_i=p(match.group("I")),
            lower_i=p(match.group("i")),
            upper_c=p(match.group("C")),
            lower_c=p(match.group("c")),
            upper_p=p(match.group("P")),
            lower_p=p(match.group("p")),
            upper_m=p(match.group("M")),
            lower_m=p(match.group("m")),
            total=p(match.group("total")),
        )


def validate_dental_formula(formula: str) -> tuple[str, DentalFormula | None]:
    try:
        df = DentalFormula.parse(formula)
        if error := df.validate():
            print(formula, error)
        return str(df), df
    except Exception:
        print(formula)
        return formula, None


def extract_dental_formula(names: Iterable[lib.CEDict]) -> Iterable[lib.CEDict]:
    for name in names:
        if "extra_fields" in name and "description" in name["extra_fields"]:
            description = name["extra_fields"]["description"]
            match = re.search(
                r"Den ?tal for ?mula(?: normally)?:?(.*=[ \d\-\\)]+)", description
            )
            if match is not None:
                dental_formula = match.group(1).strip()
                validated_df, df = validate_dental_formula(dental_formula)
                name["extra_fields"]["dental_formula"] = validated_df
                if df is not None:
                    name["extra_fields"]["dental_formula_json"] = df.to_json_string()
                    for key, value in asdict(df).items():
                        if value is None:
                            continue
                        value_str = (
                            "-".join(map(str, value))
                            if isinstance(value, tuple)
                            else str(value)
                        )
                        name["extra_fields"][f"dental_formula_{key}"] = value_str
                    if error := df.validate():
                        name["extra_fields"]["dental_formula_error"] = error
            # elif "dental" in description.lower() or "formula" in description.lower():
            #     print("!! [no dental formula]", description)
        yield name


def main() -> None:
    text = get_text()
    pages = extract_pages(text)
    paras = classify_paragraphs(pages)
    paras = consolidate_text(paras)
    paras = clean_paras(paras)
    names = extract_names(paras)
    names = clean_text(names)
    names = lib.add_parents(names)
    names = expand_abbreviations(
        names,
        overrides={
            "P. c. edulis": "Pteropus vampyrus edulis",
            "E. s. fraternus": "Macroglossus sobrinus fraternus",
            "E. s. sobrinus": "Macroglossus sobrinus sobrinus",
            "R. c. cupidus": "Hipposideros calcaratus cupidus",
            "R. c. calcaratus": "Hipposideros calcaratus calcaratus",
            "R. m. intermedia": "Mormoops megalophylla intermedia",
            "A. s. senex": "Centurio senex senex",
            "A. s. greenhalli": "Centurio senex greenhalli",
            "N. i. floridanus": "Lasiurus intermedius floridanus",
            "N. i. intermedius": "Lasiurus intermedius intermedius",
            "N. i. insularis": "Lasiurus intermedius insularis",
        },
    )
    # names = lib.validate_ce_parents(names)
    names = validate_species_indexes(
        names, ignore={"M. lyra"}  # both Megaderma spp. are numbered 1
    )
    names = validate_species_counts(names)
    names = extract_dental_formula(names)
    names = lib.flag_unrecognized_names(names)
    names = list(names)
    lib.create_csv("koopman1994.csv", names)
    # names = lib.add_classification_entries(names, dry_run=False)
    lib.print_ce_summary(names)
    # lib.format_ces(SOURCE)


if __name__ == "__main__":
    main()
