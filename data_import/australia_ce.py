import enum
import re
from collections import Counter, deque
from collections.abc import Iterable
from dataclasses import dataclass
from typing import assert_never

from taxonomy.db import helpers
from taxonomy.db.constants import Rank
from taxonomy.db.models.article.article import Article
from taxonomy.db.models.classification_entry.ce import ClassificationEntryTag

from . import lib

SOURCE = lib.Source(
    "australia-decolumnized.txt", "Mammalia Australia (Jackson & Groves 2015).pdf"
)
RefsDictT = dict[tuple[str, str], str]
COMMON_NAME_RGX = re.compile(r"^[A-Z][a-z\-’]+( [A-Z][a-z\-]+)*$")
RANK_WORDS = {
    "Subtribe",
    "Tribe",
    "Subfamily",
    "Semisuborder",
    "Suborder",
    "Family",
    "Order",
    "Infraorder",
    "Superorder",
    "Mirorder",
    "Parvorder",
    "Supercohort",
    "Subcohort",
    "Clade",
    "Superfamily",
    "Class",
    "Klasse",
    "Sub-Kingdom",
    "Series",
    "Subclass",
    "Grandes Ramas",
    "Section",
    "Cohort",
    "Grand Group",
    "Type",
    "Kind",
    "Infraclass",
    "Sub-class",
    "Grand Tribe",
    "Superlegion",
    "Group",
    "Legion",
    "Sublegion",
    "Race",
    "Sub Classe",
    "Grand Seccion",
    "Classe",
    "Sectio",
    "Tribus",
}
PATTERN = re.compile(rf'^({"|".join(RANK_WORDS)})\?? ')


class LineKind(enum.Enum):
    blank = 1
    section = 2
    continuation = 3
    taxon_header = 4
    name_header = 5
    common_name = 6
    reference = 7


@dataclass
class Line:
    kind: LineKind
    text: str
    page: int


def can_be_name_header(line: str) -> bool:
    if len(line) > 100:
        return False
    match = re.match(
        (
            r"^.* [A-Z].*, \d{4}[a-z]?: (Table |Plate |Text to Plate |col\."
            r" )?([ivxlc]+|\d+)(, (\d+|footnote))*\.$"
        ),
        line,
    )
    return bool(match) and (line[0].isupper() or line[0] in ("[", "Φ", "$", "†", "Ω"))


def classify_lines(pages: Iterable[tuple[int, list[str]]]) -> Iterable[Line]:
    found_references = False
    found_beginning = False
    in_homonyms = False
    line_kinds: deque[LineKind] = deque(maxlen=5)

    def classify_line(line: str, *, in_homonyms: bool) -> LineKind | None:
        nonlocal found_references
        if found_references:
            if not line:
                return None
            elif line.strip() == "This page intentionally left blank":
                return None
            elif line.startswith(" "):
                return LineKind.continuation
            else:
                return LineKind.reference
        else:
            if not line:
                return LineKind.blank
            elif line.strip() == "References":
                found_references = True
                return None
            spaces = lib.initial_count(line, " ")
            if spaces == 0:
                # either section continuation or name line
                if line.startswith(("Φ", "$", "Ω")):
                    return LineKind.name_header
                elif line.startswith("\\"):
                    return LineKind.common_name
                elif line_kinds[-1] == LineKind.section:
                    return LineKind.continuation
                elif line_kinds[-1] == LineKind.continuation:
                    if can_be_name_header(line):
                        return LineKind.name_header
                    else:
                        return LineKind.continuation
                elif line_kinds[-1] == LineKind.taxon_header:
                    if COMMON_NAME_RGX.match(line):
                        return LineKind.common_name
                    else:
                        if not can_be_name_header(line):
                            print("---")
                            print(line)
                        assert can_be_name_header(line), line
                        return LineKind.name_header
                elif line_kinds[-1] == LineKind.name_header:
                    return LineKind.continuation
                elif line_kinds[-1] == LineKind.common_name:
                    if not can_be_name_header(line):
                        print("---")
                        print(line)
                    assert can_be_name_header(line), line
                    return LineKind.name_header
                else:
                    assert line_kinds[-1] == LineKind.blank, (line, line_kinds[-1])
                    if line_kinds[-2] == LineKind.common_name or line.startswith(
                        ("Φ", "$", "†", "Ω")
                    ):
                        return LineKind.name_header
                    elif line_kinds[-2] == LineKind.taxon_header:
                        if COMMON_NAME_RGX.match(line):
                            return LineKind.common_name
                        else:
                            return LineKind.name_header
                    elif can_be_name_header(line):
                        return LineKind.name_header
                    else:
                        # TODO: this probably has false negatives
                        return LineKind.continuation

            elif (
                spaces == 2 or (2 < spaces <= 4 and line.lstrip()[0].islower())
            ) and ":" in line:
                # section label, like "comments"
                return LineKind.section
            elif spaces > 2:
                if in_homonyms and (
                    line_kinds[-1] in (LineKind.continuation, LineKind.section)
                ):
                    return LineKind.continuation
                else:
                    return LineKind.taxon_header
            elif "sensu" in line:
                return LineKind.taxon_header
            elif re.match(
                r"^ +([†Φ] )?(Suborder |Subfamily |Family |Cohort |Superorder )?"
                r"[A-Z][a-z]+( [a-z]+){0,2} +\(?([A-Z]\."
                r" )?[A-Z](cK)?[a-zé]+(-[A-Z][a-z]+)?"
                r"( & [A-Z](cK)?[a-z]+)?( et al\.)?, \d{4}\)?$",
                line,
            ):
                return LineKind.taxon_header
            else:
                assert False, f"unrecognized line {line}"

    for page, lines in pages:
        for line in lines:
            line = line.rstrip()
            if not found_beginning:
                if line.strip() == "Class Mammalia Linnaeus, 1758":
                    found_beginning = True
                else:
                    continue

            kind = classify_line(line, in_homonyms=in_homonyms)
            if kind is None:
                continue
            if kind is LineKind.section:
                if line.lstrip().startswith("homonyms"):
                    in_homonyms = True
                else:
                    in_homonyms = False
            elif kind in (LineKind.taxon_header, LineKind.name_header):
                in_homonyms = False
            line_kinds.append(kind)
            yield Line(kind, line, page)


def merge_continuations(lines: Iterable[Line]) -> Iterable[Line]:
    def should_merge(previous: Line, item: Line) -> bool:
        return item.kind is LineKind.continuation or (
            item.kind is LineKind.taxon_header
            and previous.kind is LineKind.taxon_header
        )

    def merge(previous: Line, item: Line) -> Line:
        return Line(previous.kind, previous.text + " " + item.text, previous.page)

    return lib.merge_adjacent(lines, should_merge, merge)


@dataclass
class Account:
    intro_line: Line
    common_name: str | None
    sections: list[tuple[str, str]]


@dataclass
class Reference:
    text: str


def merge_accounts(lines: Iterable[Line]) -> Iterable[Account | Reference]:
    current_account: Account | Reference | None = None
    for item in lines:
        match item.kind:
            case LineKind.taxon_header | LineKind.name_header:
                if current_account:
                    yield current_account
                current_account = Account(item, None, [])
            case LineKind.common_name:
                assert isinstance(current_account, Account)
                assert current_account.common_name is None
                current_account.common_name = item.text
            case LineKind.section:
                assert isinstance(current_account, Account)
                assert ":" in item.text
                label, text = item.text.split(":", maxsplit=1)
                label = label.strip()
                label = re.sub(r"\s+", " ", label)
                current_account.sections.append((label.lower(), text))
            case LineKind.reference:
                if current_account:
                    yield current_account
                    current_account = None
                yield Reference(item.text)
            case LineKind.blank | LineKind.continuation:
                pass
            case _:
                assert_never(item.kind)

    assert current_account is None


def split_accounts(
    data: Iterable[Account | Reference],
) -> tuple[list[Account], list[Reference]]:
    accounts: list[Account] = []
    refs: list[Reference] = []
    for item in data:
        if isinstance(item, Account):
            accounts.append(item)
        else:
            refs.append(item)
    return accounts, refs


def validate_accounts(accounts: Iterable[Account]) -> Iterable[Account]:
    for account in accounts:
        sections = Counter(label for label, _ in account.sections)
        for label in sections:
            if sections[label] > 1:
                print(
                    f"!! Duplicate section {label!r} for {account.intro_line.text} on page {account.intro_line.page}"
                )
        yield account


def build_refs_dict(refs: Iterable[Reference]) -> RefsDictT:
    refs_dict: RefsDictT = {}
    for ref in refs:
        # Adams M, Baverstock PR, Watts CHS, Reardon T (1987a)
        #    Electrophoretic resolution of species boundaries in
        #    Australian Microchiroptera, II. The Pipistrellus group
        #    (Chiroptera: Vespertilionidae). Australian Journal of
        #    Biological Sciences 40, 163–170.

        text = helpers.clean_string(ref.text)
        match = re.match(
            (
                r"(?P<authors>[^\(]+)( \(eds?\.\))? \((?P<year>\d{4}(–\d{4})?[a-z]?("
                r" \[[\d–-]+\])?)\)"
            ),
            text,
        )
        assert match, f"failed to match {text}"
        year = match.group("year")
        raw_authors = match.group("authors")
        num_commas = raw_authors.count(",")
        if num_commas == 0:
            authors = _translate_single_author(raw_authors)
        elif num_commas == 1:
            author1, author2 = raw_authors.split(", ")
            authors = (
                f"{_translate_single_author(author1)} &"
                f" {_translate_single_author(author2)}"
            )
        else:
            authors = raw_authors
        key = authors, year
        assert (
            key not in refs_dict
        ), f"duplicate key {key!r} (new: {text}, existing: {refs_dict[key]}"
        refs_dict[key] = text
    return refs_dict


def _translate_single_author(raw_author: str) -> str:
    for suffix in (" von", " Von", " Jr"):
        raw_author = raw_author.removesuffix(suffix)
    author = raw_author.rsplit(maxsplit=1)[0]
    if author in (
        "Cuvier",
        "Gray",
        "Geoffroy Saint-Hilaire",
        "Müller",
        "Reichenbach",
        "Ogilby",
        "Fraser",
        "Fischer",
        "Schulze",
        "Andrews",
        "Wilson",
        "Smith",
        "Scott",
        "Brehm",
        "Allen",
        "Turner",
        "Hamilton",
        "Brown",
        "Archer",
        "Taylor",
        "Murray",
        "Heller",
        "Roberts",
    ):
        author = re.sub(r"^([A-Z][a-z A-Z\-]+) ([A-Z])[A-Z]?$", r"\2. \1", raw_author)
    if author == "Gray":
        author = "J. Gray"
    return author


YEAR = r"(?P<year>\d{4})[a-z]?"
AUTHOR = r"(?P<author>(de\s|van\s|d')?[A-ZÉ][a-zA-Z\s\.&ö\-üéè,]+)"
HIGHER_TAXON_RE = re.compile(
    rf"""
    ^(?P<rank>Class|Subclass|Infraclass|Supercohort|Cohort|Subcohort|Legion|Sublegion|Infralegion|Superlegion|Superorder|Order|Suborder|Infraorder|Parvorder|Superfamily|Family|Subfamily|Tribe|Subtribe)\s
    (?P<name>[A-Z][a-z]+)\s
    {AUTHOR},\s
    {YEAR}$
    """,
    re.VERBOSE,
)
GENUS_RE = re.compile(
    rf"""
    ^(?P<name>[A-Z][a-z]+)\s
    {AUTHOR},\s
    {YEAR}$
    """,
    re.VERBOSE,
)
SPECIES_RE = re.compile(
    rf"""
    ^(?P<name>[A-Z][a-z]+\s[a-z]+)\s
    \(?{AUTHOR},\s
    {YEAR}\)?$
    """,
    re.VERBOSE,
)
SUBSPECIES_RE = re.compile(
    rf"""
    ^(?P<name>[A-Z][a-z]+\s[a-z]+\s[a-z]+)\s
    \(?{AUTHOR},\s
    {YEAR}\)?$
    """,
    re.VERBOSE,
)


def translate_taxon(line: Line, source: Article) -> lib.CEDict | None:
    name_line = line.text.strip().lstrip("Φ$†Ω").strip()
    name_line = helpers.clean_string(name_line)
    if " sensu " in name_line:
        name_line = name_line.split(" sensu ")[0]
    if match := HIGHER_TAXON_RE.fullmatch(name_line):
        rank_text = match.group("rank").lower()
        textual_rank: str | None = None
        match rank_text:
            case "class":
                rank = Rank.class_
            case "superlegion" | "legion" | "sublegion" | "infralegion":
                rank = Rank.other
                textual_rank = match.group("rank")
            case _:
                rank = Rank[rank_text]
        return lib.CEDict(
            page=str(line.page),
            name=match.group("name"),
            rank=rank,
            textual_rank=textual_rank,
            authority=match.group("author"),
            year=match.group("year"),
            article=source,
            extra_fields={"name_line": line.text},
        )
    else:
        for rgx, rank in [
            (GENUS_RE, Rank.genus),
            (SPECIES_RE, Rank.species),
            (SUBSPECIES_RE, Rank.subspecies),
        ]:
            if match := rgx.fullmatch(name_line):
                return lib.CEDict(
                    page=str(line.page),
                    name=match.group("name"),
                    rank=rank,
                    authority=match.group("author"),
                    year=match.group("year"),
                    article=source,
                    extra_fields={"name_line": line.text},
                )
        match name_line:
            case "Incertae Sedis" | "nomen dubium" | "Delphinus Incertae sedis":
                return None
            case "Family Acrobatidae Aplin (in Aplin and Archer), 1987":
                return lib.CEDict(
                    page=str(line.page),
                    name="Acrobatidae",
                    rank=Rank.family,
                    authority="Aplin (in Aplin & Archer)",
                    year="1987",
                    article=source,
                    extra_fields={"name_line": line.text},
                )
            case "Tribe Setonichini New Tribe":
                return lib.CEDict(
                    page=str(line.page),
                    name="Setonichini",
                    rank=Rank.tribe,
                    article=source,
                    extra_fields={"name_line": line.text},
                )
            case _:
                print(f"!! unrecognized taxon line {name_line!r}")
                return None


def split_text(accounts: Iterable[Account]) -> Iterable[lib.CEDict]:
    source = SOURCE.get_source()
    syn_parent: str | None = None
    syn_parent_rank: Rank | None = None
    for account in accounts:
        if account.intro_line.kind is LineKind.taxon_header:
            row = translate_taxon(account.intro_line, source)
            if row is not None:
                yield row
                syn_parent = row["name"]
                syn_parent_rank = row["rank"]
            else:
                syn_parent = None
                syn_parent_rank = None
            continue
        name_line = account.intro_line.text
        extra_fields = {"name_line": name_line, **dict(account.sections)}
        name_line = re.sub(r"^[Φ\$†Ω] ", "", name_line.rstrip("."))
        name_line = re.sub(r" \[sic\]", "", name_line)
        name_line = re.sub(r"\. \[([a-z]+)\]", r"\1", name_line)
        name_line = PATTERN.sub("", name_line).lstrip()
        match = re.match(
            (
                r"^(?P<name_author>[^,]+)(, in [^,]+)?, (?P<year>\d{4}[^:]*):"
                r" (?P<page>.*)$"
            ),
            name_line,
        )
        assert match, name_line
        year = match.group("year")[:4]
        name_author = match.group("name_author")
        data = lib.split_name_authority(name_author, quiet=True)
        tags = []
        if "original_name" not in data:
            if name_match := re.fullmatch(
                r"(?P<original_name>[A-Z][a-z]+ (Group|Division)) (?P<authority>sensu Misonne)",
                name_author,
            ):
                data = name_match.groupdict()
                tags.append(ClassificationEntryTag.Informal)
            else:
                raise ValueError(
                    f"failed to match {name_author!r} (from {name_line!r})"
                )
        yield lib.CEDict(
            page=str(account.intro_line.page),
            name=data["original_name"],
            rank=Rank.synonym,
            authority=data["authority"],
            year=year,
            page_described=match.group("page"),
            article=source,
            extra_fields=extra_fields,
            scratch_space={"raw_year": match.group("year")},
            parent=syn_parent,
            parent_rank=syn_parent_rank,
        )


def rank_key(rank: Rank | str) -> int:
    match rank:
        case Rank():
            return rank.value
        case "Superlegion":
            return 94
        case "Legion":
            return 93
        case "Sublegion":
            return 92
        case "Infralegion":
            return 91
        case _:
            raise ValueError(f"unrecognized rank {rank!r}")


def add_parents(ces: Iterable[lib.CEDict]) -> Iterable[lib.CEDict]:
    parent_stack: list[tuple[Rank | str, str]] = []
    for ce in ces:
        if ce["rank"] is Rank.synonym and not ce.get("parent"):
            if " " in ce["name"]:
                ce["rank"] = Rank.synonym_species
            else:
                ce["rank"] = Rank.synonym_genus
            yield ce
            continue
        if "parent" in ce or ce["rank"] is Rank.synonym:
            yield ce
            continue
        rank: Rank | str
        if ce["rank"] is Rank.other:
            assert "textual_rank" in ce and ce["textual_rank"] is not None
            rank = ce["textual_rank"]
        else:
            rank = ce["rank"]
        rank_value = rank_key(rank)
        while parent_stack and rank_key(parent_stack[-1][0]) <= rank_value:
            parent_stack.pop()

        if parent_stack:
            expected_parent_rank, expected_parent = parent_stack[-1]
            ce["parent"] = expected_parent
            if isinstance(expected_parent_rank, str):
                ce["parent_rank"] = Rank.other
            else:
                ce["parent_rank"] = expected_parent_rank

        parent_stack.append((rank, ce["name"]))
        yield ce


def associate_refs(
    names: Iterable[lib.CEDict], refs_dict: RefsDictT, *, verbose: bool = False
) -> Iterable[lib.CEDict]:
    for name in names:
        if "authority" in name and "scratch_space" in name:
            year = name["scratch_space"]["raw_year"]
            if "[" in year:
                year = year.split("[")[1].rstrip("]")
            key = name["authority"], year
            if key in refs_dict:
                name["citation"] = refs_dict[key]
            elif verbose:
                possible = [key for key in refs_dict if name["authority"] in key[0]]
                print("!! missing ref", key, possible)
        yield name


def print_summary(ces: Iterable[lib.CEDict]) -> Iterable[lib.CEDict]:
    for ce in ces:
        yield ce
        if ce["rank"] is Rank.synonym:
            continue
        rank_val: str | Rank
        if ce["rank"] is Rank.other:
            assert "textual_rank" in ce and ce["textual_rank"] is not None
            rank_val = ce["textual_rank"]
        else:
            rank_val = ce["rank"]
        rank = rank_key(rank_val)
        indentation = " " * (Rank.class_.value - rank)
        print(f"{indentation}{ce['rank'].name} {ce['name']}")


def main() -> None:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    line_objs = classify_lines(pages)
    line_objs = merge_continuations(line_objs)
    data = merge_accounts(line_objs)
    accounts_list, refs = split_accounts(data)
    accounts = validate_accounts(accounts_list)
    ces = split_text(accounts)
    ces = add_parents(ces)
    ces = lib.validate_ce_parents(ces)
    refs_dict = build_refs_dict(refs)
    ces = associate_refs(ces, refs_dict)
    ces = lib.add_classification_entries(
        ces, dry_run=False, strict=True, delete_uncovered=True
    )
    # ces = print_summary(ces)
    lib.print_ce_summary(ces)
    lib.format_ces(SOURCE)
    print(f"{len(refs_dict)} refs")


if __name__ == "__main__":
    main()
