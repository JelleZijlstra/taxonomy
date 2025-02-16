# goal: 1135 genera, 4629 species
# Chiroptera given 925, actual 923
# Rodentia given 2021, actual 2015
# Phyllostomidae given 143, actual 141
# Vespertilionidae given 318, actual 316

"""

The volume claims 4629 species; in fact I find 4629.
Summing up the counts per order (page 4 of MSW2) produces 4635
(3+63+5+1+63+21+2+117+29+428+19+2+925+233+271+78+5+2+18+6+1+220+7+2021+80+15).

There are discrepancies in at least two orders: Chiroptera and Rodentia.

Chiroptera is claimed to have 925 species.
This is correct but the number of species in the families actually sums to 927.
Phyllostomidae is claimed to have 143 species, but I find 141. Summing up the subfamilies
also produces 141.

Rodentia is claimed to have 2021 species, but only 2015 are listed. All the counts per family
are actually correct. The number for the suborder Hystricognathi (229) is also right,
but for Sciurognathi 1793 is claimed but the actual number is 1786. (Note also that the claimed
counts for the suborders sum to 2022, not 2021.)

"""

import enum
import re
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

from taxonomy.db.constants import Rank
from taxonomy.db.models.article.article import Article

from . import lib

SOURCE = lib.Source("msw2.txt", "Mammalia (Wilson & Reeder 1993).pdf")

LABELS = {
    "status",
    "distribution",
    "type locality",
    "comment",
    "synonyms",
    "comments",
    "type species",
}
LABELS_STARTSWITH = tuple(f"{x}:" for x in LABELS)


class LineKind(enum.Enum):
    section = 2
    continuation = 3
    genus_header = 4
    species_header = 5
    high_taxon_header = 6
    ignored = 7


@dataclass
class Line:
    kind: LineKind
    line: str
    page: int
    indentation: int


HIGHER_HEADERS = {"ORDER", "SUBORDER", "Family", "Suborder", "Subfamily"}
HIGHER_HEADERS_STARTSWITH = tuple(f"{x} " for x in HIGHER_HEADERS)
AUTHOR_REGEX = r"(?:(?:[A-ZÉ]\. |[A-Z]\.[A-Z]\. |d'|D'|M'|von |de |de la |du )*[A-Z][a-zé]+|C\. E\. H\.?\[amilton\]|G\. ?\[Baron\]|H\[amilton\])"


def classify_line(line: str, page: int, previous_lines: Sequence[Line]) -> Line:
    indentation = lib.initial_count(line, " ")
    line = line.strip()
    if not line:
        return Line(LineKind.ignored, line, page, indentation)
    if indentation > 20 or line == "Mammal Species of the World":
        # Chapter headers, page headers/footers, garbage on blank pages
        return Line(LineKind.ignored, line, page, indentation)

    if line.startswith(HIGHER_HEADERS_STARTSWITH):
        return Line(LineKind.high_taxon_header, line, page, indentation)

    if line.lower().startswith(LABELS_STARTSWITH):
        return Line(LineKind.section, line, page, indentation)

    if previous_lines and (
        previous_lines[-1].kind is LineKind.ignored
        or indentation < previous_lines[-1].indentation - 1
    ):
        if re.search(
            rf"^[A-Z][a-z]+ (?!von |du | de )(io|[a-z]{{3,}}) \(?{AUTHOR_REGEX}.*\d{{4}}",
            line,
        ):
            return Line(LineKind.species_header, line, page, indentation)
        if re.search(rf"^[A-Z][a-z]+ \(?{AUTHOR_REGEX}.*\d{{4}}", line):
            return Line(LineKind.genus_header, line, page, indentation)
        if re.search(r"\d{1,3} {20}", line):
            return Line(LineKind.ignored, line, page, indentation)
        if not any(c.isdigit() for c in line):
            return Line(LineKind.continuation, line, page, indentation)
        # print("!!", line)
        return Line(LineKind.continuation, line, page, indentation)

    return Line(LineKind.continuation, line, page, indentation)


def classify_lines(pages: lib.PagesT) -> Iterable[Line]:
    previous_lines: list[Line] = []
    for page, lines in pages:
        for line in lines:
            line_obj = classify_line(line, page, previous_lines)
            previous_lines.append(line_obj)
            if line_obj.kind is LineKind.ignored:
                continue
            yield line_obj


def merge_adjacent[
    T
](
    iterable: Iterable[T],
    should_merge: Callable[[T, T], bool],
    merge: Callable[[T, T], T],
) -> Iterable[T]:
    iterator = iter(iterable)
    try:
        previous = next(iterator)
    except StopIteration:
        return
    for item in iterator:
        if should_merge(previous, item):
            previous = merge(previous, item)
        else:
            yield previous
            previous = item
    yield previous


def merge_continuations(lines: Iterable[Line]) -> Iterable[Line]:
    def should_merge(previous: Line, item: Line) -> bool:
        return item.kind is LineKind.continuation

    def merge(previous: Line, item: Line) -> Line:
        return Line(
            previous.kind,
            previous.line + " " + item.line,
            previous.page,
            previous.indentation,
        )

    return merge_adjacent(lines, should_merge, merge)


@dataclass
class Account:
    intro_line: Line
    sections: list[tuple[str, str]]


def merge_accounts(lines: Iterable[Line]) -> Iterable[Account]:
    current_account: Account | None = None
    for item in lines:
        match item.kind:
            case (
                LineKind.high_taxon_header
                | LineKind.genus_header
                | LineKind.species_header
            ):
                if current_account:
                    yield current_account
                current_account = Account(item, [])
            case LineKind.section:
                assert current_account is not None
                assert ":" in item.line
                label, text = item.line.split(":", maxsplit=1)
                current_account.sections.append((label.lower(), text))
    assert current_account is not None
    yield current_account


def validate_accounts(accounts: Iterable[Account]) -> Iterable[Account]:
    for account in accounts:
        sections = Counter(label for label, _ in account.sections)
        for label in sections:
            if sections[label] > 1:
                print(
                    f"!! Duplicate section {label!r} for {account.intro_line.line} on page {account.intro_line.page}"
                )
        yield account


def print_linekind_summary(lines: Iterable[Line]) -> None:
    counter = Counter(x.kind for x in lines)
    for kind, count in counter.items():
        print(f"{kind.name}: {count}")


YEAR_REGEX = r"(?P<year>1\d{3}(?:-\d{2}|-\d{4}| \(\d{4}\))?)"


def extract_names(accounts: Iterable[Account]) -> Iterable[lib.CEDict]:
    source = SOURCE.get_source()
    taxon_to_source: dict[str, Article] = {"Rodentia": source}
    for child in source.get_children():
        taxon_to_source[child.title.split()[-1]] = child

    for account in accounts:
        line = account.intro_line.line
        match account.intro_line.kind:
            case LineKind.genus_header:
                rank = Rank.genus
            case LineKind.species_header:
                rank = Rank.species
            case LineKind.high_taxon_header:
                rank = Rank[account.intro_line.line.split()[0].lower()]
            case _:
                print("!!", account.intro_line)
                continue
        match: re.Match[str] | None = None
        if rank > Rank.family:
            try:
                _, taxon_name = line.lower().split()
            except ValueError:
                print(f"!! invalid higher taxon: {line!r}")
                continue
            taxon_name = taxon_name.title()
        else:
            if match := re.search(
                rf"^(?P<rank>Family|Subfamily) (?P<name>[A-Z][a-z]+) (?P<author>(d')?[A-Z][^\d]+), {YEAR_REGEX}\."
                r" (?P<verbatim>.*)$",
                line,
            ):
                assert rank.name == match.group("rank").lower(), repr(line)
            elif match := re.search(
                rf"^(?P<name>[A-Z][a-z]+) \(?(?P<author>(d'|de |du |von )?[A-ZÉ][^\d]+), {YEAR_REGEX}\)?\."
                r" (?P<verbatim>.*)$",
                line,
            ):
                assert rank is Rank.genus, repr(line)
            elif match := re.search(
                r"^(?P<name>[A-Z][a-z]+ [a-z]+) \(?(?P<author>(de la |du |de |d')?[A-ZÉ][^\d]+),"
                rf" {YEAR_REGEX}\)?\. (?P<verbatim>.*)$",
                line,
            ):
                assert rank is Rank.species, repr(line)
            else:
                print("!!", line)
                continue
            taxon_name = match.group("name")

        extra_data: dict[str, str] = {}
        for label, text in account.sections:
            if rank is Rank.genus and label not in (
                "type species",
                "synonyms",
                "comments",
            ):
                print(taxon_name, label)
            extra_data[label] = text

        if taxon_name in taxon_to_source:
            source = taxon_to_source[taxon_name]

        name: lib.CEDict = {
            "rank": rank,
            "name": taxon_name,
            "page": str(account.intro_line.page),
            "article": source,
            "extra_fields": extra_data,
        }
        if match is not None:
            name["authority"] = match.group("author")
            name["year"] = match.group("year")
            name["citation"] = match.group("verbatim")
        yield name


def main() -> None:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines, permissive=True)
    pages = lib.validate_pages(pages, verbose=False)
    line_objs = classify_lines(pages)
    line_objs = merge_continuations(line_objs)
    accounts = merge_accounts(line_objs)
    accounts = validate_accounts(accounts)
    # print_linekind_summary(a.intro_line for a in accounts)
    names = extract_names(accounts)
    # names = lib.clean_text(names)
    names = lib.add_parents(names)
    names = lib.validate_ce_parents(names)
    names = lib.no_childless_ces(names)
    # names = lib.count_by_rank(names, Rank.subfamily)
    # names = lib.count_by_rank(names, Rank.family)
    # names = lib.count_by_rank(names, Rank.suborder)
    # names = lib.count_by_rank(names, Rank.order)
    # names = list(names)
    # lib.create_csv("koopman1994.csv", names)
    names = lib.add_classification_entries(names, dry_run=False)
    lib.print_ce_summary(names)
    lib.format_ces(SOURCE, include_children=True)


if __name__ == "__main__":
    main()
