"""

This is an OK state except that many names cannot easily be matched yet.
Probably best to first do some work adding name combinations to the DB.

"""

import enum
import re
from collections import Counter, deque
from collections.abc import Iterable
from typing import Any

from taxonomy.db import models
from taxonomy.db.constants import NomenclatureStatus, Rank

from . import lib
from .lib import DataT

SOURCE = lib.Source("msw1-layout.txt", "Mammalia (Honacki et al. 1982).pdf")

LABELS = {
    "isis number",
    "distribution",
    "type locality",
    "comment",
    "reviewed by",
    "protected status",
    "comments",
}


class LineKind(enum.Enum):
    blank = 1
    section = 2
    continuation = 3
    taxon_header = 4
    high_taxon_header = 5


def extract_names(pages: Iterable[tuple[int, list[str]]]) -> DataT:
    found_beginning = False
    current_name: dict[str, Any] = {}
    current_lines: list[str] = []
    last_indent = 0
    line_kinds: deque[LineKind] = deque([], 5)
    current_label = ""

    def classify_line(line: str, spaces: int, is_first: bool) -> LineKind:
        if not line:
            return LineKind.blank
        if line.startswith(("Family ", "ORDER ")):
            return LineKind.high_taxon_header
        if spaces == 0:
            return LineKind.taxon_header
        if match := re.search(r"^([a-zA-Z]+( [a-zA-Z]+)?): ", line):
            label = match.group(1).lower()
            if label in LABELS:
                return LineKind.section
        if current_label == "isis number" and not re.search(r"\d{10}", line):
            return LineKind.taxon_header
        match line_kinds[-1]:
            case LineKind.blank:
                return LineKind.taxon_header
            case LineKind.taxon_header:
                return LineKind.continuation
            case LineKind.continuation | LineKind.section if is_first:
                if line.startswith("Nyctinomops aurispinosus"):
                    return LineKind.taxon_header
                if (
                    line[0].isupper()
                    and line.endswith(".")
                    and re.search(r", 1\d{3}\)?\.", line)
                ):
                    return LineKind.taxon_header
                else:
                    return LineKind.continuation
            case LineKind.section:
                if spaces > last_indent + 2:
                    return LineKind.continuation
                else:
                    return LineKind.taxon_header
            case LineKind.continuation:
                if spaces >= last_indent - 1:
                    return LineKind.continuation
                return LineKind.taxon_header
        assert False, line

    for page, lines in pages:
        if current_name:
            current_name["pages"].append(page)
        page_started = 0
        for line in lines:
            line = line.rstrip()
            if not line and page_started == 0:
                continue
            else:
                page_started += 1
            if not found_beginning:
                if line.strip() == "ORDER MONOTREMATA":
                    found_beginning = True
                else:
                    continue
            if line.startswith("Afanas'ev"):
                return

            spaces = lib.initial_count(line, " ")
            line = line.strip()
            if line.isnumeric():
                continue
            kind = classify_line(line, spaces, is_first=page_started == 1)
            line_kinds.append(kind)
            match kind:
                case LineKind.blank:
                    pass
                case LineKind.section:
                    match = re.search(r"^([a-zA-Z]+(?: [a-zA-Z]+)?): (.*)", line)
                    assert match is not None, repr(line)
                    label = match.group(1).lower()
                    value = match.group(2)
                    assert (
                        current_name
                    ), f"cannot start {label} with {value!r} on an empty name"
                    if label in current_name:
                        print("-----------")
                        print(f"duplicate label {label} for {current_name}")
                    current_lines = [value]
                    current_label = label
                    current_name[label] = current_lines
                case LineKind.continuation:
                    current_lines.append(line)
                case LineKind.taxon_header | LineKind.high_taxon_header:
                    if current_name:
                        yield current_name
                    current_lines = [line]
                    current_name = {"pages": [page], "name_line": current_lines}
                    current_label = ""

            last_indent = spaces
    yield current_name


def split_name_line(name: dict[str, Any]) -> dict[str, Any]:
    line = name["name_line"]
    if line.lower().startswith(("family ", "order ")):
        try:
            rank_str, taxon_name = line.lower().split()
        except ValueError:
            print(f"invalid higher taxon: {line!r}")
            return name
        return {**name, "rank": Rank[rank_str], "taxon_name": taxon_name.title()}
    if match := re.search(
        r"^(?P<name>[A-Z][a-z]+) (?P<author>(d')?[A-Z][^\d]+), (?P<year>1\d{3})\."
        r" (?P<verbatim>.*)$",
        line,
    ):
        rank = Rank.genus
    elif match := re.search(
        r"^(?P<name>[A-Z][a-z]+ [a-z]+) \(?(?P<author>(de la |du |de |d')?[A-Z][^\d]+),"
        r" (?P<year>1\d{3}(-\d{4})?)\)?\. (?P<verbatim>.*)$",
        line,
    ):
        rank = Rank.species
    else:
        print(line)
        return name
        raise ValueError(line)
    return {
        **name,
        "rank": rank,
        "taxon_name": match.group("name"),
        "authority": match.group("author"),
        "year": match.group("year"),
        "verbatim_citation": match.group("verbatim"),
    }


def process_names(names: DataT) -> DataT:
    for name in names:
        yield split_name_line(name)


def associate_name(name: dict[str, Any]) -> tuple[str, models.Name | None]:
    if "taxon" in name:
        return "taxon", name["taxon"].base_name
    taxon_name = name["taxon_name"]
    orig_name_matches = list(
        models.Name.select_valid().filter(
            models.Name.corrected_original_name == taxon_name
        )
    )
    if len(orig_name_matches) == 1:
        return "original name", orig_name_matches[0]
    if "year" in name:
        year_matches = [nam for nam in orig_name_matches if nam.year == name["year"]]
        if len(year_matches) == 1:
            return "original name plus year", year_matches[0]
    if orig_name_matches:
        return "multiple matches", None
    if " " in taxon_name:
        _, root_name = taxon_name.split()
        root_name_matches = list(
            models.Name.select_valid().filter(models.Name.root_name == root_name)
        )
        if len(root_name_matches) == 1:
            return "root name", root_name_matches[0]
        rn_year_matches = [nam for nam in root_name_matches if nam.year == name["year"]]
        if len(rn_year_matches) == 1:
            return "root name plus year", rn_year_matches[0]
    return "not found", None


def resolve_original(nam: models.Name) -> models.Name:
    match nam.nomenclature_status:
        case (
            NomenclatureStatus.name_combination
            | NomenclatureStatus.unjustified_emendation
            | NomenclatureStatus.incorrect_subsequent_spelling
        ):
            tag = models.name.name.CONSTRUCTABLE_STATUS_TO_TAG[nam.nomenclature_status]
            target = nam.get_tag_target(tag)
            if target is not None:
                return resolve_original(target)
    return nam


def get_authority(name: dict[str, Any]) -> str | None:
    if "authority" in name:
        authority = re.sub(r"[A-Z]\. ", "", name["authority"])
        return authority
    return None


def sketchy_match(name: dict[str, Any], nam: models.Name) -> bool:
    if "authority" in name:
        if get_authority(name) != nam.taxonomic_authority():
            return True
    if "year" in name:
        if abs(int(name["year"][:4]) - nam.numeric_year()) > 1:
            return True
    return False


def associate_names(names: DataT) -> DataT:
    counts: Counter[str] = Counter()
    for name in names:
        label, name_obj = associate_name(name)
        counts[label] += 1
        if name_obj is None:
            yield name
            continue
        name_obj = resolve_original(name_obj)
        if sketchy_match(name, name_obj):
            print(f"{name['name_line']} == {name_obj}")
        yield {**name, "name_obj": name_obj}
    print(counts)


def main() -> None:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines, permissive=True)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = process_names(names)
    names = list(lib.translate_to_db(names, None, SOURCE, verbose=False))
    print(
        f"Associated {len([n for n in names if 'taxon' in n])}/{len(names)} names with"
        " current valid name"
    )
    names = associate_names(names)
    # names = lib.write_to_db(names, SOURCE, dry_run=True, edit_if_no_holotype=False)
    for name in names:
        if "name_obj" not in name:
            print("not found:", name["name_line"])
    lib.print_field_counts(names)


if __name__ == "__main__":
    main()
