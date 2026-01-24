"""This is an OK state except that many names cannot easily be matched yet.
Probably best to first do some work adding name combinations to the DB.

"""

import enum
import json
import re
from collections import Counter, deque
from collections.abc import Iterable
from typing import Any

from taxonomy.db import models
from taxonomy.db.constants import NomenclatureStatus, Rank
from taxonomy.db.models.classification_entry.ce import ClassificationEntry

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

    def classify_line(line: str, spaces: int, *, is_first: bool) -> LineKind:
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


def split_name_line(
    name: dict[str, Any], parent_stack: list[tuple[Rank, str]]
) -> dict[str, Any]:
    line = name["name_line"]
    if line.lower().startswith(("family ", "order ")):
        try:
            rank_str, taxon_name = line.lower().split()
        except ValueError:
            print(f"invalid higher taxon: {line!r}")
            return name
        rank = Rank[rank_str]
        taxon_name = taxon_name.title()
        extra_fields = {}
    else:
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
            raise ValueError(line)
        taxon_name = match.group("name")
        extra_fields = {
            "authority": match.group("author"),
            "year": match.group("year"),
            "verbatim_citation": match.group("verbatim"),
        }
    while parent_stack and parent_stack[-1][0] <= rank:
        parent_stack.pop()
    if parent_stack:
        parent = parent_stack[-1]
    else:
        parent = None
    parent_stack.append((rank, taxon_name))
    return {
        **name,
        "parent": parent,
        "rank": rank,
        "taxon_name": taxon_name,
        **extra_fields,
    }


def process_names(names: DataT) -> DataT:
    parent_stack: list[tuple[Rank, str]] = []
    for name in names:
        yield split_name_line(name, parent_stack)


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
        return re.sub(r"[A-Z]\. ", "", name["authority"])
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


def check_parents(names: DataT) -> DataT:
    names = list(names)
    name_counter = Counter(name["taxon_name"] for name in names)
    for name, count in name_counter.items():
        if count > 1:
            print(
                name, count, [n["name_line"] for n in names if n["taxon_name"] == name]
            )
    for name in names:
        if name["parent"] is None:
            continue
        _parent_rank, parent_name = name["parent"]
        if parent_name not in name_counter:
            print(f"parent {parent_name} not found for {name['name_line']}")
    return names


def add_classification_entries(names: DataT, *, dry_run: bool = True) -> DataT:
    art = SOURCE.get_source()
    for name in names:
        page = ", ".join(str(p) for p in name["pages"])
        taxon_name = name["taxon_name"]
        rank = name["rank"]
        type_locality = name.get("type locality")
        authority = name.get("authority")
        year = name.get("year")
        citation = name.get("verbatim_citation")
        raw_data = json.dumps(name, ensure_ascii=False, separators=(",", ":"))
        if not dry_run:
            try:
                existing = ClassificationEntry.get(
                    name=taxon_name, rank=rank, article=art
                )
            except ClassificationEntry.DoesNotExist:
                pass
            else:
                print(f"already exists: {existing}")
                continue
            if name["parent"] is None:
                parent = None
            else:
                parent_rank, parent_name = name["parent"]
                parent = ClassificationEntry.get(
                    name=parent_name, rank=parent_rank, article=art
                )
            new_ce = ClassificationEntry.create(
                article=art,
                name=taxon_name,
                rank=rank,
                parent=parent,
                authority=authority,
                year=year,
                citation=citation,
                type_locality=type_locality,
                raw_data=raw_data,
                page=page,
            )
            print(new_ce)
        yield name


def convert_to_ce_dicts(names: DataT) -> Iterable[lib.CEDict]:
    for name in names:
        ce: lib.CEDict = {
            "name": name["taxon_name"],
            "rank": name["rank"],
            "page": ", ".join(str(p) for p in name["pages"]),
            "article": SOURCE.get_source(),
        }
        if name.get("parent") is not None:
            parent_rank, parent_name = name["parent"]
            ce["parent"] = parent_name
            ce["parent_rank"] = parent_rank
        if "authority" in name:
            ce["authority"] = name["authority"]
        if "year" in name:
            ce["year"] = name["year"]
        if "verbatim_citation" in name:
            ce["citation"] = name["verbatim_citation"]
        if "type locality" in name:
            ce["type_locality"] = name["type locality"]
        extra_fields = {k: v for k, v in name.items() if k not in ce and k != "pages"}
        if extra_fields:
            ce["extra_fields"] = extra_fields
        yield ce


def main() -> None:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines, permissive=True)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = process_names(names)
    names = check_parents(names)
    ce_dicts = convert_to_ce_dicts(names)
    lib.create_csv("msw1.csv", list(ce_dicts))
    # names = add_classification_entries(names, dry_run=False)
    # names = list(lib.translate_to_db(names, None, SOURCE, verbose=False))
    # print(
    #     f"Associated {len([n for n in names if 'taxon' in n])}/{len(names)} names with"
    #     " current valid name"
    # )
    # names = associate_names(names)
    # names = lib.write_to_db(names, SOURCE, dry_run=True, edit_if_no_holotype=False)
    # for name in names:
    #     if "name_obj" not in name:
    #         print("not found:", name["name_line"])
    # lib.print_field_counts(names)


if __name__ == "__main__":
    main()
