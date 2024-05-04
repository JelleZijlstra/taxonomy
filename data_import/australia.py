import enum
import itertools
import re
from collections import deque
from collections.abc import Iterable
from typing import Any

from taxonomy import shell
from taxonomy.db.models.name import TypeTag

from . import lib
from .lib import DataT

SOURCE = lib.Source(
    "australia-decolumnized.txt", "Australia (Jackson & Groves 2015).pdf"
)
RefsDictT = dict[tuple[str, str], str]
COMMON_NAME_RGX = re.compile(r"^[A-Z][a-z\-]+( [A-Z][a-z\-]+)*$")
RANK_WORDS = {
    "Subtribe",
    "Tribe",
    "Subfamily",
    "Suborder",
    "Family",
    "Order",
    "Infraorder",
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
}
PATTERN = re.compile(rf'^({"|".join(RANK_WORDS)})\?? ')


class LineKind(enum.Enum):
    blank = 1
    section = 2
    continuation = 3
    taxon_header = 4
    name_header = 5
    common_name = 6


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


def extract_names(pages: Iterable[tuple[int, list[str]]]) -> DataT:
    found_references = False
    found_beginning = False
    current_name: dict[str, Any] = {}
    current_lines: list[str] = []
    current_label = ""
    line_kinds: deque[LineKind] = deque([], 5)

    def start_label(label: str, line: str) -> None:
        label = re.sub(r"\s+", " ", label.strip())
        nonlocal current_lines, current_label
        assert current_name, f"cannot start {label} with {line!r} on an empty name"
        if label in current_name:
            print("-----------")
            print(f"duplicate label {label} for {current_name}")
        current_lines = [line]
        current_name[label] = current_lines
        current_label = label

    for page, lines in pages:
        if current_name:
            current_name["pages"].append(page)
        for line in lines:
            line = line.rstrip()
            if not found_beginning:
                if line.strip() == "Class Mammalia Linnaeus, 1758":
                    found_beginning = True
                else:
                    continue

            if found_references:
                if not line:
                    continue
                elif line.strip() == "This page intentionally left blank":
                    yield current_name
                    return
                elif line.startswith(" "):
                    assert current_lines
                    current_lines.append(line)
                else:
                    if current_name:
                        yield current_name
                    current_lines = [line]
                    current_name = {"raw_text": current_lines, "pages": [page], "t": 2}
            else:
                # if line_kinds:
                #     print("PREV", line_kinds[-1])
                # print("LINE", line)
                if not line:
                    line_kinds.append(LineKind.blank)
                    continue
                elif line.strip() == "References":
                    found_references = True
                    continue
                spaces = lib.initial_count(line, " ")
                if spaces == 0:
                    # either section continuation or name line
                    if line.startswith(("Φ", "$", "Ω")):
                        line_kind = LineKind.name_header
                    elif line.startswith("\\"):
                        line_kind = LineKind.common_name
                    elif line_kinds[-1] == LineKind.section:
                        line_kind = LineKind.continuation
                    elif line_kinds[-1] == LineKind.continuation:
                        if can_be_name_header(line):
                            line_kind = LineKind.name_header
                        else:
                            line_kind = LineKind.continuation
                    elif line_kinds[-1] == LineKind.taxon_header:
                        if COMMON_NAME_RGX.match(line):
                            line_kind = LineKind.common_name
                        else:
                            if not can_be_name_header(line):
                                print("---")
                                print(line)
                            assert can_be_name_header(line), line
                            line_kind = LineKind.name_header
                    elif line_kinds[-1] == LineKind.name_header:
                        line_kind = LineKind.continuation
                    elif line_kinds[-1] == LineKind.common_name:
                        if not can_be_name_header(line):
                            print("---")
                            print(line)
                        assert can_be_name_header(line), line
                        line_kind = LineKind.name_header
                    else:
                        assert line_kinds[-1] == LineKind.blank, (line, line_kinds[-1])
                        if line_kinds[-2] == LineKind.common_name or line.startswith(
                            ("Φ", "$", "†", "Ω")
                        ):
                            line_kind = LineKind.name_header
                        elif line_kinds[-2] == LineKind.taxon_header:
                            if COMMON_NAME_RGX.match(line):
                                line_kind = LineKind.common_name
                            else:
                                line_kind = LineKind.name_header
                        elif can_be_name_header(line):
                            line_kind = LineKind.name_header
                        else:
                            # TODO: this probably has false negatives
                            line_kind = LineKind.continuation

                    line_kinds.append(line_kind)
                    if line_kind == LineKind.continuation:
                        current_lines.append(line)
                    elif line_kind == LineKind.common_name:
                        pass
                    else:
                        if current_name:
                            yield current_name
                        current_lines = [line]
                        current_name = {
                            "pages": [page],
                            "name_line": current_lines,
                            "t": 1,
                        }
                elif (
                    spaces == 2 or (2 < spaces <= 4 and line.lstrip()[0].islower())
                ) and ":" in line:
                    # section label, like "comments"
                    line_kinds.append(LineKind.section)
                    try:
                        label, _ = line.strip().split(":", 1)
                    except ValueError:
                        assert False, line
                    start_label(label, line)
                elif spaces > 2:
                    if current_label == "homonyms" and (
                        len(current_lines) == 1
                        or line_kinds[-1] == LineKind.continuation
                    ):
                        current_lines.append(line)
                        line_kinds.append(LineKind.continuation)
                    else:
                        line_kinds.append(LineKind.taxon_header)
                elif "sensu" in line:
                    line_kinds.append(LineKind.taxon_header)
                elif re.match(
                    r"^ +([†Φ] )?(Suborder |Subfamily |Family |Cohort |Superorder )?"
                    r"[A-Z][a-z]+( [a-z]+){0,2} +\(?([A-Z]\."
                    r" )?[A-Z](cK)?[a-zé]+(-[A-Z][a-z]+)?"
                    r"( & [A-Z](cK)?[a-z]+)?( et al\.)?, \d{4}\)?$",
                    line,
                ):
                    line_kinds.append(LineKind.taxon_header)
                else:
                    assert False, f"unrecognized line {line}"


def build_refs_dict(refs: DataT) -> RefsDictT:
    refs_dict: RefsDictT = {}
    for ref in refs:
        # Adams M, Baverstock PR, Watts CHS, Reardon T (1987a)
        #    Electrophoretic resolution of species boundaries in
        #    Australian Microchiroptera, II. The Pipistrellus group
        #    (Chiroptera: Vespertilionidae). Australian Journal of
        #    Biological Sciences 40, 163–170.

        text = ref["raw_text"]
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
    if raw_author.endswith((" von", " Von")):
        raw_author = raw_author.rsplit(maxsplit=1)[0]
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
    ):
        author = re.sub(r"^([A-Z][a-z A-Z\-]+) ([A-Z])[A-Z]?$", r"\2. \1", raw_author)
    if author == "Gray":
        author = "J. Gray"
    return author


def split_text(names: DataT) -> DataT:
    for name in names:
        name_line = name["name_line"]
        name["raw_text"] = dict(name)
        name_line = re.sub(r"^[Φ\$†Ω] ", "", name_line.rstrip("."))
        name_line = re.sub(r" \[sic\]", "", name_line)
        name_line = re.sub(r"\. \[([a-z]+)\]", r"\1", name_line)
        name_line = PATTERN.sub("", name_line).lstrip()
        if "sensu Misonne, 1969" in name_line:
            continue
        match = re.match(
            (
                r"^(?P<orig_name_author>[^,]+)(, in [^,]+)?, (?P<raw_year>\d{4}[^:]*):"
                r" (?P<page_described>.*)$"
            ),
            name_line,
        )
        assert match, name_line
        name.update(match.groupdict())
        name["year"] = name["raw_year"][:4]
        if "type locality" in name:
            name["loc"] = name["type locality"]
            if not name["loc"].strip():
                print(name)
        if "type species" in name:
            name["verbatim_type"] = TypeTag.TypeSpeciesDetail(
                name["type species"], SOURCE.get_source()
            )
        if "type genus" in name:
            name["verbatim_type"] = TypeTag.TypeSpeciesDetail(
                name["type genus"], SOURCE.get_source()
            )
        yield name


def associate_refs(names: DataT, refs_dict: RefsDictT) -> DataT:
    for name in names:
        key = name["authority"], name["raw_year"]
        if key in refs_dict:
            name["verbatim_citation"] = refs_dict[key]
        yield name


def main() -> None:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    names_refs = extract_names(pages)
    names_refs = lib.clean_text(names_refs)
    names: DataT = list(itertools.takewhile(lambda n: n["t"] == 1, names_refs))
    refs = names_refs
    refs_dict = build_refs_dict(refs)
    shell.ns["refs_dict"] = refs_dict
    names = split_text(names)
    names = lib.translate_to_db(names, None, SOURCE, verbose=True)
    names = associate_refs(names, refs_dict)
    config = lib.NameConfig()
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(names, config, max_distance=2, try_manual=True)
    names = lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    # for name in names:
    #     print(name)
    lib.print_field_counts(names)
    # print(list(refs_dict.keys()))
    print(f"{len(refs_dict)} refs")


if __name__ == "__main__":
    main()
