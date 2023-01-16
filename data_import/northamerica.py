import re
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from . import lib
from .lib import DataT

SOURCE = lib.Source("northamerica-bhl.txt", "North America.pdf")
NAME_REGEX = re.compile(
    r"""
    (?P<year>\d{4})\.\s
    (?P<orig_name_author>[^,]+),\s
    (?P<verbatim_citation>.+)\.
    (\s\((?P<note>.+)\))?
    $
""",
    re.VERBOSE,
)
HARDCODED = {
    "Balantiopteryx io Thomas": ("Balantiopteryx io", "Thomas"),
    "Canis Lupus-Griseus Sabine": ("Canis Lupus-Griseus", "Sabine"),
}


def extract_pages(lines: Iterable[str]) -> Iterable[Tuple[int, List[str]]]:
    """Split the text into pages."""
    current_page = None
    current_lines: List[str] = []
    for line in lines:
        match = re.match(r"^(\d+) U\. S\. NATIONAL MUSEUM BULLETIN 205\s*$", line)
        if match:
            if current_page is not None:
                yield current_page, current_lines
                current_lines = []
            current_page = int(match.group(1))
        else:
            match = re.match(r"[A-Z\d]+: [A-Z\d]+(— [A-Z]+)? (\d+)\s*$", line)
            if match:
                if current_page is not None:
                    yield current_page, current_lines
                    current_lines = []
                current_page = int(match.group(2))
            else:
                current_lines.append(line)
    # last page
    assert current_page is not None
    yield current_page, current_lines


def _make_taxon(heading: str, rank: str, page: int) -> Dict[str, Any]:
    return {"rank": rank, "heading": heading, "names": [], "pages": [page]}


def extract_taxa(pages: Iterable[Tuple[int, List[str]]]) -> DataT:
    current_taxon: Dict[str, Any] = {}
    current_name: List[str] = []
    type_loc_lines: List[str] = []
    range_marker = "Range. —"

    def _assert_no_type_loc() -> None:
        assert not type_loc_lines, (
            f"duplicate type loc while processing {current_taxon} / {line} /"
            f" {type_loc_lines}"
        )
        assert (
            "type_locality" not in current_taxon
        ), f"duplicate type loc while processing {current_taxon} / {line}"

    def flush_type_loc() -> None:
        nonlocal type_loc_lines
        if type_loc_lines:
            assert (
                "type_locality" not in current_taxon
            ), f"duplicate type loc while processing {current_taxon} / {type_loc_lines}"
            current_taxon["type_locality"] = type_loc_lines
            type_loc_lines = []

    def flush_name() -> None:
        nonlocal current_name
        if current_name:
            current_taxon["names"].append(current_name)
        current_name = []

    for current_page, lines in pages:
        if current_taxon:
            current_taxon["pages"].append(current_page)
        for line in lines:
            line = line.rstrip()
            if not line:
                pass
            elif re.match(r"^[a-z]+[ -—]+group$", line) or re.match(
                r"^(Order|Suborder|Superfamily|Family|Subfamily) [A-Z]{2,} ", line
            ):
                flush_name()
                flush_type_loc()
            elif re.match(r'^(Subgenus|Genus) ([A-Z]+)[\*"\']? ', line):
                flush_name()
                flush_type_loc()
                if current_taxon:
                    yield current_taxon
                current_taxon = _make_taxon(line, "genus", current_page)
            elif (
                line.endswith("*")
                or line.endswith("#")
                or line.endswith("•")
                or re.search(r'\* \([A-Za-z,\. \-\'"]+\)$', line)
                or re.match(
                    (
                        r"^[A-Z][a-z]+( [a-z]+){1,2} (von |de )?([A-Z]\."
                        r" )*(Mc)?[A-Z][a-z]+(-[A-Z][a-z]+)?( and ([A-Z]\."
                        r" )*[A-Z][a-z]+)?$"
                    ),
                    line,
                )
                or re.match(
                    (
                        r"^[A-Z][a-z]+( [a-z]+){1,2} \(([A-Z]\."
                        r" )*[A-Z][a-z]+(-[A-Z][a-z]+)?\)$"
                    ),
                    line,
                )
            ):
                flush_name()
                flush_type_loc()
                yield current_taxon
                current_taxon = _make_taxon(line, "species", current_page)
            elif re.match(r"^\d{4}\. (?!\()", line):
                flush_name()
                _assert_no_type_loc()
                current_name = [line]
            else:
                match = re.match(
                    r"^Typ(e|ical) [lL]ocalit ?y[,\. \-]+?— (?P<rest>.*)$", line
                )
                if match:
                    line = match.group("rest")
                    flush_name()
                    _assert_no_type_loc()
                    if current_name:
                        current_taxon["names"].append(current_name)
                    if range_marker in line:
                        current_taxon["type_locality"] = [
                            line[: line.index(range_marker)]
                        ]
                    else:
                        type_loc_lines = [line]
                elif type_loc_lines:
                    if range_marker in line:
                        type_loc_lines.append(line[: line.index(range_marker)])
                        current_taxon["type_locality"] = type_loc_lines
                        type_loc_lines = []
                    else:
                        type_loc_lines.append(line)
                elif current_name:
                    current_name.append(line)
    yield current_taxon


def name_of_text(text: str, is_genus: bool) -> Dict[str, Any]:
    match = NAME_REGEX.match(text)
    assert match, f"failed to match {text}"
    name: Dict[str, Any] = {
        key: value for key, value in match.groupdict().items() if value
    }
    name["is_genus"] = is_genus
    name["raw_text"] = text
    name_author = (
        name["orig_name_author"]
        .replace(" [sic]", "")
        .replace(" (sic)", "")
        .replace("[", "")
        .replace("]", "")
    )
    assert not re.search(r"[A-Z][a-z]\.", name_author), name_author
    if name_author in HARDCODED:
        name["original_name"], name["authority"] = HARDCODED[name_author]
    else:
        name.update(lib.split_name_authority(name_author))
    orig_name = name["original_name"]
    if is_genus:
        assert orig_name.isalpha(), name
    else:
        match = re.search(r" ([a-z\-]{2,}|Lupus-Griseus)$", orig_name)
        assert match, name
        name["root_name"] = match.group(1).lower().replace("-", "")
    return name


def get_taxon_root_name(heading: str) -> Optional[str]:
    match = re.match(
        (
            r"^([A-Z][a-z]+( [a-z]+){1,2}) (von |de )?(([A-Z]\."
            r" )*[A-Z][a-z]+|\([A-Za-z\. \-]{3,}\))"
        ),
        heading,
    )
    if match:
        return match.group(1).split()[-1]
    else:
        return None


def degender(name: str) -> str:
    return re.sub(r"(a|um)$", "us", name)


def taxa_to_names(taxa: DataT) -> DataT:
    for taxon in taxa:
        is_genus = taxon["rank"] == "genus"
        names = [name_of_text(name, is_genus=is_genus) for name in taxon["names"]]
        seen_root_names: Set[str] = set()
        is_singleton = len(names) == 1
        if not is_genus:
            taxon_root_name = get_taxon_root_name(taxon["heading"])
            if not is_singleton:
                # If there is only one name, we don't care; we know we'll get the right name
                assert (
                    taxon_root_name is not None
                ), f'failed to match {taxon["heading"]}'
            if taxon_root_name:
                taxon_root_name = degender(taxon_root_name)
            assert "type_locality" in taxon, f"{taxon} missing type locality"
        for name in names:
            seen_root = False
            if not is_genus:
                root_name = degender(name["root_name"])
                if root_name in seen_root_names:
                    continue
                elif root_name == taxon_root_name and not seen_root:
                    seen_root = True
                    name["loc"] = taxon["type_locality"]
                seen_root_names.add(root_name)
            name["pages"] = taxon["pages"]
            yield name
        if not is_genus and not is_singleton:
            if taxon_root_name not in seen_root_names:
                assert (
                    False
                ), f"{taxon_root_name} is not in {seen_root_names} for {taxon}"


def split_fields(names: DataT) -> DataT:
    for name in names:
        if name["authority"] == "True" and name["year"] == "1884":
            continue
        if name["original_name"].endswith(" and"):
            continue
        if "note" in name and "type_locality" not in name:
            if name["is_genus"]:
                name["verbatim_type"] = name["note"]
            else:
                name["loc"] = name["note"]
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    taxa = extract_taxa(pages)
    taxa = lib.clean_text(taxa)
    names = taxa_to_names(taxa)
    names = split_fields(names)
    names = lib.translate_to_db(names, "SDNHM", SOURCE)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    config = lib.NameConfig(
        {},
        {
            "Merriamf": "Merriam",
            "Dobsonf": "Dobson",
            "Menegaux": "Ménègaux",
            "I. Geoffroy-Saint-Hilaire": "I. Geoffroy Saint-Hilaire",
            "E. Geoffroy-Saint-Hilaire": "É. Geoffroy Saint-Hilaire",
            "Lacepede": "Lacépède",
        },
    )
    names = lib.associate_names(names, config, try_manual=True)
    lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    # lib.print_counts_if_no_tag(names, 'loc', models.TypeTag.Coordinates)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for n in main():
        print(n)
