import enum
import re
from collections.abc import Iterable

from data_import import lib
from taxonomy.db.constants import Rank

SOURCE = lib.Source(
    "theria-north-america-true-.txt", "Theria North America (True 1884).pdf"
)


def build_ce_dict(
    page: int, name_line: str, citation_line: str, tl_lines: list[str]
) -> lib.CEDict:
    if name_line.startswith(("Genus ", "Subgenus ")):
        match = re.fullmatch(
            r"(?P<rank>Genus|Subgenus) +(?P<name>\S+) (?P<author>[a-zA-Zé \-\.&]+)",
            name_line,
        )
        assert match, f"failed to match {name_line!r}"
        name = match.group("name")
        author = match.group("author")
        rank = Rank[match.group("rank").lower()]
    else:
        match = re.fullmatch(
            r"^(?P<name>[A-Z][a-z]+( \(\?\))?( +(?!de )[a-z']+)+) +(?P<author>[a-zA-Zé \-\.&]+)$",
            name_line,
        )
        assert match, f"failed to match {name_line!r}"
        name = match.group("name")
        author = match.group("author")
        num_words = len(name.split())
        if num_words == 2:
            rank = Rank.species
        elif num_words == 3:
            rank = Rank.subspecies
        else:
            assert False, f"unexpected number of words in {name_line!r}"
    # 1893.    Ichthyomys Thomas, Proc. Zool. Soc., London, p. 337. -Type: Ichthyomys stolzmanni Thos.
    head, citation = citation_line.split(",", maxsplit=1)
    citation_match = re.fullmatch(r"(?P<year>\d{4})\. +(?P<name>.*)", head)
    assert citation_match, f"failed to match {head!r}"
    data: lib.CEDict = {
        "page": str(page),
        "name": name,
        "authority": author.strip("."),
        "rank": rank,
        "year": citation_match.group("year"),
        "citation": citation.strip(),
        "original_combination": citation_match.group("name"),
        "article": SOURCE.get_source(),
    }
    if tl_lines:
        tl_text = lib.clean_string(" ".join(tl_lines))
        assert tl_text.startswith(
            "Type locality:"
        ), f"missing type locality: {tl_text!r}"
        if "Syn" in tl_text:
            tl_text, syns = tl_text.split("Syn", maxsplit=1)
            data["comment"] = f"Syn{syns}"
        if "(Type" in tl_text:
            tl_text, type_specimen = tl_text.split("(Type", maxsplit=1)
            type_specimen = type_specimen.strip().rstrip(".")
            assert type_specimen.endswith(")"), f"failed to match {tl_lines!r}"
            data["type_specimen"] = type_specimen.rstrip(")")
        data["type_locality"] = tl_text.removeprefix("Type locality:").strip()
    return data


class LineKind(enum.Enum):
    HIGHER = enum.auto()
    NAME = enum.auto()
    DISTRIBUTION = enum.auto()


def extract_names(pages: lib.PagesT) -> Iterable[lib.CEDict]:
    last_seen = LineKind.DISTRIBUTION
    for page_no, lines in pages:
        for line in lines:
            line = re.sub(r"^[\s\-_=+|\\;:'\"<>\[\]^\.]+", "", line).strip()
            if not line:
                continue
            words = line.split()
            first_word = words[0].lower()
            if first_word in ("class", "subclass", "order", "suborder", "family"):
                assert last_seen in (
                    LineKind.DISTRIBUTION,
                    LineKind.HIGHER,
                ), f"unexpected {line}"
                if first_word == "class":
                    rank = Rank.class_
                else:
                    rank = Rank[first_word]
                name = words[1].strip(".")
                yield {
                    "page": str(page_no),
                    "name": name.title(),
                    "rank": rank,
                    "article": SOURCE.get_source(),
                }
                last_seen = LineKind.HIGHER
            elif last_seen in (LineKind.HIGHER, LineKind.DISTRIBUTION):
                # must be a name
                name, _ = line.split(",")
                cleaned_name = name.replace("?", "").strip()
                if cleaned_name.count(" ") == 2:
                    rank = Rank.subspecies
                else:
                    rank = Rank.species
                yield {
                    "page": str(page_no),
                    "name": cleaned_name,
                    "rank": rank,
                    "article": SOURCE.get_source(),
                }
                last_seen = LineKind.NAME
            else:
                last_seen = LineKind.DISTRIBUTION


def insert_species(names: Iterable[lib.CEDict]) -> Iterable[lib.CEDict]:
    seen_names: set[str] = set()
    for name in names:
        seen_names.add(name["name"])
        pieces = name["name"].split()
        assert all(len(piece) > 2 for piece in pieces), f"unexpected name: {name}"
        if name["rank"] is Rank.subspecies:
            gen, sp, ssp = name["name"].split()
            species_name = f"{gen} {sp}"
            if species_name not in seen_names:
                species_dict: lib.CEDict = {
                    "page": name["page"],
                    "name": species_name,
                    "rank": Rank.species,
                    "article": name["article"],
                }
                if "authority" in name:
                    species_dict["authority"] = name["authority"]
                if "year" in name:
                    species_dict["year"] = name["year"]
                seen_names.add(species_name)
                yield species_dict
        yield name


def main() -> None:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.insert_genera_and_species(names)
    names = lib.add_parents(names)
    names = lib.validate_ce_parents(names)
    names = lib.add_classification_entries(names, dry_run=True)

    lib.print_field_counts(dict(n) for n in names)
    lib.format_ces(SOURCE)


if __name__ == "__main__":
    main()
