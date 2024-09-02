import re
from collections.abc import Iterable

from data_import import lib
from taxonomy.db.constants import Rank

SOURCE = lib.Source(
    "Ungulata-taxonomy (Groves & Grubb 2011).txt",
    "Ungulata-taxonomy (Groves & Grubb 2011).pdf",
)


def format_page(lines: list[str]) -> tuple[int, list[str]]:
    text = "".join(lines)
    match = re.search(r"349-47558_ch01_1P\.indd (\d+)", text)
    assert match, f"Could not find page number: {text}"
    page_number = int(match.group(1))
    text = text.replace("-1—", "   ").replace(" 0—", "   ").replace("+1—", "   ")
    text = text.replace("—-1", "").replace("—0", "").replace("—+1", "")
    text = text.split("© 2011 The Johns Hopkins University Press")[0]
    return page_number, text.splitlines()


def extract_pages(lines: Iterable[str]) -> lib.PagesT:
    """Split the text into pages."""
    current_lines: list[str] = []
    for line in lines:
        line = line.replace(" ", " ")
        if line.startswith("\x0c"):
            if current_lines:
                yield format_page(current_lines)
                current_lines = []
        current_lines.append(line)
    yield format_page(current_lines)


def extract_names(pages: lib.PagesT) -> Iterable[lib.CEDict]:
    art = SOURCE.get_source()
    for page_no, lines in pages:
        lines = lib.merge_lines(lines)
        for line in lines:
            family_match = re.search(
                r"^(?P<name>[A-Z]{2,20}IDAE) (?P<author>[A-ZÜ]{3,30}( [A-Z]{3,30})?), (?P<year>\d{4})( |$)",
                line,
            )
            if family_match:
                yield {
                    "name": family_match.group("name").title(),
                    "rank": Rank.family,
                    "authority": family_match.group("author"),
                    "year": family_match.group("year"),
                    "article": art,
                    "page": str(page_no),
                }
                continue
            assert "IDAE" not in line, repr(line)
            higher_match = re.search(
                r"^(?P<rank>Subfamily|Tribe) (?P<name>[A-Z][a-z]{2,20}(inae|ini)) (?P<author>[A-Z][^\d]{3,30}), (?P<year>\d{4})( |$)",
                line,
            )
            if higher_match:
                yield {
                    "name": higher_match.group("name"),
                    "rank": Rank[higher_match.group("rank").lower()],
                    "authority": higher_match.group("author"),
                    "year": higher_match.group("year"),
                    "article": art,
                    "page": str(page_no),
                }
                continue
            genus_match = re.search(
                r"^(?P<genus_name>[A-Z][a-z]{3,30}) (?P<author>(de )?[A-Z][^\d]{2,50}), (?P<year>\d{4})( |$)",
                line,
            )
            if genus_match:
                yield {
                    "name": genus_match.group("genus_name"),
                    "rank": Rank.genus,
                    "authority": genus_match.group("author"),
                    "year": genus_match.group("year"),
                    "article": art,
                    "page": str(page_no),
                }
                continue
            species_match = re.search(
                r"^ *(?P<name>[A-Z][a-z]{3,30} [a-z]{4,20}( [a-z]{4,20})?) (?P<author>(de |von )?[A-Z][^\d]{3,30}), (?P<year>\d{4})( |$)",
                line,
            )
            if species_match is None:
                species_match = re.search(
                    r"^ *(?P<name>[A-Z][a-z]{3,30} [a-z]{4,20}( [a-z]{4,20})?) \((?P<author>(de |von )?[A-Z][^\d]{3,30}), (?P<year>\d{4})\)( |$)",
                    line,
                )
            if species_match:
                name = species_match.group("name")
                rank = Rank.species if name.count(" ") == 1 else Rank.subspecies
                if rank is Rank.species:
                    parent_rank = Rank.genus
                    parent_name = name.split(" ")[0].replace("Ozotocerus", "Ozotoceros")
                else:
                    parent_rank = Rank.species
                    parent_name = " ".join(name.split(" ")[:2])
                yield {
                    "name": name,
                    "rank": rank,
                    "authority": species_match.group("author"),
                    "year": species_match.group("year"),
                    "article": art,
                    "page": str(page_no),
                    "parent": parent_name,
                    "parent_rank": parent_rank,
                }
                continue
            syn_match = re.search(
                r"^ {2,}(\? )?(?P<year>\d{4}) (?P<name>[A-Z][a-z]{2,30} [a-z]{4,20}( var\.)?( [a-z]{4,20}( forma [a-z]+)?)?) (?P<author>([A-Z]\.|[^\.\d]){3,})\.( (?P<type_locality>.*))?",
                line,
            )
            if syn_match:
                yield {
                    "name": syn_match.group("name"),
                    "rank": Rank.synonym,
                    "authority": syn_match.group("author"),
                    "year": syn_match.group("year"),
                    "article": art,
                    "page": str(page_no),
                    # Too low quality
                    # "type_locality": syn_match.group("type_locality"),
                }
                continue


def main() -> None:
    text = lib.get_text(SOURCE)
    pages = extract_pages(text)
    pages = lib.align_columns(pages, single_column_pages={12}, dedent_left=True)
    names = extract_names(pages)
    names = lib.add_parents(names)
    names = lib.validate_ce_parents(names)
    names = lib.add_classification_entries(names, dry_run=False)
    lib.print_ce_summary(names)


if __name__ == "__main__":
    main()
