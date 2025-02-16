import re
from collections.abc import Iterable

from data_import import lib
from taxonomy.db.constants import Rank

SOURCE = lib.Source(
    "mammalia-corbet-hill-1980.txt", "Mammalia (Corbet & Hill 1980).pdf"
)


def extract_names(pages: lib.PagesT) -> Iterable[lib.CEDict]:
    art = SOURCE.get_source()
    last_genus: str | None = None
    rank: Rank | None = None
    extra_data: dict[str, str] | None = None
    for page_no, lines in pages:
        it = lib.PeekingIterator(lines)
        for line in it:
            line = line.rstrip()
            if match := re.search(r"^ {5,}(ORDER|SUBORDER) ([A-Z]+)", line):
                rank = Rank[match.group(1).lower()]
                name = match.group(2).title()
                yield {"name": name, "rank": rank, "page": str(page_no), "article": art}
            elif match := re.search(
                r"^ *(Family|Subfamily|Subgenus) ([A-Z][a-z]+)", line
            ):
                rank = Rank[match.group(1).lower()]
                name = match.group(2)
                yield {"name": name, "rank": rank, "page": str(page_no), "article": art}
            elif (
                match := re.search(r"^([A-Z][a-z]+)($|;|'|,)(?! (c\. )?\d+ sp)", line)
            ) and not re.search(r"; (c\. )?\d+ sp", line):
                name = match.group(1)
                last_genus = name
                rank = Rank.genus
                yield {"name": name, "rank": rank, "page": str(page_no), "article": art}
            elif match := re.search(r"^([A-Z])\. ([a-z]+)", line):
                initial, epithet = match.groups()
                rest = line[match.end() :].strip()
                extra_data = {}
                if "  " in rest:
                    common_name, distribution = rest.split("  ", maxsplit=1)
                    extra_data["common_name"] = common_name
                    extra_data["distribution"] = distribution
                else:
                    extra_data["distribution"] = rest
                rank = Rank.species
                assert last_genus
                if last_genus[0] != initial:
                    print(last_genus, line)
                    continue
                yield {
                    "name": f"{last_genus} {epithet}",
                    "rank": rank,
                    "page": str(page_no),
                    "article": art,
                    "extra_fields": extra_data,
                }
            elif line and rank is Rank.species:
                assert line.startswith(" "), repr(line)
                assert extra_data is not None
                line = line.strip()
                if line.startswith("("):
                    assert ")" in line, line
                    syn, line = line.lstrip("(").split(")", maxsplit=1)
                    line = line.strip()
                    if "synonyms" in extra_data:
                        extra_data["synonyms"] += f" ({syn})"
                    else:
                        extra_data["synonyms"] = f"({syn})"
                if "  " in line:
                    common_name, distribution = line.split("  ", maxsplit=1)
                    assert "common_name" in extra_data, (extra_data, common_name)
                    extra_data["common_name"] += " " + common_name
                    extra_data["distribution"] += " " + distribution
                else:
                    extra_data["distribution"] += " " + line


def main() -> None:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names: Iterable[lib.CEDict] = list(extract_names(pages))
    names = lib.add_parents(names)
    names = lib.no_childless_ces(names)
    # names = lib.count_by_rank(names, Rank.order)
    # names = lib.count_by_rank(names, Rank.family)
    names = lib.print_unrecognized_genera(names)
    names = lib.validate_ce_parents(names)
    names = lib.add_classification_entries(names, dry_run=False)

    lib.print_ce_summary(names)
    lib.format_ces(SOURCE)


if __name__ == "__main__":
    main()
